package server

import (
	"bufio"
	"fmt"
	"io"
	"net/textproto"
	"sync"

	"github.com/grafana/dskit/multierror"

	"github.com/56quarters/jankcache/cache"
	"github.com/56quarters/jankcache/core"
	"github.com/56quarters/jankcache/proto"
)

const (
	readBufSize  = 65_535  // 64KB
	writeBufSize = 262_144 // 256KB
	version      = "jankcache/0.1.0"
)

var (
	readers = sync.Pool{
		New: func() any { return bufio.NewReaderSize(nil, readBufSize) },
	}
	writers = sync.Pool{
		New: func() any { return bufio.NewWriterSize(nil, writeBufSize) },
	}
)

type countingConnection struct {
	delegate io.ReadWriter
	metrics  *Metrics
	read     uint64
	written  uint64
}

func (c *countingConnection) Read(p []byte) (int, error) {
	n, err := c.delegate.Read(p)
	c.read += uint64(n)
	return n, err
}

func (c *countingConnection) Write(p []byte) (int, error) {
	n, err := c.delegate.Write(p)
	c.written += uint64(n)
	return n, err
}

func (c *countingConnection) Close() error {
	c.metrics.BytesRead.Add(c.read)
	c.metrics.BytesWritten.Add(c.written)
	return nil
}

func newBufferedConnection(conn io.ReadWriter, metrics *Metrics) *bufferedConnection {
	counting := &countingConnection{
		delegate: conn,
		metrics:  metrics,
	}

	reader := readers.Get().(*bufio.Reader)
	writer := writers.Get().(*bufio.Writer)
	reader.Reset(counting)
	writer.Reset(counting)

	buffered := &bufferedConnection{
		delegate: counting,
		Reader:   reader,
		Writer:   writer,
	}

	return buffered
}

type bufferedConnection struct {
	delegate *countingConnection
	Reader   *bufio.Reader
	Writer   *bufio.Writer
}

func (b *bufferedConnection) Read(p []byte) (int, error) {
	return b.Reader.Read(p)
}
func (b *bufferedConnection) Write(p []byte) (int, error) {
	return b.Writer.Write(p)
}

func (b *bufferedConnection) Close() error {
	var errs multierror.MultiError

	errs.Add(b.Writer.Flush())
	errs.Add(b.delegate.Close())

	readers.Put(b.Reader)
	writers.Put(b.Writer)

	return errs.Err()
}

func NewHandler(parser *proto.Parser, adapter *cache.Adapter, metrics *Metrics) *Handler {
	return &Handler{
		parser:  parser,
		adapter: adapter,
		metrics: metrics,
	}
}

type Handler struct {
	parser  *proto.Parser
	adapter *cache.Adapter
	metrics *Metrics
}

func (h *Handler) Reject(conn io.ReadWriter, msg string, args ...any) {
	wrapped := newBufferedConnection(conn, h.metrics)
	defer func() {
		_ = wrapped.Close()
	}()

	output := proto.NewOutput(wrapped)
	output.Error(core.ServerError(msg, args...))
}

func (h *Handler) Handle(conn io.ReadWriter) error {
	wrapped := newBufferedConnection(conn, h.metrics)
	defer func() {
		_ = wrapped.Close()
	}()

	output := proto.NewOutput(wrapped)

	text := textproto.NewReader(wrapped.Reader)
	line, err := text.ReadLine()
	if err != nil {
		return err
	}

	// Pass the line we read to the parser as well as the buffered reader since
	// we'll need to read a payload of bytes (not line delimited) in the case of
	// a "set" command.
	op, err := h.parser.Parse(line, wrapped.Reader)
	if err != nil {
		output.Error(err)
		return nil
	}

	switch op.Type() {
	case proto.OpTypeCacheMemLimit:
		limitOp := op.(*proto.CacheMemLimitOp)
		err := h.adapter.CacheMemLimit(limitOp)
		if err != nil {
			output.Error(err)
		} else if !limitOp.NoReply {
			output.Ok()
		}
	case proto.OpTypeDelete:
		delOp := op.(*proto.DeleteOp)
		err := h.adapter.Delete(delOp)
		if err != nil {
			output.Error(err)
		} else if !delOp.NoReply {
			output.Deleted()
		}
	case proto.OpTypeGet:
		getOp := op.(*proto.GetOp)
		res, err := h.adapter.Get(getOp)
		if err != nil {
			output.Error(err)
		} else {
			if !getOp.Unique {
				for _, v := range res {
					output.Encode(&cache.NoCasEntry{Entry: v})
				}
			} else {
				for _, v := range res {
					output.Encode(v)
				}
			}

			output.End()
		}
	case proto.OpTypeQuit:
		return core.ErrQuit
	case proto.OpTypeSet:
		setOp := op.(*proto.SetOp)
		err := h.adapter.Set(setOp)
		if err != nil {
			output.Error(err)
		} else if !setOp.NoReply {
			output.Stored()
		}
	case proto.OpTypeStats:
		stats := NewStats(h.adapter, h.metrics)
		output.Encode(&stats)
	case proto.OpTypeVersion:
		output.Line(version)
	default:
		panic(fmt.Sprintf("unexpected operation type: %+v", op))
	}

	return nil
}
