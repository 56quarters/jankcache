package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/textproto"
	"sync"

	"github.com/56quarters/jankcache/cache"
	"github.com/56quarters/jankcache/core"
	"github.com/56quarters/jankcache/proto"
)

const (
	readBufSize  = 65_535  // 64KB
	writeBufSize = 262_144 // 256KB
)

type Handler struct {
	parser  *proto.Parser
	encoder *proto.Encoder
	adapter *cache.Adapter
	readers sync.Pool
	writers sync.Pool
}

func NewHandler(parser *proto.Parser, encoder *proto.Encoder, adapter *cache.Adapter) *Handler {
	return &Handler{
		parser:  parser,
		encoder: encoder,
		adapter: adapter,
		readers: sync.Pool{
			New: func() any { return bufio.NewReaderSize(nil, readBufSize) },
		},
		writers: sync.Pool{
			New: func() any { return bufio.NewWriterSize(nil, writeBufSize) },
		},
	}
}

func (h *Handler) Handle(read io.Reader, write io.Writer) error {
	bufRead := h.readers.Get().(*bufio.Reader)
	bufWrite := h.writers.Get().(*bufio.Writer)
	bufRead.Reset(read)
	bufWrite.Reset(write)

	defer func() {
		_ = bufWrite.Flush()
		h.readers.Put(bufRead)
		h.writers.Put(bufWrite)
	}()

	// TODO: Should we limit the "line" size here?
	text := textproto.NewReader(bufRead)
	line, err := text.ReadLine()
	if err != nil {
		return err
	}

	// Pass the line we read to the parser as well as the buffered reader since
	// we'll need to read a payload of bytes (not line delimited) in the case of
	// a "set" command.
	op, err := h.parser.Parse(line, bufRead)
	if err != nil {
		h.send(h.encoder.Error(err), bufWrite)
		return nil
	}

	switch op.Type() {
	case proto.OpTypeCacheMemLimit:
		limitOp := op.(*proto.CacheMemLimitOp)
		err := h.adapter.CacheMemLimit(limitOp)
		if err != nil {
			h.send(h.encoder.Error(err), bufWrite)
		} else if !limitOp.NoReply {
			h.send(h.encoder.Ok(), bufWrite)
		}
	case proto.OpTypeDelete:
		delOp := op.(*proto.DeleteOp)
		err := h.adapter.Delete(delOp)
		if err != nil {
			h.send(h.encoder.Error(err), bufWrite)
		} else if !delOp.NoReply {
			h.send(h.encoder.Deleted(), bufWrite)
		}
	case proto.OpTypeFlushAll:
		flushOp := op.(*proto.FlushAllOp)
		err = h.adapter.Flush(flushOp)
		if err != nil {
			h.send(h.encoder.Error(err), bufWrite)
		} else if !flushOp.NoReply {
			h.send(h.encoder.Ok(), bufWrite)
		}
	case proto.OpTypeGet:
		getOp := op.(*proto.GetOp)
		res, err := h.adapter.Get(getOp)
		if err != nil {
			h.send(h.encoder.Error(err), bufWrite)
		} else {
			for k, v := range res {
				var b *bytes.Buffer
				if getOp.Unique {
					b = h.encoder.ValueUnique(k, v.Flags, v.Value, v.Unique)
				} else {
					b = h.encoder.Value(k, v.Flags, v.Value)
				}
				h.send(b.Bytes(), bufWrite)
				h.encoder.PutBuffer(b)
			}

			h.send(h.encoder.End(), bufWrite)
		}
	case proto.OpTypeQuit:
		return core.ErrQuit
	case proto.OpTypeSet:
		setOp := op.(*proto.SetOp)
		err := h.adapter.Set(setOp)
		if err != nil {
			h.send(h.encoder.Error(err), bufWrite)
		} else if !setOp.NoReply {
			h.send(h.encoder.Stored(), bufWrite)
		}
	default:
		panic(fmt.Sprintf("unexpected operation type: %+v", op))
	}

	return nil
}

func (h *Handler) send(bytes []byte, writer io.Writer) {
	// TODO: logging or metrics when writes fail
	_, _ = writer.Write(bytes)
}
