package server

import (
	"bufio"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/56quarters/jankcache/cache"
	"github.com/56quarters/jankcache/proto"
)

const maxReadSizeBytes = 1024 * 1024

type Handler struct {
	parser  proto.Parser
	encoder proto.Encoder
	adapter *cache.Adapter
	logger  log.Logger
}

func NewHandler(parser proto.Parser, encoder proto.Encoder, adapter *cache.Adapter, logger log.Logger) *Handler {
	return &Handler{
		parser:  parser,
		encoder: encoder,
		adapter: adapter,
		logger:  logger,
	}
}

func (h *Handler) Handle(read io.Reader, write io.Writer) error {
	// TODO: sync.Pool of buffers?
	scanner := bufio.NewScanner(read)
	scanner.Buffer(nil, maxReadSizeBytes)

	if !scanner.Scan() {
		level.Debug(h.logger).Log("msg", "eof while scanning input stream")
		return io.EOF
	}

	err := scanner.Err()
	if err != nil {
		level.Debug(h.logger).Log("msg", "error while scanning input stream", "err", err)
		return err
	}

	line := scanner.Text()
	op, err := h.parser.Parse(line, scanner)
	if err != nil {
		_, _ = write.Write(h.encoder.Error(err))
		return nil
	}

	switch op.Type() {
	case proto.OpTypeGet:
		getOp := op.(proto.GetOp)
		res, err := h.adapter.Get(getOp)
		if err != nil {
			_, _ = write.Write(h.encoder.Error(err))
		} else {
			for k, v := range res {
				_, _ = write.Write(h.encoder.Value(k, v.Flags, v.Value))
			}

			_, _ = write.Write(h.encoder.ValueEnd())
		}
	case proto.OpTypeSet:
		setOp := op.(proto.SetOp)
		err := h.adapter.Set(setOp)
		if err != nil {
			_, _ = write.Write(h.encoder.Error(err))
		} else if !setOp.NoReply {
			_, _ = write.Write(h.encoder.Stored())
		}
	case proto.OpTypeDelete:
		delOp := op.(proto.DeleteOp)
		err := h.adapter.Delete(delOp)
		if err != nil {
			_, _ = write.Write(h.encoder.Error(err))
		} else if !delOp.NoReply {
			_, _ = write.Write(h.encoder.Deleted())
		}
	default:
		panic("unexpected operation type")
	}

	return nil
}
