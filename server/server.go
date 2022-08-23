package server

import (
	"bufio"
	"fmt"
	"io"

	"github.com/go-kit/log"

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

func (h *Handler) send(bytes []byte, writer io.Writer) {
	// TODO logging or metrics when writes fail
	_, _ = writer.Write(bytes)
}

func (h *Handler) Handle(read io.Reader, write io.Writer) error {
	// TODO: sync.Pool of buffers?
	scanner := bufio.NewScanner(read)
	scanner.Buffer(nil, maxReadSizeBytes)

	if !scanner.Scan() {
		return scanner.Err()
	}

	line := scanner.Text()
	op, err := h.parser.Parse(line, scanner)
	if err != nil {
		h.send(h.encoder.Error(err), write)
		return nil
	}

	switch op.Type() {
	case proto.OpTypeGet:
		getOp := op.(proto.GetOp)
		res, err := h.adapter.Get(getOp)
		if err != nil {
			h.send(h.encoder.Error(err), write)
		} else {
			for k, v := range res {
				h.send(h.encoder.Value(k, v.Flags, v.Value), write)
			}

			h.send(h.encoder.ValueEnd(), write)
		}
	case proto.OpTypeSet:
		setOp := op.(proto.SetOp)
		err := h.adapter.Set(setOp)
		if err != nil {
			h.send(h.encoder.Error(err), write)
		} else if !setOp.NoReply {
			h.send(h.encoder.Stored(), write)
		}
	case proto.OpTypeDelete:
		delOp := op.(proto.DeleteOp)
		err := h.adapter.Delete(delOp)
		if err != nil {
			h.send(h.encoder.Error(err), write)
		} else if !delOp.NoReply {
			h.send(h.encoder.Deleted(), write)
		}
	default:
		panic(fmt.Sprintf("unexpected operation type: %+v", op))
	}

	return nil
}
