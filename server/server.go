package server

import (
	"bufio"
	"fmt"
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
		panic(err.Error())
	}

	switch op.Type() {
	case proto.OpTypeGet:
		res, err := h.adapter.Get(op.(proto.GetOp))
		if err != nil {
			panic(err.Error())
		} else {
			fmt.Fprintf(write, "RES: %+v\n", res)
		}
	case proto.OpTypeSet:
		err := h.adapter.Set(op.(proto.SetOp))
		if err != nil {
			panic(err.Error())
		}
	case proto.OpTypeDelete:
		err := h.adapter.Delete(op.(proto.DeleteOp))
		if err != nil {
			fmt.Fprintf(write, "ERR: %s\n", err)
		}
	default:
		panic("unexpected operation type")
	}

	return nil
}
