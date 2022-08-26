package server

import (
	"bufio"
	"fmt"
	"io"
	"net/textproto"

	"github.com/56quarters/jankcache/cache"
	"github.com/56quarters/jankcache/core"
	"github.com/56quarters/jankcache/proto"
)

type Handler struct {
	parser  proto.Parser
	encoder proto.Encoder
	adapter *cache.Adapter
}

func NewHandler(parser proto.Parser, encoder proto.Encoder, adapter *cache.Adapter) *Handler {
	return &Handler{
		parser:  parser,
		encoder: encoder,
		adapter: adapter,
	}
}

func (h *Handler) send(bytes []byte, writer io.Writer) {
	// TODO: logging or metrics when writes fail
	_, _ = writer.Write(bytes)
}

func (h *Handler) Handle(read io.Reader, write io.Writer) error {
	// TODO: Can we use a pool for the buffer used here? Only needs to live for the
	//  single request since the parser allocates a buffer to store the "set" payload
	buf := bufio.NewReader(read)
	text := textproto.NewReader(buf)

	line, err := text.ReadLine()
	if err != nil {
		return err
	}

	// Pass the line we read to the parser as well as the buffered reader since
	// we'll need to read a payload of bytes (not line delimited) in the case of
	// a "set" command.
	op, err := h.parser.Parse(line, buf)
	if err != nil {
		h.send(h.encoder.Error(err), write)
		return nil
	}

	switch op.Type() {
	case proto.OpTypeCacheMemLimit:
		limitOp := op.(proto.CacheMemLimitOp)
		err := h.adapter.CacheMemLimit(limitOp)
		if err != nil {
			h.send(h.encoder.Error(err), write)
		} else if !limitOp.NoReply {
			h.send(h.encoder.Ok(), write)
		}
	case proto.OpTypeCas:
		casOp := op.(proto.CasOp)
		err := h.adapter.Cas(casOp)
		if err != nil {
			h.send(h.encoder.Error(err), write)
		} else if !casOp.NoReply {
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
	case proto.OpTypeFlushAll:
		flushOp := op.(proto.FlushAllOp)
		err = h.adapter.Flush(flushOp)
		if err != nil {
			h.send(h.encoder.Error(err), write)
		} else if !flushOp.NoReply {
			h.send(h.encoder.Ok(), write)
		}
	case proto.OpTypeGat:
		gatOp := op.(proto.GatOp)
		res, err := h.adapter.GetAndTouch(gatOp)
		if err != nil {
			h.send(h.encoder.Error(err), write)
		} else {
			for k, v := range res {
				if gatOp.Unique {
					h.send(h.encoder.ValueUnique(k, v.Flags, v.Value, v.Unique), write)
				} else {
					h.send(h.encoder.Value(k, v.Flags, v.Value), write)
				}
			}

			h.send(h.encoder.ValueEnd(), write)
		}
	case proto.OpTypeGet:
		getOp := op.(proto.GetOp)
		res, err := h.adapter.Get(getOp)
		if err != nil {
			h.send(h.encoder.Error(err), write)
		} else {
			for k, v := range res {
				if getOp.Unique {
					h.send(h.encoder.ValueUnique(k, v.Flags, v.Value, v.Unique), write)
				} else {
					h.send(h.encoder.Value(k, v.Flags, v.Value), write)
				}
			}

			h.send(h.encoder.ValueEnd(), write)
		}
	case proto.OpTypeQuit:
		return core.ErrQuit
	case proto.OpTypeSet:
		setOp := op.(proto.SetOp)
		err := h.adapter.Set(setOp)
		if err != nil {
			h.send(h.encoder.Error(err), write)
		} else if !setOp.NoReply {
			h.send(h.encoder.Stored(), write)
		}
	default:
		panic(fmt.Sprintf("unexpected operation type: %+v", op))
	}

	return nil
}
