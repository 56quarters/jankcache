package proto

import (
	"errors"
	"io"

	"github.com/56quarters/jankcache/server/core"
)

var (
	crlf = []byte("\r\n")
)

type Encoder struct {
	writer io.Writer
}

func NewEncoder(writer io.Writer) *Encoder {
	return &Encoder{
		writer: writer,
	}
}

func (o *Encoder) Line(line string) *Encoder {
	_, _ = o.writer.Write([]byte(line))
	_, _ = o.writer.Write(crlf)
	return o
}

func (o *Encoder) Bytes(b []byte) *Encoder {
	_, _ = o.writer.Write(b)
	_, _ = o.writer.Write(crlf)
	return o
}

func (o *Encoder) Error(err error) *Encoder {
	if errors.Is(err, core.ErrBadCommand) {
		return o.Line(err.Error())
	} else if errors.Is(err, core.ErrClient) {
		return o.Line(err.Error())
	} else if errors.Is(err, core.ErrServer) {
		return o.Line(err.Error())
	} else if errors.Is(err, core.ErrNotFound) {
		return o.Line(err.Error())
	}

	return o.Line(core.ServerError(err.Error()).Error())
}

func (o *Encoder) Encode(obj MemcachedMarshaller) *Encoder {
	obj.MarshallMemcached(o)
	return o
}

func (o *Encoder) End() *Encoder {
	return o.Line("END")
}

func (o *Encoder) Stored() *Encoder {
	return o.Line("STORED")
}

func (o *Encoder) Deleted() *Encoder {
	return o.Line("DELETED")
}

func (o *Encoder) Ok() *Encoder {
	return o.Line("OK")
}

type MemcachedMarshaller interface {
	MarshallMemcached(o *Encoder)
}
