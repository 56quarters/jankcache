package proto

import (
	"errors"
	"fmt"
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

func (e *Encoder) Line(line string) *Encoder {
	_, _ = e.writer.Write([]byte(line))
	_, _ = e.writer.Write(crlf)
	return e
}

func (e *Encoder) Bytes(b []byte) *Encoder {
	_, _ = e.writer.Write(b)
	_, _ = e.writer.Write(crlf)
	return e
}

func (e *Encoder) Error(err error) *Encoder {
	if errors.Is(err, core.ErrBadCommand) {
		return e.Line(err.Error())
	} else if errors.Is(err, core.ErrClient) {
		return e.Line(err.Error())
	} else if errors.Is(err, core.ErrServer) {
		return e.Line(err.Error())
	} else if errors.Is(err, core.ErrNotFound) {
		return e.Line(err.Error())
	}

	return e.Line(core.ServerError(err.Error()).Error())
}

func (e *Encoder) Encode(obj MemcachedMarshaller) *Encoder {
	obj.MarshallMemcached(e)
	return e
}

func (e *Encoder) Version(version string) *Encoder {
	return e.Line(fmt.Sprintf("VERSION %s", version))
}

func (e *Encoder) End() *Encoder {
	return e.Line("END")
}

func (e *Encoder) Stored() *Encoder {
	return e.Line("STORED")
}

func (e *Encoder) Deleted() *Encoder {
	return e.Line("DELETED")
}

func (e *Encoder) Ok() *Encoder {
	return e.Line("OK")
}

type MemcachedMarshaller interface {
	MarshallMemcached(o *Encoder)
}
