package proto

import (
	"errors"
	"io"

	"github.com/56quarters/jankcache/core"
)

var (
	crlf = []byte("\r\n")
)

type Output struct {
	writer io.Writer
}

func NewOutput(writer io.Writer) *Output {
	return &Output{
		writer: writer,
	}
}

func (o *Output) Line(line string) *Output {
	_, _ = o.writer.Write([]byte(line))
	_, _ = o.writer.Write(crlf)
	return o
}

func (o *Output) Bytes(b []byte) *Output {
	_, _ = o.writer.Write(b)
	_, _ = o.writer.Write(crlf)
	return o
}

func (o *Output) Error(err error) *Output {
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

func (o *Output) Encode(obj MemcachedMarshaller) *Output {
	obj.MarshallMemcached(o)
	return o
}

func (o *Output) End() *Output {
	return o.Line("END")
}

func (o *Output) Stored() *Output {
	return o.Line("STORED")
}

func (o *Output) Deleted() *Output {
	return o.Line("DELETED")
}

func (o *Output) Ok() *Output {
	return o.Line("OK")
}

type MemcachedMarshaller interface {
	MarshallMemcached(o *Output)
}
