package proto

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/56quarters/jankcache/core"
)

type Encoder struct {
	buffers sync.Pool
}

func NewEncoder() *Encoder {
	return &Encoder{
		buffers: sync.Pool{
			New: func() any { return &bytes.Buffer{} },
		},
	}
}

func (e *Encoder) Error(err error) []byte {
	if errors.Is(err, core.ErrBadCommand) {
		return []byte(fmt.Sprintf("%s\r\n", err))
	} else if errors.Is(err, core.ErrClient) {
		return []byte(fmt.Sprintf("%s\r\n", err))
	} else if errors.Is(err, core.ErrServer) {
		return []byte(fmt.Sprintf("%s\r\n", err))
	} else if errors.Is(err, core.ErrExists) {
		return []byte(fmt.Sprintf("%s\r\n", err))
	} else if errors.Is(err, core.ErrNotFound) {
		return []byte(fmt.Sprintf("%s\r\n", err))
	} else {
		return []byte(fmt.Sprintf("%s\r\n", core.ServerError(err.Error())))
	}
}

func (e *Encoder) Value(key string, flags uint32, value []byte) *bytes.Buffer {
	b := e.buffers.Get().(*bytes.Buffer)
	b.Reset()

	b.Grow(64 + len(value))
	b.WriteString(fmt.Sprintf("VALUE %s %d %d\r\n", key, flags, len(value)))
	b.Write(value)
	b.WriteString("\r\n")
	return b
}

func (e *Encoder) ValueUnique(key string, flags uint32, value []byte, unique uint64) *bytes.Buffer {
	b := e.buffers.Get().(*bytes.Buffer)
	b.Reset()

	b.Grow(64 + len(value))
	b.WriteString(fmt.Sprintf("VALUE %s %d %d %d\r\n", key, flags, len(value), unique))
	b.Write(value)
	b.WriteString("\r\n")
	return b
}

func (e *Encoder) ReturnBuffer(buf *bytes.Buffer) {
	e.buffers.Put(buf)
}

func (e *Encoder) ValueEnd() []byte {
	return []byte("END\r\n")
}

func (e *Encoder) Stored() []byte {
	return []byte("STORED\r\n")
}

func (e *Encoder) Deleted() []byte {
	return []byte("DELETED\r\n")
}

func (e *Encoder) Ok() []byte {
	return []byte("OK\r\n")
}
