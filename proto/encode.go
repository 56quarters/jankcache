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

// TODO: Change the whole API of this to accept a bytes.Buffer for everything?
//  Need to make sure that the buffer doesn't grow in proportion to the result
//  set in that case. I.e. it needs to be written to the output stream after every
//  result and we let the buffer size of the output stream determine when to flush
//  instead of buffering the entire response in a single byte.Buffer.

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
	// TODO: Why return the buffer here instead of just b.Bytes()? Because we don't
	//  want the Buffer reused before the bytes are written somewhere?
	b := e.buffers.Get().(*bytes.Buffer)
	b.Reset()

	b.WriteString(fmt.Sprintf("VALUE %s %d %d\r\n", key, flags, len(value)))
	b.Write(value)
	b.WriteString("\r\n")
	return b
}

func (e *Encoder) ValueUnique(key string, flags uint32, value []byte, unique uint64) *bytes.Buffer {
	b := e.buffers.Get().(*bytes.Buffer)
	b.Reset()

	b.WriteString(fmt.Sprintf("VALUE %s %d %d %d\r\n", key, flags, len(value), unique))
	b.Write(value)
	b.WriteString("\r\n")
	return b
}

func (e *Encoder) PutBuffer(buf *bytes.Buffer) {
	e.buffers.Put(buf)
}

func (e *Encoder) End() []byte {
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
