package proto

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/56quarters/jankcache/core"
)

type Encoder struct {
}

func (e *Encoder) Error(err error) []byte {
	if errors.Is(err, core.ErrBadCommand) {
		return []byte(fmt.Sprintf("%s\r\n", err))
	} else if errors.Is(err, core.ErrClient) {
		return []byte(fmt.Sprintf("%s\r\n", err))
	} else if errors.Is(err, core.ErrServer) {
		return []byte(fmt.Sprintf("%s\r\n", err))
	} else {
		return []byte(fmt.Sprintf("%s\r\n", core.ServerError(err.Error())))
	}
}

func (e *Encoder) Value(key string, flags uint32, value []byte) []byte {
	b := bytes.Buffer{}
	b.WriteString(fmt.Sprintf("VALUE %s %d %d\r\n", key, flags, len(value)))
	b.Write(value)
	b.WriteString("\r\n")
	return b.Bytes()
}

func (e *Encoder) ValueUnique(key string, flags uint32, value []byte, unique int64) []byte {
	b := bytes.Buffer{}
	b.WriteString(fmt.Sprintf("VALUE %s %d %d %d\r\n", key, flags, len(value), unique))
	b.Write(value)
	b.WriteString("\r\n")
	return b.Bytes()
}

func (e *Encoder) ValueEnd() []byte {
	return []byte("END\r\n")
}

func (e *Encoder) Stored() []byte {
	return []byte("STORED\r\n")
}

func (e *Encoder) NotStored() []byte {
	return []byte("NOT_STORED\r\n")
}

func (e *Encoder) Exists() []byte {
	return []byte("EXISTS\r\n")
}

func (e *Encoder) NotFound() []byte {
	return []byte("NOT_FOUND\r\n")
}

func (e *Encoder) Deleted() []byte {
	return []byte("DELETED\r\n")
}

func (e *Encoder) Ok() []byte {
	return []byte("OK\r\n")
}
