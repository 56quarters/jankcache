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
		return []byte("ERROR\r\n")
	} else if errors.Is(err, core.ErrClient) {
		return []byte(fmt.Sprintf("CLIENT_ERROR %s\r\n", err.Error()))
	} else {
		return []byte(fmt.Sprintf("SERVER_ERROR %s\r\n", err.Error()))
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
