package core

import (
	"errors"
	"io"
)

var (
	ErrBadCommand = errors.New("no such command")
	ErrClient     = errors.New("client error")
	ErrServer     = errors.New("server error")
)

type GetOp struct {
	Keys []string
}

type SetOp struct {
	Key     string
	Flags   uint32
	Expire  int64  // TODO: Size???
	Length  uint64 // TODO: Size???
	NoReply bool
	Reader  io.Reader
}

type DeleteOp struct {
	Key     string
	NoReply bool
}
