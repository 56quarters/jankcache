package core

import (
	"errors"
)

var (
	ErrBadCommand = errors.New("no such command")
	ErrClient     = errors.New("client error")
	ErrServer     = errors.New("server error")
)
