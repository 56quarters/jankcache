package core

import (
	"errors"
)

// TODO: Move these elsewhere? Parser? Split them up?

var (
	ErrBadCommand = errors.New("no such command")
	ErrClient     = errors.New("client error")
	ErrServer     = errors.New("server error")
)
