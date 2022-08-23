package core

import (
	"errors"
)

// TODO: Move these elsewhere? Parser? Split them up?

var (
	// TODO: Should these render themselves in protocol form?

	ErrBadCommand = errors.New("no such command")
	ErrClient     = errors.New("client error")
	ErrServer     = errors.New("server error")

	ErrQuit = errors.New("quit")
)
