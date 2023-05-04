package core

import (
	"errors"
	"fmt"
)

var (
	ErrBadCommand = errors.New("ERROR")
	ErrClient     = errors.New("CLIENT_ERROR")
	ErrNotFound   = errors.New("NOT_FOUND")
	ErrServer     = errors.New("SERVER_ERROR")
	ErrQuit       = errors.New("quit")
)

func ClientError(msg string, args ...any) error {
	return fmt.Errorf("%w %s", ErrClient, fmt.Sprintf(msg, args...))
}

func ServerError(msg string, args ...any) error {
	return fmt.Errorf("%w %s", ErrServer, fmt.Sprintf(msg, args...))
}

func Unimplemented(cmd string) error {
	return ServerError("%s not implemented", cmd)
}
