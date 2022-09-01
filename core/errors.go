package core

import (
	"errors"
	"fmt"
)

var (
	ErrBadCommand = errors.New("ERROR")
	ErrClient     = errors.New("CLIENT_ERROR")
	ErrExists     = errors.New("EXISTS")
	ErrNotFound   = errors.New("NOT_FOUND")
	ErrServer     = errors.New("SERVER_ERROR")

	ErrQuit = errors.New("quit")
)

func ClientError(msg string, a ...any) error {
	return fmt.Errorf("%w %s", ErrClient, fmt.Sprintf(msg, a...))
}

func ServerError(msg string, a ...any) error {
	return fmt.Errorf("%w %s", ErrServer, fmt.Sprintf(msg, a...))
}

func Unimplemented(cmd string) error {
	return ServerError("%s unimplemented", cmd)
}
