package proto

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var (
	ErrBadCommand = errors.New("no such command")
	ErrClient     = errors.New("client error")
	ErrServer     = errors.New("server error")
)

type Parser struct {
}

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

func (p *Parser) ParseGet(line string) (GetOp, error) {
	parts := strings.Split(line, " ")
	if len(parts) < 2 {
		return GetOp{}, fmt.Errorf("%w: bad line '%s'", ErrClient, line)
	}

	return GetOp{Keys: parts[1:]}, nil
}

func (p *Parser) ParseSet(line string, reader io.Reader) (SetOp, error) {
	parts := strings.Split(line, " ")
	if len(parts) < 5 {
		return SetOp{}, fmt.Errorf("%w: bad line '%s'", ErrClient, line)
	}

	flags, err := strconv.ParseUint(parts[2], 10, 16)
	if err != nil {
		return SetOp{}, fmt.Errorf("%w: bad flags '%s': %s", ErrClient, line, err)
	}

	expire, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return SetOp{}, fmt.Errorf("%w: bad expire '%s': %s", ErrClient, line, err)
	}

	length, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		return SetOp{}, fmt.Errorf("%w: bad bytes length: '%s': %s", ErrClient, line, err)
	}

	noreply := len(parts) > 5 && "noreply" == strings.ToLower(parts[5])

	return SetOp{
		Key:     parts[1],
		Flags:   uint32(flags),
		Expire:  expire,
		Length:  length,
		NoReply: noreply,
		Reader:  reader,
	}, nil
}

func (p *Parser) ParseDelete(line string) (DeleteOp, error) {
	parts := strings.Split(line, " ")
	if len(parts) < 2 {
		return DeleteOp{}, fmt.Errorf("%w: bad line '%s'", ErrClient, line)
	}

	noreply := len(parts) > 2 && "noreply" == strings.ToLower(parts[2])

	return DeleteOp{
		Key:     parts[1],
		NoReply: noreply,
	}, nil
}
