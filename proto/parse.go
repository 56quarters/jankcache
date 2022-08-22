package proto

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/56quarters/jankcache/core"
)

type OpType int

const (
	OpTypeGet = iota
	OpTypeSet
	OpTypeDelete
)

type Op interface {
	Type() OpType
}

type GetOp struct {
	Keys []string
}

func (GetOp) Type() OpType {
	return OpTypeGet
}

type SetOp struct {
	Key     string
	Flags   uint32
	Expire  int64  // TODO: Size???
	Length  uint64 // TODO: Size???
	NoReply bool
	Bytes   []byte
}

func (SetOp) Type() OpType {
	return OpTypeSet
}

type DeleteOp struct {
	Key     string
	NoReply bool
}

func (DeleteOp) Type() OpType {
	return OpTypeDelete
}

type Payload interface {
	// TODO: Make this support a reader or something for payload?
	//  implement our own version of scanner?

	Scan() bool
	Bytes() []byte
}

type Parser struct {
}

func (p *Parser) Parse(line string, payload Payload) (Op, error) {
	parts := strings.Split(line, " ")
	if len(parts) == 0 {
		panic("no parts!")
	}

	cmd := strings.ToLower(parts[0])
	switch cmd {
	case "get":
		fallthrough
	case "gets":
		return p.ParseGet(line, parts)
	case "set":
		return p.ParseSet(line, parts, payload)
	case "delete":
		return p.ParseDelete(line, parts)
	}

	return nil, fmt.Errorf("%w: supported command '%s'", core.ErrServer, line)
}

func (p *Parser) ParseGet(line string, parts []string) (GetOp, error) {
	if len(parts) < 2 {
		return GetOp{}, fmt.Errorf("%w: bad line '%s'", core.ErrClient, line)
	}

	return GetOp{Keys: parts[1:]}, nil
}

func (p *Parser) ParseSet(line string, parts []string, payload Payload) (SetOp, error) {
	if len(parts) < 5 {
		return SetOp{}, fmt.Errorf("%w: bad line '%s'", core.ErrClient, line)
	}

	flags, err := strconv.ParseUint(parts[2], 10, 16)
	if err != nil {
		return SetOp{}, fmt.Errorf("%w: bad flags '%s': %s", core.ErrClient, line, err)
	}

	expire, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return SetOp{}, fmt.Errorf("%w: bad expire '%s': %s", core.ErrClient, line, err)
	}

	length, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		return SetOp{}, fmt.Errorf("%w: bad bytes length: '%s': %s", core.ErrClient, line, err)
	}

	if !payload.Scan() {
		return SetOp{}, fmt.Errorf("%w: missing payload of %d bytes, no tokens left", core.ErrClient, length)
	}

	bytes := payload.Bytes()
	if length != uint64(len(bytes)) {
		return SetOp{}, fmt.Errorf("%w: mismatch bytes length, expected = %d, actual = %d", core.ErrClient, length, len(bytes))
	}

	noreply := len(parts) > 5 && "noreply" == strings.ToLower(parts[5])

	return SetOp{
		Key:     parts[1],
		Flags:   uint32(flags),
		Expire:  expire,
		Length:  length,
		NoReply: noreply,
		Bytes:   bytes,
	}, nil
}

func (p *Parser) ParseDelete(line string, parts []string) (DeleteOp, error) {
	if len(parts) < 2 {
		return DeleteOp{}, fmt.Errorf("%w: bad line '%s'", core.ErrClient, line)
	}

	noreply := len(parts) > 2 && "noreply" == strings.ToLower(parts[2])

	return DeleteOp{
		Key:     parts[1],
		NoReply: noreply,
	}, nil
}
