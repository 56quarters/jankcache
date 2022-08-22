package proto

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/56quarters/fauxcache/core"
)

type Parser struct {
}

func (p *Parser) ParseGet(line string) (core.GetOp, error) {
	parts := strings.Split(line, " ")
	if len(parts) < 2 {
		return core.GetOp{}, fmt.Errorf("%w: bad line '%s'", core.ErrClient, line)
	}

	return core.GetOp{Keys: parts[1:]}, nil
}

func (p *Parser) ParseSet(line string, reader io.Reader) (core.SetOp, error) {
	parts := strings.Split(line, " ")
	if len(parts) < 5 {
		return core.SetOp{}, fmt.Errorf("%w: bad line '%s'", core.ErrClient, line)
	}

	flags, err := strconv.ParseUint(parts[2], 10, 16)
	if err != nil {
		return core.SetOp{}, fmt.Errorf("%w: bad flags '%s': %s", core.ErrClient, line, err)
	}

	expire, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return core.SetOp{}, fmt.Errorf("%w: bad expire '%s': %s", core.ErrClient, line, err)
	}

	length, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		return core.SetOp{}, fmt.Errorf("%w: bad bytes length: '%s': %s", core.ErrClient, line, err)
	}

	noreply := len(parts) > 5 && "noreply" == strings.ToLower(parts[5])

	return core.SetOp{
		Key:     parts[1],
		Flags:   uint32(flags),
		Expire:  expire,
		Length:  length,
		NoReply: noreply,
		Reader:  reader,
	}, nil
}

func (p *Parser) ParseDelete(line string) (core.DeleteOp, error) {
	parts := strings.Split(line, " ")
	if len(parts) < 2 {
		return core.DeleteOp{}, fmt.Errorf("%w: bad line '%s'", core.ErrClient, line)
	}

	noreply := len(parts) > 2 && "noreply" == strings.ToLower(parts[2])

	return core.DeleteOp{
		Key:     parts[1],
		NoReply: noreply,
	}, nil
}
