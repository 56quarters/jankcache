package proto

import (
	"strconv"
	"strings"
	"time"

	"github.com/56quarters/jankcache/core"
)

type OpType int

const (
	OpTypeCacheMemLimit = iota
	OpTypeDelete
	OpTypeFlushAll
	OpTypeGat
	OpTypeGet
	OpTypeQuit
	OpTypeSet
	OpTypeStats
	OpTypeVersion
)

type Op interface {
	Type() OpType
}

type CacheMemLimitOp struct {
	Bytes   int64
	NoReply bool
}

func (CacheMemLimitOp) Type() OpType {
	return OpTypeCacheMemLimit
}

type DeleteOp struct {
	Key     string
	NoReply bool
}

func (DeleteOp) Type() OpType {
	return OpTypeDelete
}

type FlushAllOp struct {
	Delay   time.Duration
	NoReply bool
}

func (FlushAllOp) Type() OpType {
	return OpTypeFlushAll
}

type GetOp struct {
	Keys []string
}

func (GetOp) Type() OpType {
	return OpTypeGet
}

type GatOp struct {
	Keys []string
}

func (GatOp) Type() OpType {
	return OpTypeGat
}

type QuitOp struct{}

func (QuitOp) Type() OpType {
	return OpTypeQuit
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

type StatsOp struct{}

func (StatsOp) Type() OpType {
	return OpTypeStats
}

type VersionOp struct{}

func (VersionOp) Type() OpType {
	return OpTypeVersion
}

type Payload interface {
	// TODO: Make this support a reader or something for payload?
	//  implement our own version of scanner?

	Err() error
	Scan() bool
	Bytes() []byte
}

type Parser struct {
}

func (p *Parser) Parse(line string, payload Payload) (Op, error) {
	if line == "" {
		return nil, core.ErrBadCommand
	}

	parts := strings.Split(line, " ")
	if len(parts) == 0 {
		return nil, core.ErrBadCommand
	}

	cmd := strings.ToLower(parts[0])
	switch cmd {
	case "cache_memlimit":
		return p.ParseCacheMemLimit(line, parts)
	case "delete":
		return p.ParseDelete(line, parts)
	case "flush_all":
		return p.ParseFlushAll(line, parts)
	case "gat":
		fallthrough
	case "gats":
		return p.ParseGat(line, parts)
	case "get":
		fallthrough
	case "gets":
		return p.ParseGet(line, parts)
	case "quit":
		return QuitOp{}, nil
	case "set":
		return p.ParseSet(line, parts, payload)
	case "stats":
		return StatsOp{}, nil
	case "version":
		return VersionOp{}, nil
	}

	return nil, core.ErrBadCommand
}

func (p *Parser) ParseCacheMemLimit(line string, parts []string) (CacheMemLimitOp, error) {
	if len(parts) < 2 {
		return CacheMemLimitOp{}, core.ClientError("bad cache_memlimit command '%s'", line)
	}

	mb, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return CacheMemLimitOp{}, core.ClientError("bad cache size: '%s': %s", line, err)
	}

	noreply := len(parts) > 2 && "noreply" == strings.ToLower(parts[2])

	return CacheMemLimitOp{
		Bytes:   mb * 1024 * 1024,
		NoReply: noreply,
	}, nil
}

func (p *Parser) ParseDelete(line string, parts []string) (DeleteOp, error) {
	if len(parts) < 2 {
		return DeleteOp{}, core.ClientError("bad delete command '%s'", line)
	}

	noreply := len(parts) > 2 && "noreply" == strings.ToLower(parts[2])

	return DeleteOp{
		Key:     parts[1],
		NoReply: noreply,
	}, nil
}

func (p *Parser) ParseFlushAll(line string, parts []string) (FlushAllOp, error) {
	if len(parts) == 2 {
		if "noreply" == strings.ToLower(parts[1]) {
			return FlushAllOp{NoReply: true}, nil
		}

		delay, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return FlushAllOp{}, core.ClientError("bad delay: '%s': %s", line, err)
		}

		return FlushAllOp{Delay: time.Duration(delay) * time.Second}, nil
	}

	if len(parts) == 3 {
		delay, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return FlushAllOp{}, core.ClientError(" bad delay: '%s': %s", line, err)
		}

		noreply := "noreply" == strings.ToLower(parts[2])

		return FlushAllOp{
			Delay:   time.Duration(delay) * time.Second,
			NoReply: noreply,
		}, nil
	}

	return FlushAllOp{}, nil
}

func (p *Parser) ParseGat(line string, parts []string) (GatOp, error) {
	if len(parts) < 2 {
		return GatOp{}, core.ClientError("bad gat command '%s'", line)
	}

	return GatOp{Keys: parts[1:]}, nil
}

func (p *Parser) ParseGet(line string, parts []string) (GetOp, error) {
	if len(parts) < 2 {
		return GetOp{}, core.ClientError("bad get command '%s'", line)
	}

	return GetOp{Keys: parts[1:]}, nil
}

func (p *Parser) ParseSet(line string, parts []string, payload Payload) (SetOp, error) {
	if len(parts) < 5 {
		return SetOp{}, core.ClientError("bad set command '%s'", line)
	}

	flags, err := strconv.ParseUint(parts[2], 10, 16)
	if err != nil {
		return SetOp{}, core.ClientError("bad flags '%s': %s", line, err)
	}

	expire, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return SetOp{}, core.ClientError("bad expire '%s': %s", line, err)
	}

	length, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		return SetOp{}, core.ClientError("bad bytes length: '%s': %s", line, err)
	}

	if !payload.Scan() {
		err = payload.Err()
		if err != nil {
			return SetOp{}, core.ClientError("missing payload of %d bytes, no tokens left: %s", length, err)
		}

		return SetOp{}, core.ClientError("missing payload of %d bytes, no tokens left", length)
	}

	bytes := payload.Bytes()
	if length != uint64(len(bytes)) {
		return SetOp{}, core.ClientError("mismatch bytes length, expected = %d, actual = %d", length, len(bytes))
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
