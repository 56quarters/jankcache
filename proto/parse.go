package proto

import (
	"io"
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
	OpTypeGet
	OpTypeQuit
	OpTypeSet
	OpTypeStats
)

const maxPayloadSizeBytes = 1024 * 1024

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
	Keys   []string
	Unique bool
}

func (GetOp) Type() OpType {
	return OpTypeGet
}

type QuitOp struct{}

func (QuitOp) Type() OpType {
	return OpTypeQuit
}

type SetOp struct {
	Key     string
	Flags   uint32
	Expire  int64 // TODO: Size???
	NoReply bool
	Bytes   []byte
}

func (SetOp) Type() OpType {
	return OpTypeSet
}

type Parser struct {
}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) Parse(line string, payload io.Reader) (Op, error) {
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
	case "get":
		return p.ParseGet(line, parts, false)
	case "gets":
		return p.ParseGet(line, parts, true)
	case "quit":
		return QuitOp{}, nil
	case "set":
		return p.ParseSet(line, parts, payload)
	case "add", "append", "cas", "decr", "gat", "gats", "incr", "lru", "lru_crawler",
		"prepend", "replace", "shutdown", "slabs", "stats", "touch", "version", "watch":
		// Valid memcached commands that we've chosen not to implement because they
		// aren't needed for our usecase or their implementation would impact performance
		// or complexity of the commands we do support (or both).
		return nil, core.Unimplemented(cmd)
	}

	return nil, core.ErrBadCommand
}

func (p *Parser) ParseCacheMemLimit(line string, parts []string) (*CacheMemLimitOp, error) {
	if len(parts) < 2 {
		return nil, core.ClientError("bad cache_memlimit command '%s'", line)
	}

	mb, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, core.ClientError("bad cache size: invalid syntax '%s'", line)
	}

	if mb < 1 {
		return nil, core.ClientError("bad cache size: must be at least 1 mb, got %d mb", mb)
	}

	noreply := len(parts) > 2 && "noreply" == strings.ToLower(parts[2])

	return &CacheMemLimitOp{
		Bytes:   int64(mb * 1024 * 1024),
		NoReply: noreply,
	}, nil
}

func (p *Parser) ParseDelete(line string, parts []string) (*DeleteOp, error) {
	if len(parts) < 2 {
		return nil, core.ClientError("bad delete command '%s'", line)
	}

	noreply := len(parts) > 2 && "noreply" == strings.ToLower(parts[2])

	return &DeleteOp{
		Key:     parts[1],
		NoReply: noreply,
	}, nil
}

func (p *Parser) ParseFlushAll(line string, parts []string) (*FlushAllOp, error) {
	// TODO: This sucks, get rid of the duplicate code

	if len(parts) == 2 {
		if "noreply" == strings.ToLower(parts[1]) {
			return &FlushAllOp{NoReply: true}, nil
		}

		delay, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return nil, core.ClientError("bad delay: '%s': %s", line, err)
		}

		return &FlushAllOp{Delay: time.Duration(delay) * time.Second}, nil
	}

	if len(parts) == 3 {
		delay, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return nil, core.ClientError(" bad delay: '%s': %s", line, err)
		}

		noreply := "noreply" == strings.ToLower(parts[2])

		return &FlushAllOp{
			Delay:   time.Duration(delay) * time.Second,
			NoReply: noreply,
		}, nil
	}

	return &FlushAllOp{}, nil
}

func (p *Parser) ParseGet(line string, parts []string, unique bool) (*GetOp, error) {
	if len(parts) < 2 {
		return nil, core.ClientError("bad get command '%s'", line)
	}

	return &GetOp{Keys: parts[1:], Unique: unique}, nil
}

func (p *Parser) ParseSet(line string, parts []string, payload io.Reader) (*SetOp, error) {
	if len(parts) < 5 {
		return nil, core.ClientError("bad set command '%s'", line)
	}

	flags, err := strconv.ParseUint(parts[2], 10, 16)
	if err != nil {
		return nil, core.ClientError("bad flags '%s': %s", line, err)
	}

	expire, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return nil, core.ClientError("bad expire '%s': %s", line, err)
	}

	length, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		return nil, core.ClientError("bad bytes length '%s': %s", line, err)
	}

	if length > maxPayloadSizeBytes {
		return nil, core.ClientError("length of %d greater than max item size of %d", length, maxPayloadSizeBytes)
	}

	bytes := make([]byte, length)
	n, err := io.ReadFull(payload, bytes)
	if err != nil {
		return nil, core.ClientError("unable to read %d payload bytes, only read %d: %s", length, n, err)
	}

	noreply := len(parts) > 5 && "noreply" == strings.ToLower(parts[5])

	return &SetOp{
		Key:     parts[1],
		Flags:   uint32(flags),
		Expire:  expire,
		NoReply: noreply,
		Bytes:   bytes,
	}, nil
}
