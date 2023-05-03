package proto

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/56quarters/jankcache/server/core"
)

type OpType int

const (
	OpTypeCacheMemLimit = iota
	OpTypeDelete
	OpTypeGet
	OpTypeQuit
	OpTypeSet
	OpTypeVersion
	OpTypeStats
)

const maxKeySizeBytes = 250
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

type VersionOp struct{}

func (VersionOp) Type() OpType {
	return OpTypeVersion
}

type StatsOp struct{}

func (StatsOp) Type() OpType {
	return OpTypeStats
}

type SetOp struct {
	Key     string
	Flags   uint32
	Expire  int64
	NoReply bool
	Bytes   []byte
}

func (SetOp) Type() OpType {
	return OpTypeSet
}

func ParseLine(line string, payload io.Reader) (Op, error) {
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
		return parseCacheMemLimit(line, parts)
	case "delete":
		return parseDelete(line, parts)
	case "get":
		return parseGet(line, parts, false)
	case "gets":
		return parseGet(line, parts, true)
	case "quit":
		return QuitOp{}, nil
	case "set":
		return parseSet(line, parts, payload)
	case "stats":
		return StatsOp{}, nil
	case "version":
		return VersionOp{}, nil
	case "add", "append", "cas", "decr", "flush_all", "gat", "gats", "incr", "lru",
		"lru_crawler", "prepend", "replace", "shutdown", "slabs", "touch", "watch":
		// Valid memcached commands that we've chosen not to implement because they
		// aren't needed for our usecase or their implementation would impact performance
		// or complexity of the commands we do support (or both).
		return nil, core.Unimplemented(cmd)
	}

	return nil, core.ErrBadCommand
}

func parseCacheMemLimit(line string, parts []string) (*CacheMemLimitOp, error) {
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

func parseDelete(line string, parts []string) (*DeleteOp, error) {
	if len(parts) < 2 {
		return nil, core.ClientError("bad delete command '%s'", line)
	}

	key, err := validateKey(parts[1])
	if err != nil {
		return nil, core.ClientError("bad key: %s", err)
	}

	noreply := len(parts) > 2 && "noreply" == strings.ToLower(parts[2])

	return &DeleteOp{
		Key:     key,
		NoReply: noreply,
	}, nil
}

func parseGet(line string, parts []string, unique bool) (*GetOp, error) {
	if len(parts) < 2 {
		return nil, core.ClientError("bad get command '%s'", line)
	}

	keys, err := validateKeys(parts[1:])
	if err != nil {
		return nil, core.ClientError("bad key(s): %s", err)
	}

	return &GetOp{Keys: keys, Unique: unique}, nil
}

func parseSet(line string, parts []string, payload io.Reader) (*SetOp, error) {
	if len(parts) < 5 {
		return nil, core.ClientError("bad set command '%s'", line)
	}

	key, err := validateKey(parts[1])
	if err != nil {
		return nil, core.ClientError("bad key: %s", err)
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
		Key:     key,
		Flags:   uint32(flags),
		Expire:  expire,
		NoReply: noreply,
		Bytes:   bytes,
	}, nil
}

func validateKeys(keys []string) ([]string, error) {
	for _, k := range keys {
		_, err := validateKey(k)
		if err != nil {
			return nil, err
		}
	}

	return keys, nil
}

func validateKey(key string) (string, error) {
	length := len(key)
	if length > maxKeySizeBytes {
		return "", fmt.Errorf("length %d greater than max of %d", length, maxKeySizeBytes)
	}

	return key, nil
}
