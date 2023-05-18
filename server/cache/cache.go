package cache

import (
	"flag"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/go-kit/log"

	"github.com/56quarters/jankcache/server/proto"
)

const secondsInThirtyDays = 60 * 60 * 24 * 30
const maxNumCounters = 100_000

type Config struct {
	MaxSizeMb   uint64
	MaxItemSize uint64
}

func (c *Config) RegisterFlags(prefix string, fs *flag.FlagSet) {
	fs.Uint64Var(&c.MaxSizeMb, prefix+"max-size-mb", 64, "Max cache size in megabytes")
	fs.Uint64Var(&c.MaxItemSize, prefix+"max-item-size", 1024*1024, "Max size of a cache entry in bytes")
}

func (c *Config) Validate() error {
	if c.MaxSizeMb < 1 {
		return fmt.Errorf("invalid value for max-size-mb: %d", c.MaxSizeMb)
	}

	if c.MaxItemSize < 1 {
		return fmt.Errorf("invalid valid for max-item-size: %d", c.MaxItemSize)
	}

	return nil
}

type Entry struct {
	Key    string
	Unique uint64
	Flags  uint32
	Value  []byte
}

func (e *Entry) Cost() int64 {
	// unique (8 bytes) + flags (4 bytes) + key + payload
	return 12 + int64(len(e.Key)) + int64(len(e.Value))
}

func (e *Entry) MarshallMemcached(o *proto.Encoder) {
	o.Line(fmt.Sprintf("VALUE %s %d %d %d", e.Key, e.Flags, len(e.Value), e.Unique))
	o.Bytes(e.Value)
}

type NoCasEntry struct {
	*Entry
}

func (e *NoCasEntry) MarshallMemcached(o *proto.Encoder) {
	o.Line(fmt.Sprintf("VALUE %s %d %d", e.Key, e.Flags, len(e.Value)))
	o.Bytes(e.Value)
}

type Cache struct {
	delegate *ristretto.Cache
	cas      atomic.Uint64
	logger   log.Logger
}

func New(cfg Config, logger log.Logger) *Cache {
	rcache, err := ristretto.NewCache(
		&ristretto.Config{
			NumCounters:        maxNumCounters,
			MaxCost:            int64(cfg.MaxSizeMb * 1024 * 1024),
			BufferItems:        64,
			Metrics:            true,
			IgnoreInternalCost: false,
		},
	)

	if err != nil {
		// This can only happen if we pass bad config values to ristretto
		panic(fmt.Sprintf("unexpected error initializing cache: %s", err))
	}

	return NewFromBacking(rcache, logger)
}

func NewFromBacking(cache *ristretto.Cache, logger log.Logger) *Cache {
	return &Cache{
		delegate: cache,
		logger:   logger,
	}
}

func (c *Cache) MaxBytes() uint64 {
	return uint64(c.delegate.MaxCost())
}

func (c *Cache) Metrics() *ristretto.Metrics {
	return c.delegate.Metrics
}

func (c *Cache) CacheMemLimit(op *proto.CacheMemLimitOp) error {
	c.delegate.UpdateMaxCost(op.Bytes)
	return nil
}

func (c *Cache) Delete(op *proto.DeleteOp) error {
	c.delegate.Del(op.Key)
	return nil
}

func (c *Cache) Get(op *proto.GetOp) ([]*Entry, error) {
	// Slice of entries instead of a map since users can request the same
	// key multiple times and memcached will return it multiple times. We
	// don't want to deduplicate and we don't actually use the key anywhere.
	// We immediately serialize and write all entries to output.
	out := make([]*Entry, 0, len(op.Keys))
	for _, k := range op.Keys {
		e, ok := c.delegate.Get(k)
		if ok {
			out = append(out, e.(*Entry))
		}
	}

	return out, nil
}

func (c *Cache) Set(op *proto.SetOp) error {
	ttl := c.ttl(op.Expire)
	entry := &Entry{
		Key:    op.Key,
		Unique: c.unique(),
		Flags:  op.Flags,
		Value:  op.Bytes,
	}

	c.delegate.SetWithTTL(entry.Key, entry, entry.Cost(), ttl)
	return nil
}

func (c *Cache) unique() uint64 {
	return c.cas.Add(1)
}

func (c *Cache) ttl(expire int64) time.Duration {
	// TODO: Test this because it's dumb
	var ttl int64
	if expire > secondsInThirtyDays {
		now := time.Now().Unix()
		ttl = expire - now
	} else {
		ttl = expire
	}

	return time.Duration(ttl) * time.Second
}
