package cache

import (
	"flag"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/go-kit/log"

	"github.com/56quarters/jankcache/core"
	"github.com/56quarters/jankcache/proto"
)

const secondsInThirtyDays = 60 * 60 * 24 * 30
const maxNumCounters = 100_000

type Config struct {
	MaxSizeMb int64
}

func (c *Config) RegisterFlags(prefix string, fs *flag.FlagSet) {
	fs.Int64Var(&c.MaxSizeMb, prefix+"max-size-mb", 100, "Max cache size in megabytes")
}

type Entry struct {
	Unique uint64
	Flags  uint32
	Value  []byte
}

func (e Entry) Cost() int64 {
	// unique (8 bytes) + flags (4 bytes) + payload
	return 12 + int64(len(e.Value))
}

// TODO: Metrics for all of this. Profile prom counters vs atomics + pull (functions). Maybe collector? Copy all counters or something per scape?

type Adapter struct {
	delegate *ristretto.Cache
	casID    uint64
	time     core.Time
	logger   log.Logger
}

func New(cfg Config, logger log.Logger) (*Adapter, error) {
	rcache, err := ristretto.NewCache(
		&ristretto.Config{
			NumCounters: maxNumCounters, // TODO: Expose this config?
			MaxCost:     cfg.MaxSizeMb * 1024 * 1024,
			BufferItems: 64,
			Metrics:     false,
		},
	)

	if err != nil {
		return nil, err
	}

	return NewFromBacking(rcache, logger), nil
}

func NewFromBacking(cache *ristretto.Cache, logger log.Logger) *Adapter {
	return &Adapter{
		delegate: cache,
		logger:   logger,
		time:     core.DefaultTime{},
	}
}

func (a *Adapter) CacheMemLimit(op *proto.CacheMemLimitOp) error {
	a.delegate.UpdateMaxCost(op.Bytes)
	return nil
}

func (a *Adapter) Delete(op *proto.DeleteOp) error {
	a.delegate.Del(op.Key)
	return nil
}

func (a *Adapter) Flush(op *proto.FlushAllOp) error {
	if op.Delay > 0 {
		a.time.AfterFunc(op.Delay, a.delegate.Clear)
	} else {
		a.delegate.Clear()
	}

	return nil
}

func (a *Adapter) Get(op *proto.GetOp) (map[string]*Entry, error) {
	out := make(map[string]*Entry, len(op.Keys))
	for _, k := range op.Keys {
		e, ok := a.delegate.Get(k)
		if ok {
			out[k] = e.(*Entry)
		}
	}

	return out, nil
}

func (a *Adapter) Set(op *proto.SetOp) error {
	ttl := a.ttl(op.Expire)
	entry := &Entry{
		Unique: a.unique(),
		Flags:  op.Flags,
		Value:  op.Bytes,
	}

	a.delegate.SetWithTTL(op.Key, entry, entry.Cost(), ttl)
	return nil
}

func (a *Adapter) unique() uint64 {
	return atomic.AddUint64(&a.casID, 1)
}

func (a *Adapter) ttl(expire int64) time.Duration {
	// TODO: Test this because it's dumb
	var ttl int64
	if expire > secondsInThirtyDays {
		now := a.time.Now().Unix()
		ttl = expire - now
	} else {
		ttl = expire
	}

	return time.Duration(ttl) * time.Second
}
