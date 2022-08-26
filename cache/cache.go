package cache

import (
	"flag"
	"fmt"
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
	return 12 + int64(len(e.Value))
}

// TODO: Add a "job queue" chan so that we can support flush delays and `gat` commands (get and queue a job to reset with TTL)

// TODO: Metrics for all of this. Profile prom counters vs atomics + pull (functions). Maybe collector? Copy all counters or something per scape?

type Adapter struct {
	delegate *ristretto.Cache
	casID    uint64
	now      func() time.Time
	wait     bool
	logger   log.Logger
}

func New(cfg Config, logger log.Logger) (*Adapter, error) {
	rcache, err := ristretto.NewCache(
		&ristretto.Config{
			NumCounters: maxNumCounters,
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
		now:      time.Now,
		wait:     false,
		logger:   logger,
	}
}

func (a *Adapter) CacheMemLimit(op proto.CacheMemLimitOp) error {
	a.delegate.UpdateMaxCost(op.Bytes)
	return nil
}

func (a *Adapter) Cas(op proto.CasOp) error {
	e, ok := a.delegate.Get(op.Key)
	if !ok {
		return core.ErrNotFound
	}

	existing := e.(*Entry)
	if existing.Unique != op.Unique {
		return core.ErrExists
	}

	return a.Set(op.SetOp)
}

func (a *Adapter) Delete(op proto.DeleteOp) error {
	a.delegate.Del(op.Key)
	if a.wait {
		a.delegate.Wait()
	}

	return nil
}

func (a *Adapter) Flush(op proto.FlushAllOp) error {
	if op.Delay != 0 {
		return fmt.Errorf("%w: flush delay not supported", core.ErrServer)
	}

	a.delegate.Clear()
	if a.wait {
		a.delegate.Wait()
	}

	return nil
}

func (a *Adapter) Get(op proto.GetOp) (map[string]*Entry, error) {
	out := make(map[string]*Entry, len(op.Keys))
	for _, k := range op.Keys {
		e, ok := a.delegate.Get(k)
		if ok {
			out[k] = e.(*Entry)
		}
	}

	return out, nil
}

func (a *Adapter) GetAndTouch(op proto.GatOp) (map[string]*Entry, error) {
	// TODO: implement this once the cache has a job queue
	return nil, core.Unimplemented("gat")
}

func (a *Adapter) Set(op proto.SetOp) error {
	ttl := a.ttl(op.Expire)
	entry := &Entry{
		Unique: a.unique(),
		Flags:  op.Flags,
		Value:  op.Bytes,
	}

	if a.delegate.SetWithTTL(op.Key, entry, entry.Cost(), time.Duration(ttl)*time.Second) && a.wait {
		a.delegate.Wait()
	}

	return nil
}

func (a *Adapter) unique() uint64 {
	return atomic.AddUint64(&a.casID, 1)
}

func (a *Adapter) ttl(expire int64) int64 {
	// TODO: Test this because it's dumb
	var ttl int64
	if expire > secondsInThirtyDays {
		now := a.now().Unix()
		ttl = expire - now
	} else {
		ttl = expire
	}

	return ttl
}
