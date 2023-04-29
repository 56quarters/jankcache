package cache

import (
	"flag"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/go-kit/log"

	"github.com/56quarters/jankcache/proto"
)

const secondsInThirtyDays = 60 * 60 * 24 * 30
const maxNumCounters = 100_000

type Config struct {
	MaxSizeMb uint64
}

func (c *Config) RegisterFlags(prefix string, fs *flag.FlagSet) {
	fs.Uint64Var(&c.MaxSizeMb, prefix+"max-size-mb", 64, "Max cache size in megabytes")
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

func (e *Entry) MarshallMemcached(o *proto.Output) {
	o.Line(fmt.Sprintf("VALUE %s %d %d %d", e.Key, e.Flags, len(e.Value), e.Unique))
	o.Bytes(e.Value)
}

type NoCasEntry struct {
	*Entry
}

func (e *NoCasEntry) MarshallMemcached(o *proto.Output) {
	o.Line(fmt.Sprintf("VALUE %s %d %d", e.Key, e.Flags, len(e.Value)))
	o.Bytes(e.Value)
}

type Adapter struct {
	delegate *ristretto.Cache
	cas      atomic.Uint64
	logger   log.Logger
}

func New(cfg Config, logger log.Logger) (*Adapter, error) {
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
		return nil, err
	}

	return NewFromBacking(rcache, logger), nil
}

func NewFromBacking(cache *ristretto.Cache, logger log.Logger) *Adapter {
	return &Adapter{
		delegate: cache,
		logger:   logger,
	}
}

func (a *Adapter) Metrics() *ristretto.Metrics {
	return a.delegate.Metrics
}

func (a *Adapter) CacheMemLimit(op *proto.CacheMemLimitOp) error {
	a.delegate.UpdateMaxCost(op.Bytes)
	return nil
}

func (a *Adapter) Delete(op *proto.DeleteOp) error {
	a.delegate.Del(op.Key)
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
		Key:    op.Key,
		Unique: a.unique(),
		Flags:  op.Flags,
		Value:  op.Bytes,
	}

	a.delegate.SetWithTTL(entry.Key, entry, entry.Cost(), ttl)
	return nil
}

func (a *Adapter) unique() uint64 {
	return a.cas.Add(1)
}

func (a *Adapter) ttl(expire int64) time.Duration {
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
