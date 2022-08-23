package cache

import (
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/56quarters/jankcache/core"
	"github.com/56quarters/jankcache/proto"
)

const secondsInThirtyDays = 60 * 60 * 24 * 30

type Entry struct {
	Flags  uint32
	Unique int64
	Value  []byte
}

// TODO: Add a "job queue" chan so that we can support flush delays and `gat` commands (get and queue a job to reset with TTL)

type Adapter struct {
	delegate *ristretto.Cache
	now      func() time.Time
	wait     bool
	logger   log.Logger
}

func NewAdapter(cache *ristretto.Cache, logger log.Logger) *Adapter {
	return &Adapter{
		delegate: cache,
		now:      time.Now,
		wait:     true,
		logger:   logger,
	}
}

func (a *Adapter) CacheMemLimit(op proto.CacheMemLimitOp) error {
	level.Debug(a.logger).Log("msg", "cache_memlimit operation", "op", fmt.Sprintf("%+v", op))
	a.delegate.UpdateMaxCost(op.Bytes)
	return nil
}

func (a *Adapter) Delete(op proto.DeleteOp) error {
	level.Debug(a.logger).Log("msg", "delete operation", "op", fmt.Sprintf("%+v", op))

	a.delegate.Del(op.Key)
	if a.wait {
		a.delegate.Wait()
	}

	return nil
}

func (a *Adapter) Flush(op proto.FlushAllOp) error {
	level.Debug(a.logger).Log("msg", "flush_all operation", "op", fmt.Sprintf("%+v", op))

	if op.Delay != 0 {
		return fmt.Errorf("%w: flush delay not supported", core.ErrServer)
	}

	// TODO: This throws away pending gets/sets. Does that matter?
	a.delegate.Clear()
	if a.wait {
		a.delegate.Wait()
	}

	return nil
}

func (a *Adapter) Get(op proto.GetOp) (map[string]*Entry, error) {
	level.Debug(a.logger).Log("msg", "get operation", "op", fmt.Sprintf("%+v", op))

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
	return nil, fmt.Errorf("%w: gat not supported", core.ErrServer)
}

func (a *Adapter) Set(op proto.SetOp) error {
	level.Debug(a.logger).Log("msg", "set operation", "op", fmt.Sprintf("%+v", op))

	// TODO: Test this because it's dumb
	var ttl int64
	if op.Expire > secondsInThirtyDays {
		now := a.now().Unix()
		ttl = op.Expire - now
	} else {
		ttl = op.Expire
	}

	cost := int64(len(op.Bytes))
	e := &Entry{
		Flags:  op.Flags,
		Unique: 0,
		Value:  op.Bytes,
	}

	if a.delegate.SetWithTTL(op.Key, e, cost, time.Duration(ttl)*time.Second) && a.wait {
		a.delegate.Wait()
	}

	return nil
}
