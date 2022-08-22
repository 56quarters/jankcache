package cache

import (
	"io"
	"time"

	"github.com/dgraph-io/ristretto"

	"github.com/56quarters/fauxcache/core"
)

const secondsInThirtyDays = 60 * 60 * 24 * 30

type Entry struct {
	Flags  uint32
	Unique int64
	Value  []byte
}

type Adapter struct {
	delegate *ristretto.Cache
	now      func() time.Time
	wait     bool
}

func NewAdapter(cache *ristretto.Cache) *Adapter {
	return &Adapter{
		delegate: cache,
		now:      time.Now,
		wait:     true,
	}
}

func (a *Adapter) Get(op core.GetOp) (map[string]*Entry, error) {
	out := make(map[string]*Entry, len(op.Keys))

	for _, k := range op.Keys {
		e, ok := a.delegate.Get(k)
		if ok {
			out[k] = e.(*Entry)
		}
	}

	return out, nil
}
func (a *Adapter) Set(op core.SetOp) error {
	var ttl int64
	if op.Expire > secondsInThirtyDays {
		now := a.now().Unix()
		ttl = op.Expire - now
	} else {
		ttl = op.Expire
	}

	limit := io.LimitReader(op.Reader, int64(op.Length))
	bytes, err := io.ReadAll(limit)
	if err != nil {
		return err
	}

	cost := int64(len(bytes))
	e := &Entry{
		Flags:  op.Flags,
		Unique: 0,
		Value:  bytes,
	}

	if a.delegate.SetWithTTL(op.Key, e, cost, time.Duration(ttl)*time.Second) && a.wait {
		a.delegate.Wait()
	}

	return nil
}

func (a *Adapter) Delete(op core.DeleteOp) error {
	a.delegate.Del(op.Key)
	if a.wait {
		a.delegate.Wait()
	}

	return nil
}
