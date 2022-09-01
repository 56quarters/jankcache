package client

import (
	"github.com/bradfitz/gomemcache/memcache"

	"github.com/56quarters/jankcache/cache"
	"github.com/56quarters/jankcache/proto"
)

type Client interface {
	Set(item *memcache.Item) error
	Get(key string) (*memcache.Item, error)
	GetMulti(keys []string) (map[string]*memcache.Item, error)
	Delete(key string) error
	DeleteAll() error
}

func NewLocalClient(local *cache.Adapter) Client {
	return &LocalClient{
		local: local,
	}
}

type LocalClient struct {
	local *cache.Adapter
}

func (l *LocalClient) Set(item *memcache.Item) error {
	return l.local.Set(&proto.SetOp{
		Key:    item.Key,
		Flags:  item.Flags,
		Expire: int64(item.Expiration),
		Bytes:  item.Value,
	})
}

func (l *LocalClient) Get(key string) (*memcache.Item, error) {
	res, err := l.GetMulti([]string{key})
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, nil
	}

	return res[key], nil
}

func (l *LocalClient) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	res, err := l.local.Get(&proto.GetOp{Keys: keys})
	if err != nil {
		return nil, err
	}

	out := make(map[string]*memcache.Item, len(res))
	for k, v := range res {
		out[k] = &memcache.Item{
			Key:        k,
			Value:      v.Value,
			Flags:      v.Flags,
			Expiration: int32(v.Expiration.Seconds()),
		}
	}

	return out, nil
}

func (l *LocalClient) Delete(key string) error {
	return l.local.Delete(&proto.DeleteOp{Key: key})
}

func (l *LocalClient) DeleteAll() error {
	return l.local.Flush(&proto.FlushAllOp{})
}
