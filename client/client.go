package client

import (
	"fmt"
	"math"

	"github.com/bradfitz/gomemcache/memcache"
)

// TODO: Should we just use memcached Item?

type Item struct {
	Key        string
	Value      []byte
	Flags      uint32
	Expiration int64 // TODO: Should we match memcached (int32)?
}

type Client interface {
	Set(item *Item) error
	Get(key string) (*Item, error)
	GetMulti(keys []string) (map[string]*Item, error)
	Delete(key string) error
	DeleteAll() error
}

type RemoteClient struct {
	client *memcache.Client
}

func NewRemoteClient(client *memcache.Client) Client {
	return &RemoteClient{client: client}
}

func (c *RemoteClient) Set(item *Item) error {
	if item.Expiration > math.MaxInt32 {
		return fmt.Errorf("item expiration int32 overflow %d", item.Expiration)
	}

	return c.client.Set(&memcache.Item{
		Key:        item.Key,
		Value:      item.Value,
		Flags:      item.Flags,
		Expiration: int32(item.Expiration),
	})
}

func (c *RemoteClient) Get(key string) (*Item, error) {
	res, err := c.client.Get(key)
	if err != nil {
		return nil, err
	}

	return &Item{
		Key:        res.Key,
		Value:      res.Value,
		Flags:      res.Flags,
		Expiration: int64(res.Expiration),
	}, nil
}

func (c *RemoteClient) GetMulti(keys []string) (map[string]*Item, error) {
	res, err := c.client.GetMulti(keys)
	if err != nil {
		return nil, err
	}

	out := make(map[string]*Item, len(res))
	for k, v := range res {
		out[k] = &Item{
			Key:        v.Key,
			Value:      v.Value,
			Flags:      v.Flags,
			Expiration: int64(v.Expiration),
		}
	}

	return out, nil
}

func (c *RemoteClient) Delete(key string) error {
	return c.client.Delete(key)
}

func (c *RemoteClient) DeleteAll() error {
	return c.client.DeleteAll()
}
