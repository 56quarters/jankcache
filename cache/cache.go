package cache

import "github.com/dgraph-io/ristretto"

type Wrapper struct {
	delegate *ristretto.Cache
}
