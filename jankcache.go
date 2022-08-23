package main

import (
	"fmt"
	"os"

	"github.com/dgraph-io/ristretto"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/56quarters/jankcache/cache"
	"github.com/56quarters/jankcache/proto"
	"github.com/56quarters/jankcache/server"
)

func main() {

	r, err := ristretto.NewCache(
		&ristretto.Config{
			NumCounters: 100000,
			MaxCost:     10000,
			BufferItems: 64,
			Metrics:     true,
		},
	)

	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create cache: %s", err)
		os.Exit(1)
	}

	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	e := proto.Encoder{}
	p := proto.Parser{}
	c := cache.NewAdapter(r, l)

	h := server.NewHandler(p, e, c, l)

	for {
		err := h.Handle(os.Stdin, os.Stdout)
		if err != nil {
			level.Warn(l).Log("msg", "error while reading input", "err", err)
		}
	}
}
