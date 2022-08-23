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
	handler := server.NewHandler(p, e, c)

	cfg := server.TCPConfig{
		Address: "localhost",
		Port:    11211,
	}
	srv := server.NewTCP(cfg, handler, l)
	err = srv.Run()
	if err != nil {
		level.Error(l).Log("msg", "server error", "err", err)
	}
}
