package main

import (
	"fmt"
	"os"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func main() {
	logger := log.NewLogfmtLogger(os.Stderr)
	client := memcache.New("localhost:11211")

	for batch := 0; batch < 10000; batch += 1 {
		start := batch * 100
		var keys []string

		for i := 0; i < 100; i++ {
			keys = append(keys, fmt.Sprintf("somekey%d", i+start))
		}

		res, err := client.GetMulti(keys)
		if err != nil {
			level.Error(logger).Log("msg", "error getting key", "err")
			return
		}

		level.Info(logger).Log("start", start, "count", len(res))
	}
}
