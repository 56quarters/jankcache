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

	var i uint64
	for x := 0; x < 10000; x++ {
		err := client.Set(&memcache.Item{
			Key:        fmt.Sprintf("somekey%d", i),
			Value:      []byte(`{"foo":"bar"}`),
			Flags:      0,
			Expiration: 0,
		})

		if err != nil {
			level.Error(logger).Log("msg", "error setting key", "err", err)
			return
		}

		//time.Sleep(time.Millisecond)
		i++
	}
}
