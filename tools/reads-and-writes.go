package main

import (
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

const numBatches = 10
const writePercent = 0.1

func getPayloads(path string) map[string][]byte {
	out := make(map[string][]byte)
	err := filepath.WalkDir(path, func(path string, info fs.DirEntry, err error) error {
		ext := filepath.Ext(path)
		if ext == ".c" || ext == ".h" {
			contents, err := os.ReadFile(path)
			if err != nil {
				return err
			}

			out[filepath.Base(path)] = contents
		}
		return nil
	})

	if err != nil {
		panic(fmt.Sprintf("%s", err))
	}

	return out
}

func readAndWrite(payloads map[string][]byte, mc *memcache.Client, logger log.Logger) {
	for {
		keys := make([]string, 0, len(payloads))
		for k, v := range payloads {
			keys = append(keys, k)

			if rand.Float64() < writePercent {
				err := mc.Set(&memcache.Item{
					Key:        k,
					Value:      v,
					Expiration: 300,
				})

				if err != nil {
					level.Error(logger).Log("msg", "failed to set key", "key", k, "err", err)
				}
			}
		}

		// SET a subset of keys but try to GET all of them - workloads will skew read heavy
		_, err := mc.GetMulti(keys)
		if err != nil {
			level.Error(logger).Log("msg", "failed to get keys", "err", err)
		}
	}
}

func main() {
	logger := log.With(log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)), "ts", log.DefaultTimestampUTC)
	mc := memcache.New("localhost:11211")
	mc.MaxIdleConns = numBatches * 2
	mc.Timeout = 2000 * time.Millisecond

	// Get a map of all .c and .h filenames to their contents to use for keys and values
	// to test the cache. Memcached is expected to be checked out in a parallel directory
	// to jankcache.
	payloads := getPayloads("../memcached")
	wg := sync.WaitGroup{}

	for batch := 0; batch < numBatches; batch++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readAndWrite(payloads, mc, logger)
		}()
	}

	wg.Wait()
}
