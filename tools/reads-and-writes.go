package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/56quarters/jankcache/client"
)

const numBatches = 10
const batchSize = 10000

func readAndWrite(keys []string, c client.Client, logger log.Logger) {
	for {
		n := rand.Intn(len(keys))
		subset := keys[0:n]
		for _, k := range subset {
			err := c.Set(&client.Item{
				Key:        k,
				Value:      []byte(fmt.Sprintf("some value %f", rand.ExpFloat64())),
				Expiration: 300,
			})

			if err != nil {
				level.Error(logger).Log("msg", "failed to set key", "key", k, "err", err)
			}
		}

		time.Sleep(time.Millisecond * time.Duration(rand.Int63n(1000)))

		// SET a subset of keys but try to GET all of them - workloads will skew read heavy
		_, err := c.GetMulti(keys)
		if err != nil {
			level.Error(logger).Log("msg", "failed to get keys", "err", err)
		}
	}
}

func main() {
	logger := log.With(log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)), "ts", log.DefaultTimestampUTC)
	mc := memcache.New("localhost:11211")
	mc.MaxIdleConns = numBatches * 2
	jc := client.NewRemoteClient(mc)

	wg := sync.WaitGroup{}

	for batch := 0; batch < numBatches; batch += 1 {
		start := batch * batchSize
		var keys []string

		for i := 0; i < batchSize; i++ {
			keys = append(keys, fmt.Sprintf("somekey%d", i+start))
		}

		go func() {
			wg.Add(1)
			defer wg.Done()
			readAndWrite(keys, jc, logger)
		}()

	}

	wg.Wait()
}
