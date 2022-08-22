package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/dgraph-io/ristretto"

	"github.com/56quarters/fauxcache/cache"
	"github.com/56quarters/fauxcache/proto"
)

var (
	setLine  = "set somekey 32 60 13"
	setBytes = `{"foo":"bar"}`
	getLine  = "get somekey2"
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

	e := proto.Encoder{}
	p := proto.Parser{}
	c := cache.NewAdapter(r)

	setop, err := p.ParseSet(setLine, strings.NewReader(setBytes))
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to parse set: %s", err)
		os.Exit(1)
	}

	fmt.Printf("OP: %+v\n", setop)
	err = c.Set(setop)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to set op: %s", err)
		os.Exit(1)
	}

	getop, err := p.ParseGet(getLine)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to parse set: %s", err)
		os.Exit(1)
	}

	fmt.Printf("OP: %+v\n", getop)
	res, err := c.Get(getop)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to get op: %s", err)
		os.Exit(1)
	}

	fmt.Printf("GET: %+v\n", res)

	for k, v := range res {
		fmt.Printf("%s", e.Value(k, v.Flags, v.Value))
	}

	fmt.Printf("%s", e.ValueEnd())
}
