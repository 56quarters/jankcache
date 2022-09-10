package main

import (
	"context"
	"errors"
	"flag"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/56quarters/jankcache/server"
)

func main() {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	fs := flag.NewFlagSet("jankcache", flag.ExitOnError)
	cfg := server.Config{}
	cfg.RegisterFlags(fs)

	err := fs.Parse(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	} else if err != nil {
		level.Error(logger).Log("msg", "unable to parse configuration options", "err", err)
		os.Exit(1)
	}

	// TODO: Make level configurable
	logger = log.With(level.NewFilter(logger, level.AllowDebug()), "ts", log.DefaultTimestampUTC)
	ctx := context.Background()

	app, err := server.ApplicationFromConfig(cfg, logger)
	if err != nil {
		level.Error(logger).Log("msg", "unable to create application", "err", err)
		os.Exit(1)
	}

	err = app.Run(ctx)
	if err != nil {
		level.Error(logger).Log("msg", "error running application", "err", err)
		os.Exit(1)
	}
}
