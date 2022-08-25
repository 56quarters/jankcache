package main

import (
	"errors"
	"flag"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/56quarters/jankcache/cache"
	"github.com/56quarters/jankcache/proto"
	"github.com/56quarters/jankcache/server"
)

type Config struct {
	LogLevel level.Value
	Cache    cache.Config
	Server   server.TCPConfig
}

func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	c.Cache.RegisterFlags("cache.", fs)
	c.Server.RegisterFlags("server.", fs)
}

func main() {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	fs := flag.NewFlagSet("jankcache", flag.ExitOnError)
	cfg := Config{}
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

	adapter, err := cache.New(cfg.Cache, logger)
	if err != nil {
		level.Error(logger).Log("msg", "unable to initialize cache", "err", err)
		os.Exit(1)
	}

	encoder := proto.Encoder{}
	parser := proto.Parser{}
	handler := server.NewHandler(parser, encoder, adapter)

	level.Info(logger).Log("msg", "running server", "address", cfg.Server.Address)
	srv := server.NewTCP(cfg.Server, handler, logger)
	err = srv.Run()
	if err != nil {
		level.Error(logger).Log("msg", "server error", "err", err)
		os.Exit(1)
	}
}
