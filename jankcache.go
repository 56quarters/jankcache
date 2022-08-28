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

// TODO: pprof endpoint server

type Config struct {
	Cache  cache.Config
	Server server.TCPConfig
	Debug  server.DebugConfig
}

func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	c.Cache.RegisterFlags("cache.", fs)
	c.Server.RegisterFlags("server.", fs)
	c.Debug.RegisterFlags("debug.", fs)
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

	if cfg.Debug.Enabled {
		level.Info(logger).Log("msg", "running debug server", "address", cfg.Debug.Address)
		dbg := server.NewDebugServer(cfg.Debug, logger)
		go func() {
			_ = dbg.Run()
		}()
	}

	encoder := proto.NewEncoder()
	parser := proto.NewParser()
	handler := server.NewHandler(parser, encoder, adapter)

	level.Info(logger).Log("msg", "running server", "address", cfg.Server.Address)
	srv := server.NewTCPServer(cfg.Server, handler, logger)
	err = srv.Run()

	var ret int
	if err != nil {
		level.Error(logger).Log("msg", "server error", "err", err)
		ret = 1
	} else {
		level.Info(logger).Log("msg", "stopping server")
	}

	os.Exit(ret)
}
