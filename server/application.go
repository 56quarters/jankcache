package server

import (
	"context"
	"flag"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"

	"github.com/56quarters/jankcache/cache"
	"github.com/56quarters/jankcache/proto"
)

type Config struct {
	Cache  cache.Config
	Server TCPConfig
	Debug  DebugConfig
}

func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	c.Cache.RegisterFlags("cache.", fs)
	c.Server.RegisterFlags("server.", fs)
	c.Debug.RegisterFlags("debug.", fs)
}

type Application struct {
	// TODO: Create + store client that uses `adapter`

	manager *services.Manager
}

func ApplicationFromConfig(cfg Config, logger log.Logger) (*Application, error) {
	adapter, err := cache.New(cfg.Cache, logger)
	if err != nil {
		return nil, err
	}

	var srvs []services.Service
	if cfg.Debug.Enabled {
		level.Info(logger).Log("msg", "running debug server", "address", cfg.Debug.Address)
		srvs = append(srvs, NewDebugServer(cfg.Debug, logger))
	}

	encoder := proto.NewEncoder()
	parser := proto.NewParser()
	handler := NewHandler(parser, encoder, adapter)

	level.Info(logger).Log("msg", "running server", "address", cfg.Server.Address)
	srvs = append(srvs, NewTCPServer(cfg.Server, handler, logger))

	manager, err := services.NewManager(srvs...)
	if err != nil {
		return nil, err
	}

	return &Application{manager: manager}, nil
}

func (a *Application) Run(ctx context.Context) error {
	err := a.manager.StartAsync(ctx)
	if err != nil {
		return err
	}

	err = a.manager.AwaitHealthy(ctx)
	if err != nil {
		return err
	}

	err = a.manager.AwaitStopped(ctx)
	if err != nil {
		return err
	}

	return nil
}
