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

func (c *Config) RegisterFlags(prefix string, fs *flag.FlagSet) {
	c.Cache.RegisterFlags(prefix+"cache.", fs)
	c.Server.RegisterFlags(prefix+"server.", fs)
	c.Debug.RegisterFlags(prefix+"debug.", fs)
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

	// Background context here because AwaitStopped will return immediately if the
	// context provided is cancelled (we use cancellation of ctx to indicate to the
	// application that it should stop).
	err = a.manager.AwaitStopped(context.Background())
	if err != nil {
		return err
	}

	return nil
}
