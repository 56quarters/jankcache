package server

import (
	"context"
	"flag"
	"fmt"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"

	"github.com/56quarters/jankcache/server/cache"
	"github.com/56quarters/jankcache/server/proto"
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

func (c *Config) Validate() error {
	if err := c.Cache.Validate(); err != nil {
		return err
	}

	if err := c.Server.Validate(); err != nil {
		return err
	}

	return c.Debug.Validate()
}

type Server struct {
	services.Service

	logger  log.Logger
	manager *services.Manager
	watcher *services.FailureWatcher
}

func New(cfg Config, logger log.Logger) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("configuration: %w", err)
	}

	metrics := NewMetrics()
	metrics.MaxConnections.Store(cfg.Server.MaxConnections)

	rtCtx := NewRuntimeContext()
	parser := proto.NewParser(cfg.Cache.MaxItemSize)
	handler := NewHandler(cache.New(cfg.Cache, logger), parser, metrics, rtCtx)
	tcpSrv := NewTCPServer(cfg.Server, handler, metrics, logger)

	srvs := []services.Service{rtCtx, tcpSrv}
	if cfg.Debug.Enabled {
		srvs = append(srvs, NewDebugServer(cfg.Debug, logger))
	}

	manager, err := services.NewManager(srvs...)
	if err != nil {
		return nil, err
	}

	watcher := services.NewFailureWatcher()
	watcher.WatchManager(manager)

	s := &Server{
		logger:  logger,
		manager: manager,
		watcher: watcher,
	}

	s.Service = services.NewBasicService(s.starting, s.loop, s.stopping)
	return s, nil
}

func (s *Server) starting(ctx context.Context) error {
	return services.StartManagerAndAwaitHealthy(ctx, s.manager)
}

func (s *Server) loop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-s.watcher.Chan():
			return fmt.Errorf("subservice error: %w", err)
		}
	}
}

func (s *Server) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), s.manager)
}
