package server

import (
	"context"
	"flag"
	"net/http"
	_ "net/http/pprof"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
)

type DebugConfig struct {
	Enabled bool
	Address string
}

func (c *DebugConfig) RegisterFlags(prefix string, fs *flag.FlagSet) {
	fs.BoolVar(&c.Enabled, prefix+"enabled", false, "Enable debug HTTP server for profiling information")
	fs.StringVar(&c.Address, prefix+"address", "localhost:8080", "Address and port for the debug HTTP server to bind to")
}

func NewDebugServer(config DebugConfig, logger log.Logger) *DebugServer {
	s := &DebugServer{
		config: config,
		logger: logger,
	}

	s.Service = services.NewBasicService(nil, s.loop, nil)
	return s
}

type DebugServer struct {
	services.Service

	config DebugConfig
	logger log.Logger
}

func (s *DebugServer) start(ctx context.Context) error {
	return nil
}

func (s *DebugServer) loop(ctx context.Context) error {
	return http.ListenAndServe(s.config.Address, nil)
}

func (s *DebugServer) stop(err error) error {
	return nil
}
