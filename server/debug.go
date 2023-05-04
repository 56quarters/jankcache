package server

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // profiling

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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

func (c *DebugConfig) Validate() error {
	return nil
}

type DebugServer struct {
	services.Service

	config   DebugConfig
	listener net.Listener
	logger   log.Logger
}

func NewDebugServer(config DebugConfig, logger log.Logger) *DebugServer {
	s := &DebugServer{
		config: config,
		logger: logger,
	}

	s.Service = services.NewBasicService(s.start, s.loop, s.stop)
	return s
}

func (s *DebugServer) start(ctx context.Context) error {
	level.Info(s.logger).Log("msg", "starting debug server", "address", s.config.Address)

	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("unable to bind to %s: %w", s.config.Address, err)
	}

	s.listener = listener
	// Spawn a goroutine to wait for this context to be cancelled (happens when this service
	// is shutdown) and close the listener so Accept will return an error. Otherwise, the Accept()
	// call would block indefinitely.
	go s.shutdown(ctx)
	return nil
}

func (s *DebugServer) loop(_ context.Context) error {
	err := http.Serve(s.listener, nil)
	if !errors.Is(err, net.ErrClosed) {
		return err
	}

	return nil
}

func (s *DebugServer) stop(err error) error {
	if err != nil {
		level.Error(s.logger).Log("msg", "stopping debug server due to error", "err", err)
	}

	return nil
}

func (s *DebugServer) shutdown(ctx context.Context) {
	<-ctx.Done()
	level.Debug(s.logger).Log("msg", "shutting down debug server")

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			level.Warn(s.logger).Log("msg", "error closing listener", "err", err)
		}
	}
}
