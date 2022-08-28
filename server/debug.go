package server

import (
	"flag"
	"net/http"
	_ "net/http/pprof"

	"github.com/go-kit/log"
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
	return &DebugServer{
		config: config,
		logger: logger,
	}
}

type DebugServer struct {
	config DebugConfig
	logger log.Logger
}

func (s *DebugServer) Stop() {
	// nothing yet
}

func (s *DebugServer) Run() error {
	if !s.config.Enabled {
		return nil
	}

	// TODO: Create our own server so we can start/stop it instead of using the default
	return http.ListenAndServe(s.config.Address, nil)
}
