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
	fs.BoolVar(&c.Enabled, prefix+"enabled", false, "Enable debug server for profiling information")
	fs.StringVar(&c.Address, prefix+"address", "localhost:8080", "Address and port for the server to bind to")
}

func NewDebug(config DebugConfig, logger log.Logger) *DebugServer {
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

	return http.ListenAndServe(s.config.Address, nil)
}
