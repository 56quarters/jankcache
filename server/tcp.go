package server

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/56quarters/jankcache/core"
)

// TODO: Metrics for all this stuff

type TCPConfig struct {
	Address string
}

func (c *TCPConfig) RegisterFlags(prefix string, fs *flag.FlagSet) {
	fs.StringVar(&c.Address, prefix+"address", "localhost:11211", "Address and port for the server to bind to")
}

type TCP struct {
	config  TCPConfig
	handler *Handler
	logger  log.Logger
}

func NewTCP(cfg TCPConfig, handler *Handler, logger log.Logger) *TCP {
	return &TCP{
		config:  cfg,
		handler: handler,
		logger:  logger,
	}
}

func (s *TCP) Run() error {
	l, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("unable to bind to %s: %w", s.config.Address, err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			return fmt.Errorf("unable to accept connection: %w", err)
		}

		// TODO: Connection limit?
		// TODO: Idle timeout

		// TODO: Pool of routines or something? epoll?
		level.Debug(s.logger).Log("msg", "accepting connection", "remote", conn.RemoteAddr())
		go s.handle(conn)
	}
}

func (s *TCP) handle(conn net.Conn) {
	defer func() {
		// TODO: io.ReadAll?
		_ = conn.Close()
	}()

	for {
		err := s.handler.Handle(conn, conn)
		if errors.Is(err, io.EOF) {
			level.Debug(s.logger).Log("msg", "EOF closing connection", "remote", conn.RemoteAddr())
			break
		} else if errors.Is(err, core.ErrQuit) {
			level.Debug(s.logger).Log("msg", "client quit", "remote", conn.RemoteAddr())
			break
		} else if err != nil {
			level.Warn(s.logger).Log("msg", "error handling connection", "remote", conn.RemoteAddr(), "err", err)
			break
		}
	}
}
