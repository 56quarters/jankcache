package server

import (
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// TODO: Metrics for all this stuff

type TCPConfig struct {
	Address string
	Port    uint16
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
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.config.Address, s.config.Port))
	if err != nil {
		return fmt.Errorf("unable to bind to %s:%d: %w", s.config.Address, s.config.Port, err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			return fmt.Errorf("unable to accept connection: %w", err)
		}

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
			level.Info(s.logger).Log("msg", "EOF closing connection", "remote", conn.RemoteAddr())
			break
		} else if err != nil {
			level.Warn(s.logger).Log("msg", "error handling connection", "remote", conn.RemoteAddr(), "err", err)
			break
		}
	}
}
