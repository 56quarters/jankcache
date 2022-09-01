package server

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/56quarters/jankcache/core"
)

// TODO: Metrics for all this stuff

type TCPConfig struct {
	Address     string
	IdleTimeout time.Duration
}

func (c *TCPConfig) RegisterFlags(prefix string, fs *flag.FlagSet) {
	fs.StringVar(&c.Address, prefix+"address", "localhost:11211", "Address and port for the cache server to bind to")
	fs.DurationVar(&c.IdleTimeout, prefix+"idle-timeout", 60*time.Second, "Max time a connection can be idle before being closed. Set to 0 to disable")
}

type TCPServer struct {
	config   TCPConfig
	handler  *Handler
	logger   log.Logger
	listener net.Listener
	stopping int32
	time     core.Time
}

func NewTCPServer(config TCPConfig, handler *Handler, logger log.Logger) *TCPServer {
	return &TCPServer{
		config:  config,
		handler: handler,
		logger:  logger,
		time:    &core.DefaultTime{},
	}
}

func (s *TCPServer) Stop() {
	atomic.CompareAndSwapInt32(&s.stopping, 0, 1)

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			level.Warn(s.logger).Log("msg", "error closing listener", "err", err)
		}
	}
}

func (s *TCPServer) Run() error {
	var err error
	s.listener, err = net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("unable to bind to %s: %w", s.config.Address, err)
	}

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// If the server is stopping, ignore any error here since it's expected
			if atomic.LoadInt32(&s.stopping) != 0 {
				return nil
			}

			return fmt.Errorf("unable to accept connection: %w", err)
		}

		// TODO: Connection limit?
		// TODO: Pool of routines or something? epoll?
		level.Debug(s.logger).Log("msg", "accepting connection", "remote", conn.RemoteAddr())
		go s.handle(conn)
	}
}

func (s *TCPServer) handle(conn net.Conn) {
	defer func() {
		_ = conn.Close()
	}()

	for {
		if s.config.IdleTimeout > 0 {
			err := conn.SetDeadline(s.time.Now().Add(s.config.IdleTimeout))
			if err != nil {
				level.Error(s.logger).Log("msg", "unable to set idle timeout on connection", "remote", conn.RemoteAddr(), "err", err)
				return
			}
		}

		err := s.handler.Handle(conn, conn)
		if errors.Is(err, os.ErrDeadlineExceeded) {
			level.Debug(s.logger).Log("msg", "closing idle connection", "remote", conn.RemoteAddr())
			return
		} else if errors.Is(err, io.EOF) {
			level.Debug(s.logger).Log("msg", "closing EOF connection", "remote", conn.RemoteAddr())
			return
		} else if errors.Is(err, core.ErrQuit) {
			level.Debug(s.logger).Log("msg", "client quit", "remote", conn.RemoteAddr())
			return
		} else if err != nil {
			level.Warn(s.logger).Log("msg", "error handling connection", "remote", conn.RemoteAddr(), "err", err)
			return
		}
	}
}
