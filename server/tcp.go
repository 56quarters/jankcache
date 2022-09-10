package server

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"

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
	services.Service

	config   TCPConfig
	handler  *Handler
	listener net.Listener
	stopping chan struct{}
	logger   log.Logger
	time     core.Time
}

func NewTCPServer(config TCPConfig, handler *Handler, logger log.Logger) *TCPServer {
	s := &TCPServer{
		config:   config,
		handler:  handler,
		stopping: make(chan struct{}),
		logger:   logger,
		time:     &core.DefaultTime{},
	}

	s.Service = services.NewBasicService(s.start, s.loop, s.stop)
	return s
}

func (s *TCPServer) start(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.Address)
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

func (s *TCPServer) loop(context.Context) error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopping:
				// Server is shutting down, ignore the error since this is intentional.
				return nil
			default:
				return fmt.Errorf("unable to accept connection: %w", err)
			}
		}

		level.Debug(s.logger).Log("msg", "accepting connection", "remote", conn.RemoteAddr())
		go s.handle(conn)
	}
}

func (s *TCPServer) stop(err error) error {
	if err != nil {
		level.Error(s.logger).Log("msg", "stopping TCP server due to error", "err", err)
	}

	return nil
}

func (s *TCPServer) shutdown(ctx context.Context) {
	<-ctx.Done()

	close(s.stopping)
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			level.Warn(s.logger).Log("msg", "error closing listener", "err", err)
		}
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
