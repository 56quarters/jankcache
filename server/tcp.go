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
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/dskit/services"

	"github.com/56quarters/jankcache/core"
)

// TODO: Metrics for all this stuff

type TCPConfig struct {
	Address        string
	IdleTimeout    time.Duration
	MaxConnections uint64
}

func (c *TCPConfig) RegisterFlags(prefix string, fs *flag.FlagSet) {
	fs.StringVar(&c.Address, prefix+"address", "localhost:11211", "Address and port for the cache server to bind to")
	fs.DurationVar(&c.IdleTimeout, prefix+"idle-timeout", 60*time.Second, "Max time a connection can be idle before being closed. Set to 0 to disable")
	fs.Uint64Var(&c.MaxConnections, prefix+"max-connections", 1024, "Max number of client connections that can be open at once. Set to 0 to disable limit")
}

type TCPServer struct {
	services.Service

	config   TCPConfig
	handler  *Handler
	metrics  *Metrics
	listener net.Listener
	logger   log.Logger
}

func NewTCPServer(config TCPConfig, handler *Handler, metrics *Metrics, logger log.Logger) *TCPServer {
	s := &TCPServer{
		config:  config,
		handler: handler,
		metrics: metrics,
		logger:  logger,
	}

	s.Service = services.NewBasicService(s.start, s.loop, s.stop)
	return s
}

func (s *TCPServer) start(ctx context.Context) error {
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

func (s *TCPServer) loop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
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
	level.Debug(s.logger).Log("msg", "shutting down TCP server")

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			level.Warn(s.logger).Log("msg", "error closing listener", "err", err)
		}
	}
}

func (s *TCPServer) handle(conn net.Conn) {
	s.metrics.CurrentConnections.Add(1)
	s.metrics.TotalConnections.Add(1)

	defer func() {
		s.metrics.CurrentConnections.Add(-1)
		runutil.CloseWithLogOnErr(s.logger, conn, "closing connection")
	}()

	currConnections := s.metrics.CurrentConnections.Load()
	if s.config.MaxConnections > 0 && currConnections > int64(s.config.MaxConnections) {
		s.metrics.RejectedConnections.Add(1)
		s.handler.Reject(conn, "max connections")
		level.Debug(s.logger).Log("msg", "server at max connections", "current", currConnections, "max", s.config.MaxConnections)
		return
	}

	for {
		if s.config.IdleTimeout > 0 {
			err := conn.SetDeadline(time.Now().Add(s.config.IdleTimeout))
			if err != nil {
				level.Error(s.logger).Log("msg", "unable to set idle timeout on connection", "remote", conn.RemoteAddr(), "err", err)
				return
			}
		}

		err := s.handler.Handle(conn)
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
