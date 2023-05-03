package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"

	"github.com/56quarters/jankcache/server"
)

func main() {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	fs := flag.NewFlagSet("jankcache", flag.ExitOnError)
	cfg := server.Config{}
	cfg.RegisterFlags("", fs)

	err := fs.Parse(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	} else if err != nil {
		level.Error(logger).Log("msg", "unable to parse configuration options", "err", err)
		os.Exit(1)
	}

	// TODO: Make level configurable
	logger = log.With(level.NewFilter(logger, level.AllowDebug()), "ts", log.DefaultTimestampUTC)

	srv, err := server.New(cfg, logger)
	if err != nil {
		level.Error(logger).Log("msg", "unable to create application", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	shutdown(cancel, logger)

	err = services.StartAndAwaitRunning(ctx, srv)
	if err != nil {
		level.Error(logger).Log("msg", "error running application", "err", err)
		os.Exit(1)
	}

	// Block until the context is cancelled, then shutdown the server
	<-ctx.Done()

	err = services.StopAndAwaitTerminated(context.Background(), srv)
	if err != nil {
		level.Error(logger).Log("msg", "error stopping application", "err", err)
		os.Exit(1)
	}
}

// TODO: Shutdown delay?

func shutdown(cancel context.CancelFunc, logger log.Logger) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		level.Info(logger).Log("msg", "stopping on signal", "signal", sig)
		cancel()
	}()
}
