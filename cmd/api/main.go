package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/cricstar1718/taskflow/internal/api"
	"github.com/cricstar1718/taskflow/internal/config"
	"github.com/cricstar1718/taskflow/internal/events"
	"github.com/cricstar1718/taskflow/internal/logging"
	"github.com/cricstar1718/taskflow/internal/queue"
	"github.com/cricstar1718/taskflow/internal/store"
)

const service = "api"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", service, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := logging.New(cfg.LogLevel).With(slog.String("service", service))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("starting " + service)

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	q := queue.NewRedis(cfg.RedisAddr)
	defer func() { _ = q.Close() }()

	if err := api.Run(ctx, cfg, api.Deps{Store: st, Queue: q, Events: events.NewRedis(q.Client())}, log); err != nil {
		return fmt.Errorf("%s: %w", service, err)
	}

	log.Info("stopped " + service)
	return nil
}
