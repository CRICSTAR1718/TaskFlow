package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cricstar1718/taskflow/internal/domain"

	"github.com/cricstar1718/taskflow/internal/config"
	"github.com/cricstar1718/taskflow/internal/logging"
	"github.com/cricstar1718/taskflow/internal/queue"
	"github.com/cricstar1718/taskflow/internal/store"
	"github.com/cricstar1718/taskflow/internal/worker"
)

const service = "worker"

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

	log := logging.New(cfg.LogLevel).With(
		slog.String("service", service),
		slog.String("worker_id", cfg.WorkerID),
	)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("starting " + service)

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	if err := st.UpsertWorker(ctx, workerInfo(cfg)); err != nil {
		log.Warn("register worker", slog.Any("error", err))
	}

	q := queue.NewRedis(cfg.RedisAddr)
	defer func() { _ = q.Close() }()

	if err := worker.Run(ctx, cfg, worker.Deps{Jobs: st, Queue: q}, log); err != nil {
		return fmt.Errorf("%s: %w", service, err)
	}

	log.Info("stopped " + service)
	return nil
}

func workerInfo(cfg config.Config) domain.WorkerInfo {
	return domain.WorkerInfo{
		ID:          cfg.WorkerID,
		Hostname:    hostname(),
		Concurrency: cfg.WorkerConcurrency,
		StartedAt:   time.Now(),
	}
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}
