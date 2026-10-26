package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/your-org/taskflow/internal/config"
	"github.com/your-org/taskflow/internal/logging"
	"github.com/your-org/taskflow/internal/worker"
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

	log := logging.New(cfg.LogLevel).With(slog.String("service", service))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("starting " + service)

	if err := worker.Run(ctx, cfg, log); err != nil {
		return fmt.Errorf("%s: %w", service, err)
	}

	log.Info("stopped " + service)
	return nil
}
