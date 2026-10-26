// Package scheduler polls Postgres for due jobs and enqueues them into Redis.
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/your-org/taskflow/internal/config"
)

// Run polls until ctx is cancelled.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	t := time.NewTicker(cfg.SchedulerTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			// TODO: acquire taskflow:lock:scheduler, fetch due jobs, enqueue by
			// priority, compute next cron run.
			log.Debug("scheduler tick")
		}
	}
}
