// Package worker claims jobs and runs them in a bounded goroutine pool.
package worker

import (
	"context"
	"log/slog"
	"sync"

	"github.com/your-org/taskflow/internal/config"
)

// Run starts the pool and heartbeat, and returns once ctx is cancelled and
// in-flight jobs have drained.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	var wg sync.WaitGroup

	hb := NewHeartbeat(cfg.WorkerID, cfg.HeartbeatInterval, cfg.HeartbeatTTL, log)
	wg.Add(1)
	go func() { defer wg.Done(); hb.Run(ctx) }()

	pool := NewPool(cfg.WorkerConcurrency, NewExecutor(log), log)
	pool.Run(ctx)

	wg.Wait()
	return nil
}
