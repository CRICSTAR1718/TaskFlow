// Package worker claims jobs and runs them in a bounded goroutine pool.
package worker

import (
	"context"
	"log/slog"
	"sync"

	"github.com/cricstar1718/taskflow/internal/config"
)

// Deps are the collaborators cmd/worker passes in. Keeping them explicit
// avoids importing store/queue into this package's public surface.
type Deps struct {
	Jobs  JobSource
	Queue Claimer
}

// Run starts the claim loop, the executor pool and the heartbeat, and returns
// once ctx is cancelled and in-flight jobs have drained.
func Run(ctx context.Context, cfg config.Config, deps Deps, log *slog.Logger) error {
	var wg sync.WaitGroup

	hb := NewHeartbeat(cfg.WorkerID, cfg.HeartbeatInterval, cfg.HeartbeatTTL, log)
	wg.Add(1)
	go func() { defer wg.Done(); hb.Run(ctx) }()

	exec := NewExecutor(log)
	pool := NewPool(cfg.WorkerConcurrency, exec, log)

	runner := NewRunner(RunnerConfig{
		Store:       deps.Jobs,
		Queue:       deps.Queue,
		Pool:        pool,
		Executor:    exec,
		WorkerID:    cfg.WorkerID,
		Log:         log,
		BackoffBase: cfg.BackoffBase,
		BackoffMax:  cfg.BackoffMax,
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.Run(ctx) // drains the channel until ctx is cancelled
	}()

	runner.Run(ctx) // claims until ctx is cancelled
	wg.Wait()
	return nil
}
