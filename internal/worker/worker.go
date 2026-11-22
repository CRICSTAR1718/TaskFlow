// Package worker claims jobs and runs them in a bounded goroutine pool.
package worker

import (
	"context"
	"log/slog"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/cricstar1718/taskflow/internal/config"
	"github.com/cricstar1718/taskflow/internal/events"
)

// Deps are the collaborators cmd/worker passes in. Keeping them explicit
// avoids importing store/queue into this package's public surface.
type Deps struct {
	Jobs  JobSource
	Queue Claimer
	// Heartbeats is optional; when absent the worker still runs but liveness
	// cannot be reported, so it is a misconfiguration in any real deployment.
	Heartbeats *redis.Client
	// Liveness persists heartbeats to Postgres for humans. Optional.
	Liveness BeatStore
	// Events publishes status changes for the live dashboard. Optional.
	Events events.Publisher
}

// Run starts the claim loop, the executor pool and the heartbeat, and returns
// once ctx is cancelled and in-flight jobs have drained.
func Run(ctx context.Context, cfg config.Config, deps Deps, log *slog.Logger) error {
	var wg sync.WaitGroup

	hb := NewHeartbeat(cfg.WorkerID, cfg.HeartbeatInterval, cfg.HeartbeatTTL, log)
	if deps.Heartbeats != nil {
		hb = hb.WithRedis(deps.Heartbeats)
	} else {
		log.Warn("no redis client for heartbeats - this worker will look dead to recovery")
	}
	if deps.Liveness != nil {
		hb = hb.WithStore(deps.Liveness)
	}
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
		Events:      deps.Events,
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
