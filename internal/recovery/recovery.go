// Package recovery retries failed jobs, dead-letters exhausted ones and
// re-queues jobs owned by dead workers.
package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/cricstar1718/taskflow/internal/config"
)

// Deps are the collaborators cmd/recovery passes in.
type Deps struct {
	Store   Store
	Queue   Queue
	Workers Workers
}

// Run sweeps until ctx is cancelled.
func Run(ctx context.Context, cfg config.Config, deps Deps, log *slog.Logger) error {
	m := New(Config{
		Store:       deps.Store,
		Queue:       deps.Queue,
		Workers:     deps.Workers,
		Log:         log,
		StaleAfter:  cfg.HeartbeatTTL * 2,
		BackoffBase: cfg.BackoffBase,
		BackoffMax:  cfg.BackoffMax,
	})

	// Fail fast: a recovery service that cannot reach the store would otherwise
	// sit there looking healthy while every sweep does nothing.
	if err := m.Check(ctx); err != nil {
		return fmt.Errorf("recovery check: %w", err)
	}

	t := time.NewTicker(cfg.RecoveryInterval)
	defer t.Stop()
	failing := false
	for {
		select {
		case <-ctx.Done():
			log.Info("recovery stopping")
			return nil
		case <-t.C:
			if _, err := m.Sweep(ctx); err != nil {
				m.sweepFailed(err, &failing)
				continue
			}
			if failing {
				log.Info("recovery recovered")
				failing = false
			}
		}
	}
}

// Backoff returns the delay before retry number attempt (1-based): exponential
// from base, capped at maxBackoff, with up to 10% jitter.
func Backoff(attempt int, base, maxBackoff time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt && d < maxBackoff; i++ {
		d *= 2
	}
	if d > maxBackoff {
		d = maxBackoff
	}
	return d + time.Duration(rand.Int63n(int64(d)/10+1)) //nolint:gosec // jitter only; not security-sensitive
}
