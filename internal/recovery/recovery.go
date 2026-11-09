// Package recovery retries failed jobs, dead-letters exhausted ones and
// re-queues jobs owned by dead workers.
package recovery

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	"github.com/your-org/taskflow/internal/config"
)

// Run sweeps until ctx is cancelled.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	t := time.NewTicker(cfg.RecoveryInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			// TODO: retry due RETRYING jobs, DEAD-letter exhausted jobs, and
			// re-queue taskflow:inflight:{id} of workers with no taskflow:hb:{id}.
			log.Debug("recovery sweep")
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
