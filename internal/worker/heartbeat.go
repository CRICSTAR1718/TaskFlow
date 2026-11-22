package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cricstar1718/taskflow/internal/queue"
)

// Heartbeat periodically refreshes taskflow:hb:{workerID} with a TTL so
// recovery can detect dead workers.
type Heartbeat struct {
	workerID string
	interval time.Duration
	ttl      time.Duration
	log      *slog.Logger
	rdb      *redis.Client
}

// WithRedis attaches the Redis client used to publish the heartbeat.
func (h *Heartbeat) WithRedis(c *redis.Client) *Heartbeat { h.rdb = c; return h }

// NewHeartbeat creates a Heartbeat.
func NewHeartbeat(workerID string, interval, ttl time.Duration, log *slog.Logger) *Heartbeat {
	return &Heartbeat{workerID: workerID, interval: interval, ttl: ttl, log: log}
}

// Run beats until ctx is cancelled.
func (h *Heartbeat) Run(ctx context.Context) {
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := h.beat(ctx); err != nil {
				h.log.Warn("heartbeat failed", slog.String("worker_id", h.workerID), slog.Any("error", err))
			}
		}
	}
}

// beat refreshes taskflow:hb:{workerID} with the configured TTL. Recovery uses
// the absence of this key to decide a worker is dead and re-queue its jobs.
func (h *Heartbeat) beat(ctx context.Context) error {
	if h.rdb == nil {
		h.log.Debug("heartbeat (no redis client)", slog.String("worker_id", h.workerID))
		return nil
	}
	k := queue.HeartbeatKey(h.workerID)
	if err := h.rdb.Set(ctx, k, time.Now().UnixMilli(), h.ttl).Err(); err != nil {
		return fmt.Errorf("set %s: %w", k, err)
	}
	return nil
}
