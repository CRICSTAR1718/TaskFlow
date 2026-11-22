package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cricstar1718/taskflow/internal/queue"
)

// HeartbeatStore records liveness in Postgres as well as Redis. The Redis key is
// what recovery and the API use (expiry is the signal); the row is what a human
// or a report looks at, so it must not be written once and then frozen.
type Heartbeat struct {
	workerID string
	interval time.Duration
	ttl      time.Duration
	log      *slog.Logger
	rdb      *redis.Client
	store    BeatStore
}

// WithRedis attaches the Redis client used to publish the heartbeat. Without it
// the heartbeat does nothing but log, and every consumer of liveness (the API's
// alive flag, recovery's dead-worker scan) silently reports everyone as dead.
func (h *Heartbeat) WithRedis(c *redis.Client) *Heartbeat { h.rdb = c; return h }

// WithStore attaches the store so last_heartbeat is refreshed in Postgres.
func (h *Heartbeat) WithStore(bs BeatStore) *Heartbeat { h.store = bs; return h }

// BeatStore persists liveness for humans and reports.
type BeatStore interface {
	Beat(ctx context.Context, workerID string) error
}

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
	var firstErr error

	// Redis is the source of truth: the key expiring is what tells recovery the
	// worker is gone.
	if h.rdb != nil {
		k := queue.HeartbeatKey(h.workerID)
		if err := h.rdb.Set(ctx, k, time.Now().UnixMilli(), h.ttl).Err(); err != nil {
			firstErr = fmt.Errorf("set %s: %w", k, err)
		}
	} else {
		firstErr = errors.New("heartbeat has no redis client - liveness will look dead")
	}

	// Postgres is for humans. A failure here is not fatal: the Redis key still
	// expires, so recovery still works.
	if h.store != nil {
		if err := h.store.Beat(ctx, h.workerID); err != nil {
			h.log.Warn("db heartbeat", slog.String("worker_id", h.workerID), slog.Any("error", err))
		}
	}
	return firstErr
}
