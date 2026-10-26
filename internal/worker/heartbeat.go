package worker

import (
	"context"
	"log/slog"
	"time"
)

// Heartbeat periodically refreshes taskflow:hb:{workerID} with a TTL so
// recovery can detect dead workers.
type Heartbeat struct {
	workerID string
	interval time.Duration
	ttl      time.Duration
	log      *slog.Logger
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
			// TODO: SET taskflow:hb:{workerID} EX ttl
			h.log.Debug("heartbeat", slog.String("worker_id", h.workerID))
		}
	}
}
