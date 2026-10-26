// Package queue implements the priority job queue on Redis sorted sets.
package queue

import (
	"context"
	"fmt"

	"github.com/your-org/taskflow/internal/domain"
)

// Redis key helpers.
const (
	EventsChannel = "taskflow:events"
	SchedulerLock = "taskflow:lock:scheduler"
)

// Key returns the ZSET key for a priority.
func Key(p domain.Priority) string { return "taskflow:queue:" + p.String() }

// InflightKey returns the SET of job IDs a worker is running.
func InflightKey(workerID string) string { return "taskflow:inflight:" + workerID }

// HeartbeatKey returns the TTL key that proves a worker is alive.
func HeartbeatKey(workerID string) string { return "taskflow:hb:" + workerID }

// Queue is implemented by the Redis-backed queue.
type Queue interface {
	Enqueue(ctx context.Context, jobID string, p domain.Priority) error
	// Claim pops the next job (high before normal before low) and records it
	// as in-flight for workerID. It returns "" when all queues are empty.
	Claim(ctx context.Context, workerID string) (jobID string, err error)
	Ack(ctx context.Context, workerID, jobID string) error
}

// Redis is a Queue backed by Redis. The go-redis client is wired in later.
type Redis struct {
	url string
}

// NewRedis creates a Redis queue.
func NewRedis(url string) *Redis { return &Redis{url: url} }

var _ Queue = (*Redis)(nil)

func (r *Redis) Enqueue(ctx context.Context, jobID string, p domain.Priority) error {
	return fmt.Errorf("enqueue %s: not implemented", jobID)
}

func (r *Redis) Claim(ctx context.Context, workerID string) (string, error) {
	return "", nil
}

func (r *Redis) Ack(ctx context.Context, workerID, jobID string) error {
	return fmt.Errorf("ack %s: not implemented", jobID)
}
