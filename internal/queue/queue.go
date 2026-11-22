// Package queue implements the priority job queue on Redis sorted sets.
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cricstar1718/taskflow/internal/domain"
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
	// Requeue puts a job back, e.g. when a worker dies mid-run.
	Requeue(ctx context.Context, jobID string, p domain.Priority) error
	// InFlight lists the job IDs a worker claims to be running.
	InFlight(ctx context.Context, workerID string) ([]string, error)
	// Depths reports how many jobs are waiting per priority.
	Depths(ctx context.Context) (map[domain.Priority]int64, error)
}

// ErrEmpty is returned by Claim when no job is waiting.
var ErrEmpty = errors.New("queue: empty")

// claimScript pops the highest-priority non-empty queue atomically and records
// the job as in-flight for the worker. Score is the enqueue time in unix ms, so
// ZPOPMIN yields FIFO within a priority.
//
// KEYS[1..3] = queue keys for high, normal, low
// KEYS[4]    = inflight key for this worker
// ARGV[1]    = score used when re-scoring (kept for clarity)
var claimScript = redis.NewScript(`
for i = 1, 3 do
  local id = redis.call('ZPOPMIN', KEYS[i])
  if id and id[1] then
    redis.call('SADD', KEYS[4], id[1])
    return id[1]
  end
end
return nil
`)

// Redis is a Queue backed by Redis.
type Redis struct {
	client *redis.Client
}

// NewRedis connects lazily; Ping is not performed here so a temporarily
// unavailable Redis does not prevent the scheduler from starting.
func NewRedis(addr string) *Redis {
	return &Redis{client: redis.NewClient(&redis.Options{Addr: addr})}
}

// NewRedisFromClient lets tests inject a client (e.g. a miniredis-backed one).
func NewRedisFromClient(c *redis.Client) *Redis { return &Redis{client: c} }

var _ Queue = (*Redis)(nil)

// Close releases the underlying connection pool.
func (r *Redis) Close() error { return r.client.Close() }

// Ping verifies Redis is reachable.
func (r *Redis) Ping(ctx context.Context) error {
	if err := r.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("queue ping: %w", err)
	}
	return nil
}

// Enqueue adds a job to its priority queue. Score is now in unix ms.
func (r *Redis) Enqueue(ctx context.Context, jobID string, p domain.Priority) error {
	score := float64(time.Now().UnixMilli())
	if err := r.client.ZAdd(ctx, Key(p), redis.Z{Score: score, Member: jobID}).Err(); err != nil {
		return fmt.Errorf("enqueue %s: %w", jobID, err)
	}
	return nil
}

// Claim atomically pops the next job and marks it in-flight.
func (r *Redis) Claim(ctx context.Context, workerID string) (string, error) {
	keys := []string{
		Key(domain.PriorityHigh),
		Key(domain.PriorityNormal),
		Key(domain.PriorityLow),
		InflightKey(workerID),
	}
	res, err := claimScript.Run(ctx, r.client, keys).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrEmpty
	}
	if err != nil {
		return "", fmt.Errorf("claim: %w", err)
	}
	id, ok := res.(string)
	if !ok || id == "" {
		return "", ErrEmpty
	}
	return id, nil
}

// Ack clears the in-flight marker after a job is finished.
func (r *Redis) Ack(ctx context.Context, workerID, jobID string) error {
	if err := r.client.SRem(ctx, InflightKey(workerID), jobID).Err(); err != nil {
		return fmt.Errorf("ack %s: %w", jobID, err)
	}
	return nil
}

// Requeue pushes a job back onto its priority queue.
func (r *Redis) Requeue(ctx context.Context, jobID string, p domain.Priority) error {
	if err := r.Enqueue(ctx, jobID, p); err != nil {
		return fmt.Errorf("requeue %s: %w", jobID, err)
	}
	return nil
}

// InFlight returns the in-flight job IDs for a worker.
func (r *Redis) InFlight(ctx context.Context, workerID string) ([]string, error) {
	ids, err := r.client.SMembers(ctx, InflightKey(workerID)).Result()
	if err != nil {
		return nil, fmt.Errorf("inflight %s: %w", workerID, err)
	}
	return ids, nil
}

// Depths returns the number of waiting jobs per priority.
func (r *Redis) Depths(ctx context.Context) (map[domain.Priority]int64, error) {
	out := make(map[domain.Priority]int64, 3)
	for _, p := range []domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow} {
		n, err := r.client.ZCard(ctx, Key(p)).Result()
		if err != nil {
			return nil, fmt.Errorf("depth %s: %w", p.String(), err)
		}
		out[p] = n
	}
	return out, nil
}

// Client returns the underlying client, for callers that need a raw command
// outside this package's API (the worker heartbeat writes a key with a TTL).
func (r *Redis) Client() *redis.Client { return r.client }

// Heartbeats reports, for each worker ID, whether its heartbeat key still
// exists. This is the only correct source of truth for liveness: the
// workers.last_heartbeat column is only written when a worker registers, while
// the key is refreshed every heartbeat and expiring it is exactly what tells
// recovery a worker is gone.
func (r *Redis) Heartbeats(ctx context.Context, workerIDs []string) (map[string]bool, error) {
	out := make(map[string]bool, len(workerIDs))
	if len(workerIDs) == 0 {
		return out, nil
	}
	pipe := r.client.Pipeline()
	cmds := make(map[string]*redis.IntCmd, len(workerIDs))
	for _, id := range workerIDs {
		cmds[id] = pipe.Exists(ctx, HeartbeatKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("heartbeats: %w", err)
	}
	for id, cmd := range cmds {
		out[id] = cmd.Val() == 1
	}
	return out, nil
}
