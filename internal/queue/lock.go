package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Lock is a best-effort distributed lock on a Redis key, used so that only one
// scheduler replica runs a tick at a time.
type Lock struct {
	client *redis.Client
	token  string
}

// NewLock wraps the same Redis instance the queue uses.
func NewLock(q *Redis) *Lock {
	return &Lock{client: q.client, token: fmt.Sprintf("%d", time.Now().UnixNano())}
}

// unlockScript deletes the key only if we still own it: a tick that outlives
// its TTL must not delete a lock another replica has since taken.
const unlockScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// TryLock attempts to take key for ttl. On success it returns a release func.
func (l *Lock) TryLock(ctx context.Context, key string, ttl time.Duration) (bool, func(), error) {
	ok, err := l.client.SetNX(ctx, key, l.token, ttl).Result()
	if err != nil {
		return false, func() {}, fmt.Errorf("lock %s: %w", key, err)
	}
	if !ok {
		return false, func() {}, nil
	}
	release := func() {
		_ = l.client.Eval(context.WithoutCancel(ctx), unlockScript, []string{key}, l.token).Err()
	}
	return true, release, nil
}

// ErrLockLost is returned when another replica holds the lock.
var ErrLockLost = errors.New("queue: lock lost")
