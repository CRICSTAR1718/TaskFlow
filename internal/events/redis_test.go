package events

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/cricstar1718/taskflow/internal/domain"
)

// redisClient returns a client or skips: these tests need a real Redis, the
// same way the store tests need a real Postgres.
func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		t.Skipf("redis unreachable: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestPublishSubscribeRoundTrip(t *testing.T) {
	c := redisClient(t)
	bus := NewRedis(c)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := bus.Subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	want := domain.Event{
		JobID:    uuid.New(),
		Status:   domain.StatusRunning,
		WorkerID: "w1",
		Attempt:  1,
		At:       time.Now().Truncate(time.Millisecond),
	}
	if err := bus.Publish(ctx, want); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-ch:
		if got.JobID != want.JobID {
			t.Fatalf("job id = %s, want %s", got.JobID, want.JobID)
		}
		if got.Status != want.Status || got.WorkerID != want.WorkerID || got.Attempt != want.Attempt {
			t.Fatalf("event = %+v, want %+v", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("event never arrived")
	}
}

func TestSubscribeClosesOnCancel(t *testing.T) {
	c := redisClient(t)
	bus := NewRedis(c)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := bus.Subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("received an event after cancellation")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("channel was not closed on cancel: consumers would block forever")
	}
}

func TestPublishRejectsEventWithoutJobID(t *testing.T) {
	c := redisClient(t)
	bus := NewRedis(c)
	if err := bus.Publish(context.Background(), domain.Event{Status: domain.StatusRunning}); err == nil {
		t.Fatal("an event with no job id was published")
	}
}

func TestPublishStampsTime(t *testing.T) {
	c := redisClient(t)
	bus := NewRedis(c)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := bus.Subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := bus.Publish(ctx, domain.Event{JobID: uuid.New(), Status: domain.StatusQueued}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case got := <-ch:
		if got.At.IsZero() {
			t.Fatal("published event has no timestamp")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("event never arrived")
	}
}
