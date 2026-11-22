// Package events publishes and subscribes to job events over Redis pub/sub.
//
// One Redis channel carries every event, so a dashboard only needs one
// subscription and the publishers stay dumb: they never need to know who is
// listening.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/cricstar1718/taskflow/internal/domain"
)

// Channel is the Redis pub/sub channel every event is published on.
const Channel = "taskflow:events"

// Publisher emits events.
type Publisher interface {
	Publish(ctx context.Context, e domain.Event) error
}

// Subscriber receives events until ctx is cancelled.
type Subscriber interface {
	Subscribe(ctx context.Context) (<-chan domain.Event, error)
}

// Redis publishes and subscribes over Redis pub/sub.
type Redis struct {
	client *redis.Client
}

// NewRedis wraps a Redis client.
func NewRedis(c *redis.Client) *Redis { return &Redis{client: c} }

// Publish sends one event. It is a no-op when the event has no job id, because
// an event nobody can attribute to a job is noise in the dashboard.
func (r *Redis) Publish(ctx context.Context, e domain.Event) error {
	if e.JobID == uuid.Nil {
		return errors.New("events: event has no job id")
	}
	if e.At.IsZero() {
		e.At = time.Now()
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("publish: marshal: %w", err)
	}
	if err := r.client.Publish(ctx, Channel, payload).Err(); err != nil {
		return fmt.Errorf("publish %s: %w", e.Status, err)
	}
	return nil
}

// Subscribe returns a channel of events that stays open until ctx is cancelled.
//
// The returned channel is always closed on shutdown, so a consumer ranging over
// it terminates cleanly instead of blocking forever.
func (r *Redis) Subscribe(ctx context.Context) (<-chan domain.Event, error) {
	sub := r.client.Subscribe(ctx, Channel)
	// Confirm the subscription before returning, so a caller that publishes
	// immediately afterwards cannot miss its own event.
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, fmt.Errorf("subscribe %s: %w", Channel, err)
	}

	out := make(chan domain.Event, 256)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(out)
		defer func() { _ = sub.Close() }()

		msgs := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				var e domain.Event
				if err := json.Unmarshal([]byte(msg.Payload), &e); err != nil {
					// One malformed event must not end the subscription.
					continue
				}
				select {
				case out <- e:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Wait for the goroutine before the caller's ctx-aware code moves on, so the
	// channel is closed exactly once and by the time Subscribe's caller returns
	// from shutdown there is nothing still writing to it.
	context.AfterFunc(ctx, wg.Wait)
	return out, nil
}

// Publish runs a Publish in the background and logs nothing: callers that care
// about delivery use the Publisher directly. This exists for the worker, which
// must never fail a job because the dashboard could not be told about it.
func Publish(ctx context.Context, p Publisher, e domain.Event) {
	if p == nil {
		return
	}
	go func() {
		// Detach from the job's context: a cancelled job still finished.
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = p.Publish(pctx, e)
	}()
}

// Emitter publishes events in the order they were emitted.
//
// Publishing straight from a goroutine per event is wrong: the dashboard would
// occasionally show a job as SUCCEEDED and then flip it back to RUNNING,
// because two goroutines race. One FIFO queue fixes the ordering, and the queue
// is bounded so a stuck Redis can never grow memory without limit.
type Emitter struct {
	pub Publisher
	ch  chan domain.Event
	buf int

	once    sync.Once
	dropped atomic.Int64
}

// NewEmitter creates an Emitter. buf is the queue size; <=0 uses 256.
func NewEmitter(p Publisher, buf int) *Emitter {
	if buf <= 0 {
		buf = 256
	}
	return &Emitter{pub: p, buf: buf}
}

// Emit queues an event and returns immediately: a job must never wait for a
// dashboard, and must never fail because one could not be told.
func (e *Emitter) Emit(ctx context.Context, ev domain.Event) {
	if e == nil || e.pub == nil {
		return
	}
	e.once.Do(func() {
		e.ch = make(chan domain.Event, e.buf)
		go e.run()
	})
	select {
	case e.ch <- ev:
	default:
		e.dropped.Add(1)
	}
	_ = ctx
}

// Dropped reports how many events were discarded because the queue was full.
func (e *Emitter) Dropped() int64 {
	if e == nil {
		return 0
	}
	return e.dropped.Load()
}

func (e *Emitter) run() {
	for ev := range e.ch {
		pctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := e.pub.Publish(pctx, ev)
		cancel()
		if err != nil {
			e.dropped.Add(1)
		}
	}
}

// Close stops the emitter after draining what is queued.
func (e *Emitter) Close() {
	if e == nil || e.ch == nil {
		return
	}
	close(e.ch)
}
