package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/cricstar1718/taskflow/internal/domain"
	"github.com/cricstar1718/taskflow/internal/store"
	"github.com/cricstar1718/taskflow/internal/ws"
)

// controllableSub lets a test push events and simulate a dropped subscription.
type controllableSub struct {
	mu    sync.Mutex
	ch    chan domain.Event
	fail  bool
	opens int
}

func (c *controllableSub) Subscribe(ctx context.Context) (<-chan domain.Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return nil, errors.New("redis down")
	}
	c.opens++
	ch := make(chan domain.Event, 8)
	c.ch = ch
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (c *controllableSub) latest() chan domain.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ch
}

func ioutilLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestLiveForwardsEventsToHub(t *testing.T) {
	hub := ws.NewHub(ws.WithLogger(ioutilLogger()))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	sub := &controllableSub{}
	l := &live{hub: hub, sub: sub, log: ioutilLogger()}
	l.Start(ctx)

	// Wait for the subscription to be established.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && sub.latest() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if sub.latest() == nil {
		t.Fatal("live never subscribed")
	}

	client, unregister := hub.Register()
	defer unregister()

	sub.latest() <- domain.Event{JobID: uuid.New(), Status: domain.StatusSucceeded}

	select {
	case msg := <-client:
		if len(msg) == 0 {
			t.Fatal("empty message")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event was not forwarded to the client")
	}
}

func TestLiveResubscribesAfterDroppedSubscription(t *testing.T) {
	hub := ws.NewHub(ws.WithLogger(ioutilLogger()))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	sub := &controllableSub{}
	l := &live{hub: hub, sub: sub, log: ioutilLogger()}
	l.Start(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		sub.mu.Lock()
		n := sub.opens
		sub.mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if sub.latest() == nil {
		t.Fatal("live never subscribed")
	}

	// Kill the first subscription the way a Redis restart would.
	cancel()
	time.Sleep(50 * time.Millisecond)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go hub.Run(ctx2)
	l2 := &live{hub: hub, sub: sub, log: ioutilLogger()}
	l2.Start(ctx2)

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		sub.mu.Lock()
		n := sub.opens
		sub.mu.Unlock()
		if n >= 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("live did not resubscribe")
}

func TestStatsSnapshotOnlyWhenSomeoneIsWatching(t *testing.T) {
	hub := ws.NewHub(ws.WithLogger(ioutilLogger()))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	calls := 0
	src := statsFunc(func(context.Context) (any, error) {
		calls++
		return map[string]int{"x": 1}, nil
	})
	l := &live{hub: hub, stats: src, log: ioutilLogger()}
	l.Start(ctx)

	// No clients: the ticker must not hit the database.
	time.Sleep(2500 * time.Millisecond)
	if calls != 0 {
		t.Fatalf("stats fetched %d times with no clients - a wasted database round trip", calls)
	}

	client, unregister := hub.Register()
	defer unregister()
	select {
	case msg := <-client:
		if len(msg) == 0 {
			t.Fatal("empty stats frame")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("no stats snapshot arrived for a connected client")
	}
}

type statsFunc func(context.Context) (any, error)

func (f statsFunc) StatsView(ctx context.Context) (any, error) { return f(ctx) }

func TestStatsViewIncludesClientCount(t *testing.T) {
	hub := ws.NewHub(ws.WithLogger(ioutilLogger()))
	a := &API{
		store: newFakeStore(),
		queue: &fakeQueue{depths: map[domain.Priority]int64{}},
		hub:   hub,
		log:   ioutilLogger(),
	}
	view, err := a.StatsView(context.Background())
	if err != nil {
		t.Fatalf("StatsView: %v", err)
	}
	if _, ok := view.(map[string]any)["ws_clients"]; !ok {
		t.Fatal("stats view has no ws_clients field")
	}
}

var _ = store.Stats{}
