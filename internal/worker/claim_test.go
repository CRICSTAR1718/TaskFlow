package worker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cricstar1718/taskflow/internal/queue"
)

// countingClaimer returns err from every Claim call.
type countingClaimer struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (c *countingClaimer) Claim(context.Context, string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return "", c.err
}
func (c *countingClaimer) Ack(context.Context, string, string) error { return nil }

// syncBuffer is a log sink that is safe to read while the runner writes.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// An idle worker - one whose queue is simply empty - must not log a warning.
// Before this test, every empty poll logged "claim failed ... queue: empty"
// once a second, which is the loudest thing in a three-worker demo and hides
// real errors.
func TestEmptyQueueIsNotAWarning(t *testing.T) {
	sink := &syncBuffer{}
	q := &countingClaimer{err: queue.ErrEmpty}
	r := &Runner{
		queue:    q,
		log:      slog.New(slog.NewJSONHandler(sink, nil)),
		idlePoll: time.Millisecond,
		workerID: "w-test",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	sem := make(chan struct{}, 1)
	// step returns false when nothing was dispatched, so poll explicitly rather
	// than looping on its return value.
	for ctx.Err() == nil {
		r.step(ctx, sem)
	}

	q.mu.Lock()
	calls := q.calls
	q.mu.Unlock()
	if calls < 3 {
		t.Fatalf("runner only polled %d times, expected several", calls)
	}
	if out := sink.String(); out != "" {
		t.Fatalf("an empty queue should log nothing, got:\n%s", out)
	}
}

// A real failure must still be visible - but not once per second forever.
func TestRepeatedClaimFailuresAreSummarised(t *testing.T) {
	sink := &syncBuffer{}
	q := &countingClaimer{err: errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")}
	r := &Runner{
		queue:    q,
		log:      slog.New(slog.NewJSONHandler(sink, nil)),
		workerID: "w-test",
	}

	// Each bad claim sleeps a second, so keep this to three calls.
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		r.step(ctx, make(chan struct{}, 1))
		if i == 0 && !strings.Contains(sink.String(), "claim failed") {
			t.Fatal("the first real failure must be logged")
		}
	}

	if r.claimFailures < 3 {
		t.Fatalf("consecutive failures = %d, want >= 3", r.claimFailures)
	}
	if n := strings.Count(sink.String(), "claim failed"); n > 3 {
		t.Fatalf("logged %d failures for 3 bad claims", n)
	}
}
