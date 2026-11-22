package events

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/cricstar1718/taskflow/internal/domain"
)

func TestMessageEnvelopeShape(t *testing.T) {
	// A malformed or reshaped event would break every dashboard silently, so the
	// JSON contract is pinned here.
	e := domain.Event{
		JobID:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Status:   domain.StatusRetrying,
		WorkerID: "w1",
		Attempt:  2,
		// Deliberately a non-UTC location: the wire format must not depend on
		// the machine's time zone.
		At:    time.Unix(0, 0).In(time.FixedZone("IST", 5*3600+30*60)),
		Error: "boom",
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"job_id":"11111111-1111-1111-1111-111111111111","status":"RETRYING","worker_id":"w1","attempt":2,"at":"1970-01-01T00:00:00Z","error":"boom"}`
	if string(raw) != want {
		t.Fatalf("event JSON drifted:\n got %s\nwant %s", raw, want)
	}
}

func TestEmitterPreservesOrder(t *testing.T) {
	// Ordering is the whole point: a dashboard that sees SUCCEEDED before
	// RUNNING would render a job going backwards.
	pub := &orderRecorder{}
	e := NewEmitter(pub, 64)
	defer e.Close()

	for i := 0; i < 50; i++ {
		status := domain.StatusRunning
		if i%2 == 0 {
			status = domain.StatusSucceeded
		}
		e.Emit(context.Background(), domain.Event{JobID: uuid.New(), Status: status, Attempt: i})
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pub.count() == 50 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := pub.attempts()
	if len(got) != 50 {
		t.Fatalf("published %d events, want 50", len(got))
	}
	for i, attempt := range got {
		if attempt != i {
			t.Fatalf("event %d arrived as attempt %d - events were reordered", i, attempt)
		}
	}
}

func TestEmitterSurvivesFailingPublisher(t *testing.T) {
	// A broken Redis must not panic or block; events are simply dropped.
	e := NewEmitter(failingPub{}, 4)
	defer e.Close()
	for i := 0; i < 10; i++ {
		e.Emit(context.Background(), domain.Event{JobID: uuid.New(), Status: domain.StatusQueued})
	}
	time.Sleep(100 * time.Millisecond)
	if e.Dropped() == 0 {
		t.Fatal("nothing was counted as dropped")
	}
}

func TestEmitterNilIsSafe(t *testing.T) {
	var e *Emitter
	e.Emit(context.Background(), domain.Event{}) // must not panic
	if e.Dropped() != 0 {
		t.Fatal("nil emitter reported drops")
	}
	e.Close()
}

type orderRecorder struct {
	mu   sync.Mutex
	seen []int
}

func (o *orderRecorder) Publish(_ context.Context, e domain.Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seen = append(o.seen, e.Attempt)
	return nil
}

func (o *orderRecorder) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.seen)
}

func (o *orderRecorder) attempts() []int {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]int, len(o.seen))
	copy(out, o.seen)
	return out
}

type failingPub struct{}

func (failingPub) Publish(context.Context, domain.Event) error { return errors.New("redis down") }
