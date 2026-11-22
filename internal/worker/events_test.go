package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/cricstar1718/taskflow/internal/domain"
)

// recordingPublisher captures the events a job emits.
type recordingPublisher struct {
	mu     sync.Mutex
	events []domain.Event
}

func (p *recordingPublisher) Publish(_ context.Context, e domain.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
	return nil
}

func (p *recordingPublisher) statuses() []domain.Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]domain.Status, len(p.events))
	for i, e := range p.events {
		out[i] = e.Status
	}
	return out
}

// waitForStatuses waits for the async publishes to land.
func (p *recordingPublisher) waitForStatuses(t *testing.T, want int) []domain.Status {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := p.statuses(); len(got) >= want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	return p.statuses()
}

func TestSuccessEmitsRunningThenSucceeded(t *testing.T) {
	j := newJob(`{}`)
	st := &fakeStore{job: j}
	q := &fakeClaimer{ids: []string{j.ID.String()}}
	pub := &recordingPublisher{}

	r := NewRunner(RunnerConfig{
		Store: st, Queue: q, Pool: testPool(), Executor: NewExecutor(quiet()),
		WorkerID: "w1", Log: quiet(), Events: pub,
	})
	r.execute(context.Background(), j.ID.String())

	got := pub.waitForStatuses(t, 2)
	if len(got) != 2 || got[0] != domain.StatusRunning || got[1] != domain.StatusSucceeded {
		t.Fatalf("events = %v, want [RUNNING SUCCEEDED]", got)
	}
}

func TestFailureEmitsRetryingAndDead(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxRetries int
		want       domain.Status
	}{
		{"retrying", 3, domain.StatusRetrying},
		{"dead", 0, domain.StatusDead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := newJob(`{}`)
			j.Type = "test.fail"
			j.MaxRetries = tc.maxRetries
			st := &fakeStore{job: j}
			q := &fakeClaimer{ids: []string{j.ID.String()}}
			pub := &recordingPublisher{}

			r := NewRunner(RunnerConfig{
				Store: st, Queue: q, Pool: testPool(), Executor: NewExecutor(quiet()),
				WorkerID: "w1", Log: quiet(), Events: pub,
			})
			r.execute(context.Background(), j.ID.String())

			got := pub.waitForStatuses(t, 2)
			if len(got) != 2 {
				t.Fatalf("events = %v, want 2", got)
			}
			if got[0] != domain.StatusRunning {
				t.Fatalf("first event = %s, want RUNNING", got[0])
			}
			if got[1] != tc.want {
				t.Fatalf("final event = %s, want %s", got[1], tc.want)
			}

			// The event must identify the job and the worker, or the dashboard
			// cannot attribute it.
			pub.mu.Lock()
			defer pub.mu.Unlock()
			if pub.events[0].JobID != j.ID || pub.events[0].WorkerID != "w1" {
				t.Fatalf("event = %+v", pub.events[0])
			}
		})
	}
}

func TestNilPublisherIsSafe(t *testing.T) {
	j := newJob(`{}`)
	st := &fakeStore{job: j}
	q := &fakeClaimer{ids: []string{j.ID.String()}}

	r := NewRunner(RunnerConfig{Store: st, Queue: q, Pool: testPool(),
		Executor: NewExecutor(quiet()), WorkerID: "w1", Log: quiet()})

	// Must not panic with no publisher configured.
	r.execute(context.Background(), j.ID.String())
	if len(st.finishes) != 1 {
		t.Fatalf("job did not run: finishes = %v", st.finishes)
	}
}

// failingPublisher proves a broken dashboard never fails a job.
type failingPublisher struct{}

func (failingPublisher) Publish(context.Context, domain.Event) error {
	return context.DeadlineExceeded
}

func TestPublishFailureDoesNotFailTheJob(t *testing.T) {
	j := newJob(`{}`)
	st := &fakeStore{job: j}
	q := &fakeClaimer{ids: []string{j.ID.String()}}

	r := NewRunner(RunnerConfig{Store: st, Queue: q, Pool: testPool(),
		Executor: NewExecutor(quiet()), WorkerID: "w1", Log: quiet(), Events: failingPublisher{}})
	r.execute(context.Background(), j.ID.String())

	if len(st.finishes) != 1 || st.finishes[0] != domain.StatusSucceeded {
		t.Fatalf("a failing event publisher changed the job outcome: %v", st.finishes)
	}
	if _, ok := st.statusTo, true; !ok {
		_ = uuid.Nil
	}
}
