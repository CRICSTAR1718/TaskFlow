package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/cricstar1718/taskflow/internal/domain"
)

// ---------------------------------------------------------------- fakes ----

type fakeStore struct {
	due       []domain.Job
	fetchErr  error
	queued    []uuid.UUID
	nextRuns  map[uuid.UUID]*time.Time
	dead      []uuid.UUID
	markErr   error
	statusErr error
}

func (f *fakeStore) FetchDueJobs(_ context.Context, _ time.Time, _ int) ([]domain.Job, error) {
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	return f.due, nil
}

func (f *fakeStore) MarkQueued(_ context.Context, ids []uuid.UUID) error {
	if f.markErr != nil {
		return f.markErr
	}
	f.queued = append(f.queued, ids...)
	return nil
}

func (f *fakeStore) SetNextRun(_ context.Context, id uuid.UUID, t *time.Time) error {
	if f.nextRuns == nil {
		f.nextRuns = map[uuid.UUID]*time.Time{}
	}
	f.nextRuns[id] = t
	return nil
}

func (f *fakeStore) UpdateStatus(_ context.Context, id uuid.UUID, from, to domain.Status) error {
	if f.statusErr != nil {
		return f.statusErr
	}
	if to == domain.StatusDead {
		f.dead = append(f.dead, id)
	}
	return nil
}

type fakeQueue struct {
	enqueued map[string]domain.Priority
	err      error
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{enqueued: map[string]domain.Priority{}}
}

func (f *fakeQueue) Enqueue(_ context.Context, jobID string, p domain.Priority) error {
	if f.err != nil {
		return f.err
	}
	f.enqueued[jobID] = p
	return nil
}

type fakeLocker struct {
	acquired int
	busy     bool
}

func (f *fakeLocker) TryLock(_ context.Context, _ string, _ time.Duration) (bool, func(), error) {
	if f.busy {
		return false, func() {}, nil
	}
	f.acquired++
	return true, func() {}, nil
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newJob(t *testing.T, p domain.Priority, cronExpr *string) domain.Job {
	t.Helper()
	return domain.Job{
		ID:       uuid.New(),
		Type:     "test.type",
		Priority: p,
		Status:   domain.StatusPending,
		CronExpr: cronExpr,
	}
}

// ----------------------------------------------------------------- tests ---

func TestRunOnceEnqueuesDueJobsByPriority(t *testing.T) {
	high := newJob(t, domain.PriorityHigh, nil)
	low := newJob(t, domain.PriorityLow, nil)
	st := &fakeStore{due: []domain.Job{high, low}}
	q := newFakeQueue()

	s := New(st, q, nil, quietLogger())
	n, err := s.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 2 {
		t.Fatalf("count = %d, want 2", n)
	}
	if got := q.enqueued[high.ID.String()]; got != domain.PriorityHigh {
		t.Fatalf("high job priority = %v", got)
	}
	if got := q.enqueued[low.ID.String()]; got != domain.PriorityLow {
		t.Fatalf("low job priority = %v", got)
	}
	if len(st.queued) != 2 {
		t.Fatalf("MarkQueued got %d ids, want 2", len(st.queued))
	}
}

func TestRunOnceNoJobsIsNoop(t *testing.T) {
	st := &fakeStore{}
	q := newFakeQueue()
	s := New(st, q, nil, quietLogger())

	n, err := s.RunOnce(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("RunOnce = %d, %v; want 0, nil", n, err)
	}
	if len(st.queued) != 0 {
		t.Fatal("MarkQueued should not be called for an empty batch")
	}
}

func TestRunOnceEnqueueFailureKeepsJobPending(t *testing.T) {
	j := newJob(t, domain.PriorityNormal, nil)
	st := &fakeStore{due: []domain.Job{j}}
	q := newFakeQueue()
	q.err = errors.New("redis down")

	s := New(st, q, nil, quietLogger())
	n, err := s.RunOnce(context.Background())
	if err == nil {
		t.Fatal("expected an error when Redis is down")
	}
	if n != 0 {
		t.Fatalf("count = %d, want 0", n)
	}
	// Critical: the job must NOT be marked QUEUED, so the next tick retries it.
	if len(st.queued) != 0 {
		t.Fatalf("job was marked QUEUED despite enqueue failing: %v", st.queued)
	}
}

func TestRunOnceCronJobIsRescheduled(t *testing.T) {
	spec := "*/5 * * * *"
	j := newJob(t, domain.PriorityNormal, &spec)
	st := &fakeStore{due: []domain.Job{j}}
	q := newFakeQueue()

	s := New(st, q, nil, quietLogger())
	base := time.Date(2026, 11, 21, 10, 2, 0, 0, time.UTC)
	s.now = func() time.Time { return base }

	n, err := s.RunOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("RunOnce = %d, %v; want 1, nil", n, err)
	}
	if _, ok := q.enqueued[j.ID.String()]; !ok {
		t.Fatal("cron job was not enqueued")
	}
	next, ok := st.nextRuns[j.ID]
	if !ok || next == nil {
		t.Fatal("cron job next_run_at was not set")
	}
	want := time.Date(2026, 11, 21, 10, 5, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next_run_at = %v, want %v", next, want)
	}
}

func TestRunOnceBadCronMarksDead(t *testing.T) {
	spec := "not a cron"
	j := newJob(t, domain.PriorityNormal, &spec)
	st := &fakeStore{due: []domain.Job{j}}
	q := newFakeQueue()

	s := New(st, q, nil, quietLogger())
	if _, err := s.RunOnce(context.Background()); err == nil {
		t.Fatal("expected an error for an invalid cron expression")
	}
	if len(st.dead) != 1 || st.dead[0] != j.ID {
		t.Fatalf("dead = %v, want [%s]", st.dead, j.ID)
	}
	if _, ok := st.nextRuns[j.ID]; ok {
		t.Fatal("next_run_at should not be set for an invalid cron expression")
	}
}

func TestRunOnceFetchErrorPropagates(t *testing.T) {
	st := &fakeStore{fetchErr: errors.New("db down")}
	s := New(st, newFakeQueue(), nil, quietLogger())
	if _, err := s.RunOnce(context.Background()); err == nil {
		t.Fatal("expected fetch error to propagate")
	}
}

func TestTickSkipsWhenLockHeld(t *testing.T) {
	j := newJob(t, domain.PriorityNormal, nil)
	st := &fakeStore{due: []domain.Job{j}}
	q := newFakeQueue()
	l := &fakeLocker{busy: true}

	s := New(st, q, l, quietLogger())
	s.tick(context.Background())
	if len(q.enqueued) != 0 {
		t.Fatal("tick ran while another scheduler held the lock")
	}
	if len(st.queued) != 0 {
		t.Fatal("jobs marked QUEUED while lock was held elsewhere")
	}
}

func TestTickRunsWhenLockAcquired(t *testing.T) {
	j := newJob(t, domain.PriorityNormal, nil)
	st := &fakeStore{due: []domain.Job{j}}
	q := newFakeQueue()
	l := &fakeLocker{}

	s := New(st, q, l, quietLogger())
	s.tick(context.Background())
	if l.acquired != 1 {
		t.Fatalf("lock acquired %d times, want 1", l.acquired)
	}
	if len(q.enqueued) != 1 {
		t.Fatalf("enqueued %d jobs, want 1", len(q.enqueued))
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	st := &fakeStore{}
	s := New(st, newFakeQueue(), nil, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, 10*time.Millisecond) }()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}
