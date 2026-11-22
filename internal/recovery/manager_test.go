package recovery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/cricstar1718/taskflow/internal/domain"
	"github.com/cricstar1718/taskflow/internal/store"
)

// ------------------------------------------------------------------ fakes ---

type fakeStore struct {
	due      []store.RecoverableJob
	stale    []store.RecoverableJob
	requeued []requeued
}

type requeued struct {
	id       uuid.UUID
	next     time.Time
	terminal bool
}

func (f *fakeStore) ClaimDue(_ context.Context, _ time.Time, _ int) ([]store.RecoverableJob, error) {
	return f.due, nil
}

func (f *fakeStore) FindStaleRunning(_ context.Context, _ time.Duration, _ int) ([]store.RecoverableJob, error) {
	return f.stale, nil
}

func (f *fakeStore) Requeue(_ context.Context, id uuid.UUID, next time.Time, _ string, terminal bool) error {
	f.requeued = append(f.requeued, requeued{id: id, next: next, terminal: terminal})
	return nil
}

type fakeQueue struct {
	enqueued []string
	fail     bool
	inflight map[string][]string
}

func (f *fakeQueue) Enqueue(_ context.Context, id string, _ domain.Priority) error {
	if f.fail {
		return errors.New("redis down")
	}
	f.enqueued = append(f.enqueued, id)
	return nil
}

func (f *fakeQueue) Requeue(_ context.Context, id string, _ domain.Priority) error {
	f.enqueued = append(f.enqueued, id)
	return nil
}

func (f *fakeQueue) InFlight(_ context.Context, workerID string) ([]string, error) {
	return f.inflight[workerID], nil
}

func (f *fakeQueue) Depths(context.Context) (map[domain.Priority]int64, error) {
	if f.fail {
		return nil, errors.New("redis down")
	}
	return map[domain.Priority]int64{}, nil
}

type fakeWorkers struct{ workers []domain.WorkerInfo }

func (f *fakeWorkers) ListWorkers(context.Context) ([]domain.WorkerInfo, error) {
	return f.workers, nil
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newManager(st Store, q Queue, w Workers) *Manager {
	return New(Config{Store: st, Queue: q, Workers: w, Log: quiet(),
		StaleAfter: time.Minute, Batch: 10,
		BackoffBase: time.Second, BackoffMax: time.Minute})
}

func job(attempt, maxRetries int) store.RecoverableJob {
	return store.RecoverableJob{
		ID: uuid.New(), Priority: domain.PriorityNormal,
		Attempt: attempt, MaxRetries: maxRetries,
	}
}

// ------------------------------------------------------------------ tests ---

func TestSweepRetriesJobWithBackoff(t *testing.T) {
	j := job(2, 5)
	st := &fakeStore{due: []store.RecoverableJob{j}}
	q := &fakeQueue{}
	m := newManager(st, q, &fakeWorkers{})
	m.Now = func() time.Time { return time.Unix(1_000_000, 0) }

	res, err := m.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if res.Retried != 1 || res.Dead != 0 {
		t.Fatalf("result = %+v, want 1 retried, 0 dead", res)
	}
	if len(q.enqueued) != 1 || q.enqueued[0] != j.ID.String() {
		t.Fatalf("enqueued = %v, want [%s]", q.enqueued, j.ID)
	}
	if len(st.requeued) != 1 || st.requeued[0].terminal {
		t.Fatalf("requeued = %+v, want one non-terminal requeue", st.requeued)
	}
	// attempt 2 => 2*base, plus up to 10% jitter
	now := time.Unix(1_000_000, 0)
	got := st.requeued[0].next
	lo, hi := now.Add(2*time.Second), now.Add(2*time.Second+200*time.Millisecond)
	if got.Before(lo) || got.After(hi) {
		t.Fatalf("next_run_at = %v, want between %v and %v", got, lo, hi)
	}
}

func TestSweepDeadLettersExhaustedJob(t *testing.T) {
	j := job(3, 3) // attempt == max_retries
	st := &fakeStore{due: []store.RecoverableJob{j}}
	q := &fakeQueue{}
	m := newManager(st, q, &fakeWorkers{})

	res, err := m.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if res.Dead != 1 || res.Retried != 0 {
		t.Fatalf("result = %+v, want 1 dead, 0 retried", res)
	}
	if len(q.enqueued) != 0 {
		t.Fatalf("a dead job must never be re-queued, enqueued = %v", q.enqueued)
	}
	if len(st.requeued) != 1 || !st.requeued[0].terminal {
		t.Fatalf("requeued = %+v, want one terminal requeue", st.requeued)
	}
}

func TestSweepDoesNotEnqueueWhenRedisIsDown(t *testing.T) {
	j := job(1, 3)
	st := &fakeStore{due: []store.RecoverableJob{j}}
	q := &fakeQueue{fail: true}
	m := newManager(st, q, &fakeWorkers{})

	res, err := m.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep returned a hard error for one flaky job: %v", err)
	}

	if res.Retried != 0 {
		t.Fatalf("counted a retry that never happened: %+v", res)
	}
	if len(st.requeued) != 0 {
		t.Fatalf("job row was changed even though the enqueue failed: %+v", st.requeued)
	}
}

func TestSweepReclaimsJobFromDeadWorker(t *testing.T) {
	j := job(1, 3)
	j.WorkerID = "worker-dead"
	st := &fakeStore{stale: []store.RecoverableJob{j}}
	q := &fakeQueue{}
	m := newManager(st, q, &fakeWorkers{})

	res, err := m.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if res.Reclaimed != 1 {
		t.Fatalf("result = %+v, want 1 reclaimed", res)
	}
	if len(q.enqueued) != 1 || q.enqueued[0] != j.ID.String() {
		t.Fatalf("enqueued = %v, want the reclaimed job", q.enqueued)
	}
	if len(st.requeued) != 1 {
		t.Fatalf("job was not put back: %+v", st.requeued)
	}
}

func TestSweepDrainsInFlightOfDeadWorkers(t *testing.T) {
	live, dead := "worker-live", "worker-dead"
	q := &fakeQueue{inflight: map[string][]string{
		dead: {"job-a", "job-b"},
		live: {"job-c"},
	}}
	st := &fakeStore{}
	m := newManager(st, q, &fakeWorkers{workers: []domain.WorkerInfo{
		{ID: live, LastHeartbeat: time.Now()},
		{ID: dead, LastHeartbeat: time.Now().Add(-10 * time.Minute)},
	}})

	if _, err := m.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if len(q.enqueued) != 2 {
		t.Fatalf("enqueued = %v, want the two jobs of the dead worker only", q.enqueued)
	}
	for _, id := range q.enqueued {
		if id == "job-c" {
			t.Fatal("re-queued a job belonging to a live worker")
		}
	}
}

func TestSweepQuietWhenNothingToDo(t *testing.T) {
	m := newManager(&fakeStore{}, &fakeQueue{}, &fakeWorkers{})
	res, err := m.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res != (SweepResult{}) {
		t.Fatalf("result = %+v, want zero", res)
	}
}

func TestLastErrTruncates(t *testing.T) {
	long := make([]byte, truncatedErrorLimit+50)
	for i := range long {
		long[i] = 'e'
	}
	s := string(long)
	j := store.RecoverableJob{LastError: &s}
	got := lastErr(j)
	if len(got) != truncatedErrorLimit+3 {
		t.Fatalf("len = %d, want %d", len(got), truncatedErrorLimit+3)
	}

	if got := lastErr(store.RecoverableJob{}); got != "attempt failed" {
		t.Fatalf("nil last_error = %q", got)
	}
}

func TestCheckReportsStoreFailure(t *testing.T) {
	m := newManager(&failingStore{}, &fakeQueue{}, &fakeWorkers{})
	if err := m.Check(context.Background()); err == nil {
		t.Fatal("Check passed on an unusable store")
	}
}

func TestCheckReportsQueueFailure(t *testing.T) {
	m := newManager(&fakeStore{}, &fakeQueue{fail: true}, &fakeWorkers{})
	err := m.Check(context.Background())
	if err == nil {
		t.Fatal("Check passed with Redis down")
	}
	if !strings.Contains(err.Error(), "queue not ready") {
		t.Fatalf("error = %v, want a queue-not-ready message", err)
	}
}

func TestCheckPassesWhenHealthy(t *testing.T) {
	m := newManager(&fakeStore{}, &fakeQueue{}, &fakeWorkers{})
	if err := m.Check(context.Background()); err != nil {
		t.Fatalf("Check on a healthy system: %v", err)
	}
}

func TestSweepFailedLogsOnce(t *testing.T) {
	var buf bytes.Buffer
	m := New(Config{Store: &failingStore{}, Queue: &fakeQueue{}, Log: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))})

	failing := false
	m.sweepFailed(errors.New("boom"), &failing)
	m.sweepFailed(errors.New("boom"), &failing)

	if n := strings.Count(buf.String(), "level=ERROR"); n != 1 {
		t.Fatalf("logged %d errors, want exactly 1 (rest should be debug)", n)
	}
}

// failingStore simulates the store being unreachable.
type failingStore struct{}

func (failingStore) ClaimDue(context.Context, time.Time, int) ([]store.RecoverableJob, error) {
	return nil, errors.New("connection refused")
}

func (failingStore) FindStaleRunning(context.Context, time.Duration, int) ([]store.RecoverableJob, error) {
	return nil, errors.New("connection refused")
}

func (failingStore) Requeue(context.Context, uuid.UUID, time.Time, string, bool) error {
	return errors.New("connection refused")
}
