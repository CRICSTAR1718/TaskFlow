package worker

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
	job        domain.Job
	getErr     error
	statusFrom []domain.Status
	statusTo   []domain.Status
	statusErr  error
	runs       []domain.JobRun
	finishes   []domain.Status
	attempt    int
}

func (f *fakeStore) GetJob(_ context.Context, _ uuid.UUID) (domain.Job, error) {
	if f.getErr != nil {
		return domain.Job{}, f.getErr
	}
	return f.job, nil
}

func (f *fakeStore) CreateRun(_ context.Context, r domain.JobRun) (domain.JobRun, error) {
	if r.Attempt == 0 {
		r.Attempt = 1
	}
	r.ID = uuid.New()
	f.runs = append(f.runs, r)
	return r, nil
}

func (f *fakeStore) FinishRun(_ context.Context, _ uuid.UUID, s domain.Status, _ string) error {
	f.finishes = append(f.finishes, s)
	return nil
}

func (f *fakeStore) IncrementAttempt(_ context.Context, _ uuid.UUID) (int, error) {
	f.attempt++
	return f.attempt, nil
}

func (f *fakeStore) UpdateStatus(_ context.Context, _ uuid.UUID, from, to domain.Status) error {
	if f.statusErr != nil {
		return f.statusErr
	}
	f.statusFrom = append(f.statusFrom, from)
	f.statusTo = append(f.statusTo, to)
	return nil
}

type fakeClaimer struct {
	ids    []string
	claims int
	acks   []string
	err    error
}

func (f *fakeClaimer) Claim(_ context.Context, _ string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	if f.claims >= len(f.ids) {
		return "", ErrNoWork
	}
	id := f.ids[f.claims]
	f.claims++
	return id, nil
}

func (f *fakeClaimer) Ack(_ context.Context, _, jobID string) error {
	f.acks = append(f.acks, jobID)
	return nil
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testPool() *Pool { return NewPool(2, NewExecutor(quiet()), quiet()) }

func newJob(payload string) domain.Job {
	return domain.Job{
		ID:             uuid.New(),
		Type:           "test.ok",
		Payload:        []byte(payload),
		Status:         domain.StatusQueued,
		TimeoutSeconds: 5,
		MaxRetries:     2,
	}
}

// ----------------------------------------------------------------- tests ---

func TestExecuteSuccessMarksSucceeded(t *testing.T) {
	j := newJob(`{}`)
	st := &fakeStore{job: j}
	q := &fakeClaimer{ids: []string{j.ID.String()}}

	r := NewRunner(RunnerConfig{Store: st, Queue: q, Pool: testPool(), Executor: NewExecutor(quiet()), WorkerID: "w1", Log: quiet()})
	r.execute(context.Background(), j.ID.String())

	if len(st.runs) != 1 {
		t.Fatalf("CreateRun called %d times, want 1", len(st.runs))
	}
	if len(st.finishes) != 1 || st.finishes[0] != domain.StatusSucceeded {
		t.Fatalf("FinishRun = %v, want [SUCCEEDED]", st.finishes)
	}
	// QUEUED->RUNNING then RUNNING->SUCCEEDED
	if len(st.statusTo) != 2 || st.statusTo[0] != domain.StatusRunning || st.statusTo[1] != domain.StatusSucceeded {
		t.Fatalf("status transitions = %v, want [RUNNING SUCCEEDED]", st.statusTo)
	}
	if len(q.acks) != 1 || q.acks[0] != j.ID.String() {
		t.Fatalf("acks = %v, want the job id", q.acks)
	}
}

func TestExecuteFailureSchedulesRetry(t *testing.T) {
	j := newJob(`{}`)
	j.Type = "test.fail"
	st := &fakeStore{job: j}
	q := &fakeClaimer{ids: []string{j.ID.String()}}

	r := NewRunner(RunnerConfig{Store: st, Queue: q, Pool: testPool(), Executor: NewExecutor(quiet()), WorkerID: "w1", Log: quiet()})
	r.execute(context.Background(), j.ID.String())

	if len(st.finishes) != 1 || st.finishes[0] != domain.StatusRetrying {
		t.Fatalf("FinishRun = %v, want [RETRYING]", st.finishes)
	}
	last := st.statusTo[len(st.statusTo)-1]
	if last != domain.StatusRetrying {
		t.Fatalf("final status = %s, want RETRYING", last)
	}
	if len(q.acks) != 1 {
		t.Fatalf("claim was not released: acks = %v", q.acks)
	}
}

func TestExecuteExhaustedRetriesMarksDead(t *testing.T) {
	j := newJob(`{}`)
	j.Type = "test.fail"
	j.MaxRetries = 0 // first attempt is already over budget
	st := &fakeStore{job: j}
	q := &fakeClaimer{ids: []string{j.ID.String()}}

	r := NewRunner(RunnerConfig{Store: st, Queue: q, Pool: testPool(), Executor: NewExecutor(quiet()), WorkerID: "w1", Log: quiet()})
	r.execute(context.Background(), j.ID.String())

	if len(st.finishes) != 1 || st.finishes[0] != domain.StatusRetrying {
		t.Fatalf("failed run should be finished as RETRYING, got %v", st.finishes)
	}
	last := st.statusTo[len(st.statusTo)-1]
	if last != domain.StatusDead {
		t.Fatalf("final status = %s, want DEAD (retries exhausted)", last)
	}
	if len(q.acks) != 1 {
		t.Fatal("claim was not released for a dead job")
	}
}

func TestExecuteUnknownJobReleasesClaim(t *testing.T) {
	j := newJob(`{}`)
	st := &fakeStore{getErr: errors.New("not found")}
	q := &fakeClaimer{ids: []string{j.ID.String()}}

	r := NewRunner(RunnerConfig{Store: st, Queue: q, Pool: testPool(), Executor: NewExecutor(quiet()), WorkerID: "w1", Log: quiet()})
	r.execute(context.Background(), j.ID.String())

	if len(q.acks) != 1 {
		t.Fatalf("claim leaked: acks = %v", q.acks)
	}
	if len(st.runs) != 0 {
		t.Fatal("a run row was created for a job that could not be loaded")
	}
}

func TestExecuteNotQueuedDropsClaim(t *testing.T) {
	j := newJob(`{}`)
	st := &fakeStore{job: j, statusErr: errors.New("conflict")}
	q := &fakeClaimer{ids: []string{j.ID.String()}}

	r := NewRunner(RunnerConfig{Store: st, Queue: q, Pool: testPool(), Executor: NewExecutor(quiet()), WorkerID: "w1", Log: quiet()})
	r.execute(context.Background(), j.ID.String())

	if len(st.runs) != 0 {
		t.Fatal("ran a job whose status transition failed")
	}
	if len(q.acks) != 1 {
		t.Fatal("claim leaked when the job was not QUEUED")
	}
}

func TestExecuteInvalidIDReleasesClaim(t *testing.T) {
	st := &fakeStore{}
	q := &fakeClaimer{}

	r := NewRunner(RunnerConfig{Store: st, Queue: q, Pool: testPool(), Executor: NewExecutor(quiet()), WorkerID: "w1", Log: quiet()})
	r.execute(context.Background(), "not-a-uuid")

	if len(q.acks) != 1 || q.acks[0] != "not-a-uuid" {
		t.Fatalf("acks = %v, want [not-a-uuid]", q.acks)
	}
}

func TestRunClaimsUntilCancelled(t *testing.T) {
	j1 := newJob(`{}`)
	j2 := newJob(`{}`)
	st := &fakeStore{job: j1}
	q := &fakeClaimer{ids: []string{j1.ID.String(), j2.ID.String()}}

	r := NewRunner(RunnerConfig{
		Store: st, Queue: q, Pool: NewPool(1, NewExecutor(quiet()), quiet()),
		Executor: NewExecutor(quiet()), WorkerID: "w1", Log: quiet(), IdlePoll: time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
	if q.claims < 1 {
		t.Fatalf("claim loop never claimed (claims=%d)", q.claims)
	}
}

func TestExponentialBackoff(t *testing.T) {
	f := exponentialBackoff(2*time.Second, 5*time.Minute)
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{20, 5 * time.Minute},
	}
	for _, c := range cases {
		if got := f(c.attempt, 0, 0); got != c.want {
			t.Fatalf("backoff(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("short", 10); got != "short" {
		t.Fatalf("got %q", got)
	}
	long := make([]byte, 50)
	for i := range long {
		long[i] = 'x'
	}
	if got := truncate(string(long), 10); got != "xxxxxxxxxx..." {
		t.Fatalf("got %q", got)
	}
}
