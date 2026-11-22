package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/cricstar1718/taskflow/internal/domain"
)

func TestRunsReturnsAttemptsNewestFirst(t *testing.T) {
	st := recoveryStore(t)
	truncateAll(t, st)
	ctx := context.Background()

	j, err := st.CreateJob(ctx, domain.Job{Type: "noop", RunAt: time.Now()})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	first, err := st.CreateRun(ctx, domain.JobRun{JobID: j.ID, WorkerID: "w1", Attempt: 1,
		StartedAt: time.Now().Add(-2 * time.Minute)})
	if err != nil {
		t.Fatalf("create run 1: %v", err)
	}
	if err := st.FinishRun(ctx, first.ID, domain.StatusRetrying, "boom"); err != nil {
		t.Fatalf("finish run 1: %v", err)
	}
	if _, err := st.CreateRun(ctx, domain.JobRun{JobID: j.ID, WorkerID: "w2", Attempt: 2,
		StartedAt: time.Now()}); err != nil {
		t.Fatalf("create run 2: %v", err)
	}

	runs, err := st.Runs(ctx, j.ID)
	if err != nil {
		t.Fatalf("Runs: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("got %d runs, want 2", len(runs))
	}
	if runs[0].Attempt != 2 {
		t.Fatalf("first run is attempt %d, want the newest (2)", runs[0].Attempt)
	}
	if runs[1].Error == nil || *runs[1].Error != "boom" {
		t.Fatalf("second run error = %v, want boom", runs[1].Error)
	}

	other, _ := st.CreateJob(ctx, domain.Job{Type: "noop", RunAt: time.Now()})
	none, err := st.Runs(ctx, other.ID)
	if err != nil {
		t.Fatalf("Runs(empty): %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("got %d runs for a job that never ran", len(none))
	}
}

func TestRetryJobResetsAttempt(t *testing.T) {
	st := recoveryStore(t)
	truncateAll(t, st)
	ctx := context.Background()

	j, err := st.CreateJob(ctx, domain.Job{Type: "noop", RunAt: time.Now()})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	// Exhaust it: RUNNING -> DEAD with attempt = max_retries.
	for i := 0; i < 3; i++ {
		if _, err := st.IncrementAttempt(ctx, j.ID); err != nil {
			t.Fatalf("increment: %v", err)
		}
	}
	if err := st.UpdateStatus(ctx, j.ID, domain.StatusPending, domain.StatusRunning); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := st.UpdateStatus(ctx, j.ID, domain.StatusRunning, domain.StatusDead); err != nil {
		t.Fatalf("mark dead: %v", err)
	}

	if err := st.RetryJob(ctx, j.ID); err != nil {
		t.Fatalf("RetryJob: %v", err)
	}
	got, _ := st.GetJob(ctx, j.ID)
	if got.Status != domain.StatusPending {
		t.Fatalf("status = %s, want PENDING", got.Status)
	}
	if got.Attempt != 0 {
		t.Fatalf("attempt = %d, want 0", got.Attempt)
	}
	if got.NextRunAt == nil {
		t.Fatal("next_run_at was not set, so the scheduler would never pick it up")
	}

	// Only DEAD jobs may be retried.
	if err := st.RetryJob(ctx, j.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("retrying a PENDING job = %v, want ErrConflict", err)
	}
	if err := st.RetryJob(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retrying a missing job = %v, want ErrNotFound", err)
	}
}
