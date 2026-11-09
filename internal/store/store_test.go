package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/your-org/taskflow/internal/domain"
)

// testStore returns a Store bound to a transaction that is rolled back when
// the test finishes, so tests never leave rows behind. It skips the test when
// DATABASE_URL is unset, Postgres is unreachable, or migrations are missing.
func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	ctx := context.Background()

	poolStore, err := New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres unreachable: %v", err)
	}
	t.Cleanup(poolStore.Close)

	// Skip cleanly when migrations have not been applied yet.
	if _, err := poolStore.ListJobs(ctx, Filter{Limit: 1}); err != nil {
		t.Skipf("schema not migrated (run make migrate-up): %v", err)
	}

	tx, txStore, err := poolStore.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(ctx) })
	return txStore
}

func mustJob(t *testing.T, s *Store, mutate func(*domain.Job)) domain.Job {
	t.Helper()
	j := domain.Job{
		Type:           "test." + uuid.NewString()[:8],
		Payload:        []byte(`{"k":"v"}`),
		Priority:       domain.PriorityNormal,
		TimeoutSeconds: 30,
		MaxRetries:     2,
	}
	if mutate != nil {
		mutate(&j)
	}
	created, err := s.CreateJob(context.Background(), j)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	return created
}

func TestCreateAndGetJob(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	created := mustJob(t, s, func(j *domain.Job) {
		j.Priority = domain.PriorityHigh
		j.Payload = []byte(`{"a":1}`)
	})
	if created.ID == uuid.Nil {
		t.Fatal("expected generated ID")
	}
	if created.Status != domain.StatusPending {
		t.Fatalf("status = %q, want PENDING", created.Status)
	}
	if created.Priority != domain.PriorityHigh {
		t.Fatalf("priority = %v, want high", created.Priority)
	}

	got, err := s.GetJob(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Type != created.Type || got.Attempt != 0 {
		t.Fatalf("unexpected job: %+v", got)
	}
	if string(got.Payload) != `{"a":1}` {
		t.Fatalf("payload = %s", got.Payload)
	}

	if _, err := s.GetJob(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetJob missing = %v, want ErrNotFound", err)
	}
}

func TestListJobsFiltersAndCursor(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	typ := "list." + uuid.NewString()[:8]
	var ids []uuid.UUID
	for i := 0; i < 5; i++ {
		p := domain.PriorityNormal
		if i == 4 {
			p = domain.PriorityHigh
		}
		j := mustJob(t, s, func(j *domain.Job) {
			j.Type = typ
			j.Priority = p
		})
		ids = append(ids, j.ID)
	}
	// A job of a different type must not appear.
	mustJob(t, s, func(j *domain.Job) { j.Type = "other." + uuid.NewString()[:8] })

	page1, err := s.ListJobs(ctx, Filter{Type: typ, Limit: 3})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(page1) != 3 {
		t.Fatalf("page1 len = %d, want 3", len(page1))
	}

	page2, err := s.ListJobs(ctx, Filter{
		Type:   typ,
		Limit:  3,
		Cursor: encodeCursor(page1[len(page1)-1].CreatedAt, page1[len(page1)-1].ID),
	})
	if err != nil {
		t.Fatalf("ListJobs page2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("page2 len = %d, want 2", len(page2))
	}
	seen := map[uuid.UUID]bool{}
	for _, j := range append(page1, page2...) {
		if seen[j.ID] {
			t.Fatalf("duplicate job %s across pages", j.ID)
		}
		seen[j.ID] = true
	}
	for _, id := range ids {
		if !seen[id] {
			t.Fatalf("job %s missing from pages", id)
		}
	}

	high, err := s.ListJobs(ctx, Filter{Type: typ, Priority: domain.PriorityHigh})
	if err != nil {
		t.Fatalf("ListJobs priority: %v", err)
	}
	if len(high) != 1 || high[0].ID != ids[4] {
		t.Fatalf("priority filter = %+v", high)
	}
}

func TestUpdateStatusOptimistic(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	j := mustJob(t, s, nil)

	if err := s.UpdateStatus(ctx, j.ID, domain.StatusPending, domain.StatusQueued); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	// Same transition again must conflict: status is no longer PENDING.
	if err := s.UpdateStatus(ctx, j.ID, domain.StatusPending, domain.StatusQueued); !errors.Is(err, ErrConflict) {
		t.Fatalf("second UpdateStatus = %v, want ErrConflict", err)
	}
	if err := s.UpdateStatus(ctx, uuid.New(), domain.StatusPending, domain.StatusQueued); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateStatus missing = %v, want ErrNotFound", err)
	}

	got, _ := s.GetJob(ctx, j.ID)
	if got.Status != domain.StatusQueued {
		t.Fatalf("status = %q, want QUEUED", got.Status)
	}
}

func TestFetchDueJobsAndMarkQueued(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now()

	past := now.Add(-time.Minute)
	future := now.Add(time.Hour)
	due := mustJob(t, s, func(j *domain.Job) {
		j.RunAt = past
		j.NextRunAt = &past
	})
	// High priority due job should sort first.
	dueHigh := mustJob(t, s, func(j *domain.Job) {
		j.Priority = domain.PriorityHigh
		j.RunAt = past
		j.NextRunAt = &past
	})
	mustJob(t, s, func(j *domain.Job) {
		j.RunAt = future
		j.NextRunAt = &future
	})
	mustJob(t, s, func(j *domain.Job) {
		j.Status = domain.StatusSucceeded // terminal: never due
	})

	jobs, err := s.FetchDueJobs(ctx, now, 50)
	if err != nil {
		t.Fatalf("FetchDueJobs: %v", err)
	}
	// Other committed rows may also be due; ensure our jobs are present and
	// the high-priority one sorts before the normal one.
	pos := map[uuid.UUID]int{}
	for i, j := range jobs {
		pos[j.ID] = i
	}
	if _, ok := pos[due.ID]; !ok {
		t.Fatalf("due job %s not fetched", due.ID)
	}
	if _, ok := pos[dueHigh.ID]; !ok {
		t.Fatalf("high-priority due job %s not fetched", dueHigh.ID)
	}
	if pos[dueHigh.ID] > pos[due.ID] {
		t.Fatal("high priority job should sort before normal")
	}

	var ids []uuid.UUID
	for _, j := range jobs {
		if j.ID == due.ID || j.ID == dueHigh.ID {
			ids = append(ids, j.ID)
		}
	}
	if err := s.MarkQueued(ctx, ids); err != nil {
		t.Fatalf("MarkQueued: %v", err)
	}
	for _, id := range ids {
		got, err := s.GetJob(ctx, id)
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if got.Status != domain.StatusQueued {
			t.Fatalf("job %s status = %q, want QUEUED", id, got.Status)
		}
	}
}

func TestRunLifecycle(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	j := mustJob(t, s, nil)

	run, err := s.CreateRun(ctx, domain.JobRun{
		JobID:    j.ID,
		WorkerID: "w-test",
		Attempt:  1,
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if run.ID == uuid.Nil || run.Status != domain.StatusRunning {
		t.Fatalf("run = %+v", run)
	}

	if err := s.FinishRun(ctx, run.ID, domain.StatusSucceeded, ""); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	if err := s.FinishRun(ctx, uuid.New(), domain.StatusSucceeded, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FinishRun missing = %v, want ErrNotFound", err)
	}
}

func TestAttemptNextRunCancel(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	j := mustJob(t, s, nil)

	n, err := s.IncrementAttempt(ctx, j.ID)
	if err != nil || n != 1 {
		t.Fatalf("IncrementAttempt = %d, %v", n, err)
	}
	n, err = s.IncrementAttempt(ctx, j.ID)
	if err != nil || n != 2 {
		t.Fatalf("IncrementAttempt = %d, %v", n, err)
	}
	if _, err := s.IncrementAttempt(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("IncrementAttempt missing = %v, want ErrNotFound", err)
	}

	next := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	if err := s.SetNextRun(ctx, j.ID, &next); err != nil {
		t.Fatalf("SetNextRun: %v", err)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.NextRunAt == nil || !got.NextRunAt.Equal(next) {
		t.Fatalf("next_run_at = %v, want %v", got.NextRunAt, next)
	}
	if err := s.SetNextRun(ctx, j.ID, nil); err != nil {
		t.Fatalf("SetNextRun nil: %v", err)
	}

	if err := s.CancelJob(ctx, j.ID); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	got, _ = s.GetJob(ctx, j.ID)
	if got.Status != domain.StatusCancelled {
		t.Fatalf("status = %q, want CANCELLED", got.Status)
	}
	// Cancelling again conflicts (already terminal).
	if err := s.CancelJob(ctx, j.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("second CancelJob = %v, want ErrConflict", err)
	}
	if err := s.CancelJob(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CancelJob missing = %v, want ErrNotFound", err)
	}
}

func TestWorkersAndStats(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	wid := "w-" + uuid.NewString()[:8]
	w := domain.WorkerInfo{ID: wid, Hostname: "host1", Concurrency: 4}
	if err := s.UpsertWorker(ctx, w); err != nil {
		t.Fatalf("UpsertWorker: %v", err)
	}
	w.Concurrency = 8
	if err := s.UpsertWorker(ctx, w); err != nil {
		t.Fatalf("UpsertWorker update: %v", err)
	}

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	var found *domain.WorkerInfo
	for i := range workers {
		if workers[i].ID == wid {
			found = &workers[i]
		}
	}
	if found == nil {
		t.Fatalf("worker %s not listed", wid)
	}
	if found.Concurrency != 8 || found.Hostname != "host1" {
		t.Fatalf("worker = %+v", found)
	}

	before, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	j := mustJob(t, s, func(j *domain.Job) { j.Type = "stats." + uuid.NewString()[:8] })
	run, err := s.CreateRun(ctx, domain.JobRun{JobID: j.ID, WorkerID: wid, Attempt: 1})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	// Ensure finished_at - started_at is measurably positive.
	time.Sleep(10 * time.Millisecond)
	if err := s.FinishRun(ctx, run.ID, domain.StatusSucceeded, ""); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}

	after, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if after.JobsByStatus[domain.StatusPending] != before.JobsByStatus[domain.StatusPending]+1 {
		t.Fatalf("pending delta = %d -> %d",
			before.JobsByStatus[domain.StatusPending], after.JobsByStatus[domain.StatusPending])
	}
	if after.RunsLastHour != before.RunsLastHour+1 {
		t.Fatalf("runs last hour = %d -> %d", before.RunsLastHour, after.RunsLastHour)
	}
	if after.AvgRunDurationLastHour <= 0 {
		t.Fatalf("avg duration = %v, want > 0", after.AvgRunDurationLastHour)
	}
}
