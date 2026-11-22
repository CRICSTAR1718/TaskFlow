package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cricstar1718/taskflow/internal/domain"
)

// recoveryStore mirrors testStore: it skips when DATABASE_URL is unset, when
// Postgres is unreachable, or when migrations have not been applied. A
// developer with a stale password must not see a red build because of it.
func recoveryStore(t *testing.T) *Store {
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

	if _, err := poolStore.ListJobs(ctx, Filter{Limit: 1}); err != nil {
		t.Skipf("schema not migrated (run make migrate-up): %v", err)
	}
	return poolStore
}

// truncateAll empties the tables. Worker-level tests need rows committed across
// two connections, so unlike testStore these cannot run inside one transaction.
func truncateAll(t *testing.T, st *Store) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Skipf("postgres unreachable: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `TRUNCATE job_runs, jobs, workers RESTART IDENTITY CASCADE`); err != nil {
		t.Skipf("truncate refused (read-only role?): %v", err)
	}
}

func TestClaimDueMovesDueJobsToRunning(t *testing.T) {
	st := recoveryStore(t)
	truncateAll(t, st)
	ctx := context.Background()

	dueAt := time.Now().Add(-time.Minute)
	due, err := st.CreateJob(ctx, domain.Job{Type: "noop", RunAt: dueAt, NextRunAt: &dueAt})
	if err != nil {
		t.Fatalf("create due job: %v", err)
	}
	futureAt := time.Now().Add(time.Hour)
	future, err := st.CreateJob(ctx, domain.Job{Type: "noop", RunAt: futureAt, NextRunAt: &futureAt})
	if err != nil {
		t.Fatalf("create future job: %v", err)
	}

	got, err := st.ClaimDue(ctx, time.Now(), 10)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(got) != 1 || got[0].ID != due.ID {
		t.Fatalf("claimed %v, want only %s", got, due.ID)
	}

	// Claiming must actually move the row, so a second sweep cannot double-claim.
	again, err := st.ClaimDue(ctx, time.Now(), 10)
	if err != nil {
		t.Fatalf("ClaimDue #2: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("double-claimed: %v", again)
	}

	j, _ := st.GetJob(ctx, due.ID)
	if j.Status != domain.StatusRunning {
		t.Fatalf("due job status = %s, want RUNNING", j.Status)
	}
	f, _ := st.GetJob(ctx, future.ID)
	if f.Status != domain.StatusPending {
		t.Fatalf("future job status = %s, want PENDING", f.Status)
	}
}

func TestRequeueRetryAndDead(t *testing.T) {
	st := recoveryStore(t)
	truncateAll(t, st)
	ctx := context.Background()

	j, _ := st.CreateJob(ctx, domain.Job{Type: "noop", RunAt: time.Now()})
	run, err := st.CreateRun(ctx, domain.JobRun{JobID: j.ID, WorkerID: "w1", Attempt: 1})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	next := time.Now().Add(5 * time.Second)
	if err := st.Requeue(ctx, j.ID, next, "boom", false); err != nil {
		t.Fatalf("Requeue: %v", err)
	}

	got, _ := st.GetJob(ctx, j.ID)
	if got.Status != domain.StatusRetrying {
		t.Fatalf("status = %s, want RETRYING", got.Status)
	}
	if got.LastError == nil || *got.LastError != "boom" {
		t.Fatalf("last_error = %v, want boom", got.LastError)
	}
	if got.NextRunAt == nil {
		t.Fatal("next_run_at was not set")
	}

	// The abandoned run must be closed, or durations and success rate lie.
	var finished *time.Time
	var runStatus string
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Skipf("postgres unreachable: %v", err)
	}
	defer pool.Close()
	if err := pool.QueryRow(ctx,
		`SELECT finished_at, status FROM job_runs WHERE id = $1::uuid`, run.ID.String(),
	).Scan(&finished, &runStatus); err != nil {
		t.Fatalf("read run: %v", err)
	}
	if finished == nil {
		t.Fatal("run row was left open (finished_at IS NULL)")
	}
	if runStatus != string(domain.StatusRetrying) {
		t.Fatalf("run status = %s, want RETRYING", runStatus)
	}

	// Terminal path.
	if err := st.Requeue(ctx, j.ID, time.Now(), "gave up", true); err != nil {
		t.Fatalf("Requeue terminal: %v", err)
	}
	dead, _ := st.GetJob(ctx, j.ID)
	if dead.Status != domain.StatusDead {
		t.Fatalf("status = %s, want DEAD", dead.Status)
	}
	if dead.NextRunAt != nil {
		t.Fatalf("DEAD job still has next_run_at = %v", dead.NextRunAt)
	}
}

func TestFindStaleRunning(t *testing.T) {
	st := recoveryStore(t)
	truncateAll(t, st)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Skipf("postgres unreachable: %v", err)
	}
	defer pool.Close()

	orphan := uuid.New()
	live := uuid.New()

	// A job RUNNING under a worker that died 10 minutes ago.
	if _, err := pool.Exec(ctx,
		`INSERT INTO jobs (id, type, payload, status, run_at) VALUES ($1::uuid,'noop','{}','RUNNING', now())`,
		orphan.String()); err != nil {
		t.Skipf("insert running job: %v", err)
	}
	if _, err := st.CreateRun(ctx, domain.JobRun{JobID: orphan, WorkerID: "dead-worker", Attempt: 1}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	// The run has to be as old as the dead heartbeat: a worker that stopped
	// beating 10 minutes ago cannot have started a run one second ago.
	// FindStaleRunning now requires exactly that, so a row that disappears from
	// the workers table cannot make live work look abandoned.
	if _, err := pool.Exec(ctx,
		`UPDATE job_runs SET started_at = now() - interval '10 minutes' WHERE job_id = $1::uuid`,
		orphan.String()); err != nil {
		t.Fatalf("age the run: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO workers (id, hostname, concurrency, started_at, last_heartbeat)
		 VALUES ('dead-worker','h',1, now(), now() - interval '10 minutes')`); err != nil {
		t.Fatalf("insert dead worker: %v", err)
	}

	// A job RUNNING under a healthy worker.
	if _, err := pool.Exec(ctx,
		`INSERT INTO jobs (id, type, payload, status, run_at) VALUES ($1::uuid,'noop','{}','RUNNING', now())`,
		live.String()); err != nil {
		t.Skipf("insert running job: %v", err)
	}
	if _, err := st.CreateRun(ctx, domain.JobRun{JobID: live, WorkerID: "live-worker", Attempt: 1}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO workers (id, hostname, concurrency, started_at, last_heartbeat)
		 VALUES ('live-worker','h',1, now(), now())`); err != nil {
		t.Fatalf("insert live worker: %v", err)
	}

	got, err := st.FindStaleRunning(ctx, time.Minute, 10)
	if err != nil {
		t.Fatalf("FindStaleRunning: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("found %d stale jobs, want 1: %v", len(got), got)
	}
	if got[0].ID != orphan {
		t.Fatalf("stale job = %s, want the orphan %s", got[0].ID, orphan)
	}
	if got[0].WorkerID != "dead-worker" {
		t.Fatalf("worker_id = %q, want dead-worker", got[0].WorkerID)
	}
}
