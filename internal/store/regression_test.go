package store

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// These tests cover three queue bugs that only showed up when the stack started
// cold with an empty queue. They live in one file, and share one lock, because
// each of them empties the job tables: `go test ./...` runs packages in parallel
// against a single database (that is what CI does too), so two tests clearing
// tables at the same time would fail each other. Holding a Postgres advisory
// lock for the duration of each test keeps them honest without serialising the
// whole package.
var dbTestLock sync.Mutex

// exclusive empties the job tables and holds a database-wide lock until the test
// finishes, so no other test (in this package or any other, in this or another
// process) can be emptying them at the same time.
func exclusive(t *testing.T) (*Store, context.Context) {
	t.Helper()
	dbTestLock.Lock()
	t.Cleanup(dbTestLock.Unlock)

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	ctx := context.Background()

	st, err := New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres unreachable: %v", err)
	}
	t.Cleanup(st.Close)

	// A Postgres advisory lock gives the same guarantee across processes: `go
	// test ./...` runs packages in parallel, and internal/worker's database
	// tests empty the same tables. The lock must live on one pinned connection -
	// it is session scoped, so taking it through the pool would drop it as soon
	// as that connection went back to the pool.
	lockConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Skipf("postgres unreachable: %v", err)
	}
	t.Cleanup(func() {
		if err := lockConn.Close(context.WithoutCancel(ctx)); err != nil {
			t.Logf("close lock connection: %v", err)
		}
	})

	var locked bool
	for i := 0; i < 120; i++ {
		if err := lockConn.QueryRow(ctx, `SELECT pg_try_advisory_lock(8245173)`).Scan(&locked); err != nil {
			t.Skipf("advisory lock unavailable: %v", err)
		}
		if locked {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !locked {
		t.Skip("another test held the database lock for 30s")
	}

	if _, err := st.db.Exec(ctx, `TRUNCATE job_runs, jobs CASCADE`); err != nil {
		t.Skipf("truncate refused (read-only role?): %v", err)
	}
	return st, ctx
}

// ---------------------------------------------------------------------------
// Bug 1: a worker heartbeats for as long as it runs, but its row can vanish
// underneath it (an operator tidying up, a restored dump, my demo reset). Beat
// used to be a plain UPDATE, so it then failed with "not found" every interval
// for the rest of the process's life, and the worker never re-registered.
// ---------------------------------------------------------------------------

func TestBeatRecreatesAMissingWorker(t *testing.T) {
	st, ctx := exclusive(t)

	if err := st.Beat(ctx, "w-orphan"); err != nil {
		t.Fatalf("Beat on a missing worker must succeed, got %v", err)
	}

	var alive bool
	if err := st.db.QueryRow(ctx,
		`SELECT alive FROM workers WHERE id = 'w-orphan'`).Scan(&alive); err != nil {
		t.Fatalf("the row was not created: %v", err)
	}
	if !alive {
		t.Fatal("a re-created worker must be alive")
	}
}

func TestBeatRefreshesWithoutLosingRegistration(t *testing.T) {
	st, ctx := exclusive(t)

	if _, err := st.db.Exec(ctx,
		`INSERT INTO workers (id, hostname, concurrency) VALUES ('w-1', 'host-a', 8)`); err != nil {
		t.Fatalf("insert worker: %v", err)
	}
	if _, err := st.db.Exec(ctx,
		`UPDATE workers SET last_heartbeat = now() - interval '1 hour', alive = false WHERE id = 'w-1'`); err != nil {
		t.Fatalf("age the heartbeat: %v", err)
	}
	if err := st.Beat(ctx, "w-1"); err != nil {
		t.Fatalf("Beat: %v", err)
	}

	var hostname string
	var concurrency int
	var age float64
	var alive bool
	if err := st.db.QueryRow(ctx,
		`SELECT hostname, concurrency, alive, extract(epoch from (now() - last_heartbeat))
		 FROM workers WHERE id = 'w-1'`).Scan(&hostname, &concurrency, &alive, &age); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if hostname != "host-a" || concurrency != 8 {
		t.Fatalf("registration data was clobbered: hostname=%q concurrency=%d", hostname, concurrency)
	}
	if !alive {
		t.Fatal("a beating worker must be marked alive")
	}
	if age > 5 {
		t.Fatalf("last_heartbeat was not refreshed (%.0fs old)", age)
	}
}

// ---------------------------------------------------------------------------
// Bug 2: a job that started moments ago cannot be "stale", whatever the workers
// table says. Without this guard, losing a workers row made recovery reclaim
// jobs that were running perfectly well.
// ---------------------------------------------------------------------------

func TestFindStaleRunningIgnoresFreshRuns(t *testing.T) {
	st, ctx := exclusive(t)

	insert := func(startedAt time.Time) uuid.UUID {
		var id uuid.UUID
		if err := st.db.QueryRow(ctx, `
			INSERT INTO jobs (type, payload, priority, status)
			VALUES ('sleep', '{}', 2, 'RUNNING') RETURNING id`).Scan(&id); err != nil {
			t.Fatalf("insert job: %v", err)
		}
		if _, err := st.db.Exec(ctx, `
			INSERT INTO job_runs (job_id, worker_id, attempt, status, started_at)
			VALUES ($1, 'w-gone', 1, 'RUNNING', $2)`, id, startedAt); err != nil {
			t.Fatalf("insert run: %v", err)
		}
		return id
	}

	// No workers row at all for 'w-gone' - the worst case.
	fresh := insert(time.Now().Add(-2 * time.Second))
	old := insert(time.Now().Add(-2 * time.Minute))

	got, err := st.FindStaleRunning(ctx, 30*time.Second, 10)
	if err != nil {
		t.Fatalf("FindStaleRunning: %v", err)
	}
	var sawFresh, sawOld bool
	for _, j := range got {
		if j.ID == fresh {
			sawFresh = true
		}
		if j.ID == old {
			sawOld = true
		}
	}
	if sawFresh {
		t.Fatal("a run that started 2s ago must not be treated as stale")
	}
	if !sawOld {
		t.Fatal("a run with no heartbeat for 2 minutes must still be reclaimed")
	}
}

// ---------------------------------------------------------------------------
// Bug 3: the scheduler enqueues to Redis and then marks rows QUEUED, so a worker
// can finish a job between those two statements. MarkQueued must leave rows that
// have already moved on alone - it used to reset them to QUEUED for ever.
// ---------------------------------------------------------------------------

func TestMarkQueuedLeavesFinishedJobsAlone(t *testing.T) {
	st, ctx := exclusive(t)

	mk := func(status string) uuid.UUID {
		var id uuid.UUID
		if err := st.db.QueryRow(ctx, `
			INSERT INTO jobs (type, payload, status) VALUES ('sleep','{}',$1) RETURNING id`,
			status).Scan(&id); err != nil {
			t.Fatalf("insert %s job: %v", status, err)
		}
		return id
	}
	pending := mk("PENDING")
	retrying := mk("RETRYING")
	succeeded := mk("SUCCEEDED")
	dead := mk("DEAD")

	if err := st.MarkQueued(ctx, []uuid.UUID{pending, retrying, succeeded, dead}); err != nil {
		t.Fatalf("MarkQueued: %v", err)
	}

	want := map[uuid.UUID]string{
		pending:   "QUEUED",
		retrying:  "QUEUED",
		succeeded: "SUCCEEDED",
		dead:      "DEAD",
	}
	for id, w := range want {
		var got string
		if err := st.db.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1::uuid`, id).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", id, err)
		}
		if got != w {
			t.Fatalf("job %s was left %s, want %s", id, got, w)
		}
	}
}

// The demo's own helpers assert on the same tables, so they get the same
// treatment: one lock, one empty database.
func TestCompletionByPriority(t *testing.T) {
	st, ctx := exclusive(t)

	base := time.Now().Add(-time.Hour)
	insert := func(priority int, createdOffset, ranOffset time.Duration, runSeconds float64, status string) {
		var id uuid.UUID
		if err := st.db.QueryRow(ctx, `
			INSERT INTO jobs (type, payload, priority, status, created_at)
			VALUES ('sleep','{}',$1,$2,$3) RETURNING id`,
			priority, status, base.Add(createdOffset)).Scan(&id); err != nil {
			t.Fatalf("insert job: %v", err)
		}
		started := base.Add(createdOffset + ranOffset)
		if _, err := st.db.Exec(ctx, `
			INSERT INTO job_runs (job_id, worker_id, attempt, status, started_at, finished_at)
			VALUES ($1,'w',1,$2,$3,$4)`,
			id, status, started, started.Add(time.Duration(runSeconds*float64(time.Second)))); err != nil {
			t.Fatalf("insert run: %v", err)
		}
	}

	// HIGH: 10s queue wait, 1s run. LOW: 30s queue wait, 1s run.
	for i := 0; i < 4; i++ {
		insert(3, 0, 10*time.Second, 1, "SUCCEEDED")
		insert(1, 0, 30*time.Second, 1, "SUCCEEDED")
	}

	rows, err := st.CompletionByPriority(ctx)
	if err != nil {
		t.Fatalf("CompletionByPriority: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d priority rows, want 2: %v", len(rows), rows)
	}
	var high, low PriorityCompletion
	for _, r := range rows {
		switch r.Priority.String() {
		case "high":
			high = r
		case "low":
			low = r
		}
	}
	if high.AvgSecondsToFinish >= low.AvgSecondsToFinish {
		t.Fatalf("high %.2fs should finish before low %.2fs",
			high.AvgSecondsToFinish, low.AvgSecondsToFinish)
	}
	// The control: run time is identical, so the difference is queue wait.
	if diff := high.AvgRunSeconds - low.AvgRunSeconds; diff > 0.2 || diff < -0.2 {
		t.Fatalf("run times should match (%.2f vs %.2f); the table would be misleading",
			high.AvgRunSeconds, low.AvgRunSeconds)
	}
}

func TestCompletionByPriorityIgnoresFailures(t *testing.T) {
	st, ctx := exclusive(t)

	var failed uuid.UUID
	if err := st.db.QueryRow(ctx, `
		INSERT INTO jobs (type, payload, priority, status, created_at)
		VALUES ('fail','{}',3,'DEAD', now() - interval '10 minutes') RETURNING id`).Scan(&failed); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	if _, err := st.db.Exec(ctx, `
		INSERT INTO job_runs (job_id, worker_id, attempt, status, started_at, finished_at)
		VALUES ($1,'w',1,'DEAD', now() - interval '9 minutes', now() - interval '8 minutes')`,
		failed); err != nil {
		t.Fatalf("insert run: %v", err)
	}

	rows, err := st.CompletionByPriority(ctx)
	if err != nil {
		t.Fatalf("CompletionByPriority: %v", err)
	}
	for _, r := range rows {
		if r.Jobs != 0 {
			t.Fatalf("a failed job was counted as completing: %v", r)
		}
	}
}

func TestSnapshotCountsRecoveredJobs(t *testing.T) {
	st, ctx := exclusive(t)

	var id uuid.UUID
	if err := st.db.QueryRow(ctx, `
		INSERT INTO jobs (type, payload, status, last_error)
		VALUES ('sleep','{}','SUCCEEDED','reclaimed: worker stopped heartbeating') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	if _, err := st.db.Exec(ctx, `
		INSERT INTO job_runs (job_id, worker_id, attempt, status, finished_at)
		VALUES ($1,'w',1,'SUCCEEDED', now())`, id); err != nil {
		t.Fatalf("insert run: %v", err)
	}

	snap, err := st.SnapshotForDemo(ctx)
	if err != nil {
		t.Fatalf("SnapshotForDemo: %v", err)
	}
	if snap.RecoveredJobs != 1 {
		t.Fatalf("recovered = %d, want 1", snap.RecoveredJobs)
	}
}

func TestWaitForJobsReturnsWhenEverythingIsDone(t *testing.T) {
	st, ctx := exclusive(t)

	// Nothing left to do: it must return immediately rather than sit out the
	// whole deadline.
	start := time.Now()
	active, err := st.WaitForJobs(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("WaitForJobs: %v", err)
	}
	if active != 0 {
		t.Fatalf("active = %d, want 0", active)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("waiting took %s for an empty queue", took)
	}

	// A recurring job is a template: it stays PENDING between fires by design.
	// Counting it would mean the drain never completes.
	if _, err := st.db.Exec(ctx, `
		INSERT INTO jobs (type, payload, status, cron_expr, next_run_at)
		VALUES ('echo','{}','PENDING','*/10 * * * *', now() + interval '10 seconds')`); err != nil {
		t.Fatalf("insert cron job: %v", err)
	}
	active, err = st.WaitForJobs(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("WaitForJobs: %v", err)
	}
	if active != 0 {
		t.Fatalf("active = %d, want 0 - a cron template must not block the drain", active)
	}
}
