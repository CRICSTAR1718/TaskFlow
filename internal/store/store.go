// Package store is the Postgres repository for jobs, runs and workers.
package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cricstar1718/taskflow/internal/domain"
)

// Sentinel errors returned by Store methods.
var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
)

// DBTX is the subset of *pgxpool.Pool and pgx.Tx used by Store, so methods can
// run on the pool or inside a caller-managed transaction.
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Repository is the persistence surface consumers depend on. Consumers
// should redeclare the narrow subset of methods they need rather than
// depending on the whole store.
type Repository interface {
	CreateJob(ctx context.Context, job domain.Job) (domain.Job, error)
	GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error)
	ListJobs(ctx context.Context, f Filter) ([]domain.Job, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) error
	FetchDueJobs(ctx context.Context, now time.Time, limit int) ([]domain.Job, error)
	MarkQueued(ctx context.Context, ids []uuid.UUID) error
	CreateRun(ctx context.Context, run domain.JobRun) (domain.JobRun, error)
	FinishRun(ctx context.Context, runID uuid.UUID, status domain.Status, errMsg string) error
	IncrementAttempt(ctx context.Context, id uuid.UUID) (int, error)
	SetNextRun(ctx context.Context, id uuid.UUID, nextRunAt *time.Time) error
	CancelJob(ctx context.Context, id uuid.UUID) error
	UpsertWorker(ctx context.Context, w domain.WorkerInfo) error
	ListWorkers(ctx context.Context) ([]domain.WorkerInfo, error)
	Stats(ctx context.Context) (Stats, error)
}

// Filter narrows ListJobs results. Zero values mean "no filter".
type Filter struct {
	Status   domain.Status   // "" matches every status
	Priority domain.Priority // 0 matches every priority
	Type     string          // "" matches every job type
	Limit    int             // page size; <=0 uses the default, capped at MaxLimit
	Cursor   string          // opaque cursor from a previous page
}

const (
	defaultLimit = 50
	maxLimit     = 200
)

// Stats summarizes jobs and recent run activity.
type Stats struct {
	JobsByStatus           map[domain.Status]int64 `json:"jobs_by_status"`
	RunsLastHour           int64                   `json:"runs_last_hour"`
	AvgRunDurationLastHour time.Duration           `json:"avg_run_duration_last_hour"`
}

// Store persists TaskFlow state in PostgreSQL via pgxpool.
type Store struct {
	db   DBTX // pool or transaction
	pool *pgxpool.Pool
}

var _ Repository = (*Store)(nil)

// New connects to Postgres at dsn and returns a Store backed by a pool.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{db: pool, pool: pool}, nil
}

// Begin starts a transaction and returns it together with a Store bound to
// that transaction. The caller must Commit or Rollback the tx; the returned
// Store must not outlive it.
func (s *Store) Begin(ctx context.Context) (pgx.Tx, *Store, error) {
	if s.pool == nil {
		return nil, nil, errors.New("store is not pool-backed")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin tx: %w", err)
	}
	return tx, &Store{db: tx}, nil
}

// Close releases the connection pool. Safe to call on a tx-bound Store.
func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// Ping verifies connectivity.
func (s *Store) Ping(ctx context.Context) error {
	var one int
	return s.db.QueryRow(ctx, `SELECT 1`).Scan(&one)
}

// jobCols is the canonical column list for jobs queries; priority is cast to
// int so pgx can scan it into a plain int.
const jobCols = `id, type, payload, priority::int, status, cron_expr, run_at,
	next_run_at, timeout_seconds, max_retries, attempt, last_error,
	created_at, updated_at`

// scanner is implemented by both pgx.Row and pgx.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanJob(row scanner) (domain.Job, error) {
	var (
		j         domain.Job
		id        pgtype.UUID
		payload   []byte
		priority  int
		status    string
		lastError string
	)
	err := row.Scan(
		&id, &j.Type, &payload, &priority, &status, &j.CronExpr,
		&j.RunAt, &j.NextRunAt, &j.TimeoutSeconds, &j.MaxRetries,
		&j.Attempt, &lastError, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return domain.Job{}, err
	}
	j.ID = uuid.UUID(id.Bytes)
	j.Payload = payload
	j.Priority = domain.Priority(priority)
	j.Status = domain.Status(status)
	if lastError != "" {
		j.LastError = &lastError
	}
	return j, nil
}

// CreateJob inserts a job and returns the stored row. Empty fields fall back
// to the column defaults (PENDING, priority normal, timeout 60s).
func (s *Store) CreateJob(ctx context.Context, j domain.Job) (domain.Job, error) {
	if j.Priority == 0 {
		j.Priority = domain.PriorityNormal
	}
	if j.TimeoutSeconds == 0 {
		j.TimeoutSeconds = 60
	}
	status := j.Status
	if status == "" {
		status = domain.StatusPending
	}
	payload := j.Payload
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	runAt := j.RunAt
	if runAt.IsZero() {
		runAt = time.Now()
	}

	row := s.db.QueryRow(ctx,
		`INSERT INTO jobs (type, payload, priority, status, cron_expr, run_at,
			next_run_at, timeout_seconds, max_retries)
		 VALUES ($1, $2::jsonb, $3, $4, $5, $6, $7, $8, $9)
		 RETURNING `+jobCols,
		j.Type, string(payload), int(j.Priority), string(status), j.CronExpr,
		runAt, j.NextRunAt, j.TimeoutSeconds, j.MaxRetries,
	)
	created, err := scanJob(row)
	if err != nil {
		return domain.Job{}, fmt.Errorf("create job: %w", err)
	}
	return created, nil
}

// GetJob loads a job by ID, returning ErrNotFound when absent.
func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	j, err := scanJob(s.db.QueryRow(ctx,
		`SELECT `+jobCols+` FROM jobs WHERE id = $1::uuid`, id.String()))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, ErrNotFound
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

// encodeCursor packs (created_at, id) into an opaque pagination cursor.
func encodeCursor(createdAt time.Time, id uuid.UUID) string {
	raw := strconv.FormatInt(createdAt.UnixNano(), 10) + "_" + id.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(s string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor: %w", err)
	}
	parts := strings.SplitN(string(raw), "_", 2)
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, errors.New("invalid cursor")
	}
	nano, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor: %w", err)
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor: %w", err)
	}
	return time.Unix(0, nano).UTC(), id, nil
}

// ListJobs returns jobs newest-first with keyset pagination on
// (created_at, id). Pass the Cursor from the last item of the previous page
// to fetch the next one.
func (s *Store) ListJobs(ctx context.Context, f Filter) ([]domain.Job, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}

	var (
		where []string
		args  []any
	)
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.Status != "" {
		add(`status = $%d`, string(f.Status))
	}
	if f.Priority != 0 {
		add(`priority = $%d`, int(f.Priority))
	}
	if f.Type != "" {
		add(`type = $%d`, f.Type)
	}
	if f.Cursor != "" {
		createdAt, id, err := decodeCursor(f.Cursor)
		if err != nil {
			return nil, err
		}
		args = append(args, createdAt, id.String())
		where = append(where, fmt.Sprintf(
			`(created_at, id) < ($%[1]d, $%[2]d::uuid)`, len(args)-1, len(args)))
	}

	query := `SELECT ` + jobCols + ` FROM jobs`
	if len(where) > 0 {
		query += ` WHERE ` + strings.Join(where, ` AND `)
	}
	args = append(args, limit)
	query += fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT $%d`, len(args))

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	jobs := []domain.Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("list jobs: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return jobs, nil
}

// UpdateStatus moves a job from one status to another using an optimistic
// check: the UPDATE only applies when the row is still in `from`. Returns
// ErrNotFound when the job does not exist and ErrConflict when it exists but
// is no longer in `from`.
func (s *Store) UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) error {
	tag, err := s.db.Exec(ctx,
		`UPDATE jobs SET status = $3 WHERE id = $1::uuid AND status = $2`,
		id.String(), string(from), string(to))
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	exists, err := s.exists(ctx, id)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}
	return ErrConflict
}

// exists reports whether a job row exists.
func (s *Store) exists(ctx context.Context, id uuid.UUID) (bool, error) {
	var one int
	err := s.db.QueryRow(ctx, `SELECT 1 FROM jobs WHERE id = $1::uuid`, id.String()).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check job exists: %w", err)
	}
	return true, nil
}

// FetchDueJobs locks up to limit due jobs (PENDING or RETRYING whose
// next_run_at has arrived), highest priority first. It uses SELECT ... FOR
// UPDATE SKIP LOCKED, so it must run inside a transaction -- use Begin and the
// returned Store -- typically followed by MarkQueued in the same transaction.
func (s *Store) FetchDueJobs(ctx context.Context, now time.Time, limit int) ([]domain.Job, error) {
	if limit <= 0 {
		limit = defaultLimit
	}
	rows, err := s.db.Query(ctx,
		`SELECT `+jobCols+` FROM jobs
		 WHERE status IN ('PENDING', 'RETRYING')
		   AND next_run_at IS NOT NULL
		   AND next_run_at <= $1
		 ORDER BY priority DESC, next_run_at ASC, id
		 LIMIT $2
		 FOR UPDATE SKIP LOCKED`,
		now, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch due jobs: %w", err)
	}
	defer rows.Close()

	jobs := []domain.Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("fetch due jobs: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch due jobs: %w", err)
	}
	return jobs, nil
}

// MarkQueued sets status QUEUED for the given job IDs. Call it in the same
// transaction as FetchDueJobs so selection and marking are atomic.
func (s *Store) MarkQueued(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = id.String()
	}
	// Only rows still waiting may be marked QUEUED. The scheduler decided these
	// ids were due a moment ago; a worker can already have claimed and finished
	// one of them since (the queue entry usually reaches a worker before this
	// statement runs). Without the status guard this UPDATE reset a SUCCEEDED or
	// DEAD job back to QUEUED, where nothing would ever run it again - the job
	// silently stayed QUEUED forever.
	if _, err := s.db.Exec(ctx,
		`UPDATE jobs SET status = 'QUEUED'
		 WHERE id IN (SELECT unnest($1::text[])::uuid)
		   AND status IN ('PENDING', 'RETRYING')`,
		strs); err != nil {
		return fmt.Errorf("mark queued: %w", err)
	}
	return nil
}

// CreateRun records a job execution attempt and returns the stored row.
func (s *Store) CreateRun(ctx context.Context, run domain.JobRun) (domain.JobRun, error) {
	status := run.Status
	if status == "" {
		status = domain.StatusRunning
	}
	// Let Postgres stamp started_at when the caller did not supply one, so
	// started_at and finished_at come from the same clock.
	var startedAt any
	if !run.StartedAt.IsZero() {
		startedAt = run.StartedAt
	}
	errMsg := ""
	if run.Error != nil {
		errMsg = *run.Error
	}

	var id pgtype.UUID
	err := s.db.QueryRow(ctx,
		`INSERT INTO job_runs (job_id, worker_id, attempt, status, error,
			started_at, finished_at)
		 VALUES ($1::uuid, $2, $3, $4, $5, COALESCE($6::timestamptz, clock_timestamp()), $7)
		 RETURNING id, started_at`,
		run.JobID.String(), run.WorkerID, run.Attempt, string(status), errMsg,
		startedAt, run.FinishedAt,
	).Scan(&id, &run.StartedAt)
	if err != nil {
		return domain.JobRun{}, fmt.Errorf("create run: %w", err)
	}
	run.ID = uuid.UUID(id.Bytes)
	run.Status = status
	return run, nil
}

// FinishRun marks a job run finished with the given status and error text.
// Returns ErrNotFound when the run does not exist.
func (s *Store) FinishRun(ctx context.Context, runID uuid.UUID, status domain.Status, errMsg string) error {
	tag, err := s.db.Exec(ctx,
		// clock_timestamp(), not now(): now() is frozen for the whole
		// transaction, so a run created and finished in one tx would get
		// identical timestamps (and a non-positive duration).
		`UPDATE job_runs
		 SET status = $2, error = $3, finished_at = clock_timestamp()
		 WHERE id = $1::uuid`,
		runID.String(), string(status), errMsg)
	if err != nil {
		return fmt.Errorf("finish run: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// IncrementAttempt bumps jobs.attempt by one and returns the new value.
// Returns ErrNotFound when the job does not exist.
func (s *Store) IncrementAttempt(ctx context.Context, id uuid.UUID) (int, error) {
	var attempt int
	err := s.db.QueryRow(ctx,
		`UPDATE jobs SET attempt = attempt + 1 WHERE id = $1::uuid RETURNING attempt`,
		id.String()).Scan(&attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("increment attempt: %w", err)
	}
	return attempt, nil
}

// SetNextRun updates jobs.next_run_at (nil clears it). Returns ErrNotFound
// when the job does not exist.
func (s *Store) SetNextRun(ctx context.Context, id uuid.UUID, nextRunAt *time.Time) error {
	tag, err := s.db.Exec(ctx,
		`UPDATE jobs SET next_run_at = $2 WHERE id = $1::uuid`, id.String(), nextRunAt)
	if err != nil {
		return fmt.Errorf("set next run: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// CancelJob moves a job to CANCELLED. Only non-terminal, cancellable states
// (PENDING, QUEUED, RUNNING, RETRYING) can be cancelled: ErrNotFound when the
// job does not exist, ErrConflict when it is already in another state.
func (s *Store) CancelJob(ctx context.Context, id uuid.UUID) error {
	tag, err := s.db.Exec(ctx,
		`UPDATE jobs SET status = 'CANCELLED'
		 WHERE id = $1::uuid
		   AND status IN ('PENDING', 'QUEUED', 'RUNNING', 'RETRYING')`,
		id.String())
	if err != nil {
		return fmt.Errorf("cancel job: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	exists, err := s.exists(ctx, id)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}
	return ErrConflict
}

// UpsertWorker registers or refreshes a worker heartbeat.
func (s *Store) UpsertWorker(ctx context.Context, w domain.WorkerInfo) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO workers (id, hostname, concurrency, alive, last_heartbeat)
		 VALUES ($1, $2, $3, true, now())
		 ON CONFLICT (id) DO UPDATE
		 SET hostname = $2, concurrency = $3, alive = true, last_heartbeat = now()`,
		w.ID, w.Hostname, w.Concurrency)
	if err != nil {
		return fmt.Errorf("upsert worker: %w", err)
	}
	return nil
}

// ListWorkers returns all registered workers, most recently heartbeating first.
func (s *Store) ListWorkers(ctx context.Context) ([]domain.WorkerInfo, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, hostname, concurrency, started_at, last_heartbeat
		 FROM workers ORDER BY last_heartbeat DESC`)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()

	workers := []domain.WorkerInfo{}
	for rows.Next() {
		var w domain.WorkerInfo
		if err := rows.Scan(&w.ID, &w.Hostname, &w.Concurrency,
			&w.StartedAt, &w.LastHeartbeat); err != nil {
			return nil, fmt.Errorf("list workers: %w", err)
		}
		workers = append(workers, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	return workers, nil
}

// Stats returns job counts grouped by status plus run volume and average
// duration for runs finished in the last hour.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	st := Stats{JobsByStatus: map[domain.Status]int64{}}

	rows, err := s.db.Query(ctx,
		`SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		return Stats{}, fmt.Errorf("stats jobs: %w", err)
	}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			return Stats{}, fmt.Errorf("stats jobs: %w", err)
		}
		st.JobsByStatus[domain.Status(status)] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Stats{}, fmt.Errorf("stats jobs: %w", err)
	}

	var avgSeconds float64
	err = s.db.QueryRow(ctx,
		`SELECT count(*),
		        COALESCE(AVG(EXTRACT(EPOCH FROM (finished_at - started_at))), 0)
		 FROM job_runs
		 WHERE finished_at >= now() - interval '1 hour'`).
		Scan(&st.RunsLastHour, &avgSeconds)
	if err != nil {
		return Stats{}, fmt.Errorf("stats runs: %w", err)
	}
	st.AvgRunDurationLastHour = time.Duration(avgSeconds * float64(time.Second))
	return st, nil
}

// RecoverableJob is a job that a recovery sweep may act on.
type RecoverableJob struct {
	ID         uuid.UUID
	Priority   domain.Priority
	Attempt    int
	MaxRetries int
	LastError  *string
	WorkerID   string // set for stale jobs: who was holding it
}

// ClaimDue locks up to limit PENDING/RETRYING jobs whose next_run_at has
// arrived and moves them to RUNNING, returning what it moved. The status write
// happens inside the same transaction as the SELECT, so a crash between the two
// cannot lose a job.
//
// Recovery calls this; the rows are immediately put back to RETRYING (with a
// backoff) or DEAD by the caller, which is why RUNNING is only ever a
// momentary state here. UPDATE ... RETURNING means every returned row is
// exclusively ours even without FOR UPDATE SKIP LOCKED.
func (s *Store) ClaimDue(ctx context.Context, now time.Time, limit int) ([]RecoverableJob, error) {
	if limit <= 0 {
		limit = defaultLimit
	}
	rows, err := s.db.Query(ctx,
		`UPDATE jobs SET status = 'RUNNING', updated_at = clock_timestamp()
		 WHERE id IN (
			SELECT id FROM jobs
			WHERE status IN ('PENDING','RETRYING')
			  AND next_run_at IS NOT NULL
			  AND next_run_at <= $1
			ORDER BY priority DESC, next_run_at ASC, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		 )
		 RETURNING id, priority::int, attempt, max_retries, last_error`,
		now, limit)
	if err != nil {
		return nil, fmt.Errorf("claim due: %w", err)
	}
	defer rows.Close()

	out := []RecoverableJob{}
	for rows.Next() {
		var j RecoverableJob
		var prio int
		if err := rows.Scan(&j.ID, &prio, &j.Attempt, &j.MaxRetries, &j.LastError); err != nil {
			return nil, fmt.Errorf("claim due: %w", err)
		}
		j.Priority = domain.Priority(prio)
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim due: %w", err)
	}
	return out, nil
}

// FindStaleRunning returns RUNNING jobs whose current run belongs to a worker
// that has not heartbeated within staleAfter (or to a worker row that is gone).
// Jobs with no open run are excluded: they are owned by a live worker that has
// not written its run row yet.
func (s *Store) FindStaleRunning(ctx context.Context, staleAfter time.Duration, limit int) ([]RecoverableJob, error) {
	if limit <= 0 {
		limit = defaultLimit
	}
	rows, err := s.db.Query(ctx,
		// r.started_at < $1 as well as the heartbeat check: a run that began less
		// than staleAfter ago cannot have "stopped heartbeating", because
		// staleAfter is by definition how long a worker is allowed to stay quiet.
		// Without this, deleting or losing a workers row (a demo reset, an
		// operator tidying up, a restored dump) made every job running at that
		// instant look abandoned, and recovery reclaimed jobs that were fine.
		`SELECT j.id, j.priority::int, j.attempt, j.max_retries, j.last_error,
		        COALESCE(r.worker_id, '')
		 FROM jobs j
		 JOIN job_runs r ON r.job_id = j.id
		 WHERE j.status = 'RUNNING'
		   AND r.finished_at IS NULL
		   AND r.started_at < $1
		   AND NOT EXISTS (
		       SELECT 1 FROM workers w
		       WHERE w.id = r.worker_id
		         AND w.last_heartbeat > $1
		   )
		 ORDER BY j.priority DESC, j.id
		 LIMIT $2`,
		time.Now().Add(-staleAfter), limit)
	if err != nil {
		return nil, fmt.Errorf("find stale running: %w", err)
	}
	defer rows.Close()

	out := []RecoverableJob{}
	for rows.Next() {
		var j RecoverableJob
		var prio int
		if err := rows.Scan(&j.ID, &prio, &j.Attempt, &j.MaxRetries, &j.LastError, &j.WorkerID); err != nil {
			return nil, fmt.Errorf("find stale running: %w", err)
		}
		j.Priority = domain.Priority(prio)
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("find stale running: %w", err)
	}
	return out, nil
}

// Requeue moves a job to RETRYING (or DEAD when terminal) with a new next_run_at,
// records last_error and closes any open run as failed. It is unconditional on
// the old status because the caller has already claimed the row; ErrNotFound is
// returned when the job disappeared (deleted while we worked on it).
func (s *Store) Requeue(ctx context.Context, id uuid.UUID, nextRunAt time.Time, lastError string, terminal bool) error {
	status := domain.StatusRetrying
	if terminal {
		status = domain.StatusDead
	}
	if lastError == "" {
		lastError = "attempt failed"
	}

	tx, txStore, err := s.Begin(ctx)
	if err != nil {
		return fmt.Errorf("requeue: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := txStore.db.Exec(ctx,
		`UPDATE jobs
		 SET status = $2::text,
		     next_run_at = CASE WHEN $5::boolean THEN NULL ELSE $3::timestamptz END,
		     last_error = $4::text,
		     updated_at = clock_timestamp()
		 WHERE id = $1::uuid`,
		id.String(), string(status), nextRunAt, lastError, terminal)
	if err != nil {
		return fmt.Errorf("requeue: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	// Close the abandoned run so durations and success-rate stats stay honest.
	if _, err := txStore.db.Exec(ctx,
		`UPDATE job_runs
		 SET status = $2, error = $3, finished_at = clock_timestamp()
		 WHERE job_id = $1::uuid AND finished_at IS NULL`,
		id.String(), string(status), lastError); err != nil {
		return fmt.Errorf("requeue: close run: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("requeue: commit: %w", err)
	}
	return nil
}

// Beat refreshes workers.last_heartbeat and marks the worker alive. It reports
// ErrNotFound when the row is gone (deleted while the worker was running).
func (s *Store) Beat(ctx context.Context, workerID string) error {
	// Upsert, not UPDATE. A worker heartbeats until it exits, so its row has to
	// exist for as long as it runs - but the row can vanish underneath it (a
	// demo reset, an operator truncating stale rows, a restored dump). With a
	// plain UPDATE the heartbeat then failed with "not found" forever, and
	// recovery treated the missing row as a dead worker and reclaimed jobs that
	// were running fine.
	//
	// hostname and concurrency are left alone on conflict: they were recorded at
	// registration and do not change; only liveness and the timestamp do.
	_, err := s.db.Exec(ctx,
		`INSERT INTO workers (id, hostname, concurrency, alive, last_heartbeat)
		 VALUES ($1, '', 0, true, clock_timestamp())
		 ON CONFLICT (id) DO UPDATE
		 SET last_heartbeat = clock_timestamp(), alive = true`,
		workerID)
	if err != nil {
		return fmt.Errorf("beat: %w", err)
	}
	return nil
}

// MarkWorkerDead flags a worker whose heartbeat expired, so the workers table
// stops claiming it is alive. Best effort: the Redis key is the real signal.
func (s *Store) MarkWorkerDead(ctx context.Context, workerID string) error {
	if _, err := s.db.Exec(ctx,
		`UPDATE workers SET alive = false WHERE id = $1`, workerID); err != nil {
		return fmt.Errorf("mark worker dead: %w", err)
	}
	return nil
}
