package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/cricstar1718/taskflow/internal/domain"
	"github.com/cricstar1718/taskflow/internal/events"
	"github.com/cricstar1718/taskflow/internal/queue"
	"github.com/cricstar1718/taskflow/internal/store"
)

// ErrNoWork is returned by Claim when every priority queue is empty.
var ErrNoWork = errors.New("worker: no work")

// maxPayloadLogLimit keeps oversized handler output out of the database and logs.
const maxPayloadLogLimit = 2000

// JobSource is the slice of the store the runner needs. Declared here, at the
// consumer, so the pool can be tested with fakes.
type JobSource interface {
	GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error)
	CreateRun(ctx context.Context, run domain.JobRun) (domain.JobRun, error)
	FinishRun(ctx context.Context, runID uuid.UUID, status domain.Status, errMsg string) error
	IncrementAttempt(ctx context.Context, id uuid.UUID) (int, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) error
	SetNextRun(ctx context.Context, id uuid.UUID, nextRunAt *time.Time) error
}

// Claimer is the slice of the queue the runner needs. Claim returns
// queue.ErrEmpty when there is nothing waiting.
type Claimer interface {
	Claim(ctx context.Context, workerID string) (string, error)
	Ack(ctx context.Context, workerID, jobID string) error
}

// Runner claims jobs and executes them through the pool.
type Runner struct {
	store    JobSource
	queue    Claimer
	pool     *Pool
	exec     *Executor
	workerID string
	log      *slog.Logger

	// Backoff computes the retry delay for a failed attempt. Swappable in
	// tests; defaults to the same exponential policy recovery uses.
	Backoff func(attempt int, base, maxDelay time.Duration) time.Duration

	// idlePoll is how long the claim loop waits when the queue is empty.
	idlePoll time.Duration

	// emitter publishes status changes for the live dashboard in order.
	// Optional: nil means nobody is told, and a publish failure never fails a
	// job.
	emitter *events.Emitter

	// claimFailures counts consecutive non-empty claim errors, so a broken
	// Redis is reported once and then summarised instead of spamming.
	claimFailures int
}

// RunnerConfig configures a Runner.
type RunnerConfig struct {
	Store       JobSource
	Queue       Claimer
	Pool        *Pool
	Executor    *Executor
	WorkerID    string
	Log         *slog.Logger
	IdlePoll    time.Duration
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// Events is optional; when set every status change is published for the
	// live dashboard. A publish failure never fails a job.
	Events events.Publisher
}

// NewRunner builds a Runner. IdlePoll defaults to 100ms, which keeps queue-wait
// latency low without hammering Redis.
func NewRunner(c RunnerConfig) *Runner {
	log := c.Log
	if log == nil {
		log = slog.Default()
	}
	idle := c.IdlePoll
	if idle <= 0 {
		idle = 100 * time.Millisecond
	}
	r := &Runner{
		store:    c.Store,
		queue:    c.Queue,
		pool:     c.Pool,
		exec:     c.Executor,
		workerID: c.WorkerID,
		log:      log,
		idlePoll: idle,
		Backoff:  exponentialBackoff(c.BackoffBase, c.BackoffMax),
	}
	if c.Events != nil {
		r.emitter = events.NewEmitter(c.Events, 256)
	}
	return r
}

// Run claims until ctx is cancelled. Each claimed job is handed to the pool and
// finished in its own goroutine, so a slow job never blocks the claim loop.
func (r *Runner) Run(ctx context.Context) {
	sem := make(chan struct{}, r.pool.size)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("claim loop stopping", slog.String("worker_id", r.workerID))
			return
		default:
		}

		// Bound in-flight work to the pool size.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		if !r.step(ctx, sem) {
			<-sem
		}
	}
}

// step claims and dispatches one job. It reports whether work was dispatched.
func (r *Runner) step(ctx context.Context, sem chan struct{}) bool {
	jobID, err := r.queue.Claim(ctx, r.workerID)
	// An empty queue is the normal state of a healthy idle worker, not a fault.
	// Comparing by message is not enough: queue.ErrEmpty is wrapped by
	// queue.Claim, so a plain equality check misses it and every idle worker
	// logged a WARN once a second - the noisiest thing in a demo.
	if errors.Is(err, ErrNoWork) || errors.Is(err, queue.ErrEmpty) {
		r.claimFailures = 0
		// Nothing queued: wait briefly, then let the caller try again.
		select {
		case <-ctx.Done():
		case <-time.After(r.idlePoll):
		}
		return false
	}
	if err != nil {
		// Only a genuine failure reaches here: log the first one immediately,
		// then back off so a broken Redis cannot fill the log at 1 line/second.
		r.claimFailures++
		if r.claimFailures == 1 || r.claimFailures%30 == 0 {
			r.log.Warn("claim failed",
				slog.Any("error", err), slog.Int("consecutive_failures", r.claimFailures))
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
		return false
	}

	go func() {
		defer func() { <-sem }()
		r.execute(ctx, jobID)
	}()
	return true
}

// execute runs one claimed job end to end and always releases the claim.
func (r *Runner) execute(ctx context.Context, jobID string) {
	id, err := uuid.Parse(jobID)
	if err != nil {
		r.log.Error("claimed job with invalid id", slog.String("job_id", jobID))
		r.release(context.WithoutCancel(ctx), jobID)
		return
	}

	// Use a fresh context for bookkeeping so a shutdown signal does not leave
	// a job stuck in RUNNING with no run row finished.
	bg := context.WithoutCancel(ctx)

	job, err := r.store.GetJob(bg, id)
	if err != nil {
		r.log.Error("load claimed job", slog.String("job_id", jobID), slog.Any("error", err))
		r.release(bg, jobID)
		return
	}

	if err := r.claim(bg, id, job.Status); err != nil {
		r.log.Warn("job is not claimable, dropping claim",
			slog.String("job_id", jobID), slog.String("status", string(job.Status)), slog.Any("error", err))
		r.release(bg, jobID)
		return
	}

	r.emit(bg, id, domain.StatusRunning, job.Attempt+1, "")

	attempt, err := r.store.IncrementAttempt(bg, id)
	if err != nil {
		r.log.Error("increment attempt", slog.String("job_id", jobID), slog.Any("error", err))
		attempt = job.Attempt + 1
	}

	run, err := r.store.CreateRun(bg, domain.JobRun{
		JobID:    id,
		WorkerID: r.workerID,
		Attempt:  attempt,
	})
	if err != nil {
		r.log.Error("create run", slog.String("job_id", jobID), slog.Any("error", err))
	}

	timeout := time.Duration(job.TimeoutSeconds) * time.Second
	task := Task{
		JobID:   jobID,
		Type:    job.Type,
		Payload: job.Payload,
		Timeout: timeout,
	}

	started := time.Now()
	runErr := r.exec.Execute(ctx, task)
	elapsed := time.Since(started)

	if runErr == nil {
		r.finishSuccess(bg, id, run, elapsed)
	} else {
		r.finishFailure(bg, id, job, run, attempt, runErr, elapsed)
	}

	r.release(bg, jobID)
}

// emit publishes a status change without ever blocking or failing the job.
func (r *Runner) emit(ctx context.Context, jobID uuid.UUID, status domain.Status, attempt int, errMsg string) {
	if r.emitter == nil {
		return
	}
	r.emitter.Emit(ctx, domain.Event{
		JobID:    jobID,
		Status:   status,
		WorkerID: r.workerID,
		Attempt:  attempt,
		At:       time.Now(),
		Error:    errMsg,
	})
}

func (r *Runner) finishSuccess(ctx context.Context, id uuid.UUID, run domain.JobRun, elapsed time.Duration) {
	if run.ID != uuid.Nil {
		if err := r.store.FinishRun(ctx, run.ID, domain.StatusSucceeded, ""); err != nil {
			r.log.Warn("finish run", slog.Any("error", err))
		}
	}
	if err := r.store.UpdateStatus(ctx, id, domain.StatusRunning, domain.StatusSucceeded); err != nil {
		r.log.Warn("mark succeeded", slog.Any("error", err))
	}
	r.emit(ctx, id, domain.StatusSucceeded, run.Attempt, "")
	r.log.Info("job succeeded",
		slog.String("job_id", id.String()),
		slog.Int("attempt", run.Attempt),
		slog.Duration("elapsed", elapsed))
}

func (r *Runner) finishFailure(ctx context.Context, id uuid.UUID, job domain.Job, run domain.JobRun, attempt int, runErr error, elapsed time.Duration) {
	msg := truncate(runErr.Error(), maxPayloadLogLimit)

	if run.ID != uuid.Nil {
		if err := r.store.FinishRun(ctx, run.ID, domain.StatusRetrying, msg); err != nil {
			r.log.Warn("finish failed run", slog.Any("error", err))
		}
	}

	// Retries are counted against max_retries; attempt is 1-based.
	if attempt > job.MaxRetries {
		if err := r.store.UpdateStatus(ctx, id, domain.StatusRunning, domain.StatusDead); err != nil {
			r.log.Warn("mark dead", slog.Any("error", err))
		}
		r.emit(ctx, id, domain.StatusDead, attempt, msg)
		r.log.Error("job dead-lettered",
			slog.String("job_id", id.String()),
			slog.Int("attempt", attempt),
			slog.Int("max_retries", job.MaxRetries),
			slog.String("error", msg))
		return
	}

	// Exponential backoff is applied by the scheduler tick: the job goes to
	// RETRYING with a future run_at, and the scheduler picks it up when due.
	delay := r.Backoff(attempt, 0, 0)
	nextAt := time.Now().Add(delay)
	if err := r.store.UpdateStatus(ctx, id, domain.StatusRunning, domain.StatusRetrying); err != nil {
		r.log.Warn("mark retrying", slog.Any("error", err))
		return
	}
	// Store the delay. Without this the row keeps its old next_run_at and the
	// recovery sweep retries immediately, so the backoff is never applied.
	if err := r.store.SetNextRun(ctx, id, &nextAt); err != nil {
		r.log.Warn("set next run", slog.String("job_id", id.String()), slog.Any("error", err))
	}
	r.emit(ctx, id, domain.StatusRetrying, attempt, msg)
	r.log.Warn("job failed, will retry",
		slog.String("job_id", id.String()),
		slog.Int("attempt", attempt),
		slog.Duration("backoff", delay),
		slog.String("error", msg))
}

// release clears the in-flight marker. Errors are logged, never fatal.
// errNotClaimable marks a claim that was abandoned because the job belongs to
// someone else now. Kept as a sentinel so callers and tests can tell it apart
// from a store failure.
var errNotClaimable = errors.New("not claimable")

// waitingStatuses are the states a job can legitimately be in when the worker is
// handed it. The scheduler enqueues to Redis and only then marks the rows QUEUED
// (so a Redis outage cannot lose a job), and retries take the same path, so a
// fast worker routinely sees PENDING or RETRYING rather than QUEUED. Dropping
// the claim in that window left the job stuck: the queue entry was consumed, and
// the scheduler only re-fires jobs whose next_run_at has passed.
var waitingStatuses = []domain.Status{
	domain.StatusQueued, domain.StatusPending, domain.StatusRetrying,
}

// claim moves a job to RUNNING. Every transition is guarded by the source
// status, so exactly one worker can win: if two queue entries for the same job
// are in flight, only the first UPDATE matches a row. A job in any other state
// (RUNNING, SUCCEEDED, DEAD, CANCELLED) belongs to someone else or is finished,
// and the claim is dropped.
//
// observed is the status just read for the job. It is tried first because it is
// almost always still current; the rest of the waiting states cover the case
// where the scheduler moved the row in between.
func (r *Runner) claim(ctx context.Context, id uuid.UUID, observed domain.Status) error {
	for _, from := range claimOrder(observed) {
		err := r.store.UpdateStatus(ctx, id, from, domain.StatusRunning)
		if err == nil {
			return nil
		}
		if errors.Is(err, store.ErrNotFound) {
			// The job was deleted; no point trying the other states.
			return err
		}
	}
	return fmt.Errorf("%w: could not move the job to RUNNING from any waiting state", errNotClaimable)
}

// claimOrder lists the source statuses to try, observed first when it is one of
// them. A job that is already RUNNING, SUCCEEDED, DEAD or CANCELLED is never a
// candidate, whatever it looked like a moment ago.
func claimOrder(observed domain.Status) []domain.Status {
	order := make([]domain.Status, 0, len(waitingStatuses))
	for _, s := range waitingStatuses {
		if s == observed {
			order = append(order, s)
		}
	}
	for _, s := range waitingStatuses {
		if s != observed {
			order = append(order, s)
		}
	}
	return order
}

func (r *Runner) release(ctx context.Context, jobID string) {
	if err := r.queue.Ack(ctx, r.workerID, jobID); err != nil {
		r.log.Warn("ack failed", slog.String("job_id", jobID), slog.Any("error", err))
	}
}

// exponentialBackoff mirrors recovery.Backoff: base doubling, capped, +<=10% jitter.
func exponentialBackoff(base, maxDelay time.Duration) func(int, time.Duration, time.Duration) time.Duration {
	if base <= 0 {
		base = 2 * time.Second
	}
	if maxDelay <= 0 {
		maxDelay = 5 * time.Minute
	}
	return func(attempt int, b, m time.Duration) time.Duration {
		if b <= 0 {
			b = base
		}
		if m <= 0 {
			m = maxDelay
		}
		if attempt < 1 {
			attempt = 1
		}
		d := b
		for i := 1; i < attempt && d < m; i++ {
			d *= 2
		}
		if d > m {
			d = m
		}
		return d
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
