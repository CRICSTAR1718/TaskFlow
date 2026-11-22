package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/cricstar1718/taskflow/internal/domain"
	"github.com/cricstar1718/taskflow/internal/store"
)

// Defaults. Config carries the same knobs; these only guard against a zero value.
const (
	defaultBatch        = 50
	defaultStaleWorker  = 30 * time.Second
	truncatedErrorLimit = 2000
)

// ErrConflict is re-exported by the store when a status transition loses a race.
// The manager treats it as "someone else owns this job now" and moves on.
var errNotFound = errors.New("recovery: not found")

// Store is the slice of the store the manager needs.
type Store interface {
	// ClaimDue moves up to limit jobs out of PENDING/RETRYING and into RUNNING.
	ClaimDue(ctx context.Context, now time.Time, limit int) ([]store.RecoverableJob, error)
	// FindStaleRunning returns RUNNING jobs whose worker stopped heartbeating.
	FindStaleRunning(ctx context.Context, staleAfter time.Duration, limit int) ([]store.RecoverableJob, error)
	// Requeue transitions a job back to RETRYING/PENDING with a new next_run_at.
	Requeue(ctx context.Context, id uuid.UUID, nextRunAt time.Time, lastError string, terminal bool) error
}

// Queue is the slice of the Redis queue the manager needs.
type Queue interface {
	Enqueue(ctx context.Context, jobID string, p domain.Priority) error
	Requeue(ctx context.Context, jobID string, p domain.Priority) error
	InFlight(ctx context.Context, workerID string) ([]string, error)
	// Ack removes a member from a worker's in-flight set.
	Ack(ctx context.Context, workerID, jobID string) error
	Depths(ctx context.Context) (map[domain.Priority]int64, error)
}

// Workers lists workers and records those that stopped heartbeating.
type Workers interface {
	ListWorkers(ctx context.Context) ([]domain.WorkerInfo, error)
	MarkWorkerDead(ctx context.Context, workerID string) error
}

// Manager runs the recovery sweep.
type Manager struct {
	store   Store
	queue   Queue
	workers Workers
	log     *slog.Logger

	StaleAfter time.Duration
	Batch      int

	// Backoff is swappable for tests. Defaults to Backoff(base,max).
	Backoff func(attempt int) time.Duration

	// Now is swappable for tests.
	Now func() time.Time

	// drained remembers workers already swept, so a dead worker is marked dead
	// in the database once instead of on every tick.
	drained map[string]bool
}

// Config configures a Manager.
type Config struct {
	Store       Store
	Queue       Queue
	Workers     Workers
	Log         *slog.Logger
	StaleAfter  time.Duration
	Batch       int
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

// New builds a Manager.
func New(c Config) *Manager {
	log := c.Log
	if log == nil {
		log = slog.Default()
	}
	stale := c.StaleAfter
	if stale <= 0 {
		stale = defaultStaleWorker
	}
	batch := c.Batch
	if batch <= 0 {
		batch = defaultBatch
	}
	base, maxDelay := c.BackoffBase, c.BackoffMax
	if base <= 0 {
		base = 2 * time.Second
	}
	if maxDelay < base {
		maxDelay = 5 * time.Minute
	}
	m := &Manager{
		store:      c.Store,
		queue:      c.Queue,
		workers:    c.Workers,
		log:        log,
		StaleAfter: stale,
		Batch:      batch,
		Now:        time.Now,
		drained:    map[string]bool{},
	}
	m.Backoff = func(attempt int) time.Duration { return Backoff(attempt, base, maxDelay) }
	return m
}

// Check proves the sweep can actually run: a read, a write-capable statement
// and a live queue. Called once at startup so a misconfigured service never
// looks healthy. Errors here are fatal for the caller.
func (m *Manager) Check(ctx context.Context) error {
	if _, err := m.store.FindStaleRunning(ctx, m.StaleAfter, 1); err != nil {
		return fmt.Errorf("store not ready: %w", err)
	}
	if _, err := m.store.ClaimDue(ctx, m.Now(), 0); err != nil {
		return fmt.Errorf("store not ready: %w", err)
	}
	if _, err := m.queue.Depths(ctx); err != nil {
		return fmt.Errorf("queue not ready: %w", err)
	}
	return nil
}

// sweepFailed logs the first failure loudly and the rest at debug level, so a
// long outage does not fill the log with one warning per tick.
func (m *Manager) sweepFailed(err error, already *bool) {
	if *already {
		m.log.Debug("recovery sweep still failing", slog.Any("error", err))
		return
	}
	m.log.Error("recovery sweep failing - logs are not being retried until this clears",
		slog.Any("error", err))
	*already = true
}

// SweepResult reports what one sweep did. Handy in tests and for /metrics later.
type SweepResult struct {
	Retried   int // RETRYING jobs put back on the queue
	Dead      int // jobs that exhausted max_retries
	Reclaimed int // RUNNING jobs rescued from a dead worker
}

// Sweep performs one full recovery pass. The order matters:
//  1. reclaim jobs whose worker died (they are RUNNING, so nothing else will touch them)
//  2. retry RETRYING jobs whose backoff has elapsed
func (m *Manager) Sweep(ctx context.Context) (SweepResult, error) {
	var res SweepResult

	reclaimed, err := m.reclaimStale(ctx)
	res.Reclaimed = reclaimed
	if err != nil {
		return res, fmt.Errorf("reclaim stale: %w", err)
	}

	retried, dead, err := m.retryDue(ctx)
	res.Retried, res.Dead = retried, dead
	if err != nil {
		return res, fmt.Errorf("retry due: %w", err)
	}

	if res.Reclaimed+res.Retried+res.Dead > 0 {
		m.log.Info("recovery sweep",
			slog.Int("reclaimed", res.Reclaimed),
			slog.Int("retried", res.Retried),
			slog.Int("dead", res.Dead))
	}
	return res, nil
}

// reclaimStale re-queues jobs held by workers that stopped heartbeating.
//
// This is the at-least-once half of the delivery guarantee: a worker that dies
// mid-job leaves its jobs RUNNING forever unless someone rescues them.
func (m *Manager) reclaimStale(ctx context.Context) (int, error) {
	jobs, err := m.store.FindStaleRunning(ctx, m.StaleAfter, m.Batch)
	if err != nil {
		return 0, err
	}

	// Drain dead workers' in-flight sets independently of the RUNNING scan: a
	// worker can die holding a claim with no open run row, and that entry would
	// otherwise leak forever. A late Ack from a zombie process must not
	// resurrect a member in a queue we already re-filled.
	if err := m.drainInFlight(ctx); err != nil {
		m.log.Warn("drain inflight", slog.Any("error", err))
	}

	if len(jobs) == 0 {
		return 0, nil
	}

	n := 0
	for _, j := range jobs {
		if ierr := m.reset(ctx, j, "worker stopped heartbeating", false); ierr != nil {
			m.log.Warn("reclaim job",
				slog.String("job_id", j.ID.String()),
				slog.Any("error", ierr))
			continue
		}
		n++
	}
	return n, nil
}

// retryDue re-queues RETRYING jobs whose backoff elapsed, and dead-letters the
// ones that are out of retries.
func (m *Manager) retryDue(ctx context.Context) (retried, dead int, err error) {
	jobs, err := m.store.ClaimDue(ctx, m.Now(), m.Batch)
	if err != nil {
		return 0, 0, err
	}

	for _, j := range jobs {
		// Out of retries: DEAD, never queued again.
		if j.Attempt >= j.MaxRetries && j.MaxRetries > 0 {
			if derr := m.store.Requeue(ctx, j.ID, m.Now(), lastErr(j), true); derr != nil {
				m.log.Warn("dead-letter job",
					slog.String("job_id", j.ID.String()),
					slog.Any("error", derr))
				continue
			}
			m.log.Warn("job dead-lettered",
				slog.String("job_id", j.ID.String()),
				slog.Int("attempt", j.Attempt),
				slog.Int("max_retries", j.MaxRetries),
				slog.String("last_error", lastErr(j)))
			dead++
			continue
		}

		priority := j.Priority
		if priority == 0 {
			priority = domain.PriorityNormal
		}
		// Enqueue before the status write, for the same reason the scheduler
		// does it in that order: if Redis is down the row stays RETRYING and
		// the next sweep tries again.
		if qerr := m.queue.Enqueue(ctx, j.ID.String(), priority); qerr != nil {
			m.log.Warn("requeue failed, will retry next sweep",
				slog.String("job_id", j.ID.String()),
				slog.Any("error", qerr))
			continue
		}
		if serr := m.store.Requeue(ctx, j.ID, m.nextRun(j), lastErr(j), false); serr != nil {
			if errors.Is(serr, errNotFound) {
				m.log.Debug("job vanished before it could be re-queued", slog.String("job_id", j.ID.String()))
				continue
			}
			m.log.Warn("mark retrying",
				slog.String("job_id", j.ID.String()),
				slog.Any("error", serr))
			continue
		}
		m.log.Info("job retried",
			slog.String("job_id", j.ID.String()),
			slog.Int("attempt", j.Attempt),
			slog.Time("run_at", m.nextRun(j)))
		retried++
	}
	return retried, dead, nil
}

// reset puts a RUNNING job back in the pool after its worker died.
func (m *Manager) reset(ctx context.Context, j store.RecoverableJob, reason string, terminal bool) error {
	priority := j.Priority
	if priority == 0 {
		priority = domain.PriorityNormal
	}
	if !terminal {
		if err := m.queue.Enqueue(ctx, j.ID.String(), priority); err != nil {
			return fmt.Errorf("enqueue: %w", err)
		}
	}
	if err := m.store.Requeue(ctx, j.ID, m.Now(), reason, terminal); err != nil {
		return fmt.Errorf("requeue: %w", err)
	}
	m.log.Info("job reclaimed from a dead worker",
		slog.String("job_id", j.ID.String()),
		slog.String("worker_id", j.WorkerID),
		slog.String("reason", reason))
	if m.workers != nil && j.WorkerID != "" {
		if err := m.workers.MarkWorkerDead(ctx, j.WorkerID); err != nil {
			m.log.Debug("mark worker dead", slog.String("worker_id", j.WorkerID), slog.Any("error", err))
		}
	}
	return nil
}

// drainInFlight clears in-flight sets for workers that are no longer alive, so
// their claimed jobs cannot be double-counted.
func (m *Manager) drainInFlight(ctx context.Context) error {
	if m.workers == nil {
		return nil
	}
	workers, err := m.workers.ListWorkers(ctx)
	if err != nil {
		return err
	}
	cutoff := m.Now().Add(-m.StaleAfter)
	for _, w := range workers {
		if w.LastHeartbeat.After(cutoff) {
			continue
		}
		if m.drained[w.ID] {
			continue
		}
		ids, err := m.queue.InFlight(ctx, w.ID)
		if err != nil {
			return fmt.Errorf("inflight %s: %w", w.ID, err)
		}
		for _, id := range ids {
			if aerr := m.queue.Requeue(ctx, id, domain.PriorityNormal); aerr != nil {
				return fmt.Errorf("requeue inflight %s: %w", id, aerr)
			}
			// Clear the member, or the same job is re-queued on every sweep and
			// the dead worker's set never empties.
			if derr := m.queue.Ack(ctx, w.ID, id); derr != nil {
				return fmt.Errorf("clear inflight %s of %s: %w", id, w.ID, derr)
			}
		}
		if len(ids) > 0 {
			m.log.Info("drained dead worker in-flight set",
				slog.String("worker_id", w.ID),
				slog.Int("jobs", len(ids)))
		}
		m.drained[w.ID] = true
	}
	return nil
}

// nextRun schedules the retry with exponential backoff.
func (m *Manager) nextRun(j store.RecoverableJob) time.Time {
	return m.Now().Add(m.Backoff(j.Attempt))
}

func lastErr(j store.RecoverableJob) string {
	if j.LastError == nil {
		return "attempt failed"
	}
	s := *j.LastError
	if len(s) > truncatedErrorLimit {
		return s[:truncatedErrorLimit] + "..."
	}
	return s
}
