// Package scheduler polls Postgres for due jobs and enqueues them into Redis.
//
// One scheduler runs at a time. It holds the taskflow:lock:scheduler key for
// the lifetime of each tick so that, if several replicas start, only one of
// them claims work. Inside a tick it:
//
//  1. takes the lock (SET NX PX, released on exit)
//  2. fetches a batch of due jobs with SELECT ... FOR UPDATE SKIP LOCKED
//  3. enqueues each one into its priority queue in Redis
//  4. marks the batch QUEUED in Postgres
//  5. computes next_run_at for cron jobs and returns them to PENDING
//
// Step 4 happens after step 3 on purpose: if the process dies between the two,
// the job is still PENDING with a due run_at, so the next tick picks it up
// again. The reverse order would lose the job.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"

	"github.com/cricstar1718/taskflow/internal/domain"
	"github.com/cricstar1718/taskflow/internal/queue"
)

// BatchSize is how many due jobs one tick will claim.
const BatchSize = 100

// LockTTL bounds how long a crashed scheduler can block another one.
const LockTTL = 10 * time.Second

// Store is the slice of the Postgres store the scheduler needs. Declared here,
// at the consumer, per the project's interface guidance.
type Store interface {
	FetchDueJobs(ctx context.Context, now time.Time, limit int) ([]domain.Job, error)
	MarkQueued(ctx context.Context, ids []uuid.UUID) error
	SetNextRun(ctx context.Context, id uuid.UUID, nextRunAt *time.Time) error
	UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) error
}

// Enqueuer is the slice of the Redis queue the scheduler needs.
type Enqueuer interface {
	Enqueue(ctx context.Context, jobID string, p domain.Priority) error
}

// Locker guards against two schedulers running the same tick.
type Locker interface {
	// TryLock returns (true, release, nil) when the lock was acquired.
	TryLock(ctx context.Context, key string, ttl time.Duration) (bool, func(), error)
}

// Scheduler owns the polling loop.
type Scheduler struct {
	store  Store
	queue  Enqueuer
	locker Locker
	log    *slog.Logger

	// ParseCron is swappable in tests; defaults to a standard 5-field parser
	// with descriptors (@every, @daily, ...).
	ParseCron func(spec string) (Schedule, error)

	now func() time.Time
}

// Schedule is the subset of cron.Schedule we use.
type Schedule interface {
	Next(t time.Time) time.Time
}

// New builds a Scheduler.
func New(st Store, q Enqueuer, l Locker, log *slog.Logger) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	return &Scheduler{
		store:  st,
		queue:  q,
		locker: l,
		log:    log,
		ParseCron: func(spec string) (Schedule, error) {
			s, err := parser.Parse(spec)
			if err != nil {
				return nil, fmt.Errorf("parse cron %q: %w", spec, err)
			}
			return s, nil
		},
		now: time.Now,
	}
}

// Run polls until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context, tick time.Duration) error {
	if tick <= 0 {
		tick = time.Second
	}
	s.log.Info("scheduler polling", slog.Duration("tick", tick), slog.Int("batch", BatchSize))

	t := time.NewTicker(tick)
	defer t.Stop()

	// Do one pass immediately so a fresh start does not wait a full tick.
	s.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("scheduler stopping")
			return nil
		case <-t.C:
			s.tick(ctx)
		}
	}
}

// tick runs one scheduling pass. Errors are logged, never fatal: a transient
// database or Redis hiccup must not kill the loop.
func (s *Scheduler) tick(ctx context.Context) {
	if s.locker != nil {
		ok, release, err := s.locker.TryLock(ctx, queue.SchedulerLock, LockTTL)
		if err != nil {
			s.log.Warn("scheduler lock error", slog.Any("error", err))
			return
		}
		if !ok {
			s.log.Debug("scheduler lock held elsewhere, skipping tick")
			return
		}
		defer release()
	}

	n, err := s.RunOnce(ctx)
	if err != nil {
		s.log.Error("scheduler tick failed", slog.Any("error", err))
		return
	}
	if n > 0 {
		s.log.Info("scheduled jobs", slog.Int("count", n))
	}
}

// RunOnce performs a single scheduling pass and returns how many jobs were
// enqueued. It is exported so tests and one-shot scripts can drive it.
func (s *Scheduler) RunOnce(ctx context.Context) (int, error) {
	now := s.now()

	due, err := s.store.FetchDueJobs(ctx, now, BatchSize)
	if err != nil {
		return 0, fmt.Errorf("fetch due jobs: %w", err)
	}
	if len(due) == 0 {
		return 0, nil
	}

	enqueued := make([]uuid.UUID, 0, len(due))
	var errs []error

	for _, j := range due {
		// A cron job is a template: the same row runs again later, so it is
		// rescheduled rather than moved to QUEUED and forgotten.
		if j.CronExpr != nil && *j.CronExpr != "" {
			if err := s.reschedule(ctx, j, now); err != nil {
				errs = append(errs, err)
				continue
			}
			enqueued = append(enqueued, j.ID)
			continue
		}

		if err := s.queue.Enqueue(ctx, j.ID.String(), j.Priority); err != nil {
			errs = append(errs, fmt.Errorf("enqueue %s: %w", j.ID, err))
			continue
		}
		enqueued = append(enqueued, j.ID)
	}

	// Mark QUEUED only after the enqueue succeeded, so nothing is lost if
	// Redis was down: the row stays PENDING and the next tick retries.
	if len(enqueued) > 0 {
		if err := s.store.MarkQueued(ctx, enqueued); err != nil {
			return len(enqueued), fmt.Errorf("mark queued: %w", err)
		}
	}
	if len(errs) > 0 {
		return len(enqueued), fmt.Errorf("%d job(s) failed: %w", len(errs), errors.Join(errs...))
	}
	return len(enqueued), nil
}

// reschedule enqueues a cron job now and moves its next_run_at forward,
// leaving it PENDING so the next tick can pick it up again.
func (s *Scheduler) reschedule(ctx context.Context, j domain.Job, now time.Time) error {
	if err := s.queue.Enqueue(ctx, j.ID.String(), j.Priority); err != nil {
		return fmt.Errorf("enqueue cron job %s: %w", j.ID, err)
	}

	sched, err := s.ParseCron(*j.CronExpr)
	if err != nil {
		// A bad cron expression must not spin: park the job as DEAD.
		if terr := s.store.UpdateStatus(ctx, j.ID, j.Status, domain.StatusDead); terr != nil {
			return fmt.Errorf("cron job %s: %w (and marking dead failed: %w)", j.ID, err, terr)
		}
		return fmt.Errorf("cron job %s: %w (marked DEAD)", j.ID, err)
	}

	next := sched.Next(now)
	if err := s.store.SetNextRun(ctx, j.ID, &next); err != nil {
		return fmt.Errorf("set next run %s: %w", j.ID, err)
	}
	// Cron rows stay PENDING between runs; MarkQueued must not flip them.
	return nil
}

// Config wires the production dependencies for Run.
type Config struct {
	Store  Store
	Queue  Enqueuer
	Locker Locker
	Log    *slog.Logger
	Tick   time.Duration
}

// DefaultTick is used when the caller does not pass one.
const DefaultTick = time.Second

// Start is the convenience used by cmd/scheduler: it builds a Scheduler and
// polls until ctx is cancelled.
func Start(ctx context.Context, cfg Config) error {
	if cfg.Store == nil || cfg.Queue == nil {
		return errors.New("scheduler: store and queue are required")
	}
	tick := cfg.Tick
	if tick <= 0 {
		tick = DefaultTick
	}
	s := New(cfg.Store, cfg.Queue, cfg.Locker, cfg.Log)
	return s.Run(ctx, tick)
}
