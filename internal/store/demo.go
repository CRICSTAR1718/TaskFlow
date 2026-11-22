package store

import (
	"context"
	"fmt"
	"time"

	"github.com/cricstar1718/taskflow/internal/domain"
)

// PriorityCompletion answers "how quickly does this priority finish?" - the
// number the Phase II demo puts on screen to prove high priority really is
// served first.
type PriorityCompletion struct {
	Priority domain.Priority
	// Jobs is how many jobs of this priority reached SUCCEEDED.
	Jobs int64
	// AvgSecondsToFinish is the mean wall-clock time from a job being created to
	// its first successful run finishing. It includes queue wait, which is
	// exactly what priority is supposed to change.
	AvgSecondsToFinish float64
	// AvgRunSeconds is the mean time the handler itself took, so the two can be
	// told apart: a difference here would mean the handlers differ, not the
	// scheduling.
	AvgRunSeconds float64
}

// CompletionByPriority reports completion statistics per priority for jobs that
// reached SUCCEEDED. Jobs that never finished are excluded on purpose: mixing
// failures in would make a priority look slow because of retries.
func (s *Store) CompletionByPriority(ctx context.Context) ([]PriorityCompletion, error) {
	rows, err := s.db.Query(ctx, `
		WITH first_success AS (
			SELECT DISTINCT ON (job_id) job_id, started_at, finished_at
			FROM job_runs
			WHERE status = 'SUCCEEDED' AND finished_at IS NOT NULL
			ORDER BY job_id, started_at
		)
		SELECT j.priority,
		       count(*) AS jobs,
		       COALESCE(AVG(EXTRACT(EPOCH FROM (f.finished_at - j.created_at))), 0) AS secs_to_finish,
		       COALESCE(AVG(EXTRACT(EPOCH FROM (f.finished_at - f.started_at))), 0) AS run_secs
		FROM jobs j
		JOIN first_success f ON f.job_id = j.id
		WHERE j.status = 'SUCCEEDED'
		GROUP BY j.priority
		ORDER BY j.priority DESC`)
	if err != nil {
		return nil, fmt.Errorf("completion by priority: %w", err)
	}
	defer rows.Close()

	var out []PriorityCompletion
	for rows.Next() {
		var raw int16
		var pc PriorityCompletion
		if err := rows.Scan(&raw, &pc.Jobs, &pc.AvgSecondsToFinish, &pc.AvgRunSeconds); err != nil {
			return nil, fmt.Errorf("completion by priority: %w", err)
		}
		pc.Priority = domain.Priority(raw)
		out = append(out, pc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("completion by priority: %w", err)
	}
	return out, nil
}

// DemoSnapshot is the single row of facts the demo needs after hard data has
// been collected.
type DemoSnapshot struct {
	JobsByStatus map[domain.Status]int64
	// RecoveredJobs counts jobs that succeeded after being reclaimed from a
	// worker that stopped heartbeating - the proof that recovery works.
	RecoveredJobs int64
	// RunsLastHour and AvgRunDuration mirror Stats, kept here so the demo needs
	// one round trip.
	RunsLastHour     int64
	AvgRunDurationMs int64
}

// SnapshotForDemo gathers the end-of-demo numbers.
func (s *Store) SnapshotForDemo(ctx context.Context) (DemoSnapshot, error) {
	st, err := s.Stats(ctx)
	if err != nil {
		return DemoSnapshot{}, err
	}

	var recovered int64
	// A job that was reclaimed carries the reason recovery wrote into last_error.
	// It is the only durable trace that a worker died mid-job.
	if err := s.db.QueryRow(ctx, `
		SELECT count(*) FROM jobs
		WHERE status = 'SUCCEEDED' AND last_error LIKE '%heartbeat%'`).Scan(&recovered); err != nil {
		return DemoSnapshot{}, fmt.Errorf("recovered jobs: %w", err)
	}

	return DemoSnapshot{
		JobsByStatus:     st.JobsByStatus,
		RecoveredJobs:    recovered,
		RunsLastHour:     st.RunsLastHour,
		AvgRunDurationMs: st.AvgRunDurationLastHour.Milliseconds(),
	}, nil
}

// WaitForJobs blocks until every job submitted before the demo started has left
// the active states, or the deadline passes. Returns the number still active so
// the caller can report honestly instead of pretending everything finished.
func (s *Store) WaitForJobs(ctx context.Context, deadline time.Duration) (active int64, err error) {
	end := time.Now().Add(deadline)
	for {
		// cron_expr IS NULL: a recurring job is a template that stays PENDING
		// between runs by design, so counting it would never let the drain reach
		// zero. Everything that is actually work in flight is still counted.
		err := s.db.QueryRow(ctx, `
			SELECT count(*) FROM jobs
			WHERE status IN ('PENDING','QUEUED','RUNNING','RETRYING')
			  AND cron_expr IS NULL`).Scan(&active)
		if err != nil {
			return 0, fmt.Errorf("count active jobs: %w", err)
		}
		if active == 0 || time.Now().After(end) {
			return active, nil
		}
		select {
		case <-ctx.Done():
			return active, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ResetDemoData empties the job tables so a run starts from zero. Only used by
// the demo, and never by anything that runs in production paths.
func (s *Store) ResetDemoData(ctx context.Context) error {
	// Deliberately NOT truncating workers: a worker's liveness is not the demo's
	// data to clear. Wiping it made every running job look abandoned and recovery
	// reclaimed jobs that were executing perfectly well.
	if _, err := s.db.Exec(ctx, `TRUNCATE job_runs, jobs CASCADE`); err != nil {
		return fmt.Errorf("reset demo data: %w", err)
	}
	return nil
}

// RecoveredCount is how many jobs succeeded after being reclaimed from a worker
// that stopped heartbeating.
func (s *Store) RecoveredCount(ctx context.Context) (int64, error) {
	var n int64
	if err := s.db.QueryRow(ctx, `
		SELECT count(*) FROM jobs
		WHERE status = 'SUCCEEDED' AND last_error LIKE '%heartbeat%'`).Scan(&n); err != nil {
		return 0, fmt.Errorf("recovered count: %w", err)
	}
	return n, nil
}

// ActiveJobs is how much real work is still waiting or executing. Cron templates
// are excluded: they stay PENDING between fires by design and never drain.
func (s *Store) ActiveJobs(ctx context.Context) (int64, error) {
	var n int64
	if err := s.db.QueryRow(ctx, `
		SELECT count(*) FROM jobs
		WHERE status IN ('PENDING','QUEUED','RUNNING','RETRYING')
		  AND cron_expr IS NULL`).Scan(&n); err != nil {
		return 0, fmt.Errorf("active jobs: %w", err)
	}
	return n, nil
}

// InFlightCount is how many jobs a worker claims to be running right now.
func (s *Store) InFlightCount(ctx context.Context, workerID string) (int64, error) {
	var n int64
	if err := s.db.QueryRow(ctx, `
		SELECT count(*) FROM jobs
		WHERE status = 'RUNNING' AND id IN (
			SELECT job_id FROM job_runs WHERE worker_id = $1 AND status = 'RUNNING')`,
		workerID).Scan(&n); err != nil {
		return 0, fmt.Errorf("in-flight count: %w", err)
	}
	return n, nil
}

// InFlightForWorker returns the worker id and the job ids it is running.
func (s *Store) InFlightForWorker(ctx context.Context, workerID string) (string, []string, error) {
	rows, err := s.db.Query(ctx, `
		SELECT DISTINCT j.id::text
		FROM jobs j
		JOIN job_runs r ON r.job_id = j.id
		WHERE j.status = 'RUNNING' AND r.worker_id = $1 AND r.status = 'RUNNING'`,
		workerID)
	if err != nil {
		return "", nil, fmt.Errorf("in-flight jobs: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return "", nil, fmt.Errorf("in-flight jobs: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return "", nil, fmt.Errorf("in-flight jobs: %w", err)
	}
	return workerID, ids, nil
}
