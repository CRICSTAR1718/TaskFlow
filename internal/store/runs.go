package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/cricstar1718/taskflow/internal/domain"
)

// Runs returns every attempt recorded for a job, newest first. The API uses it
// to answer "what happened to this job", which is the question people actually
// ask when something fails.
func (s *Store) Runs(ctx context.Context, jobID uuid.UUID) ([]domain.JobRun, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, job_id, worker_id, attempt, status, error, started_at, finished_at
		 FROM job_runs
		 WHERE job_id = $1::uuid
		 ORDER BY started_at DESC, attempt DESC`,
		jobID.String())
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()

	out := []domain.JobRun{}
	for rows.Next() {
		var r domain.JobRun
		var status, errMsg string
		if err := rows.Scan(&r.ID, &r.JobID, &r.WorkerID, &r.Attempt, &status, &errMsg,
			&r.StartedAt, &r.FinishedAt); err != nil {
			return nil, fmt.Errorf("list runs: %w", err)
		}
		r.Status = domain.Status(status)
		if errMsg != "" {
			r.Error = &errMsg
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	return out, nil
}

// RetryJob resets a DEAD job so it runs again: status PENDING, attempt back to
// zero, next_run_at now and last_error cleared. Only DEAD jobs qualify: retrying
// a SUCCEEDED job would hide a bug, and retrying a live one would duplicate work.
//
// Resetting attempt is the part that is easy to forget. Without it a retried job
// is already over its max_retries budget and dead-letters on its first failure,
// which makes the endpoint look like it did nothing.
func (s *Store) RetryJob(ctx context.Context, id uuid.UUID) error {
	tag, err := s.db.Exec(ctx,
		`UPDATE jobs
		 SET status = 'PENDING', attempt = 0, next_run_at = clock_timestamp(),
		     last_error = '', updated_at = clock_timestamp()
		 WHERE id = $1::uuid AND status = 'DEAD'`,
		id.String())
	if err != nil {
		return fmt.Errorf("retry job: %w", err)
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
