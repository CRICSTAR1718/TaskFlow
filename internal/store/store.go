// Package store is the Postgres repository for jobs, runs and workers.
package store

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/your-org/taskflow/internal/domain"
)

// ErrNotFound is returned when a row does not exist.
var ErrNotFound = errors.New("not found")

// Store persists TaskFlow state in PostgreSQL.
// The pgx pool is wired in later (go get github.com/jackc/pgx/v5).
type Store struct {
	dsn string
}

// New creates a Store for the given connection string.
func New(dsn string) *Store { return &Store{dsn: dsn} }

// Ping verifies connectivity.
func (s *Store) Ping(ctx context.Context) error { return ctx.Err() }

// Close releases resources.
func (s *Store) Close() {}

// GetJob loads a job by ID.
func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	return domain.Job{}, ErrNotFound
}
