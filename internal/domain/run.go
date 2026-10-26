package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// JobRun records one execution attempt of a Job.
type JobRun struct {
	ID         uuid.UUID       `json:"id"`
	JobID      uuid.UUID       `json:"job_id"`
	WorkerID   string          `json:"worker_id"`
	Attempt    int             `json:"attempt"`
	Status     Status          `json:"status"`
	StartedAt  time.Time       `json:"started_at"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
	Error      *string         `json:"error,omitempty"`
	Output     json.RawMessage `json:"output,omitempty"`
}

// WorkerInfo describes a registered worker process.
type WorkerInfo struct {
	ID            string    `json:"id"`
	Hostname      string    `json:"hostname"`
	Concurrency   int       `json:"concurrency"`
	StartedAt     time.Time `json:"started_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// Event is published on taskflow:events whenever a job changes status.
type Event struct {
	JobID    uuid.UUID `json:"job_id"`
	Status   Status    `json:"status"`
	WorkerID string    `json:"worker_id,omitempty"`
	Attempt  int       `json:"attempt"`
	At       time.Time `json:"at"`
	Error    string    `json:"error,omitempty"`
}
