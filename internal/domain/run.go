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

// MarshalJSON pins every event timestamp to UTC.
//
// time.Time writes whatever location is attached to the value, so an Event that
// came back from the database would serialise as "2026-11-22T18:00:00+05:30" on a
// machine in IST while the same instant on a CI box serialises as
// "2026-11-22T12:30:00Z". Browsers parse both, but the dashboard sorts the
// timeline as text, so one canonical form: RFC 3339, always UTC.
func (e Event) MarshalJSON() ([]byte, error) {
	type wireEvent struct {
		JobID    uuid.UUID `json:"job_id"`
		Status   Status    `json:"status"`
		WorkerID string    `json:"worker_id,omitempty"`
		Attempt  int       `json:"attempt"`
		At       time.Time `json:"at"`
		Error    string    `json:"error,omitempty"`
	}
	return json.Marshal(wireEvent{
		JobID:    e.JobID,
		Status:   e.Status,
		WorkerID: e.WorkerID,
		Attempt:  e.Attempt,
		At:       e.At.UTC(),
		Error:    e.Error,
	})
}
