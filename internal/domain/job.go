// Package domain defines the core TaskFlow types shared by every service.
package domain

import (
	"encoding/json"
	"time"
)

// Job is a unit of work, one-off or recurring (cron).
type Job struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Priority       Priority        `json:"priority"`
	Status         Status          `json:"status"`
	CronExpr       string          `json:"cron_expr,omitempty"`
	NextRunAt      time.Time       `json:"next_run_at"`
	Attempts       int             `json:"attempts"`
	MaxRetries     int             `json:"max_retries"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	LastError      string          `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// JobRun records a single execution attempt of a Job.
type JobRun struct {
	ID         string     `json:"id"`
	JobID      string     `json:"job_id"`
	WorkerID   string     `json:"worker_id"`
	Attempt    int        `json:"attempt"`
	Status     Status     `json:"status"`
	Error      string     `json:"error,omitempty"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// Worker describes a registered worker process.
type Worker struct {
	ID            string    `json:"id"`
	Hostname      string    `json:"hostname"`
	Concurrency   int       `json:"concurrency"`
	Alive         bool      `json:"alive"`
	StartedAt     time.Time `json:"started_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}
