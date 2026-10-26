package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// Validation limits.
const (
	MinTimeoutSeconds = 1
	MaxTimeoutSeconds = 3600
	MinRetries        = 0
	MaxRetries        = 10
	MaxPayloadBytes   = 64 * 1024
)

// ErrInvalidJob is wrapped by every error returned from Job.Validate.
var ErrInvalidJob = errors.New("invalid job")

// Job is a unit of work, one-off or recurring (when CronExpr is set).
type Job struct {
	ID             uuid.UUID       `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Priority       Priority        `json:"priority"`
	Status         Status          `json:"status"`
	CronExpr       *string         `json:"cron_expr,omitempty"`
	RunAt          time.Time       `json:"run_at"`
	NextRunAt      *time.Time      `json:"next_run_at,omitempty"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	MaxRetries     int             `json:"max_retries"`
	Attempt        int             `json:"attempt"`
	LastError      *string         `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// Validate checks user-supplied fields and returns every problem found,
// joined into one error that wraps ErrInvalidJob.
func (j Job) Validate() error {
	var errs []error
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if j.Type == "" {
		add("type is required")
	}
	if !j.Priority.Valid() {
		add("unknown priority %d (want high, normal or low)", int(j.Priority))
	}
	if j.TimeoutSeconds < MinTimeoutSeconds || j.TimeoutSeconds > MaxTimeoutSeconds {
		add("timeout_seconds %d out of range [%d, %d]", j.TimeoutSeconds, MinTimeoutSeconds, MaxTimeoutSeconds)
	}
	if j.MaxRetries < MinRetries || j.MaxRetries > MaxRetries {
		add("max_retries %d out of range [%d, %d]", j.MaxRetries, MinRetries, MaxRetries)
	}
	if len(j.Payload) > MaxPayloadBytes {
		add("payload is %d bytes, limit is %d", len(j.Payload), MaxPayloadBytes)
	} else if len(j.Payload) > 0 && !json.Valid(j.Payload) {
		add("payload is not valid JSON")
	}
	if j.CronExpr != nil {
		if _, err := cron.ParseStandard(*j.CronExpr); err != nil {
			add("cron_expr %q: %v", *j.CronExpr, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrInvalidJob, errors.Join(errs...))
}
