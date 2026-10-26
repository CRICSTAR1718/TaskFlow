package domain

import "fmt"

// Status is the lifecycle state of a job or job run.
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusQueued    Status = "QUEUED"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusRetrying  Status = "RETRYING"
	StatusDead      Status = "DEAD"
	StatusCancelled Status = "CANCELLED"
)

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusQueued, StatusRunning, StatusSucceeded,
		StatusRetrying, StatusDead, StatusCancelled:
		return true
	}
	return false
}

// Terminal reports whether no further transitions happen from s.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusDead || s == StatusCancelled
}

// Priority orders jobs in the queue; higher runs first.
type Priority int

const (
	PriorityLow    Priority = 1
	PriorityNormal Priority = 2
	PriorityHigh   Priority = 3
)

// String returns high, normal or low.
func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	}
	return fmt.Sprintf("priority(%d)", int(p))
}

// MarshalText encodes the priority as its name in JSON.
func (p Priority) MarshalText() ([]byte, error) {
	if p < PriorityLow || p > PriorityHigh {
		return nil, fmt.Errorf("invalid priority %d", int(p))
	}
	return []byte(p.String()), nil
}

// UnmarshalText decodes high, normal or low.
func (p *Priority) UnmarshalText(b []byte) error {
	switch string(b) {
	case "high":
		*p = PriorityHigh
	case "normal", "":
		*p = PriorityNormal
	case "low":
		*p = PriorityLow
	default:
		return fmt.Errorf("invalid priority %q", b)
	}
	return nil
}
