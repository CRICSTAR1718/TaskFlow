package domain

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
	_, ok := transitions[s]
	return ok
}

// Terminal reports whether no transition leaves s.
func (s Status) Terminal() bool {
	to, ok := transitions[s]
	return ok && len(to) == 0
}

// transitions lists the allowed next states for each status.
//
//   - RUNNING→QUEUED re-queues a job whose worker died.
//   - SUCCEEDED→PENDING reschedules a cron job for its next run.
var transitions = map[Status][]Status{
	StatusPending:   {StatusQueued, StatusCancelled},
	StatusQueued:    {StatusRunning, StatusCancelled},
	StatusRunning:   {StatusSucceeded, StatusRetrying, StatusDead, StatusQueued, StatusCancelled},
	StatusRetrying:  {StatusQueued, StatusDead, StatusCancelled},
	StatusSucceeded: {StatusPending},
	StatusDead:      {},
	StatusCancelled: {},
}

// CanTransition reports whether a job may move from one status to another.
func CanTransition(from, to Status) bool {
	for _, next := range transitions[from] {
		if next == to {
			return true
		}
	}
	return false
}
