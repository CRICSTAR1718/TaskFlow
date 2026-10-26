// Package events publishes and subscribes to job/worker events over Redis pub/sub.
package events

import (
	"context"
	"time"

	"github.com/your-org/taskflow/internal/domain"
)

// Event is broadcast on taskflow:events and forwarded to dashboards.
type Event struct {
	Type     string        `json:"type"` // e.g. job.status_changed, worker.dead
	JobID    string        `json:"job_id,omitempty"`
	WorkerID string        `json:"worker_id,omitempty"`
	Status   domain.Status `json:"status,omitempty"`
	At       time.Time     `json:"at"`
}

// Publisher emits events.
type Publisher interface {
	Publish(ctx context.Context, e Event) error
}

// Subscriber receives events until ctx is cancelled.
type Subscriber interface {
	Subscribe(ctx context.Context) (<-chan Event, error)
}
