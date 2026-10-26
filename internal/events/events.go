// Package events publishes and subscribes to job events over Redis pub/sub.
package events

import (
	"context"

	"github.com/your-org/taskflow/internal/domain"
)

// Publisher emits events.
type Publisher interface {
	Publish(ctx context.Context, e domain.Event) error
}

// Subscriber receives events until ctx is cancelled.
type Subscriber interface {
	Subscribe(ctx context.Context) (<-chan domain.Event, error)
}
