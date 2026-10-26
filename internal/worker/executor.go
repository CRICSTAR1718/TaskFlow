package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/your-org/taskflow/internal/handlers"
)

// Task is a claimed job ready to run.
type Task struct {
	JobID   string
	Type    string
	Payload []byte
	Timeout time.Duration
}

// Executor runs a Task through its registered handler under a timeout.
type Executor struct {
	log *slog.Logger
}

// NewExecutor creates an Executor.
func NewExecutor(log *slog.Logger) *Executor { return &Executor{log: log} }

// Execute runs the handler, recovering panics into errors.
func (e *Executor) Execute(ctx context.Context, t Task) (err error) {
	h, ok := handlers.Get(t.Type)
	if !ok {
		return fmt.Errorf("no handler for job type %q", t.Type)
	}
	if t.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.Timeout)
		defer cancel()
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	if err := h.Handle(ctx, t.Payload); err != nil {
		return fmt.Errorf("handle %s: %w", t.Type, err)
	}
	return nil
}
