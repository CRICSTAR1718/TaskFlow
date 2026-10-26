// Package handlers defines the JobHandler interface and its registry.
package handlers

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// JobHandler executes one job type. Implementations must honour ctx
// cancellation, which carries the job's timeout.
type JobHandler interface {
	Handle(ctx context.Context, payload []byte) error
}

// HandlerFunc adapts a function to JobHandler.
type HandlerFunc func(ctx context.Context, payload []byte) error

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, payload []byte) error { return f(ctx, payload) }

// The handler registry is the one piece of package-level state in TaskFlow.
var (
	mu       sync.RWMutex
	registry = map[string]JobHandler{}
)

// Register adds a handler for a job type. It panics on duplicates, since that
// is a programming error caught at start-up.
func Register(jobType string, h JobHandler) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := registry[jobType]; dup {
		panic(fmt.Sprintf("handlers: duplicate registration for %q", jobType))
	}
	registry[jobType] = h
}

// Get returns the handler for a job type.
func Get(jobType string) (JobHandler, bool) {
	mu.RLock()
	defer mu.RUnlock()
	h, ok := registry[jobType]
	return h, ok
}

// Types lists registered job types in sorted order.
func Types() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
