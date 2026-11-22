package worker

import (
	"context"
	"errors"

	"github.com/cricstar1718/taskflow/internal/handlers"
)

// Test handlers are registered once, through a sync.Once, because the registry
// is global and Register panics on a duplicate name.
type okHandler struct{}

func (okHandler) Handle(context.Context, []byte) error { return nil }

type failHandler struct{}

func (failHandler) Handle(context.Context, []byte) error { return errors.New("boom") }

func init() {
	registerIfAbsent("test.ok", okHandler{})
	registerIfAbsent("test.fail", failHandler{})
}

func registerIfAbsent(name string, h handlers.JobHandler) {
	if _, ok := handlers.Get(name); ok {
		return
	}
	handlers.Register(name, h)
}
