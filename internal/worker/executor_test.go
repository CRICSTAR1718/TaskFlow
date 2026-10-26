package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	_ "github.com/your-org/taskflow/internal/handlers" // registers sample handlers
)

func TestExecute(t *testing.T) {
	e := NewExecutor(slog.New(slog.NewTextHandler(io.Discard, nil)))
	tests := []struct {
		name    string
		task    Task
		wantErr bool
	}{
		{"ok", Task{Type: "echo", Payload: []byte(`{}`)}, false},
		{"unknown type", Task{Type: "nope"}, true},
		{"handler error", Task{Type: "fail"}, true},
		{"timeout", Task{Type: "sleep", Payload: []byte(`{"seconds":5}`), Timeout: 20 * time.Millisecond}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := e.Execute(context.Background(), tt.task); (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
