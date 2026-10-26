package queue

import (
	"testing"

	"github.com/your-org/taskflow/internal/domain"
)

func TestKeys(t *testing.T) {
	tests := []struct{ got, want string }{
		{Key(domain.PriorityHigh), "taskflow:queue:high"},
		{Key(domain.PriorityLow), "taskflow:queue:low"},
		{InflightKey("w1"), "taskflow:inflight:w1"},
		{HeartbeatKey("w1"), "taskflow:hb:w1"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("got %q want %q", tt.got, tt.want)
		}
	}
}
