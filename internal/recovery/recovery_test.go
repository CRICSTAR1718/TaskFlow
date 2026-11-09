package recovery

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	base, maxBackoff := time.Second, 10*time.Second
	tests := []struct {
		attempt int
		min     time.Duration
	}{
		{1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second}, {10, maxBackoff},
	}
	for _, tt := range tests {
		got := Backoff(tt.attempt, base, maxBackoff)
		if got < tt.min || got > tt.min+tt.min/10 {
			t.Errorf("attempt %d: got %s, want ~%s", tt.attempt, got, tt.min)
		}
	}
}
