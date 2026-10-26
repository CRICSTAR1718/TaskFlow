package handlers

import (
	"context"
	"testing"
	"time"
)

func TestSampleHandlers(t *testing.T) {
	tests := []struct {
		name    string
		typ     string
		payload string
		timeout time.Duration
		wantErr bool
	}{
		{"echo", "echo", `{}`, time.Second, false},
		{"sleep ok", "sleep", `{"seconds":0.01}`, time.Second, false},
		{"sleep timeout", "sleep", `{"seconds":5}`, 20 * time.Millisecond, true},
		{"sleep bad payload", "sleep", `nope`, time.Second, true},
		{"fail", "fail", `{}`, time.Second, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, ok := Get(tt.typ)
			if !ok {
				t.Fatalf("handler %q not registered", tt.typ)
			}
			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()
			if err := h.Handle(ctx, []byte(tt.payload)); (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetUnknown(t *testing.T) {
	if _, ok := Get("nope"); ok {
		t.Error("expected miss")
	}
}
