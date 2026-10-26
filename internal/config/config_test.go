package config

import (
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr bool
		check   func(t *testing.T, c Config)
	}{
		{
			name: "defaults",
			check: func(t *testing.T, c Config) {
				if c.HTTPAddr != ":8080" {
					t.Errorf("HTTPAddr = %q", c.HTTPAddr)
				}
			},
		},
		{
			name: "overrides",
			env:  map[string]string{"WORKER_CONCURRENCY": "7", "HEARTBEAT_TTL": "30s"},
			check: func(t *testing.T, c Config) {
				if c.WorkerConcurrency != 7 || c.HeartbeatTTL != 30*time.Second {
					t.Errorf("got %d / %s", c.WorkerConcurrency, c.HeartbeatTTL)
				}
			},
		},
		{name: "bad int", env: map[string]string{"MAX_RETRIES": "x"}, wantErr: true},
		{name: "bad duration", env: map[string]string{"BACKOFF_BASE": "soon"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			c, err := Load()
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.check != nil {
				tt.check(t, c)
			}
		})
	}
}
