package config

import (
	"strings"
	"testing"
	"time"
)

func lookupFrom(env map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := env[k]; return v, ok }
}

func TestLoadFrom(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string // substring; "" means success
		check   func(t *testing.T, c Config)
	}{
		{
			name: "defaults",
			env:  map[string]string{"API_KEY": "k"},
			check: func(t *testing.T, c Config) {
				if c.WorkerConcurrency != 50 || c.HeartbeatInterval != 5*time.Second ||
					c.HeartbeatTTL != 15*time.Second || c.SchedulerTick != time.Second ||
					c.BackoffBase != 2*time.Second || c.BackoffMax != 5*time.Minute ||
					c.HTTPAddr != ":8080" || c.RedisAddr != "localhost:6379" {
					t.Errorf("unexpected defaults: %+v", c)
				}
				if c.WorkerID == "" || !strings.Contains(c.WorkerID, "-") {
					t.Errorf("WorkerID = %q, want hostname-random", c.WorkerID)
				}
			},
		},
		{
			name: "overrides",
			env: map[string]string{
				"API_KEY": "k", "WORKER_CONCURRENCY": "7", "WORKER_ID": "w1",
				"HEARTBEAT_TTL": "30s", "REDIS_ADDR": "redis:6380", "LOG_LEVEL": "DEBUG",
			},
			check: func(t *testing.T, c Config) {
				if c.WorkerConcurrency != 7 || c.WorkerID != "w1" || c.HeartbeatTTL != 30*time.Second ||
					c.RedisAddr != "redis:6380" || c.LogLevel != "debug" {
					t.Errorf("unexpected: %+v", c)
				}
			},
		},
		{name: "missing api key", env: map[string]string{}, wantErr: "API_KEY is required"},
		{name: "bad int", env: map[string]string{"API_KEY": "k", "WORKER_CONCURRENCY": "x"}, wantErr: "WORKER_CONCURRENCY"},
		{name: "bad duration", env: map[string]string{"API_KEY": "k", "BACKOFF_BASE": "soon"}, wantErr: "BACKOFF_BASE"},
		{name: "concurrency zero", env: map[string]string{"API_KEY": "k", "WORKER_CONCURRENCY": "0"}, wantErr: "WORKER_CONCURRENCY 0"},
		{name: "ttl not above interval", env: map[string]string{"API_KEY": "k", "HEARTBEAT_TTL": "5s"}, wantErr: "HEARTBEAT_TTL"},
		{name: "backoff max below base", env: map[string]string{"API_KEY": "k", "BACKOFF_MAX": "1s"}, wantErr: "BACKOFF_MAX"},
		{name: "redis addr without port", env: map[string]string{"API_KEY": "k", "REDIS_ADDR": "localhost"}, wantErr: "REDIS_ADDR"},
		{name: "bad log level", env: map[string]string{"API_KEY": "k", "LOG_LEVEL": "loud"}, wantErr: "LOG_LEVEL"},
		{name: "empty value uses default", env: map[string]string{"API_KEY": "k", "HTTP_ADDR": ""}, check: func(t *testing.T, c Config) {
			if c.HTTPAddr != ":8080" {
				t.Errorf("HTTPAddr = %q", c.HTTPAddr)
			}
		}},
		{name: "reports all errors", env: map[string]string{"WORKER_CONCURRENCY": "x", "SCHEDULER_TICK": "y"}, wantErr: "SCHEDULER_TICK"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := LoadFrom(lookupFrom(tt.env))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, c)
			}
		})
	}
}

func TestWorkerIDUnique(t *testing.T) {
	env := lookupFrom(map[string]string{"API_KEY": "k"})
	a, _ := LoadFrom(env)
	b, _ := LoadFrom(env)
	if a.WorkerID == b.WorkerID {
		t.Errorf("default worker IDs collide: %q", a.WorkerID)
	}
}
