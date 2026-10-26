#!/usr/bin/env bash
# setup.sh — creates the TaskFlow monorepo skeleton.
# Usage: MODULE=github.com/<org>/taskflow ./setup.sh [target-dir]
# Only the Go standard library is used so `go build ./...` works offline.
set -euo pipefail

MODULE="${MODULE:-github.com/your-org/taskflow}"
ROOT="${1:-.}"
mkdir -p "$ROOT"
cd "$ROOT"

mkdir -p cmd/{api,scheduler,worker,recovery} \
  internal/{config,domain,store,queue,scheduler,worker,handlers,recovery,api,ws,events,logging} \
  migrations web deploy scripts/{loadtest,seed} docs .github/workflows

# ---------------------------------------------------------------- go.mod
cat > go.mod <<'EOF'
module __MODULE__

go 1.22
EOF

# ---------------------------------------------------------------- cmd/*
gen_main() { # $1=service  $2=body between "starting" and shutdown
  cat > "cmd/$1/main.go" <<'EOF'
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"__MODULE__/internal/config"
	"__MODULE__/internal/logging"
	"__MODULE__/internal/__PKG__"
)

const service = "__SVC__"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", service, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := logging.New(cfg.LogLevel).With(slog.String("service", service))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("starting " + service)

	if err := __PKG__.Run(ctx, cfg, log); err != nil {
		return fmt.Errorf("%s: %w", service, err)
	}

	log.Info("stopped " + service)
	return nil
}
EOF
  sed -i "s/__SVC__/$1/g; s/__PKG__/$2/g" "cmd/$1/main.go"
}
gen_main api api
gen_main scheduler scheduler
gen_main worker worker
gen_main recovery recovery

# ---------------------------------------------------------------- config
cat > internal/config/config.go <<'EOF'
// Package config loads service configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds every setting used by the TaskFlow services.
type Config struct {
	Env      string
	LogLevel string

	HTTPAddr string
	APIKey   string

	DatabaseURL string
	RedisURL    string

	WorkerID          string
	WorkerConcurrency int
	HeartbeatInterval time.Duration
	HeartbeatTTL      time.Duration

	SchedulerPollInterval time.Duration
	SchedulerBatchSize    int
	SchedulerLockTTL      time.Duration

	RecoveryInterval time.Duration
	MaxRetries       int
	BackoffBase      time.Duration
	BackoffMax       time.Duration

	ShutdownTimeout time.Duration
}

// Load reads configuration from the environment, applying defaults.
func Load() (Config, error) {
	var errs []error
	str := func(key, def string) string {
		if v, ok := os.LookupEnv(key); ok && v != "" {
			return v
		}
		return def
	}
	integer := func(key string, def int) int {
		v := str(key, "")
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
			return def
		}
		return n
	}
	dur := func(key string, def time.Duration) time.Duration {
		v := str(key, "")
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
			return def
		}
		return d
	}

	host, _ := os.Hostname()

	cfg := Config{
		Env:      str("APP_ENV", "development"),
		LogLevel: str("LOG_LEVEL", "info"),

		HTTPAddr: str("HTTP_ADDR", ":8080"),
		APIKey:   str("API_KEY", "dev-api-key"),

		DatabaseURL: str("DATABASE_URL", "postgres://taskflow:taskflow@localhost:5432/taskflow?sslmode=disable"),
		RedisURL:    str("REDIS_URL", "redis://localhost:6379/0"),

		WorkerID:          str("WORKER_ID", host),
		WorkerConcurrency: integer("WORKER_CONCURRENCY", 50),
		HeartbeatInterval: dur("HEARTBEAT_INTERVAL", 5*time.Second),
		HeartbeatTTL:      dur("HEARTBEAT_TTL", 15*time.Second),

		SchedulerPollInterval: dur("SCHEDULER_POLL_INTERVAL", time.Second),
		SchedulerBatchSize:    integer("SCHEDULER_BATCH_SIZE", 100),
		SchedulerLockTTL:      dur("SCHEDULER_LOCK_TTL", 10*time.Second),

		RecoveryInterval: dur("RECOVERY_INTERVAL", 5*time.Second),
		MaxRetries:       integer("MAX_RETRIES", 3),
		BackoffBase:      dur("BACKOFF_BASE", 2*time.Second),
		BackoffMax:       dur("BACKOFF_MAX", 5*time.Minute),

		ShutdownTimeout: dur("SHUTDOWN_TIMEOUT", 30*time.Second),
	}

	if len(errs) > 0 {
		return Config{}, fmt.Errorf("invalid configuration: %w", joinErrs(errs))
	}
	return cfg, nil
}

func joinErrs(errs []error) error {
	if len(errs) == 1 {
		return errs[0]
	}
	msg := ""
	for i, e := range errs {
		if i > 0 {
			msg += "; "
		}
		msg += e.Error()
	}
	return fmt.Errorf("%s", msg)
}
EOF

cat > internal/config/config_test.go <<'EOF'
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
EOF

# ---------------------------------------------------------------- logging
cat > internal/logging/logging.go <<'EOF'
// Package logging builds the slog JSON logger shared by all services.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON slog.Logger writing to stdout at the given level
// (debug, info, warn, error). Unknown levels fall back to info.
func New(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(strings.ToLower(level))); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
EOF

# ---------------------------------------------------------------- domain
cat > internal/domain/status.go <<'EOF'
package domain

import "fmt"

// Status is the lifecycle state of a job or job run.
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusQueued    Status = "QUEUED"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusRetrying  Status = "RETRYING"
	StatusDead      Status = "DEAD"
	StatusCancelled Status = "CANCELLED"
)

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusQueued, StatusRunning, StatusSucceeded,
		StatusRetrying, StatusDead, StatusCancelled:
		return true
	}
	return false
}

// Terminal reports whether no further transitions happen from s.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusDead || s == StatusCancelled
}

// Priority orders jobs in the queue; higher runs first.
type Priority int

const (
	PriorityLow    Priority = 1
	PriorityNormal Priority = 2
	PriorityHigh   Priority = 3
)

// String returns high, normal or low.
func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	}
	return fmt.Sprintf("priority(%d)", int(p))
}

// MarshalText encodes the priority as its name in JSON.
func (p Priority) MarshalText() ([]byte, error) {
	if p < PriorityLow || p > PriorityHigh {
		return nil, fmt.Errorf("invalid priority %d", int(p))
	}
	return []byte(p.String()), nil
}

// UnmarshalText decodes high, normal or low.
func (p *Priority) UnmarshalText(b []byte) error {
	switch string(b) {
	case "high":
		*p = PriorityHigh
	case "normal", "":
		*p = PriorityNormal
	case "low":
		*p = PriorityLow
	default:
		return fmt.Errorf("invalid priority %q", b)
	}
	return nil
}
EOF

cat > internal/domain/job.go <<'EOF'
// Package domain defines the core TaskFlow types shared by every service.
package domain

import (
	"encoding/json"
	"time"
)

// Job is a unit of work, one-off or recurring (cron).
type Job struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Priority       Priority        `json:"priority"`
	Status         Status          `json:"status"`
	CronExpr       string          `json:"cron_expr,omitempty"`
	NextRunAt      time.Time       `json:"next_run_at"`
	Attempts       int             `json:"attempts"`
	MaxRetries     int             `json:"max_retries"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	LastError      string          `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// JobRun records a single execution attempt of a Job.
type JobRun struct {
	ID         string     `json:"id"`
	JobID      string     `json:"job_id"`
	WorkerID   string     `json:"worker_id"`
	Attempt    int        `json:"attempt"`
	Status     Status     `json:"status"`
	Error      string     `json:"error,omitempty"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// Worker describes a registered worker process.
type Worker struct {
	ID            string    `json:"id"`
	Hostname      string    `json:"hostname"`
	Concurrency   int       `json:"concurrency"`
	Alive         bool      `json:"alive"`
	StartedAt     time.Time `json:"started_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}
EOF

cat > internal/domain/domain_test.go <<'EOF'
package domain

import (
	"encoding/json"
	"testing"
)

func TestPriorityJSON(t *testing.T) {
	tests := []struct {
		in   string
		want Priority
		ok   bool
	}{
		{`"high"`, PriorityHigh, true},
		{`"normal"`, PriorityNormal, true},
		{`"low"`, PriorityLow, true},
		{`"urgent"`, 0, false},
	}
	for _, tt := range tests {
		var p Priority
		err := json.Unmarshal([]byte(tt.in), &p)
		if (err == nil) != tt.ok || (tt.ok && p != tt.want) {
			t.Errorf("%s: got %v, err %v", tt.in, p, err)
		}
	}
}

func TestStatusTerminal(t *testing.T) {
	if !StatusDead.Terminal() || StatusRunning.Terminal() {
		t.Error("unexpected terminal result")
	}
}
EOF

# ---------------------------------------------------------------- store
cat > internal/store/store.go <<'EOF'
// Package store is the Postgres repository for jobs, runs and workers.
package store

import (
	"context"
	"errors"

	"__MODULE__/internal/domain"
)

// ErrNotFound is returned when a row does not exist.
var ErrNotFound = errors.New("not found")

// Store persists TaskFlow state in PostgreSQL.
// The pgx pool is wired in later (go get github.com/jackc/pgx/v5).
type Store struct {
	dsn string
}

// New creates a Store for the given connection string.
func New(dsn string) *Store { return &Store{dsn: dsn} }

// Ping verifies connectivity.
func (s *Store) Ping(ctx context.Context) error { return ctx.Err() }

// Close releases resources.
func (s *Store) Close() {}

// GetJob loads a job by ID.
func (s *Store) GetJob(ctx context.Context, id string) (domain.Job, error) {
	return domain.Job{}, ErrNotFound
}
EOF

# ---------------------------------------------------------------- queue
cat > internal/queue/queue.go <<'EOF'
// Package queue implements the priority job queue on Redis sorted sets.
package queue

import (
	"context"
	"fmt"

	"__MODULE__/internal/domain"
)

// Redis key helpers.
const (
	EventsChannel = "taskflow:events"
	SchedulerLock = "taskflow:lock:scheduler"
)

// Key returns the ZSET key for a priority.
func Key(p domain.Priority) string { return "taskflow:queue:" + p.String() }

// InflightKey returns the SET of job IDs a worker is running.
func InflightKey(workerID string) string { return "taskflow:inflight:" + workerID }

// HeartbeatKey returns the TTL key that proves a worker is alive.
func HeartbeatKey(workerID string) string { return "taskflow:hb:" + workerID }

// Queue is implemented by the Redis-backed queue.
type Queue interface {
	Enqueue(ctx context.Context, jobID string, p domain.Priority) error
	// Claim pops the next job (high before normal before low) and records it
	// as in-flight for workerID. It returns "" when all queues are empty.
	Claim(ctx context.Context, workerID string) (jobID string, err error)
	Ack(ctx context.Context, workerID, jobID string) error
}

// Redis is a Queue backed by Redis. The go-redis client is wired in later.
type Redis struct {
	url string
}

// NewRedis creates a Redis queue.
func NewRedis(url string) *Redis { return &Redis{url: url} }

var _ Queue = (*Redis)(nil)

func (r *Redis) Enqueue(ctx context.Context, jobID string, p domain.Priority) error {
	return fmt.Errorf("enqueue %s: not implemented", jobID)
}

func (r *Redis) Claim(ctx context.Context, workerID string) (string, error) {
	return "", nil
}

func (r *Redis) Ack(ctx context.Context, workerID, jobID string) error {
	return fmt.Errorf("ack %s: not implemented", jobID)
}
EOF

cat > internal/queue/queue_test.go <<'EOF'
package queue

import (
	"testing"

	"__MODULE__/internal/domain"
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
EOF

# ---------------------------------------------------------------- events
cat > internal/events/events.go <<'EOF'
// Package events publishes and subscribes to job/worker events over Redis pub/sub.
package events

import (
	"context"
	"time"

	"__MODULE__/internal/domain"
)

// Event is broadcast on taskflow:events and forwarded to dashboards.
type Event struct {
	Type     string        `json:"type"` // e.g. job.status_changed, worker.dead
	JobID    string        `json:"job_id,omitempty"`
	WorkerID string        `json:"worker_id,omitempty"`
	Status   domain.Status `json:"status,omitempty"`
	At       time.Time     `json:"at"`
}

// Publisher emits events.
type Publisher interface {
	Publish(ctx context.Context, e Event) error
}

// Subscriber receives events until ctx is cancelled.
type Subscriber interface {
	Subscribe(ctx context.Context) (<-chan Event, error)
}
EOF

# ---------------------------------------------------------------- ws
cat > internal/ws/hub.go <<'EOF'
// Package ws fans events out to connected dashboard clients.
package ws

import (
	"context"
	"sync"
)

// Hub tracks clients and broadcasts messages to all of them.
// The gorilla/websocket upgrade is wired in later.
type Hub struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

// NewHub creates an empty hub.
func NewHub() *Hub { return &Hub{clients: make(map[chan []byte]struct{})} }

// Register adds a client and returns its message channel and an unregister func.
func (h *Hub) Register() (<-chan []byte, func()) {
	ch := make(chan []byte, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.clients, ch)
		h.mu.Unlock()
	}
}

// Broadcast sends msg to every client, dropping it for slow clients.
func (h *Hub) Broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
		}
	}
}

// Run blocks until ctx is cancelled.
func (h *Hub) Run(ctx context.Context) { <-ctx.Done() }
EOF

# ---------------------------------------------------------------- handlers
cat > internal/handlers/handlers.go <<'EOF'
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
EOF

cat > internal/handlers/sample.go <<'EOF'
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

func init() {
	Register("echo", HandlerFunc(echo))
	Register("sleep", HandlerFunc(sleep))
	Register("fail", HandlerFunc(fail))
}

// echo logs its payload.
func echo(ctx context.Context, payload []byte) error {
	slog.InfoContext(ctx, "echo", slog.String("payload", string(payload)))
	return nil
}

// sleep waits for {"seconds": N} or until ctx ends (useful for timeout tests).
func sleep(ctx context.Context, payload []byte) error {
	var p struct {
		Seconds float64 `json:"seconds"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("decode sleep payload: %w", err)
	}
	select {
	case <-time.After(time.Duration(p.Seconds * float64(time.Second))):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// fail always errors (useful for retry/dead-letter tests).
func fail(_ context.Context, _ []byte) error {
	return errors.New("intentional failure")
}
EOF

cat > internal/handlers/handlers_test.go <<'EOF'
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
EOF

# ---------------------------------------------------------------- scheduler
cat > internal/scheduler/scheduler.go <<'EOF'
// Package scheduler polls Postgres for due jobs and enqueues them into Redis.
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"__MODULE__/internal/config"
)

// Run polls until ctx is cancelled.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	t := time.NewTicker(cfg.SchedulerPollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			// TODO: acquire taskflow:lock:scheduler, fetch due jobs, enqueue by
			// priority, compute next cron run.
			log.Debug("scheduler tick")
		}
	}
}
EOF

# ---------------------------------------------------------------- recovery
cat > internal/recovery/recovery.go <<'EOF'
// Package recovery retries failed jobs, dead-letters exhausted ones and
// re-queues jobs owned by dead workers.
package recovery

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	"__MODULE__/internal/config"
)

// Run sweeps until ctx is cancelled.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	t := time.NewTicker(cfg.RecoveryInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			// TODO: retry due RETRYING jobs, DEAD-letter exhausted jobs, and
			// re-queue taskflow:inflight:{id} of workers with no taskflow:hb:{id}.
			log.Debug("recovery sweep")
		}
	}
}

// Backoff returns the delay before retry number attempt (1-based): exponential
// from base, capped at max, with up to 10% jitter.
func Backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d + time.Duration(rand.Int63n(int64(d)/10+1))
}
EOF

cat > internal/recovery/recovery_test.go <<'EOF'
package recovery

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	base, max := time.Second, 10*time.Second
	tests := []struct {
		attempt int
		min     time.Duration
	}{
		{1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second}, {10, max},
	}
	for _, tt := range tests {
		got := Backoff(tt.attempt, base, max)
		if got < tt.min || got > tt.min+tt.min/10 {
			t.Errorf("attempt %d: got %s, want ~%s", tt.attempt, got, tt.min)
		}
	}
}
EOF

# ---------------------------------------------------------------- worker
cat > internal/worker/worker.go <<'EOF'
// Package worker claims jobs and runs them in a bounded goroutine pool.
package worker

import (
	"context"
	"log/slog"
	"sync"

	"__MODULE__/internal/config"
)

// Run starts the pool and heartbeat, and returns once ctx is cancelled and
// in-flight jobs have drained.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	var wg sync.WaitGroup

	hb := NewHeartbeat(cfg.WorkerID, cfg.HeartbeatInterval, cfg.HeartbeatTTL, log)
	wg.Add(1)
	go func() { defer wg.Done(); hb.Run(ctx) }()

	pool := NewPool(cfg.WorkerConcurrency, NewExecutor(log), log)
	pool.Run(ctx)

	wg.Wait()
	return nil
}
EOF

cat > internal/worker/pool.go <<'EOF'
package worker

import (
	"context"
	"log/slog"
	"sync"
)

// Pool bounds concurrent job execution.
type Pool struct {
	size int
	exec *Executor
	log  *slog.Logger
	jobs chan Task
}

// NewPool creates a pool of size concurrent workers.
func NewPool(size int, exec *Executor, log *slog.Logger) *Pool {
	if size < 1 {
		size = 1
	}
	return &Pool{size: size, exec: exec, log: log, jobs: make(chan Task)}
}

// Submit hands a task to the pool, blocking until a goroutine is free.
func (p *Pool) Submit(ctx context.Context, t Task) error {
	select {
	case p.jobs <- t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run starts the goroutines and blocks until ctx is cancelled and all running
// tasks finish. TODO: add the claim loop that feeds Submit from the queue.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.size; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case t := <-p.jobs:
					if err := p.exec.Execute(ctx, t); err != nil {
						p.log.Warn("job failed", slog.String("job_id", t.JobID), slog.Any("error", err))
					}
				}
			}
		}()
	}
	wg.Wait()
}
EOF

cat > internal/worker/executor.go <<'EOF'
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"__MODULE__/internal/handlers"
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
EOF

cat > internal/worker/heartbeat.go <<'EOF'
package worker

import (
	"context"
	"log/slog"
	"time"
)

// Heartbeat periodically refreshes taskflow:hb:{workerID} with a TTL so
// recovery can detect dead workers.
type Heartbeat struct {
	workerID string
	interval time.Duration
	ttl      time.Duration
	log      *slog.Logger
}

// NewHeartbeat creates a Heartbeat.
func NewHeartbeat(workerID string, interval, ttl time.Duration, log *slog.Logger) *Heartbeat {
	return &Heartbeat{workerID: workerID, interval: interval, ttl: ttl, log: log}
}

// Run beats until ctx is cancelled.
func (h *Heartbeat) Run(ctx context.Context) {
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// TODO: SET taskflow:hb:{workerID} EX ttl
			h.log.Debug("heartbeat", slog.String("worker_id", h.workerID))
		}
	}
}
EOF

cat > internal/worker/executor_test.go <<'EOF'
package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	_ "__MODULE__/internal/handlers" // registers sample handlers
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
EOF

# ---------------------------------------------------------------- api
cat > internal/api/api.go <<'EOF'
// Package api exposes the REST API and WebSocket endpoint.
package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"__MODULE__/internal/config"
	"__MODULE__/internal/ws"
)

// Run serves HTTP until ctx is cancelled, then shuts down gracefully.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	hub := ws.NewHub()
	go hub.Run(ctx)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           NewRouter(cfg, log),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("listening", slog.String("addr", cfg.HTTPAddr))
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("listen: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}
EOF

cat > internal/api/router.go <<'EOF'
package api

import (
	"log/slog"
	"net/http"

	"__MODULE__/internal/config"
)

// NewRouter wires routes and middleware. chi replaces ServeMux later.
func NewRouter(cfg config.Config, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealth)

	protected := http.NewServeMux()
	protected.HandleFunc("GET /api/v1/jobs", notImplemented)
	protected.HandleFunc("POST /api/v1/jobs", notImplemented)
	mux.Handle("/api/", APIKeyAuth(cfg.APIKey)(protected))

	return Recoverer(log)(RequestLogger(log)(mux))
}
EOF

cat > internal/api/middleware.go <<'EOF'
package api

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"time"
)

type middleware func(http.Handler) http.Handler

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// RequestLogger logs one line per request.
func RequestLogger(log *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			log.InfoContext(r.Context(), "request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Duration("duration", time.Since(start)))
		})
	}
}

// Recoverer turns panics into 500s.
func Recoverer(log *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if v := recover(); v != nil {
					log.ErrorContext(r.Context(), "panic", slog.Any("value", v))
					writeError(w, http.StatusInternalServerError, "internal error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// APIKeyAuth requires the X-API-Key header to match key.
func APIKeyAuth(key string) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := r.Header.Get("X-API-Key")
			if subtle.ConstantTimeCompare([]byte(got), []byte(key)) != 1 {
				writeError(w, http.StatusUnauthorized, "invalid api key")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
EOF

cat > internal/api/handlers.go <<'EOF'
package api

import (
	"encoding/json"
	"net/http"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func notImplemented(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "not implemented")
}
EOF

cat > internal/api/api_test.go <<'EOF'
package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"__MODULE__/internal/config"
)

func TestRouter(t *testing.T) {
	h := NewRouter(config.Config{APIKey: "k"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	tests := []struct {
		name, path, key string
		want            int
	}{
		{"health open", "/healthz", "", http.StatusOK},
		{"no key", "/api/v1/jobs", "", http.StatusUnauthorized},
		{"wrong key", "/api/v1/jobs", "x", http.StatusUnauthorized},
		{"good key", "/api/v1/jobs", "k", http.StatusNotImplemented},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.key != "" {
				req.Header.Set("X-API-Key", tt.key)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}
EOF

# ---------------------------------------------------------------- scripts
cat > scripts/loadtest/main.go <<'EOF'
// Command loadtest submits many jobs to the API to exercise the system.
package main

import (
	"flag"
	"fmt"
)

func main() {
	jobs := flag.Int("jobs", 100, "number of jobs to submit")
	url := flag.String("url", "http://localhost:8080", "API base URL")
	flag.Parse()
	// TODO: POST /api/v1/jobs concurrently and report latency.
	fmt.Printf("loadtest: would submit %d jobs to %s\n", *jobs, *url)
}
EOF

cat > scripts/seed/main.go <<'EOF'
// Command seed inserts sample jobs for local development.
package main

import "fmt"

func main() {
	// TODO: create sample echo/sleep/fail jobs through the API.
	fmt.Println("seed: nothing to seed yet")
}
EOF

# ---------------------------------------------------------------- migrations
cat > migrations/000001_init.up.sql <<'EOF'
CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT        NOT NULL,
    type            TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    priority        SMALLINT    NOT NULL DEFAULT 2 CHECK (priority BETWEEN 1 AND 3),
    status          TEXT        NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','QUEUED','RUNNING','SUCCEEDED','RETRYING','DEAD','CANCELLED')),
    cron_expr       TEXT        NOT NULL DEFAULT '',
    next_run_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INT         NOT NULL DEFAULT 0,
    max_retries     INT         NOT NULL DEFAULT 3,
    timeout_seconds INT         NOT NULL DEFAULT 60,
    last_error      TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX jobs_due_idx ON jobs (next_run_at) WHERE status IN ('PENDING','RETRYING');

CREATE TABLE job_runs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id      UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id   TEXT        NOT NULL,
    attempt     INT         NOT NULL,
    status      TEXT        NOT NULL,
    error       TEXT        NOT NULL DEFAULT '',
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
CREATE INDEX job_runs_job_idx ON job_runs (job_id, started_at DESC);

CREATE TABLE workers (
    id             TEXT PRIMARY KEY,
    hostname       TEXT        NOT NULL,
    concurrency    INT         NOT NULL,
    alive          BOOLEAN     NOT NULL DEFAULT true,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()
);
EOF

cat > migrations/000001_init.down.sql <<'EOF'
DROP TABLE IF EXISTS workers;
DROP TABLE IF EXISTS job_runs;
DROP TABLE IF EXISTS jobs;
EOF

# ---------------------------------------------------------------- deploy
cat > deploy/Dockerfile <<'EOF'
# Builds any service: docker build -f deploy/Dockerfile --build-arg SERVICE=api .
FROM golang:1.22-alpine AS build
ARG SERVICE
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
EOF

cat > deploy/Caddyfile <<'EOF'
{$SITE_ADDRESS:localhost} {
	encode gzip

	handle /api/* {
		reverse_proxy api:8080
	}
	handle /ws {
		reverse_proxy api:8080
	}
	handle {
		root * /srv/web
		try_files {path} /index.html
		file_server
	}
}
EOF

# ---------------------------------------------------------------- root files
cat > docker-compose.yml <<'EOF'
# Infrastructure only; app services are added later.
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-taskflow}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-taskflow}
      POSTGRES_DB: ${POSTGRES_DB:-taskflow}
    ports:
      - "${POSTGRES_PORT:-5432}:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 5s
      timeout: 3s
      retries: 10

  redis:
    image: redis:7
    command: ["redis-server", "--appendonly", "yes"]
    ports:
      - "${REDIS_PORT:-6379}:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 10

volumes:
  postgres_data:
  redis_data:
EOF

cat > .env.example <<'EOF'
# --- General
APP_ENV=development
LOG_LEVEL=info
SHUTDOWN_TIMEOUT=30s

# --- API
HTTP_ADDR=:8080
API_KEY=dev-api-key

# --- Postgres (docker-compose + services)
POSTGRES_USER=taskflow
POSTGRES_PASSWORD=taskflow
POSTGRES_DB=taskflow
POSTGRES_PORT=5432
DATABASE_URL=postgres://taskflow:taskflow@localhost:5432/taskflow?sslmode=disable

# --- Redis
REDIS_PORT=6379
REDIS_URL=redis://localhost:6379/0

# --- Worker
WORKER_ID=worker-1
WORKER_CONCURRENCY=50
HEARTBEAT_INTERVAL=5s
HEARTBEAT_TTL=15s

# --- Scheduler
SCHEDULER_POLL_INTERVAL=1s
SCHEDULER_BATCH_SIZE=100
SCHEDULER_LOCK_TTL=10s

# --- Recovery
RECOVERY_INTERVAL=5s
MAX_RETRIES=3
BACKOFF_BASE=2s
BACKOFF_MAX=5m

# --- Caddy (deploy)
SITE_ADDRESS=localhost
EOF

cat > .gitignore <<'EOF'
# Go
/bin/
*.exe
*.test
*.out
coverage.*

# Env
.env

# Web
web/node_modules/
web/dist/

# IDE / OS
.idea/
.vscode/
.DS_Store
EOF

cat > .golangci.yml <<'EOF'
version: "2"
run:
  timeout: 5m
linters:
  default: standard
  enable:
    - bodyclose
    - errorlint
    - gocritic
    - misspell
    - noctx
    - revive
    - unconvert
formatters:
  enable:
    - gofmt
    - goimports
EOF

cat > .github/workflows/ci.yml <<'EOF'
name: ci
on:
  push:
    branches: [main]
  pull_request:

jobs:
  go:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - run: go build ./...
      - run: go vet ./...
      - run: go test -race ./...
      - uses: golangci/golangci-lint-action@v7
EOF

cat > Makefile <<'EOF'
-include .env
export

BIN := bin
SERVICES := api scheduler worker recovery
MIGRATE := migrate -path migrations -database "$(DATABASE_URL)"

.PHONY: up down build test lint migrate-up migrate-down run-api run-worker run-scheduler run-recovery loadtest

up:
	docker compose up -d --wait

down:
	docker compose down

build:
	@for s in $(SERVICES); do go build -o $(BIN)/$$s ./cmd/$$s || exit 1; done

test:
	go test -race ./...

lint:
	golangci-lint run ./...

migrate-up:
	$(MIGRATE) up

migrate-down:
	$(MIGRATE) down 1

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

run-scheduler:
	go run ./cmd/scheduler

run-recovery:
	go run ./cmd/recovery

loadtest:
	go run ./scripts/loadtest
EOF

cat > README.md <<'EOF'
# TaskFlow

A distributed job scheduler: Go services over Redis (queues, heartbeats, pub/sub)
and PostgreSQL (durable state), with a React dashboard.

| Service     | Role                                                        |
|-------------|-------------------------------------------------------------|
| `api`       | REST API + WebSocket hub (:8080)                            |
| `scheduler` | Enqueues due jobs by priority, computes next cron run       |
| `worker`    | Claims jobs, runs handlers in a goroutine pool, heartbeats  |
| `recovery`  | Retries with backoff, dead-letters, re-queues dead workers' jobs |

## Quick start

```sh
cp .env.example .env
make up            # postgres + redis
make migrate-up    # requires golang-migrate CLI
make run-api       # in separate terminals: run-scheduler, run-worker, run-recovery
```

`make test`, `make lint`, `make build` do what they say. See `docs/`.
EOF

touch web/.gitkeep docs/.gitkeep

# ---------------------------------------------------------------- finalise
grep -rl '__MODULE__' . --include='*.go' --include='go.mod' | xargs sed -i "s#__MODULE__#${MODULE}#g"
command -v gofmt >/dev/null && gofmt -w cmd internal scripts

echo "TaskFlow skeleton created in $(pwd) (module ${MODULE})"
