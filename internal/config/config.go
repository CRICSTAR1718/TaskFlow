// Package config loads service configuration from environment variables.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds every setting used by the TaskFlow services.
type Config struct {
	DatabaseURL string
	RedisAddr   string
	APIKey      string
	HTTPAddr    string

	WorkerConcurrency int
	WorkerID          string
	HeartbeatInterval time.Duration
	HeartbeatTTL      time.Duration

	SchedulerTick    time.Duration
	RecoveryInterval time.Duration
	BackoffBase      time.Duration
	BackoffMax       time.Duration

	LogLevel        string
	ShutdownTimeout time.Duration
}

// Load reads configuration from the process environment.
func Load() (Config, error) { return LoadFrom(os.LookupEnv) }

// LoadFrom reads configuration through lookup (os.LookupEnv in production),
// applies defaults, and validates. All problems are reported together.
func LoadFrom(lookup func(string) (string, bool)) (Config, error) {
	r := reader{lookup: lookup}

	cfg := Config{
		DatabaseURL: r.str("DATABASE_URL", "postgres://taskflow:taskflow@localhost:5432/taskflow?sslmode=disable"),
		RedisAddr:   r.str("REDIS_ADDR", "localhost:6379"),
		APIKey:      r.str("API_KEY", ""),
		HTTPAddr:    r.str("HTTP_ADDR", ":8080"),

		WorkerConcurrency: r.integer("WORKER_CONCURRENCY", 50),
		HeartbeatInterval: r.duration("HEARTBEAT_INTERVAL", 5*time.Second),
		HeartbeatTTL:      r.duration("HEARTBEAT_TTL", 15*time.Second),

		SchedulerTick:    r.duration("SCHEDULER_TICK", time.Second),
		RecoveryInterval: r.duration("RECOVERY_INTERVAL", 5*time.Second),
		BackoffBase:      r.duration("BACKOFF_BASE", 2*time.Second),
		BackoffMax:       r.duration("BACKOFF_MAX", 5*time.Minute),

		LogLevel:        strings.ToLower(r.str("LOG_LEVEL", "info")),
		ShutdownTimeout: r.duration("SHUTDOWN_TIMEOUT", 30*time.Second),
	}

	cfg.WorkerID = r.str("WORKER_ID", "")
	if cfg.WorkerID == "" {
		id, err := defaultWorkerID()
		if err != nil {
			r.errs = append(r.errs, fmt.Errorf("WORKER_ID: %w", err))
		}
		cfg.WorkerID = id
	}

	if err := errors.Join(append(r.errs, cfg.Validate())...); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

// Validate checks that the values are usable. It returns every problem found.
func (c Config) Validate() error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.DatabaseURL == "" {
		bad("DATABASE_URL is required")
	}
	if c.APIKey == "" {
		bad("API_KEY is required")
	}
	if _, _, err := net.SplitHostPort(c.RedisAddr); err != nil {
		bad("REDIS_ADDR %q must be host:port", c.RedisAddr)
	}
	if _, _, err := net.SplitHostPort(c.HTTPAddr); err != nil {
		bad("HTTP_ADDR %q must be [host]:port", c.HTTPAddr)
	}
	if c.WorkerConcurrency < 1 || c.WorkerConcurrency > 10000 {
		bad("WORKER_CONCURRENCY %d out of range [1, 10000]", c.WorkerConcurrency)
	}
	if c.WorkerID == "" {
		bad("WORKER_ID must not be empty")
	}
	if c.HeartbeatInterval <= 0 {
		bad("HEARTBEAT_INTERVAL must be positive")
	}
	if c.HeartbeatTTL <= c.HeartbeatInterval {
		bad("HEARTBEAT_TTL (%s) must be greater than HEARTBEAT_INTERVAL (%s)", c.HeartbeatTTL, c.HeartbeatInterval)
	}
	if c.SchedulerTick <= 0 {
		bad("SCHEDULER_TICK must be positive")
	}
	if c.RecoveryInterval <= 0 {
		bad("RECOVERY_INTERVAL must be positive")
	}
	if c.BackoffBase <= 0 {
		bad("BACKOFF_BASE must be positive")
	}
	if c.BackoffMax < c.BackoffBase {
		bad("BACKOFF_MAX (%s) must be at least BACKOFF_BASE (%s)", c.BackoffMax, c.BackoffBase)
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		bad("LOG_LEVEL %q must be debug, info, warn or error", c.LogLevel)
	}
	if c.ShutdownTimeout <= 0 {
		bad("SHUTDOWN_TIMEOUT must be positive")
	}
	return errors.Join(errs...)
}

// defaultWorkerID returns "<hostname>-<8 random hex chars>".
func defaultWorkerID() (string, error) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return host, fmt.Errorf("generate random suffix: %w", err)
	}
	return host + "-" + hex.EncodeToString(b), nil
}

// reader pulls typed values out of a lookup function, collecting parse errors.
type reader struct {
	lookup func(string) (string, bool)
	errs   []error
}

func (r *reader) str(key, def string) string {
	if v, ok := r.lookup(key); ok && v != "" {
		return v
	}
	return def
}

func (r *reader) integer(key string, def int) int {
	v := r.str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

func (r *reader) duration(key string, def time.Duration) time.Duration {
	v := r.str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not a duration (e.g. 5s, 2m)", key, v))
		return def
	}
	return d
}
