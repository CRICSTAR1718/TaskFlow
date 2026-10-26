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
