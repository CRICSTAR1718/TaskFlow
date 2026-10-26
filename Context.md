PROJECT CONTEXT — TaskFlow: a distributed job scheduler.
Stack: Go 1.22+ (all backend), Redis 7, PostgreSQL 16, React + Vite + TypeScript (dashboard only), Docker Compose.
Go libs: go-chi/chi v5, redis/go-redis v9, jackc/pgx v5 (pgxpool), robfig/cron v3, gorilla/websocket, log/slog, golang-migrate.
Module path: github.com/<org>/taskflow

Architecture:
- cmd/api        → REST API + WebSocket hub (port 8080)
- cmd/scheduler  → polls Postgres for due jobs, enqueues into Redis by priority, computes next cron run
- cmd/worker     → claims jobs from Redis, runs handlers in a goroutine pool with context timeouts, sends heartbeats
- cmd/recovery   → retries failed jobs with exponential backoff, dead-letters exhausted jobs, re-queues jobs from dead workers
Job states: PENDING → QUEUED → RUNNING → SUCCEEDED | RETRYING → … | DEAD | CANCELLED
Priorities: high (3), normal (2), low (1). Redis keys: taskflow:queue:{high|normal|low} (ZSET, score = enqueue unix ms),
taskflow:inflight:{workerID} (SET), taskflow:hb:{workerID} (key with 15s TTL), taskflow:events (pub/sub), taskflow:lock:scheduler.
Targets: 100+ parallel jobs across 3 workers; dead worker recovery < 15s; dashboard update < 1s.
Out of scope: multi-tenant auth (single API key via X-API-Key header), DAG workflows, arbitrary code execution, Kubernetes.

Rules: idiomatic Go, small packages, interfaces at consumer side, errors wrapped with %w, context passed everywhere,
config from env vars, table-driven tests, no global state except the handler registry. Show full file contents, not diffs.