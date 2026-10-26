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
