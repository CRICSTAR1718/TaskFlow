# Quickstart

Two ways in. Both assume Docker Desktop (or any Docker engine) is running.

## The whole system, one command

```bash
cp .env.example .env          # once; sets API_KEY etc.
docker compose up -d --build --wait
```

That starts Postgres, Redis, applies the schema, then the api, scheduler,
recovery, worker and dashboard. The `--wait` returns only when everything is
healthy, so a failure is visible immediately instead of five minutes later.

```bash
open http://localhost:3000          # dashboard
curl -s localhost:8080/healthz      # api
docker compose ps                   # what is up
```

The api and worker talk to each other through Redis only, and the dashboard reads
the api. Nothing else has to be installed on the host - the schema is applied by
the `migrate` service, which is a small Go program in this repo (no
`golang-migrate` binary, no `migrate/migrate` image download).

### Scale the workers

```bash
docker compose up -d --scale worker=3
```

Each replica generates its own id (`<hostname>-<random>`), so three workers
register as three distinct rows. Watch it happen:

```bash
docker compose logs -f worker
curl -s localhost:8080/api/v1/workers -H "X-API-Key: dev-api-key"
```

### Prove recovery works

1. Submit a long job: `{"type":"sleep","payload":{"seconds":30}}`
2. `docker kill $(docker compose ps -q worker | head -1)`
3. Watch the recovery service:

```bash
docker compose logs -f recovery
# recovery sweep  reclaimed=1 retried=1 dead=0
```

The job is reclaimed from the dead worker, retried on a surviving one, and ends
`SUCCEEDED` with `last_error: "worker stopped heartbeating"` - the crash is
recorded on the job rather than silently swallowed.

### Clean slate

```bash
docker compose down -v     # containers and volumes
docker compose up -d --build --wait
```

## Just the infrastructure, services on the host

Better when you are editing Go code and want fast restarts:

```bash
docker compose up -d postgres redis
cp .env.example .env
(export $(grep -v '^#' .env | xargs); go run ./migrate -dir migrations)

go run ./cmd/api          # terminal 1
go run ./cmd/scheduler    # terminal 2
go run ./cmd/worker       # terminal 3
go run ./cmd/recovery     # terminal 4
cd web && npm run dev     # terminal 5 -> http://localhost:5173
```

`make up`, `make test`, `make migrate-up`, `make seed`, `make reset` wrap the
common ones.

## Ports

| what | port | override |
|---|---|---|
| dashboard | 3000 | `WEB_PORT` |
| api | 8080 | `HTTP_PORT` |
| postgres | 5432 | `POSTGRES_PORT` |
| redis | 6379 | `REDIS_PORT` |

If something is already on one of those ports, set the variable in `.env` rather
than editing the compose file.

## Troubleshooting

**`api` restarts with `config: API_KEY is required`** - you copied `.env.example`
to `.env` but did not set `API_KEY`, or compose is not reading the file. The
compose file marks `.env` as optional, so a missing file is silent by design;
`docker compose config` shows what each service will actually get.

**Everything is `PENDING` and nothing runs** - the scheduler or the workers are
down: `docker compose ps`, then `docker compose logs scheduler worker`.

**Jobs sit in `RUNNING` after a restart** - that is the recovery service's job;
give it `RECOVERY_INTERVAL` (5s) plus the heartbeat TTL (15s).

**`port is already allocated`** - set `HTTP_PORT` / `WEB_PORT` / `POSTGRES_PORT`
in `.env`.

**Dashboard loads but every request 401s** - the key in the browser must match
`API_KEY` in `.env`. Paste it on the Submit Job page, or set `VITE_API_KEY` for
the dev server.
