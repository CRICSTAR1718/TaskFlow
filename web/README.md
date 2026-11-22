# TaskFlow dashboard

React + TypeScript + Tailwind, talking to the Go API over REST and a websocket.

```bash
npm install
npm run dev      # http://localhost:5173, proxies /api and /ws to :8080
npm run lint     # eslint, zero warnings allowed
npm run build    # tsc -b + vite build -> dist/
```

Start the backend first:

```bash
docker compose up -d postgres redis
go run ./cmd/api        # terminal 1
go run ./cmd/scheduler  # terminal 2
go run ./cmd/worker     # terminal 3
```

## Configuration

| variable | default | meaning |
|---|---|---|
| `VITE_API_URL` | *(empty)* | API origin. Empty means "same origin", which is what dev (Vite proxy), `docker compose` (Caddy) and any reverse proxy want. |
| `VITE_WS_URL` | *(empty)* | Websocket URL. Empty means `ws(s)://<this host>/ws?key=<api key>`. |
| `VITE_API_KEY` | *(empty)* | Sent as `X-API-Key`. See below. |
| `VITE_PROXY_TARGET` | `http://localhost:8080` | Where the **dev server** proxies `/api` and `/ws`. |

### About the API key

A static bundle cannot keep a secret - anything baked into the JS is readable by
anyone who loads the page. `VITE_API_KEY` is therefore a convenience for a
single-user demo, not authentication in any real sense:

- leave it unset and paste the key into the **Submit Job** page; it is stored in
  `localStorage` under `taskflow.apiKey` and sent on every request;
- the key still travels in the websocket query string (`/ws?key=...`) because a
  browser cannot set headers on a handshake. That is why the API logs a warning
  about the query-string key, and why `/ws` is registered outside the
  `X-API-Key` middleware but validates `?key=` itself.

For a real deployment the API needs a session cookie or an OAuth proxy in front
of it; a shared key in a public bundle is not it.

## Layout

```
src/
  api/client.ts        typed fetch wrapper, ApiError, liveSocketUrl()
  api/types.ts         mirrors internal/domain + internal/api - the contract lives here
  hooks/useLiveEvents.ts  one websocket for the app, backoff + cache patching
  components/          Layout, StatusBadge, loading/error/empty states
  pages/               Overview, Jobs, Workers, SubmitJob
```

## Live updates

`useLiveEvents` opens a single `GET /ws` connection for the whole app and:

- patches the TanStack Query cache the moment a `job_event` arrives, so a row
  changes colour before any refetch completes;
- treats the socket as a *hint*: every event also invalidates the affected
  queries, and all queries poll every 15s regardless. If `/ws` never connects the
  dashboard still works, it just updates more slowly. That is deliberate - a demo
  should not break because a proxy forgot to upgrade a websocket.
- reconnects with exponential backoff (0.5s â†’ 15s, jittered, reset on success).

## Docker

```bash
docker build -t taskflow-web .
docker run -p 3000:80 taskflow-web
```

Caddy serves `dist/` and proxies `/api/*` and `/ws*` to the host named `api:8080`,
so it is meant to run inside the compose network (`docker compose up web`).
