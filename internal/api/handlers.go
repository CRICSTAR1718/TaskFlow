package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/cricstar1718/taskflow/internal/domain"
	"github.com/cricstar1718/taskflow/internal/handlers"
	"github.com/cricstar1718/taskflow/internal/store"
)

// maxBodyBytes caps request bodies. A job payload is JSONB, not a file upload.
const maxBodyBytes = 1 << 20 // 1 MiB

// defaultMaxRetries mirrors the max_retries column default in migrations.
const defaultMaxRetries = 3

// Store is the slice of the persistence layer the API needs.
type Store interface {
	CreateJob(ctx context.Context, j domain.Job) (domain.Job, error)
	GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error)
	ListJobs(ctx context.Context, f store.Filter) ([]domain.Job, error)
	CancelJob(ctx context.Context, id uuid.UUID) error
	RetryJob(ctx context.Context, id uuid.UUID) error
	Stats(ctx context.Context) (store.Stats, error)
	ListWorkers(ctx context.Context) ([]domain.WorkerInfo, error)
	Runs(ctx context.Context, jobID uuid.UUID) ([]domain.JobRun, error)
	Ping(ctx context.Context) error
}

// Queue is the slice of Redis the API needs.
type Queue interface {
	Enqueue(ctx context.Context, jobID string, p domain.Priority) error
	Depths(ctx context.Context) (map[domain.Priority]int64, error)
	Heartbeats(ctx context.Context, workerIDs []string) (map[string]bool, error)
	Ping(ctx context.Context) error
}

// API carries the dependencies for every handler.
type API struct {
	store Store
	queue Queue
	log   *slog.Logger

	// staleAfter is how long a worker may go without a heartbeat before it is
	// reported as not alive. Defaults to 30s (2x the default HEARTBEAT_TTL).
	staleAfter time.Duration
}

// apiError is the consistent error shape: {"error":{"code":...,"message":...}}.
type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeJSON encodes before writing a status line. Encoding after WriteHeader
// would turn a marshalling bug into an empty 200, which is the worst possible
// failure mode: the client cannot tell it apart from success.
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"internal","message":"failed to encode response"}}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]apiError{"error": {Code: code, Message: msg}})
}

// fail maps a store/queue error to the right status code in one place, so every
// endpoint reports the same thing for the same condition.
func (a *API) fail(w http.ResponseWriter, r *http.Request, op string, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "job not found")
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", "the job is not in a state that allows this")
	default:
		a.log.ErrorContext(r.Context(), op, slog.Any("error", err))
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

// ---------------------------------------------------------------- health ----

func (a *API) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady pings both dependencies. It is the endpoint a load balancer or
// `docker compose` healthcheck should use: 200 only when the service can serve.
func (a *API) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	out := map[string]string{"status": "ok", "postgres": "ok", "redis": "ok"}
	code := http.StatusOK

	if err := a.store.Ping(ctx); err != nil {
		out["postgres"] = err.Error()
		out["status"] = "degraded"
		code = http.StatusServiceUnavailable
	}
	if err := a.queue.Ping(ctx); err != nil {
		out["redis"] = err.Error()
		out["status"] = "degraded"
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, out)
}

// ------------------------------------------------------------------ jobs ----

// createJobRequest is the POST body. Pointers distinguish "absent" from "zero",
// which matters for priority and run_at.
type createJobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Priority       string          `json:"priority,omitempty"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
	CronExpr       *string         `json:"cron_expr,omitempty"`
	TimeoutSeconds int             `json:"timeout_seconds,omitempty"`
	MaxRetries     *int            `json:"max_retries,omitempty"`
}

// validate checks the request and returns the domain.Job to insert.
func (req createJobRequest) validate() (domain.Job, error) {
	if req.Type == "" {
		return domain.Job{}, errors.New("type is required")
	}
	if _, ok := handlers.Get(req.Type); !ok && len(handlers.Types()) > 0 {
		return domain.Job{}, fmt.Errorf("unknown job type %q (known: %v)", req.Type, handlers.Types())
	}

	j := domain.Job{
		Type:           req.Type,
		Payload:        req.Payload,
		Priority:       domain.PriorityNormal,
		TimeoutSeconds: req.TimeoutSeconds,
		MaxRetries:     defaultMaxRetries,
		CronExpr:       req.CronExpr,
	}
	// Omitted max_retries must mean "the schema default", not zero. Sending an
	// explicit 0 would silently turn every API-created job into a no-retry job.
	if req.MaxRetries != nil {
		j.MaxRetries = *req.MaxRetries
	}
	if len(j.Payload) == 0 {
		j.Payload = json.RawMessage(`{}`)
	}
	if !json.Valid(j.Payload) {
		return domain.Job{}, errors.New("payload must be valid JSON")
	}
	if req.Priority != "" {
		p, err := domain.ParsePriority(req.Priority)
		if err != nil {
			return domain.Job{}, err
		}
		j.Priority = p
	}
	if req.RunAt != nil {
		j.RunAt = *req.RunAt
	}
	if req.TimeoutSeconds < 0 || req.TimeoutSeconds > 3600 {
		return domain.Job{}, errors.New("timeout_seconds must be between 1 and 3600")
	}
	if j.MaxRetries < 0 || j.MaxRetries > 10 {
		return domain.Job{}, errors.New("max_retries must be between 0 and 10")
	}

	// A job is only visible to the scheduler once next_run_at is set; without it
	// nobody would ever pick the job up.
	runAt := j.RunAt
	if runAt.IsZero() {
		runAt = time.Now()
	}
	j.NextRunAt = &runAt
	return j, nil
}

func (a *API) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	body := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "too_large", "body exceeds 1MB")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must be JSON")
		return
	}

	job, err := req.validate()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	created, err := a.store.CreateJob(r.Context(), job)
	if err != nil {
		a.fail(w, r, "create job", err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (a *API) handleListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.Filter{
		Type:   q.Get("type"),
		Cursor: q.Get("cursor"),
	}
	if s := q.Get("status"); s != "" {
		st := domain.Status(s)
		if !st.Valid() {
			writeError(w, http.StatusBadRequest, "invalid_request", "unknown status "+s)
			return
		}
		f.Status = st
	}
	if p := q.Get("priority"); p != "" {
		pr, err := domain.ParsePriority(p)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		f.Priority = pr
	}
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "invalid_request", "limit must be a positive integer")
			return
		}
		f.Limit = n
	}

	jobs, err := a.store.ListJobs(r.Context(), f)
	if err != nil {
		a.fail(w, r, "list jobs", err)
		return
	}
	if jobs == nil {
		jobs = []domain.Job{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "count": len(jobs)})
}

func (a *API) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, ok := jobID(w, r)
	if !ok {
		return
	}
	job, err := a.store.GetJob(r.Context(), id)
	if err != nil {
		a.fail(w, r, "get job", err)
		return
	}
	runs, err := a.store.Runs(r.Context(), id)
	if err != nil {
		a.fail(w, r, "list runs", err)
		return
	}
	if runs == nil {
		runs = []domain.JobRun{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job, "runs": runs})
}

func (a *API) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id, ok := jobID(w, r)
	if !ok {
		return
	}
	if err := a.store.CancelJob(r.Context(), id); err != nil {
		a.fail(w, r, "cancel job", err)
		return
	}
	a.respondWithJob(w, r, id, http.StatusOK)
}

// handleRetryJob moves a DEAD job back to PENDING with attempt reset so the
// scheduler picks it up again on its next tick.
func (a *API) handleRetryJob(w http.ResponseWriter, r *http.Request) {
	id, ok := jobID(w, r)
	if !ok {
		return
	}
	if err := a.store.RetryJob(r.Context(), id); err != nil {
		a.fail(w, r, "retry job", err)
		return
	}
	a.log.InfoContext(r.Context(), "job retried by request", slog.String("job_id", id.String()))
	a.respondWithJob(w, r, id, http.StatusOK)
}

// respondWithJob re-reads the job so the client sees the new state.
func (a *API) respondWithJob(w http.ResponseWriter, r *http.Request, id uuid.UUID, status int) {
	job, err := a.store.GetJob(r.Context(), id)
	if err != nil {
		a.fail(w, r, "reload job", err)
		return
	}
	writeJSON(w, status, job)
}

// jobID parses the {id} path value, writing 400 when it is not a UUID.
func jobID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", "id must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

// ------------------------------------------------------------ observability --

func (a *API) handleStats(w http.ResponseWriter, r *http.Request) {
	st, err := a.store.Stats(r.Context())
	if err != nil {
		a.fail(w, r, "stats", err)
		return
	}
	depths, err := a.queue.Depths(r.Context())
	if err != nil {
		a.fail(w, r, "queue depths", err)
		return
	}
	byStatus := map[string]int64{}
	for s, n := range st.JobsByStatus {
		byStatus[string(s)] = n
	}
	queue := map[string]int64{}
	for p, n := range depths {
		queue[p.String()] = n
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"jobs_by_status":                byStatus,
		"queue_depths":                  queue,
		"runs_last_hour":                st.RunsLastHour,
		"avg_run_duration_ms_last_hour": st.AvgRunDurationLastHour.Milliseconds(),
	})
}

func (a *API) handleWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := a.store.ListWorkers(r.Context())
	if err != nil {
		a.fail(w, r, "list workers", err)
		return
	}
	if workers == nil {
		workers = []domain.WorkerInfo{}
	}
	// Liveness comes from the heartbeat key in Redis, not the DB column: the
	// column is written once at registration, so using it would report a
	// perfectly healthy worker as dead a few seconds after it starts.
	ids := make([]string, 0, len(workers))
	for _, wk := range workers {
		ids = append(ids, wk.ID)
	}
	alive, err := a.queue.Heartbeats(r.Context(), ids)
	if err != nil {
		a.log.WarnContext(r.Context(), "heartbeat lookup failed, falling back to the db column", slog.Any("error", err))
		cutoff := time.Now().Add(-a.staleAfter)
		alive = map[string]bool{}
		for _, wk := range workers {
			alive[wk.ID] = wk.LastHeartbeat.After(cutoff)
		}
	}

	type workerView struct {
		domain.WorkerInfo
		Alive bool `json:"alive"`
	}
	out := make([]workerView, 0, len(workers))
	for _, wk := range workers {
		out = append(out, workerView{WorkerInfo: wk, Alive: alive[wk.ID]})
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": out, "count": len(out)})
}

func (a *API) handleHandlers(w http.ResponseWriter, _ *http.Request) {
	types := handlers.Types()
	if types == nil {
		types = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"handlers": types, "count": len(types)})
}
