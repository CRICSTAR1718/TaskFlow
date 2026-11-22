package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/cricstar1718/taskflow/internal/config"
	"github.com/cricstar1718/taskflow/internal/domain"
	"github.com/cricstar1718/taskflow/internal/handlers"
	"github.com/cricstar1718/taskflow/internal/store"
)

// ------------------------------------------------------------------ fakes ---

type fakeStore struct {
	jobs      map[uuid.UUID]domain.Job
	runs      []domain.JobRun
	pingErr   error
	getErr    error
	statusErr error
	cancelErr error
	created   []domain.Job
	retryErr  error
	workers   []domain.WorkerInfo
	stats     store.Stats
}

func newFakeStore() *fakeStore {
	return &fakeStore{jobs: map[uuid.UUID]domain.Job{}}
}

func (f *fakeStore) CreateJob(_ context.Context, j domain.Job) (domain.Job, error) {
	j.ID = uuid.New()
	j.Status = domain.StatusPending
	j.CreatedAt = time.Now()
	f.jobs[j.ID] = j
	f.created = append(f.created, j)
	return j, nil
}

func (f *fakeStore) GetJob(_ context.Context, id uuid.UUID) (domain.Job, error) {
	if f.getErr != nil {
		return domain.Job{}, f.getErr
	}
	j, ok := f.jobs[id]
	if !ok {
		return domain.Job{}, store.ErrNotFound
	}
	return j, nil
}

func (f *fakeStore) ListJobs(_ context.Context, flt store.Filter) ([]domain.Job, error) {
	out := []domain.Job{}
	for _, j := range f.jobs {
		if flt.Status != "" && j.Status != flt.Status {
			continue
		}
		if flt.Priority != 0 && j.Priority != flt.Priority {
			continue
		}
		if flt.Type != "" && j.Type != flt.Type {
			continue
		}
		out = append(out, j)
	}
	return out, nil
}

func (f *fakeStore) CancelJob(_ context.Context, id uuid.UUID) error {
	if f.cancelErr != nil {
		return f.cancelErr
	}
	j, ok := f.jobs[id]
	if !ok {
		return store.ErrNotFound
	}
	j.Status = domain.StatusCancelled
	f.jobs[id] = j
	return nil
}

func (f *fakeStore) UpdateStatus(_ context.Context, id uuid.UUID, from, to domain.Status) error {
	if f.statusErr != nil {
		return f.statusErr
	}
	j, ok := f.jobs[id]
	if !ok {
		return store.ErrNotFound
	}
	if j.Status != from {
		return store.ErrConflict
	}
	j.Status = to
	f.jobs[id] = j
	return nil
}

func (f *fakeStore) RetryJob(_ context.Context, id uuid.UUID) error {
	if f.retryErr != nil {
		return f.retryErr
	}
	j, ok := f.jobs[id]
	if !ok {
		return store.ErrNotFound
	}
	if j.Status != domain.StatusDead {
		return store.ErrConflict
	}
	j.Status = domain.StatusPending
	j.Attempt = 0
	f.jobs[id] = j
	return nil
}

func (f *fakeStore) Stats(context.Context) (store.Stats, error) {
	return f.stats, nil
}

func (f *fakeStore) ListWorkers(context.Context) ([]domain.WorkerInfo, error) {
	return f.workers, nil
}

func (f *fakeStore) Runs(_ context.Context, jobID uuid.UUID) ([]domain.JobRun, error) {
	return f.runs, nil
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }

type fakeQueue struct {
	depths  map[domain.Priority]int64
	pingErr error
	enqueue int
	alive   map[string]bool
	hbErr   error
}

func (f *fakeQueue) Enqueue(context.Context, string, domain.Priority) error {
	f.enqueue++
	return nil
}

func (f *fakeQueue) Depths(context.Context) (map[domain.Priority]int64, error) {
	return f.depths, nil
}

func (f *fakeQueue) Heartbeats(_ context.Context, ids []string) (map[string]bool, error) {
	if f.hbErr != nil {
		return nil, f.hbErr
	}
	out := map[string]bool{}
	for _, id := range ids {
		out[id] = f.alive[id]
	}
	return out, nil
}

func (f *fakeQueue) Ping(context.Context) error { return f.pingErr }

// ------------------------------------------------------------------ setup ---

const testKey = "k"

func newTestServer(t *testing.T) (http.Handler, *fakeStore, *fakeQueue) {
	t.Helper()
	st := newFakeStore()
	q := &fakeQueue{alive: map[string]bool{}, depths: map[domain.Priority]int64{
		domain.PriorityHigh: 1, domain.PriorityNormal: 2, domain.PriorityLow: 3,
	}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewRouter(config.Config{APIKey: testKey}, st, q, log), st, q
}

func do(t *testing.T, h http.Handler, method, path, body string, withKey bool) *httptest.ResponseRecorder {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rdr)
	if withKey {
		req.Header.Set("X-API-Key", testKey)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return v
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var v struct {
		Error apiError `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	if v.Error.Code == "" {
		t.Fatalf("error body has no code: %s", rec.Body.String())
	}
	return v.Error.Code
}

// -------------------------------------------------------------- routing ----

func TestAuthAndHealth(t *testing.T) {
	h, _, _ := newTestServer(t)

	cases := []struct {
		name, method, path string
		withKey            bool
		want               int
	}{
		{"healthz open", http.MethodGet, "/healthz", false, http.StatusOK},
		{"readyz open", http.MethodGet, "/readyz", false, http.StatusOK},
		{"list without key", http.MethodGet, "/api/v1/jobs", false, http.StatusUnauthorized},
		{"stats without key", http.MethodGet, "/api/v1/stats", false, http.StatusUnauthorized},
		{"handlers without key", http.MethodGet, "/api/v1/handlers", false, http.StatusUnauthorized},
		{"with key", http.MethodGet, "/api/v1/jobs", true, http.StatusOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := do(t, h, c.method, c.path, "", c.withKey).Code; got != c.want {
				t.Fatalf("status = %d, want %d", got, c.want)
			}
		})
	}
}

func TestWrongKeyIsUnauthorized(t *testing.T) {
	h, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("X-API-Key", "wrong")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
	if code := errorCode(t, rec); code != "unauthorized" {
		t.Fatalf("code = %s", code)
	}
}

func TestReadyReportsDependencies(t *testing.T) {
	h, st, q := newTestServer(t)

	if rec := do(t, h, http.MethodGet, "/readyz", "", false); rec.Code != http.StatusOK {
		t.Fatalf("healthy readyz = %d", rec.Code)
	}

	st.pingErr = errors.New("connection refused")
	rec := do(t, h, http.MethodGet, "/readyz", "", false)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("degraded readyz = %d, want 503", rec.Code)
	}
	body := decode[map[string]string](t, rec)
	if body["status"] != "degraded" || body["postgres"] == "ok" {
		t.Fatalf("body = %v", body)
	}

	st.pingErr = nil
	q.pingErr = errors.New("redis down")
	if rec := do(t, h, http.MethodGet, "/readyz", "", false); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("redis down readyz = %d, want 503", rec.Code)
	}
}

// ---------------------------------------------------------------- create ----

func TestCreateJob(t *testing.T) {
	h, st, _ := newTestServer(t)

	rec := do(t, h, http.MethodPost, "/api/v1/jobs",
		`{"type":"echo","payload":{"n":1},"priority":"high"}`, true)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body)
	}
	job := decode[domain.Job](t, rec)

	if job.Priority != domain.PriorityHigh {
		t.Fatalf("priority = %v", job.Priority)
	}
	if job.NextRunAt == nil {
		t.Fatal("next_run_at was not set - the scheduler would never pick this job up")
	}
	if len(st.created) != 1 {
		t.Fatalf("stored %d jobs", len(st.created))
	}
}

func TestCreateJobDefaults(t *testing.T) {
	h, _, _ := newTestServer(t)

	rec := do(t, h, http.MethodPost, "/api/v1/jobs", `{"type":"echo"}`, true)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body)
	}
	job := decode[domain.Job](t, rec)
	if job.Priority != domain.PriorityNormal {
		t.Fatalf("priority default = %v, want normal", job.Priority)
	}
	if string(job.Payload) != "{}" {
		t.Fatalf("payload default = %s, want {}", job.Payload)
	}
	if job.NextRunAt == nil {
		t.Fatal("next_run_at missing on a defaulted job")
	}
}

func TestCreateJobDefaultsMaxRetries(t *testing.T) {
	h, st, _ := newTestServer(t)

	rec := do(t, h, http.MethodPost, "/api/v1/jobs", `{"type":"echo"}`, true)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body)
	}
	if got := st.created[0].MaxRetries; got != defaultMaxRetries {
		t.Fatalf("max_retries = %d, want %d (an omitted field must not mean zero, or no API job ever retries)", got, defaultMaxRetries)
	}

	// An explicit zero is still honoured: some jobs must not retry.
	rec = do(t, h, http.MethodPost, "/api/v1/jobs", `{"type":"echo","max_retries":0}`, true)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := st.created[1].MaxRetries; got != 0 {
		t.Fatalf("explicit max_retries=0 became %d", got)
	}
}

func TestCreateJobValidation(t *testing.T) {
	h, _, _ := newTestServer(t)

	cases := []struct{ name, body string }{
		{"missing type", `{}`},
		{"unknown type", `{"type":"does-not-exist"}`},
		{"bad json", `{"type":`},
		{"bad priority", `{"type":"echo","priority":"urgent"}`},
		{"timeout too big", `{"type":"echo","timeout_seconds":99999}`},
		{"negative retries", `{"type":"echo","max_retries":-1}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := do(t, h, http.MethodPost, "/api/v1/jobs", c.body, true)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body)
			}
			errorCode(t, rec)
		})
	}
}

func TestCreateJobRejectsOversizedBody(t *testing.T) {
	h, _, _ := newTestServer(t)

	big := `{"type":"echo","payload":"` + strings.Repeat("x", 2<<20) + `"}`
	rec := do(t, h, http.MethodPost, "/api/v1/jobs", big, true)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
}

// ------------------------------------------------------------------ read ----

func TestGetJobAndRuns(t *testing.T) {
	h, st, _ := newTestServer(t)
	id := uuid.New()
	errMsg := "boom"
	st.jobs[id] = domain.Job{ID: id, Type: "echo", Status: domain.StatusDead, Priority: domain.PriorityNormal}
	st.runs = []domain.JobRun{{ID: uuid.New(), JobID: id, Attempt: 1, Status: domain.StatusRetrying, Error: &errMsg}}

	rec := do(t, h, http.MethodGet, "/api/v1/jobs/"+id.String(), "", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body)
	}
	got := decode[struct {
		Job  domain.Job      `json:"job"`
		Runs []domain.JobRun `json:"runs"`
	}](t, rec)
	if got.Job.ID != id {
		t.Fatalf("job id = %s", got.Job.ID)
	}
	if len(got.Runs) != 1 || got.Runs[0].Error == nil {
		t.Fatalf("runs = %+v", got.Runs)
	}
}

func TestGetJobNotFound(t *testing.T) {
	h, _, _ := newTestServer(t)
	rec := do(t, h, http.MethodGet, "/api/v1/jobs/"+uuid.New().String(), "", true)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rec.Code)
	}
	if code := errorCode(t, rec); code != "not_found" {
		t.Fatalf("code = %s", code)
	}
}

func TestGetJobBadID(t *testing.T) {
	h, _, _ := newTestServer(t)
	rec := do(t, h, http.MethodGet, "/api/v1/jobs/not-a-uuid", "", true)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestListJobsFilters(t *testing.T) {
	h, st, _ := newTestServer(t)
	st.jobs[uuid.New()] = domain.Job{ID: uuid.New(), Type: "echo", Status: domain.StatusQueued, Priority: domain.PriorityHigh}
	st.jobs[uuid.New()] = domain.Job{ID: uuid.New(), Type: "sleep", Status: domain.StatusSucceeded, Priority: domain.PriorityLow}

	cases := []struct {
		query string
		want  int
	}{
		{"", 2},
		{"?status=QUEUED", 1},
		{"?priority=low", 1},
		{"?type=sleep", 1},
		{"?status=DEAD", 0},
	}
	for _, c := range cases {
		rec := do(t, h, http.MethodGet, "/api/v1/jobs"+c.query, "", true)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d", c.query, rec.Code)
		}
		got := decode[struct {
			Count int `json:"count"`
		}](t, rec)
		if got.Count != c.want {
			t.Fatalf("%s: count = %d, want %d", c.query, got.Count, c.want)
		}
	}
}

func TestListJobsRejectsBadFilters(t *testing.T) {
	h, _, _ := newTestServer(t)
	for _, q := range []string{"?status=NOPE", "?priority=urgent", "?limit=0", "?limit=abc"} {
		rec := do(t, h, http.MethodGet, "/api/v1/jobs"+q, "", true)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", q, rec.Code)
		}
	}
}

// ----------------------------------------------------------------- mutate ----

func TestCancelJob(t *testing.T) {
	h, st, _ := newTestServer(t)
	id := uuid.New()
	st.jobs[id] = domain.Job{ID: id, Status: domain.StatusPending, Priority: domain.PriorityNormal}

	rec := do(t, h, http.MethodPost, "/api/v1/jobs/"+id.String()+"/cancel", "", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body)
	}
	if job := decode[domain.Job](t, rec); job.Status != domain.StatusCancelled {
		t.Fatalf("status = %s, want CANCELLED", job.Status)
	}
}

func TestCancelJobConflict(t *testing.T) {
	h, st, _ := newTestServer(t)
	id := uuid.New()
	st.jobs[id] = domain.Job{ID: id, Status: domain.StatusSucceeded, Priority: domain.PriorityNormal}
	st.cancelErr = store.ErrConflict

	rec := do(t, h, http.MethodPost, "/api/v1/jobs/"+id.String()+"/cancel", "", true)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	if code := errorCode(t, rec); code != "conflict" {
		t.Fatalf("code = %s", code)
	}
}

func TestRetryDeadJob(t *testing.T) {
	h, st, q := newTestServer(t)
	id := uuid.New()
	st.jobs[id] = domain.Job{ID: id, Status: domain.StatusDead, Priority: domain.PriorityHigh, Attempt: 4}

	rec := do(t, h, http.MethodPost, "/api/v1/jobs/"+id.String()+"/retry", "", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body)
	}

	job := decode[domain.Job](t, rec)
	if job.Status != domain.StatusPending {
		t.Fatalf("status = %s, want PENDING", job.Status)
	}
	if job.Attempt != 0 {
		t.Fatalf("attempt = %d, want 0: a retried job that keeps its attempt count dead-letters on its first failure", job.Attempt)
	}
	// The API must not enqueue directly: the scheduler owns delivery, and doing
	// it here would race its tick and can double-queue the job.
	if q.enqueue != 0 {
		t.Fatalf("API enqueued the job %d times; the scheduler should do that", q.enqueue)
	}
}

func TestRetryNonDeadJobConflicts(t *testing.T) {
	h, st, _ := newTestServer(t)
	id := uuid.New()
	st.jobs[id] = domain.Job{ID: id, Status: domain.StatusSucceeded, Priority: domain.PriorityNormal}

	rec := do(t, h, http.MethodPost, "/api/v1/jobs/"+id.String()+"/retry", "", true)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
}

// ------------------------------------------------------------------- misc ----

func TestStats(t *testing.T) {
	h, st, _ := newTestServer(t)
	st.stats = store.Stats{
		JobsByStatus:           map[domain.Status]int64{domain.StatusQueued: 3, domain.StatusDead: 1},
		RunsLastHour:           12,
		AvgRunDurationLastHour: 1500 * time.Millisecond,
	}

	rec := do(t, h, http.MethodGet, "/api/v1/stats", "", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	got := decode[struct {
		JobsByStatus map[string]int64 `json:"jobs_by_status"`
		QueueDepths  map[string]int64 `json:"queue_depths"`
		AvgMS        int64            `json:"avg_run_duration_ms_last_hour"`
	}](t, rec)

	if got.JobsByStatus["QUEUED"] != 3 || got.JobsByStatus["DEAD"] != 1 {
		t.Fatalf("jobs_by_status = %v", got.JobsByStatus)
	}
	if got.QueueDepths["high"] != 1 || got.QueueDepths["normal"] != 2 {
		t.Fatalf("queue_depths = %v", got.QueueDepths)
	}
	if got.AvgMS != 1500 {
		t.Fatalf("avg ms = %d, want 1500", got.AvgMS)
	}
}

func TestWorkersAliveFlag(t *testing.T) {
	h, st, q := newTestServer(t)
	// The DB column is stale for both, as it is in production: registration is
	// the only thing that writes it. Redis decides.
	st.workers = []domain.WorkerInfo{
		{ID: "fresh", LastHeartbeat: time.Now().Add(-10 * time.Minute)},
		{ID: "stale", LastHeartbeat: time.Now().Add(-10 * time.Minute)},
	}
	q.alive["fresh"] = true
	q.alive["stale"] = false

	rec := do(t, h, http.MethodGet, "/api/v1/workers", "", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	got := decode[struct {
		Workers []struct {
			ID    string `json:"id"`
			Alive bool   `json:"alive"`
		} `json:"workers"`
	}](t, rec)

	if len(got.Workers) != 2 {
		t.Fatalf("workers = %+v", got.Workers)
	}
	for _, w := range got.Workers {
		if w.ID == "stale" && w.Alive {
			t.Fatal("a worker whose heartbeat key expired is reported alive")
		}
		if w.ID == "fresh" && !w.Alive {
			t.Fatal("a live worker is reported dead because the db column is stale")
		}
	}
}

func TestHandlersListsRegisteredTypes(t *testing.T) {
	handlers.Register("api_test_type", handlers.HandlerFunc(func(context.Context, []byte) error { return nil }))
	h, _, _ := newTestServer(t)

	rec := do(t, h, http.MethodGet, "/api/v1/handlers", "", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	got := decode[struct {
		Handlers []string `json:"handlers"`
	}](t, rec)
	found := false
	for _, name := range got.Handlers {
		if name == "api_test_type" {
			found = true
		}
	}
	if !found {
		t.Fatalf("handlers = %v, want api_test_type", got.Handlers)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h, _, _ := newTestServer(t)
	rec := do(t, h, http.MethodDelete, "/api/v1/jobs", "", true)
	if rec.Code != http.StatusMethodNotAllowed && rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 405 or 404", rec.Code)
	}
}

func TestWorkersFallsBackWhenRedisIsDown(t *testing.T) {
	h, st, q := newTestServer(t)
	st.workers = []domain.WorkerInfo{{ID: "w", LastHeartbeat: time.Now()}}
	q.hbErr = errors.New("redis down")

	rec := do(t, h, http.MethodGet, "/api/v1/workers", "", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 with a degraded liveness guess", rec.Code)
	}
	got := decode[struct {
		Workers []struct {
			Alive bool `json:"alive"`
		} `json:"workers"`
	}](t, rec)
	if len(got.Workers) != 1 || !got.Workers[0].Alive {
		t.Fatalf("fallback liveness failed: %+v", got.Workers)
	}
}

func TestRetryNonDeadJobDoesNotEnqueue(t *testing.T) {
	h, st, q := newTestServer(t)
	id := uuid.New()
	st.jobs[id] = domain.Job{ID: id, Status: domain.StatusQueued, Priority: domain.PriorityNormal}

	if rec := do(t, h, http.MethodPost, "/api/v1/jobs/"+id.String()+"/retry", "", true); rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	if q.enqueue != 0 {
		t.Fatal("a rejected retry still enqueued the job")
	}
}

func TestRequestIDIsEchoed(t *testing.T) {
	h, _, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-API-Key", testKey)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("X-Request-ID"); got == "" {
		t.Fatal("no X-Request-ID header on the response")
	}

	req.Header.Set("X-Request-ID", "abc-123")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got != "abc-123" {
		t.Fatalf("X-Request-ID = %q, want the inbound id to be preserved", got)
	}
}

func TestCORSDisabledByDefault(t *testing.T) {
	h, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("CORS header = %q, want none: a wildcard would let any site call the API", got)
	}
}

func TestCORSAllowsConfiguredOrigin(t *testing.T) {
	st, q := newFakeStore(), &fakeQueue{alive: map[string]bool{}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewRouter(config.Config{APIKey: testKey, WebOrigin: "http://localhost:5173"}, st, q, log)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	req.Header.Set("X-API-Key", testKey)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Fatalf("Allow-Origin = %q", got)
	}

	// Preflight must not require the API key.
	pre := httptest.NewRequest(http.MethodOptions, "/api/v1/jobs", nil)
	pre.Header.Set("Origin", "http://localhost:5173")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, pre)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight status = %d, want 204", rec.Code)
	}
}
