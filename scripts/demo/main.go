// Command demo is the Phase II intermediate demonstration, as a program.
//
// It runs the scenario end to end against a real API and prints a summary table:
//
//  1. reset the database so the numbers are honest (optional)
//  2. submit high/normal/low "sleep" jobs, failing jobs, and a recurring cron job
//  3. print live stats once a second while the workers drain the backlog
//  4. prove priority: compare how long each priority waited before it ran
//  5. kill a worker mid-run and show its jobs recovered and finished anyway
//
// Why a Go program instead of scripts/demo.sh: it runs identically on Windows,
// macOS and Linux, it can talk to the API and to Postgres in the same process,
// and every number it prints comes from a query you can re-run yourself. A demo
// that cannot lie about its own output is worth more than a pretty one.
//
//	docker compose up -d --scale worker=3
//	go run ./scripts/demo
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/cricstar1718/taskflow/internal/domain"
	"github.com/cricstar1718/taskflow/internal/store"
)

type config struct {
	apiURL      string
	apiKey      string
	databaseURL string
	high        int
	normal      int
	low         int
	failJobs    int
	sleep       float64
	statsTick   time.Duration
	killWorker  bool
	drain       time.Duration
	reset       bool
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.apiURL, "api", env("API_URL", "http://localhost:8080"), "API base URL")
	flag.StringVar(&cfg.apiKey, "key", env("API_KEY", "dev-api-key"), "API key")
	flag.StringVar(&cfg.databaseURL, "database", env("DATABASE_URL",
		"postgres://taskflow:taskflow@localhost:5432/taskflow?sslmode=disable"),
		"Postgres DSN, used for the priority and recovery queries")
	flag.IntVar(&cfg.high, "high", 10, "high-priority jobs")
	flag.IntVar(&cfg.normal, "normal", 30, "normal-priority jobs")
	flag.IntVar(&cfg.low, "low", 30, "low-priority jobs")
	flag.IntVar(&cfg.failJobs, "failing", 5, "jobs that fail and retry")
	flag.Float64Var(&cfg.sleep, "payload-seconds", 1, "how long each sleep job runs")
	flag.DurationVar(&cfg.statsTick, "stats-interval", time.Second, "how often to print live stats")
	flag.BoolVar(&cfg.killWorker, "kill-worker", true, "kill a worker mid-run to show recovery")
	flag.DurationVar(&cfg.drain, "drain-timeout", 3*time.Minute, "how long to wait for the backlog to clear")
	flag.BoolVar(&cfg.reset, "reset", true, "TRUNCATE jobs before starting, so the numbers are this run only")
	flag.Parse()

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\ndemo failed: %v\n", err)
		os.Exit(1)
	}
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

func run(cfg config) error {
	ctx := context.Background()

	st, err := store.New(ctx, cfg.databaseURL)
	if err != nil {
		return fmt.Errorf("connect to postgres (needed for the priority and recovery evidence): %w", err)
	}
	defer st.Close()

	client := &apiClient{base: cfg.apiURL, key: cfg.apiKey, http: &http.Client{Timeout: 10 * time.Second}}

	banner("TaskFlow - Phase II demonstration")
	fmt.Printf("  api       %s\n", cfg.apiURL)
	fmt.Printf("  database  %s\n", redact(cfg.databaseURL))
	fmt.Printf("  plan      %d high + %d normal + %d low sleep jobs (%.1fs each),\n"+
		"            %d failing jobs (retry then DEAD), 1 cron job every 10s\n",
		cfg.high, cfg.normal, cfg.low, cfg.sleep, cfg.failJobs)

	if err := checkAPI(ctx, client); err != nil {
		return err
	}

	if cfg.reset {
		fmt.Println("\n-- reset: clearing previous jobs so every number below is this run only")
		if err := st.ResetDemoData(ctx); err != nil {
			return fmt.Errorf("reset: %w", err)
		}
	}

	// ---------------------------------------------------------------- submit --
	banner("1. submitting jobs")
	start := time.Now()
	var submitted int

	submit := func(n int, p domain.Priority, body map[string]any) error {
		for i := 0; i < n; i++ {
			body["priority"] = p.String()
			if err := client.createJob(ctx, body); err != nil {
				return err
			}
			submitted++
		}
		return nil
	}

	sleepBody := map[string]any{"type": "sleep", "payload": map[string]any{"seconds": cfg.sleep}}
	groups := []struct {
		name string
		n    int
		prio domain.Priority
	}{
		{"high", cfg.high, domain.PriorityHigh},
		{"normal", cfg.normal, domain.PriorityNormal},
		{"low", cfg.low, domain.PriorityLow},
	}
	for _, g := range groups {
		if err := submit(g.n, g.prio, sleepBody); err != nil {
			return fmt.Errorf("submit %s jobs: %w", g.name, err)
		}
		fmt.Printf("  %-6s %3d submitted\n", g.name, g.n)
	}

	if err := submit(cfg.failJobs, domain.PriorityNormal,
		map[string]any{"type": "fail", "payload": map[string]any{"message": "demo failure"}}); err != nil {
		return fmt.Errorf("submit failing jobs: %w", err)
	}
	fmt.Printf("  %-6s %3d submitted (they retry, then go DEAD)\n", "fail", cfg.failJobs)

	// The recurring job is what proves the scheduler works: it re-arms itself
	// every 10 seconds instead of running once.
	if err := client.createJob(ctx, map[string]any{
		"type":      "echo",
		"payload":   map[string]any{"kind": "recurring"},
		"priority":  "high",
		"cron_expr": "*/10 * * * *",
	}); err != nil {
		return fmt.Errorf("submit cron job: %w", err)
	}
	submitted++
	fmt.Printf("  %-6s %3d submitted (cron */10 * * * *)\n", "cron", 1)
	fmt.Printf("\n  %d jobs submitted in %s\n", submitted, time.Since(start).Round(time.Millisecond))

	// ------------------------------------------------------- live stats loop --
	banner("2. live stats, once a second, until the backlog drains")
	active, err := watchDrain(ctx, st, cfg)
	if err != nil {
		return err
	}
	if active > 0 {
		fmt.Printf("\n  ! %d jobs still active after %s - reporting what we have\n", active, cfg.drain)
	}

	// ----------------------------------------------------- priority evidence --
	banner("3. priority evidence: high must finish sooner than low")
	completion, err := st.CompletionByPriority(ctx)
	if err != nil {
		return fmt.Errorf("priority stats: %w", err)
	}
	printPriorityTable(completion)

	// -------------------------------------------------------------- recovery --
	var reclaimed int64
	if cfg.killWorker {
		banner("4. recovery: killing a worker mid-run")
		reclaimed, err = killAndShowRecovery(ctx, client, st)
		if err != nil {
			fmt.Printf("\n  ! recovery demo incomplete: %v\n", err)
			fmt.Println("    (this step needs the docker compose stack; -kill-worker=false skips it)")
		}
	} else {
		banner("4. recovery: skipped (-kill-worker=false)")
	}

	// --------------------------------------------------------------- summary --
	banner("5. summary")
	snap, err := st.SnapshotForDemo(ctx)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	activeEnd, err := st.ActiveJobs(ctx)
	if err != nil {
		return fmt.Errorf("active jobs: %w", err)
	}
	printSummary(snap, completion, submitted, time.Since(start), reclaimed, activeEnd)
	return nil
}

// watchDrain prints stats every tick until nothing is active, and returns the
// number of jobs still active when it stopped.
func watchDrain(ctx context.Context, st *store.Store, cfg config) (int64, error) {
	start := time.Now()
	ticker := time.NewTicker(cfg.statsTick)
	defer ticker.Stop()
	deadline := time.After(cfg.drain)

	fmt.Printf("  %-6s %7s %6s %7s %8s %9s %4s\n",
		"t", "PENDING", "QUEUED", "RUNNING", "RETRYING", "SUCCEEDED", "DEAD")
	for {
		snap, err := st.SnapshotForDemo(ctx)
		if err != nil {
			return 0, err
		}
		by := snap.JobsByStatus
		fmt.Printf("  %-6s %7d %6d %7d %8d %9d %4d\n",
			time.Since(start).Round(time.Second),
			by[domain.StatusPending], by[domain.StatusQueued], by[domain.StatusRunning],
			by[domain.StatusRetrying], by[domain.StatusSucceeded], by[domain.StatusDead])

		active := by[domain.StatusPending] + by[domain.StatusQueued] +
			by[domain.StatusRunning] + by[domain.StatusRetrying]
		if active == 0 {
			return 0, nil
		}

		select {
		case <-ctx.Done():
			return active, ctx.Err()
		case <-deadline:
			return active, nil
		case <-ticker.C:
		}
	}
}

func printPriorityTable(rows []store.PriorityCompletion) {
	if len(rows) == 0 {
		fmt.Println("  nothing finished, so there is nothing to compare")
		return
	}
	fmt.Printf("\n  %-8s %6s %20s %13s\n", "priority", "jobs", "avg seconds to run", "avg run time")
	fmt.Printf("  %-8s %6s %20s %13s\n", "--------", "------", "------------------", "------------")
	for _, r := range rows {
		fmt.Printf("  %-8s %6d %20.2f %13.2f\n", r.Priority.String(), r.Jobs, r.AvgSecondsToFinish, r.AvgRunSeconds)
	}

	// State the finding instead of making the audience squint at the table.
	// The recurring job runs at high priority, so it contributes more than the
	// submitted backlog; say so rather than letting the count look wrong.
	var high, low *store.PriorityCompletion
	for i := range rows {
		switch rows[i].Priority {
		case domain.PriorityHigh:
			high = &rows[i]
		case domain.PriorityLow:
			low = &rows[i]
		}
	}
	if high != nil {
		fmt.Printf("\n  high includes the recurring job, which runs every 10s\n")
	}
	if high == nil || low == nil || high.AvgSecondsToFinish == 0 || low.AvgSecondsToFinish == 0 {
		return
	}
	fmt.Printf("\n  high priority finished in %.2fs on average, low in %.2fs", high.AvgSecondsToFinish, low.AvgSecondsToFinish)
	if high.AvgSecondsToFinish < low.AvgSecondsToFinish {
		fmt.Printf(" -> %.1fx sooner\n", low.AvgSecondsToFinish/high.AvgSecondsToFinish)
	} else {
		// Never hide a result that does not support the claim.
		fmt.Printf(" -> high did NOT finish first this run, see the table above\n")
	}
}

// killAndShowRecovery stops one worker container and returns how many jobs were
// reclaimed. It needs docker and the compose stack; when either is missing it
// says so rather than inventing a result.
func killAndShowRecovery(ctx context.Context, client *apiClient, st *store.Store) (int64, error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return 0, fmt.Errorf("docker is not on PATH")
	}

	out, err := exec.CommandContext(ctx, "docker", "compose", "ps", "-q", "worker").Output()
	if err != nil {
		return 0, fmt.Errorf("docker compose ps: %w", err)
	}
	ids := strings.Fields(string(out))
	if len(ids) == 0 {
		return 0, fmt.Errorf("no worker containers - is the compose stack up?")
	}
	fmt.Printf("  %d worker container(s) running\n", len(ids))

	busy, err := pickBusyWorker(ctx, client, st)
	if err != nil {
		return 0, err
	}
	if busy == "" {
		// The backlog already drained; make a worker busy on purpose so the kill
		// proves something.
		fmt.Println("  nothing in flight - submitting a 30s job so a worker is definitely busy")
		if err := client.createJob(ctx, map[string]any{
			"type": "sleep", "payload": map[string]any{"seconds": 30}, "priority": "normal",
		}); err != nil {
			return 0, err
		}
		time.Sleep(3 * time.Second)
		if busy, err = pickBusyWorker(ctx, client, st); err != nil {
			return 0, err
		}
		if busy == "" {
			return 0, fmt.Errorf("could not find a busy worker")
		}
	}

	holder, inFlight, err := st.InFlightForWorker(ctx, busy)
	if err != nil {
		return 0, err
	}
	fmt.Printf("  worker %s is running %d job(s)\n", holder, len(inFlight))
	for _, id := range inFlight {
		fmt.Printf("    - %s\n", id)
	}

	// Map the worker id back to a container: the container hostname is a prefix
	// of the worker id, because WORKER_ID defaults to <hostname>-<random>.
	target := ""
	for _, cid := range ids {
		// #nosec G204 -- cid comes from `docker compose ps -q worker` output, not
		// from user input; it is a container id for a container we just listed.
		host, hErr := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.Config.Hostname}}", cid).Output()
		if hErr != nil {
			continue
		}
		if strings.HasPrefix(busy, strings.TrimSpace(string(host))) {
			target = cid
			break
		}
	}
	if target == "" {
		target = ids[0]
		fmt.Println("  (could not match the worker id to a container; killing the first one)")
	}

	short := target
	if len(short) > 12 {
		short = short[:12]
	}
	fmt.Printf("  docker kill %s\n", short)
	if err := exec.CommandContext(ctx, "docker", "kill", target).Run(); err != nil {
		return 0, fmt.Errorf("docker kill: %w", err)
	}

	fmt.Println("  waiting for the recovery service to reclaim those jobs...")
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		n, err := st.RecoveredCount(ctx)
		if err != nil {
			return 0, err
		}
		if n > 0 {
			fmt.Printf("  %d job(s) reclaimed from the dead worker and finished on a survivor\n", n)
			return n, nil
		}
		time.Sleep(time.Second)
	}
	return 0, fmt.Errorf("nothing reclaimed within 45s - check `docker compose logs recovery`")
}

// pickBusyWorker returns an alive worker id that currently holds in-flight jobs.
func pickBusyWorker(ctx context.Context, client *apiClient, st *store.Store) (string, error) {
	workers, err := client.workers(ctx)
	if err != nil {
		return "", err
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	for _, w := range workers {
		if !w.Alive {
			continue
		}
		n, err := st.InFlightCount(ctx, w.ID)
		if err != nil {
			return "", err
		}
		if n > 0 {
			return w.ID, nil
		}
	}
	return "", nil
}

func printSummary(snap store.DemoSnapshot, completion []store.PriorityCompletion,
	submitted int, elapsed time.Duration, reclaimed, active int64) {
	by := snap.JobsByStatus
	fmt.Printf("\n  %-28s %d\n", "submitted", submitted)
	fmt.Printf("  %-28s %s\n", "wall clock", elapsed.Round(time.Second))
	fmt.Printf("  %-28s %d\n", "succeeded", by[domain.StatusSucceeded])
	fmt.Printf("  %-28s %d\n", "dead (retries exhausted)", by[domain.StatusDead])
	fmt.Printf("  %-28s %d\n", "still active", active)
	fmt.Printf("  %-28s %d\n", "runs last hour", snap.RunsLastHour)
	fmt.Printf("  %-28s %dms\n", "avg run duration", snap.AvgRunDurationMs)
	fmt.Printf("  %-28s %d\n", "recovered from a dead worker", snap.RecoveredJobs)
	if reclaimed == 0 && snap.RecoveredJobs > 0 {
		// Be explicit, because this number is the one people quote. It counts
		// jobs whose last_error mentions a heartbeat, and that string is not
		// cleared when such a job later succeeds - so on a database that has
		// seen earlier runs it is a lifetime total, not this run's.
		fmt.Printf("  %-28s %s\n", "", "(lifetime total; none reclaimed in this run)")
	}
	if reclaimed > 0 {
		fmt.Printf("  %-28s %d\n", "reclaimed during this run", reclaimed)
	}

	if len(completion) > 0 {
		fmt.Printf("\n  %-8s %6s %20s\n", "priority", "jobs", "avg seconds to run")
		for _, r := range completion {
			fmt.Printf("  %-8s %6d %20.2f\n", r.Priority.String(), r.Jobs, r.AvgSecondsToFinish)
		}
	}

	fmt.Println("\n  Every number above is a query over the jobs and job_runs tables:")
	fmt.Println("    SELECT status, count(*) FROM jobs GROUP BY status;")
	fmt.Println("    go run ./scripts/demo -reset=false    # repeat without clearing the data")
}

func banner(title string) {
	fmt.Printf("\n%s\n%s\n", strings.Repeat("=", 72), title)
}

// apiClient is the smallest thing that can submit jobs and read workers.
type apiClient struct {
	base string
	key  string
	http *http.Client
}

func (c *apiClient) do(ctx context.Context, method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-Key", c.key)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	payload, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(payload)))
	}
	if out != nil && len(payload) > 0 {
		return json.Unmarshal(payload, out)
	}
	return nil
}

func (c *apiClient) createJob(ctx context.Context, body map[string]any) error {
	return c.do(ctx, http.MethodPost, "/api/v1/jobs", body, nil)
}

type workerInfo struct {
	ID    string `json:"id"`
	Alive bool   `json:"alive"`
}

func (c *apiClient) workers(ctx context.Context) ([]workerInfo, error) {
	var out struct {
		Workers []workerInfo `json:"workers"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/v1/workers", nil, &out); err != nil {
		return nil, err
	}
	return out.Workers, nil
}

func checkAPI(ctx context.Context, c *apiClient) error {
	if err := c.do(ctx, http.MethodGet, "/healthz", nil, nil); err != nil {
		return fmt.Errorf("the api is not answering at %s: %w\n"+
			"  start the stack first:  docker compose up -d --scale worker=3", c.base, err)
	}
	// /api/v1/stats is behind the key, so this also proves the key is right.
	if err := c.do(ctx, http.MethodGet, "/api/v1/stats", nil, nil); err != nil {
		return fmt.Errorf("the api rejected the key %q: %w", c.key, err)
	}
	return nil
}

// redact hides the password in a DSN before it is printed.
func redact(dsn string) string {
	at := strings.LastIndex(dsn, "@")
	scheme := strings.Index(dsn, "//")
	if at < 0 || scheme < 0 || at < scheme {
		return dsn
	}
	return dsn[:scheme+2] + "****:****" + dsn[at:]
}
