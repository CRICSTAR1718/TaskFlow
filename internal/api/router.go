package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/cricstar1718/taskflow/internal/config"
)

// NewRouter wires every route and middleware.
//
// Go 1.22's ServeMux patterns ("POST /api/v1/jobs/{id}/cancel") give method
// routing and path parameters without chi, so there is one less dependency.
func NewRouter(cfg config.Config, st Store, q Queue, log *slog.Logger) http.Handler {
	a := &API{
		store:      st,
		queue:      q,
		log:        log,
		staleAfter: 30 * time.Second,
	}

	mux := http.NewServeMux()

	// Health endpoints stay open: they are what a load balancer or the compose
	// healthcheck calls, and they expose no data.
	mux.HandleFunc("GET /healthz", a.handleHealth)
	mux.HandleFunc("GET /readyz", a.handleReady)

	protected := http.NewServeMux()
	protected.HandleFunc("POST /api/v1/jobs", a.handleCreateJob)
	protected.HandleFunc("GET /api/v1/jobs", a.handleListJobs)
	protected.HandleFunc("GET /api/v1/jobs/{id}", a.handleGetJob)
	protected.HandleFunc("POST /api/v1/jobs/{id}/cancel", a.handleCancelJob)
	protected.HandleFunc("POST /api/v1/jobs/{id}/retry", a.handleRetryJob)
	protected.HandleFunc("GET /api/v1/stats", a.handleStats)
	protected.HandleFunc("GET /api/v1/workers", a.handleWorkers)
	protected.HandleFunc("GET /api/v1/handlers", a.handleHandlers)

	mux.Handle("/api/", APIKeyAuth(cfg.APIKey)(protected))

	return RequestID(Recoverer(log)(RequestLogger(log)(CORS(cfg.WebOrigin)(mux))))
}
