package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/cricstar1718/taskflow/internal/config"
	"github.com/cricstar1718/taskflow/internal/ws"
)

// NewRouter wires every route and middleware.
//
// Go 1.22's ServeMux patterns ("POST /api/v1/jobs/{id}/cancel") give method
// routing and path parameters without chi, so there is one less dependency.
func NewRouter(cfg config.Config, st Store, q Queue, log *slog.Logger, opts ...Option) http.Handler {
	a := &API{
		store:      st,
		queue:      q,
		log:        log,
		staleAfter: 30 * time.Second,
		apiKey:     cfg.APIKey,
	}
	for _, o := range opts {
		o(a)
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

	// The websocket authenticates with ?key= inside the handler, because a
	// browser cannot set headers on a handshake; it is therefore registered
	// outside the X-API-Key middleware.
	mux.HandleFunc("GET /ws", a.handleWS)

	return RequestID(Recoverer(log)(RequestLogger(log)(CORS(cfg.WebOrigin)(mux))))
}

// Option customises the router, used to inject the websocket hub.
type Option func(*API)

// WithHub enables GET /ws.
func WithHub(h *ws.Hub) Option { return func(a *API) { a.hub = h } }
