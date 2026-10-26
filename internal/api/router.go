package api

import (
	"log/slog"
	"net/http"

	"github.com/your-org/taskflow/internal/config"
)

// NewRouter wires routes and middleware. chi replaces ServeMux later.
func NewRouter(cfg config.Config, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealth)

	protected := http.NewServeMux()
	protected.HandleFunc("GET /api/v1/jobs", notImplemented)
	protected.HandleFunc("POST /api/v1/jobs", notImplemented)
	mux.Handle("/api/", APIKeyAuth(cfg.APIKey)(protected))

	return Recoverer(log)(RequestLogger(log)(mux))
}
