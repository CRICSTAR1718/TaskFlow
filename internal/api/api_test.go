package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/your-org/taskflow/internal/config"
)

func TestRouter(t *testing.T) {
	h := NewRouter(config.Config{APIKey: "k"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	tests := []struct {
		name, path, key string
		want            int
	}{
		{"health open", "/healthz", "", http.StatusOK},
		{"no key", "/api/v1/jobs", "", http.StatusUnauthorized},
		{"wrong key", "/api/v1/jobs", "x", http.StatusUnauthorized},
		{"good key", "/api/v1/jobs", "k", http.StatusNotImplemented},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.key != "" {
				req.Header.Set("X-API-Key", tt.key)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}
