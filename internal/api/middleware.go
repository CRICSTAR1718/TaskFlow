package api

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"time"
)

type middleware func(http.Handler) http.Handler

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// RequestLogger logs one line per request.
func RequestLogger(log *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			log.InfoContext(r.Context(), "request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Duration("duration", time.Since(start)))
		})
	}
}

// Recoverer turns panics into 500s.
func Recoverer(log *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if v := recover(); v != nil {
					log.ErrorContext(r.Context(), "panic", slog.Any("value", v))
					writeError(w, http.StatusInternalServerError, "internal error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// APIKeyAuth requires the X-API-Key header to match key.
func APIKeyAuth(key string) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := r.Header.Get("X-API-Key")
			if subtle.ConstantTimeCompare([]byte(got), []byte(key)) != 1 {
				writeError(w, http.StatusUnauthorized, "invalid api key")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
