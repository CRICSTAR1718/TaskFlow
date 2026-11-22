// Package api exposes the REST API and WebSocket endpoint.
package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/cricstar1718/taskflow/internal/config"
	"github.com/cricstar1718/taskflow/internal/ws"
)

// Deps are the collaborators cmd/api passes in.
type Deps struct {
	Store Store
	Queue Queue
}

// Run serves HTTP until ctx is cancelled, then shuts down gracefully.
func Run(ctx context.Context, cfg config.Config, deps Deps, log *slog.Logger) error {
	hub := ws.NewHub()
	go hub.Run(ctx)

	// Fail fast rather than serve 503s from every endpoint.
	if err := deps.Store.Ping(ctx); err != nil {
		return fmt.Errorf("postgres not ready: %w", err)
	}
	if err := deps.Queue.Ping(ctx); err != nil {
		return fmt.Errorf("redis not ready: %w", err)
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           NewRouter(cfg, deps.Store, deps.Queue, log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("listening", slog.String("addr", cfg.HTTPAddr))
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("listen: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}
