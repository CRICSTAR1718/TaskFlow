package api

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"time"

	"github.com/cricstar1718/taskflow/internal/events"
	"github.com/cricstar1718/taskflow/internal/ws"
)

// statsSnapshotInterval is how often connected dashboards receive a stats frame.
const statsSnapshotInterval = 2 * time.Second

// StatsSource produces the snapshot pushed over the websocket.
type StatsSource interface {
	StatsView(ctx context.Context) (any, error)
}

// live wires the websocket hub to the Redis event stream and to the stats
// snapshot ticker. It is the only place that knows both exist.
type live struct {
	hub   *ws.Hub
	sub   events.Subscriber
	stats StatsSource
	log   *slog.Logger
}

// Start pumps events and stats into the hub until ctx is cancelled.
func (l *live) Start(ctx context.Context) {
	if l.sub != nil {
		go l.pumpEvents(ctx)
	}
	if l.stats != nil {
		go l.pumpStats(ctx)
	}
}

func (l *live) pumpEvents(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		ch, err := l.sub.Subscribe(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			l.log.Warn("event subscribe failed, retrying", slog.Any("error", err))
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}
		for e := range ch {
			l.hub.BroadcastEvent(e)
		}
		// The channel closes when its context ends. If that was not us, Redis
		// dropped the subscription, so loop and resubscribe.
		if ctx.Err() != nil {
			return
		}
		l.log.Warn("event subscription closed, resubscribing")
		if !sleepCtx(ctx, time.Second) {
			return
		}
	}
}

func (l *live) pumpStats(ctx context.Context) {
	t := time.NewTicker(statsSnapshotInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Do not pay for a database round trip when nobody is watching.
			if l.hub.Clients() == 0 {
				continue
			}
			view, err := l.stats.StatsView(ctx)
			if err != nil {
				l.log.Debug("ws stats snapshot", slog.Any("error", err))
				continue
			}
			l.hub.BroadcastMessage("stats", view)
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// handleWS upgrades to a websocket. Browsers cannot set request headers on a
// websocket handshake, so the key travels as ?key=.
func (a *API) handleWS(w http.ResponseWriter, r *http.Request) {
	got := r.URL.Query().Get("key")
	if got == "" {
		got = r.Header.Get("X-API-Key")
	}
	if subtle.ConstantTimeCompare([]byte(got), []byte(a.apiKey)) != 1 {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid or missing key")
		return
	}
	if a.hub == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "live updates are not enabled")
		return
	}
	a.hub.Serve(w, r)
}
