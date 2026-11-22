// Package ws fans events out to connected dashboard clients.
//
// Design notes:
//   - One goroutine writes to each connection. Calling Write from several
//     goroutines corrupts a websocket, so every send goes through a channel.
//   - A client that stops reading is dropped rather than allowed to block the
//     broadcaster: a slow dashboard must never stall the API.
//   - Ping/pong keepalive detects half-open TCP connections, which are the
//     normal failure mode for a laptop that closed its lid.
package ws

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/cricstar1718/taskflow/internal/domain"
)

const (
	// sendBuffer is how many messages may queue for one client before it is
	// considered slow and disconnected.
	sendBuffer = 64

	defaultPingInterval = 30 * time.Second
	defaultWriteTimeout = 10 * time.Second
	// maxMessageBytes is generous: clients only ever send small control frames.
	maxMessageBytes = 4096
)

// Message is the envelope every client receives.
//
//	{"type":"job_event","data":{...domain.Event...}}
//	{"type":"stats","data":{...}}
type Message struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

// Hub tracks clients and broadcasts messages to all of them.
type Hub struct {
	mu      sync.RWMutex
	clients map[*client]struct{}

	log          *slog.Logger
	pingInterval time.Duration
	writeTimeout time.Duration

	// counters, mostly for tests and an eventual /metrics endpoint
	joined  int64
	dropped int64
}

type client struct {
	send chan []byte
	// closeOnce guards the channel close. Three things can remove a client:
	// its own handler returning, being dropped for being slow, and hub
	// shutdown. Without this, two of them closing the same channel panics -
	// and a drop happens inside Broadcast, which is not a request goroutine, so
	// the panic would take the whole API process down.
	closeOnce sync.Once
}

func (c *client) close() {
	c.closeOnce.Do(func() { close(c.send) })
}

// NewHub creates an empty hub.
func NewHub(opts ...Option) *Hub {
	h := &Hub{
		clients:      make(map[*client]struct{}),
		log:          slog.Default(),
		pingInterval: defaultPingInterval,
		writeTimeout: defaultWriteTimeout,
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// Option configures a Hub.
type Option func(*Hub)

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option { return func(h *Hub) { h.log = l } }

// WithPingInterval sets the keepalive interval.
func WithPingInterval(d time.Duration) Option { return func(h *Hub) { h.pingInterval = d } }

// Register adds a client and returns its message channel and an unregister func.
// The channel is closed when the client is removed or dropped.
func (h *Hub) Register() (<-chan []byte, func()) {
	c := &client{send: make(chan []byte, sendBuffer)}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.joined++
	h.mu.Unlock()

	return c.send, func() {
		h.mu.Lock()
		_, present := h.clients[c]
		delete(h.clients, c)
		h.mu.Unlock()
		if present {
			c.close()
		}
	}
}

// Clients reports how many clients are connected.
func (h *Hub) Clients() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// Broadcast sends msg to every client, dropping it for slow clients.
func (h *Hub) Broadcast(msg []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c.send <- msg:
		default:
			// The client's buffer is full: it is not reading. Disconnect it
			// rather than letting one dashboard apply backpressure to the API.
			h.dropped++
			delete(h.clients, c)
			c.close()
		}
	}
}

// BroadcastMessage marshals and broadcasts a typed message.
func (h *Hub) BroadcastMessage(t string, data any) {
	raw, err := json.Marshal(Message{Type: t, Data: data})
	if err != nil {
		h.log.Error("ws: encode message", slog.String("type", t), slog.Any("error", err))
		return
	}
	h.Broadcast(raw)
}

// BroadcastEvent sends a job event.
func (h *Hub) BroadcastEvent(e domain.Event) { h.BroadcastMessage("job_event", e) }

// Run keeps the hub alive until ctx is cancelled, then disconnects everyone.
func (h *Hub) Run(ctx context.Context) {
	<-ctx.Done()

	h.mu.Lock()
	for c := range h.clients {
		delete(h.clients, c)
		c.close()
	}
	h.mu.Unlock()
	h.log.Info("ws hub stopped")
}

// Serve upgrades an HTTP request to a websocket and pumps messages until the
// connection dies. It blocks, so call it from a handler.
//
// Authentication is the caller's job: the API checks the ?key= query parameter
// before calling this, because browsers cannot set headers on a websocket.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// The API key in the query string is the auth; an origin check here
		// would break dashboards served from another port in development.
		InsecureSkipVerify: true,
	})
	if err != nil {
		h.log.Warn("ws: accept", slog.Any("error", err))
		return
	}
	conn.SetReadLimit(maxMessageBytes)
	defer conn.CloseNow() //nolint:errcheck // best effort on the way out

	msgs, unregister := h.Register()
	defer unregister()

	h.log.Debug("ws: client connected", slog.Int("clients", h.Clients()))

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Reader: we do not expect data, but a read is how pongs are processed and
	// how a client disconnect is noticed.
	go func() {
		defer cancel()
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}()

	ping := time.NewTicker(h.pingInterval)
	defer ping.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			if err := h.write(ctx, conn, msg); err != nil {
				return
			}
		case <-ping.C:
			pctx, pcancel := context.WithTimeout(ctx, h.writeTimeout)
			err := conn.Ping(pctx)
			pcancel()
			if err != nil {
				return
			}
		}
	}
}

func (h *Hub) write(ctx context.Context, conn *websocket.Conn, msg []byte) error {
	wctx, cancel := context.WithTimeout(ctx, h.writeTimeout)
	defer cancel()
	if err := conn.Write(wctx, websocket.MessageText, msg); err != nil {
		if !errors.Is(err, context.Canceled) {
			h.log.Debug("ws: write", slog.Any("error", err))
		}
		return err
	}
	return nil
}
