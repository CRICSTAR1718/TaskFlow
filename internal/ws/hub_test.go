package ws

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/cricstar1718/taskflow/internal/domain"
)

func quietHub(t *testing.T) *Hub {
	t.Helper()
	h := NewHub(WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), WithPingInterval(50*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go h.Run(ctx)
	return h
}

// ---------------------------------------------------------------- hub unit --

func TestRegisterAndBroadcast(t *testing.T) {
	h := quietHub(t)

	ch, unregister := h.Register()
	defer unregister()

	if h.Clients() != 1 {
		t.Fatalf("clients = %d, want 1", h.Clients())
	}

	h.Broadcast([]byte(`{"type":"stats"}`))
	select {
	case msg := <-ch:
		if string(msg) != `{"type":"stats"}` {
			t.Fatalf("got %s", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("broadcast never arrived")
	}
}

func TestUnregisterClosesChannelOnce(t *testing.T) {
	h := quietHub(t)
	ch, unregister := h.Register()
	unregister()
	unregister() // a double unregister must not panic on a closed channel

	if _, ok := <-ch; ok {
		t.Fatal("channel should be closed after unregister")
	}
	if h.Clients() != 0 {
		t.Fatalf("clients = %d, want 0", h.Clients())
	}
}

func TestSlowClientIsDropped(t *testing.T) {
	h := quietHub(t)
	// Register but never read: the buffer fills and the client is dropped
	// rather than allowed to block the broadcaster.
	_, unregister := h.Register()
	defer unregister()

	for i := 0; i < sendBuffer+5; i++ {
		h.Broadcast([]byte(`{"type":"stats"}`))
	}
	if h.Clients() != 0 {
		t.Fatalf("clients = %d, want the slow client dropped", h.Clients())
	}
}

func TestBroadcastEventShape(t *testing.T) {
	h := quietHub(t)
	ch, unregister := h.Register()
	defer unregister()

	h.BroadcastEvent(domain.Event{Status: domain.StatusRunning, Attempt: 2})
	msg := <-ch

	var got struct {
		Type string       `json:"type"`
		Data domain.Event `json:"data"`
	}
	if err := json.Unmarshal(msg, &got); err != nil {
		t.Fatalf("decode %s: %v", msg, err)
	}
	if got.Type != "job_event" {
		t.Fatalf("type = %q, want job_event", got.Type)
	}
	if got.Data.Status != domain.StatusRunning || got.Data.Attempt != 2 {
		t.Fatalf("data = %+v", got.Data)
	}
}

func TestRunDisconnectsEveryone(t *testing.T) {
	h := NewHub(WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.Run(ctx); close(done) }()

	ch, _ := h.Register()
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
	if _, ok := <-ch; ok {
		t.Fatal("client channel left open after shutdown")
	}
}

// ------------------------------------------------------ real connections ----

// newWSServer starts a real HTTP server so the tests exercise an actual
// websocket handshake rather than a mocked connection.
func newWSServer(t *testing.T, h *Hub) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(h.Serve))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func TestWebsocketDeliversMessages(t *testing.T) {
	h := quietHub(t)
	wsURL := newWSServer(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	// Wait for the server to register the client: Serve registers after Accept.
	waitFor(t, func() bool { return h.Clients() == 1 })

	h.BroadcastEvent(domain.Event{Status: domain.StatusSucceeded, Attempt: 1})

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var msg struct {
		Type string       `json:"type"`
		Data domain.Event `json:"data"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msg.Type != "job_event" || msg.Data.Status != domain.StatusSucceeded {
		t.Fatalf("message = %+v", msg)
	}

	// Closing the client must remove it from the hub.
	_ = conn.Close(websocket.StatusNormalClosure, "done")
	waitFor(t, func() bool { return h.Clients() == 0 })
}

func TestWebsocketSurvivesKeepalivePings(t *testing.T) {
	h := quietHub(t) // ping interval 50ms
	wsURL := newWSServer(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	waitFor(t, func() bool { return h.Clients() == 1 })

	// Several ping intervals pass while we are idle. The connection must stay
	// open and still deliver a message afterwards.
	time.Sleep(300 * time.Millisecond)
	h.BroadcastEvent(domain.Event{Status: domain.StatusQueued})

	if _, _, err := conn.Read(ctx); err != nil {
		t.Fatalf("connection died during idle pings: %v", err)
	}
}

func TestConcurrentClients(t *testing.T) {
	h := quietHub(t)
	wsURL := newWSServer(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const n = 5
	conns := make([]*websocket.Conn, 0, n)
	for i := 0; i < n; i++ {
		c, _, err := websocket.Dial(ctx, wsURL, nil)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, c)
	}
	for _, c := range conns {
		defer c.CloseNow()
	}
	waitFor(t, func() bool { return h.Clients() == n })

	h.BroadcastEvent(domain.Event{Status: domain.StatusRunning})

	for i, c := range conns {
		rctx, rcancel := context.WithTimeout(ctx, time.Second)
		_, _, err := c.Read(rctx)
		rcancel()
		if err != nil {
			t.Fatalf("client %d did not receive the broadcast: %v", i, err)
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within 3s")
}
