package api

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
	"github.com/google/uuid"

	"github.com/cricstar1718/taskflow/internal/config"
	"github.com/cricstar1718/taskflow/internal/domain"
	"github.com/cricstar1718/taskflow/internal/ws"
)

func newLiveServer(t *testing.T) (*httptest.Server, string, *ws.Hub) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub := ws.NewHub(ws.WithLogger(log))
	hctx, hcancel := context.WithCancel(context.Background())
	t.Cleanup(hcancel)
	go hub.Run(hctx)

	st := newFakeStore()
	q := &fakeQueue{alive: map[string]bool{}}
	srv := httptest.NewServer(NewRouter(config.Config{APIKey: testKey}, st, q, log, WithHub(hub)))
	t.Cleanup(srv.Close)

	return srv, "ws" + strings.TrimPrefix(srv.URL, "http"), hub
}

func dialWS(t *testing.T, base, query string) (*websocket.Conn, *http.Response) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, base+"/ws"+query, nil)
	if err != nil {
		if resp != nil {
			return nil, resp
		}
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn, resp
}

func TestWSServesEventsToAClient(t *testing.T) {
	_, wsURL, hub := newLiveServer(t)

	conn, _ := dialWS(t, wsURL, "?key="+testKey)
	waitForClients(t, hub, 1)

	hub.BroadcastEvent(domain.Event{
		JobID: uuid.New(), Status: domain.StatusRunning, WorkerID: "w1", Attempt: 1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var msg struct {
		Type string       `json:"type"`
		Data domain.Event `json:"data"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("decode %s: %v", data, err)
	}
	if msg.Type != "job_event" || msg.Data.Status != domain.StatusRunning {
		t.Fatalf("message = %+v", msg)
	}
}

func TestWSRequiresAPIKey(t *testing.T) {
	_, wsURL, _ := newLiveServer(t)

	// No key at all: the handshake must be rejected with 401, not upgraded.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, wsURL+"/ws", nil)
	if err == nil {
		t.Fatal("websocket accepted without a key")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		t.Fatalf("status = %d, want 401", code)
	}

	// Wrong key: same.
	if _, resp, err := websocket.Dial(ctx, wsURL+"/ws?key=nope", nil); err == nil {
		t.Fatal("websocket accepted a wrong key")
	} else if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong key status = %v", resp)
	}
}

func TestWSWithoutHubIsUnavailable(t *testing.T) {
	h, _, _ := newTestServer(t) // no WithHub
	rec := do(t, h, http.MethodGet, "/ws?key="+testKey, "", false)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when live updates are disabled", rec.Code)
	}
}

func TestWSDisconnectRemovesTheClient(t *testing.T) {
	_, wsURL, hub := newLiveServer(t)

	conn, _ := dialWS(t, wsURL, "?key="+testKey)
	waitForClients(t, hub, 1)

	if err := conn.Close(websocket.StatusNormalClosure, "bye"); err != nil {
		t.Fatalf("close: %v", err)
	}
	waitForClients(t, hub, 0)
}

func waitForClients(t *testing.T, hub *ws.Hub, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hub.Clients() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("hub clients = %d, want %d", hub.Clients(), want)
}
