// Package ws fans events out to connected dashboard clients.
package ws

import (
	"context"
	"sync"
)

// Hub tracks clients and broadcasts messages to all of them.
// The gorilla/websocket upgrade is wired in later.
type Hub struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

// NewHub creates an empty hub.
func NewHub() *Hub { return &Hub{clients: make(map[chan []byte]struct{})} }

// Register adds a client and returns its message channel and an unregister func.
func (h *Hub) Register() (<-chan []byte, func()) {
	ch := make(chan []byte, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.clients, ch)
		h.mu.Unlock()
	}
}

// Broadcast sends msg to every client, dropping it for slow clients.
func (h *Hub) Broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
		}
	}
}

// Run blocks until ctx is cancelled.
func (h *Hub) Run(ctx context.Context) { <-ctx.Done() }
