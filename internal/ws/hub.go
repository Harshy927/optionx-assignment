// Package ws implements the WebSocket streaming layer: a hub that instrument
// actors publish position/P&L updates into, and per-client connections that
// subscribe to a set of instruments and receive throttled, coalesced
// updates over their own socket.
//
// Backpressure policy (see README for the full design rationale): each
// client owns a small in-memory buffer of "latest update per instrument."
// Publishing into that buffer is always a non-blocking operation -- if the
// client is slow to drain it, newer updates simply overwrite older ones for
// the same instrument rather than queuing. A per-client ticker flushes the
// buffer to the socket at most once every 100ms. This means:
//   - The tick-processing loop (via instrument.Actor.Publish) never blocks
//     on a slow or stalled client, because Hub.Publish is a fan-out of
//     non-blocking sends.
//   - A stalled client is never disconnected either -- it simply falls
//     behind and, once it starts reading again, sees only the most recent
//     state, not a backlog of stale intermediate ticks. This fits a
//     position/P&L feed well: subscribers care about "what is true now,"
//     not "every value it passed through."
package ws

import (
	"sync"

	"github.com/optionx/backend-assignment/internal/instrument"
)

// Hub fans out instrument.Update values to every subscribed client. It
// implements instrument.Publisher, so an instrument.Registry can be wired
// directly to a Hub via Registry.SetPublisher.
type Hub struct {
	mu      sync.RWMutex
	clients map[*Client]struct{}
}

// NewHub creates an empty hub.
func NewHub() *Hub {
	return &Hub{clients: make(map[*Client]struct{})}
}

// Publish implements instrument.Publisher. It is called synchronously by an
// instrument actor's single-writer goroutine, so it must never block: it
// only ever performs a non-blocking, per-client buffer write (see
// Client.receive), regardless of how many clients are subscribed or how
// slow any of them are.
func (h *Hub) Publish(u instrument.Update) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		c.receive(u)
	}
}

// register adds a client to the hub's fan-out set. Called by Client.run.
func (h *Hub) register(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = struct{}{}
}

// unregister removes a client from the hub's fan-out set. Called once a
// client's connection closes.
func (h *Hub) unregister(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
}

// ClientCount returns the number of currently-registered clients. Exposed
// for tests and observability, not used in the hot path.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
