package ws

import (
	"net/http"

	"github.com/gorilla/websocket"
)

// upgrader accepts connections from any origin. This is a local, single-user
// development assignment with no browser-based cross-origin client -- in a
// production deployment, CheckOrigin should validate against a configured
// allow-list. Flagged here as a known gap alongside the endpoint's lack of
// authentication (see README's security notes).
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Handler returns an http.HandlerFunc that upgrades the connection to a
// WebSocket and runs a Client against hub for its lifetime. Mount it at
// whatever path cmd/server chooses (e.g. GET /ws).
//
// SECURITY NOTE: this endpoint has no authentication -- any client that can
// reach it can subscribe to any instrument's position/P&L stream. That is
// acceptable for this local, single-developer assignment, but would need an
// auth handshake (e.g. a token in the initial HTTP request) before being
// exposed beyond localhost.
func Handler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			// Upgrade itself already wrote an error response to w.
			return
		}
		client := NewClient(hub, conn)
		client.Run(r.Context())
	}
}
