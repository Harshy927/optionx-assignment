package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jmoiron/sqlx"

	"github.com/optionx/backend-assignment/internal/instrument"
	"github.com/optionx/backend-assignment/internal/ws"
)

// NewRouter builds the chi router for the service. reg is the instrument
// actor registry that order/position routes are wired against. hub is the
// WebSocket hub position/P&L updates stream through; it may be nil, in
// which case GET /ws is not registered (used by tests that don't need
// streaming).
func NewRouter(db *sqlx.DB, reg *instrument.Registry, hub *ws.Hub) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/health", HealthHandler(db))

	r.Post("/orders", PlaceOrderHandler(reg))
	r.Post("/orders/bracket", PlaceBracketHandler(reg))
	r.Delete("/orders/{id}", CancelOrderHandler(reg))
	r.Get("/positions/{token}", GetPositionHandler(reg))

	if hub != nil {
		r.Get("/ws", ws.Handler(hub))
	}

	return r
}
