package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/optionx/backend-assignment/internal/instrument"
)

type positionResponse struct {
	Token         string          `json:"token"`
	Qty           int64           `json:"qty"`
	AvgPrice      float64         `json:"avg_price"`
	RealizedPnL   float64         `json:"realized_pnl"`
	UnrealizedPnL float64         `json:"unrealized_pnl"`
	LastLTP       float64         `json:"last_ltp"`
	Orders        []orderResponse `json:"orders"`
}

// GetPositionHandler returns the handler for GET /positions/{token}. It
// returns the instrument's current position (qty, avg price, realized and
// unrealized P&L) plus every order the actor has seen for that instrument.
// An instrument with no orders/ticks yet (no actor spawned) is reported as
// a flat, zero position rather than a 404 -- there is nothing erroneous
// about asking for an instrument's position before it has traded.
func GetPositionHandler(reg *instrument.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := chi.URLParam(r, "token")
		if token == "" {
			writeJSONError(w, http.StatusBadRequest, "token is required")
			return
		}

		act, ok := reg.Peek(token)
		if !ok {
			writeJSON(w, http.StatusOK, positionResponse{Token: token})
			return
		}

		snap, err := act.Snapshot(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to read position: "+err.Error())
			return
		}

		orders := make([]orderResponse, 0, len(snap.Orders))
		for _, o := range snap.Orders {
			orders = append(orders, toOrderResponse(o))
		}

		writeJSON(w, http.StatusOK, positionResponse{
			Token:         token,
			Qty:           snap.Position.Qty,
			AvgPrice:      snap.Position.AvgPrice,
			RealizedPnL:   snap.Position.RealizedPnL,
			UnrealizedPnL: instrument.UnrealizedPnL(snap.Position, snap.LastLTP),
			LastLTP:       snap.LastLTP,
			Orders:        orders,
		})
	}
}
