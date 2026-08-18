package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/optionx/backend-assignment/internal/instrument"
	"github.com/optionx/backend-assignment/internal/order"
)

// placeOrderRequest is the JSON body accepted by POST /orders.
type placeOrderRequest struct {
	Token      string  `json:"token"`
	Side       string  `json:"side"` // "buy" | "sell"
	Type       string  `json:"type"` // "market" | "limit"
	Qty        int64   `json:"qty"`
	LimitPrice float64 `json:"limit_price,omitempty"` // required when type == "limit"
}

// orderResponse is the JSON shape returned for an order in API responses.
type orderResponse struct {
	ID         string  `json:"id"`
	Token      string  `json:"token"`
	Side       string  `json:"side"`
	Type       string  `json:"type"`
	Qty        int64   `json:"qty"`
	LimitPrice float64 `json:"limit_price,omitempty"`
	Status     string  `json:"status"`
	EntryID    string  `json:"entry_id,omitempty"`   // set only for a bracket's target/stop children
	SiblingID  string  `json:"sibling_id,omitempty"` // set only for a bracket's target/stop children
}

func toOrderResponse(o order.Order) orderResponse {
	return orderResponse{
		ID: o.ID, Token: o.Token, Side: string(o.Side), Type: string(o.Type),
		Qty: o.Qty, LimitPrice: o.LimitPrice, Status: string(o.Status),
		EntryID: o.EntryID, SiblingID: o.SiblingID,
	}
}

type fillResponse struct {
	Price float64 `json:"price"`
	Qty   int64   `json:"qty"`
}

type placeOrderResponse struct {
	Outcome string        `json:"outcome"` // "filled" | "resting" | "rejected"
	Order   orderResponse `json:"order"`
	Fill    *fillResponse `json:"fill,omitempty"`
	Error   string        `json:"error,omitempty"`
}

// newOrderID generates a short random, URL-safe order identifier. It does
// not need to be globally unique in the cryptographic sense -- just
// unpredictable enough to avoid accidental collisions between concurrent
// requests -- so 16 random bytes (32 hex chars) is comfortably sufficient
// without adding a UUID dependency for this one use.
func newOrderID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// PlaceOrderHandler returns the handler for POST /orders. It decodes the
// request, generates an order ID, routes it to the token's instrument actor
// (creating one on first use), and records the order->token mapping in the
// registry so a later cancel can find the right actor.
func PlaceOrderHandler(reg *instrument.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req placeOrderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
			return
		}

		id, err := newOrderID()
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to generate order id")
			return
		}

		o := order.Order{
			ID:         id,
			Token:      req.Token,
			Side:       order.Side(req.Side),
			Type:       order.Type(req.Type),
			Qty:        req.Qty,
			LimitPrice: req.LimitPrice,
		}
		if err := o.Validate(); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}

		act := reg.Actor(o.Token)
		res, err := act.PlaceOrder(r.Context(), o)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to place order: "+err.Error())
			return
		}

		if res.Outcome != instrument.PlaceOutcomeRejected {
			reg.IndexOrder(o.ID, o.Token)
		}

		resp := placeOrderResponse{
			Outcome: string(res.Outcome),
			Order:   toOrderResponse(res.Order),
			Error:   res.Error,
		}
		if res.Fill != nil {
			resp.Fill = &fillResponse{Price: res.Fill.Price, Qty: res.Fill.Qty}
		}

		status := http.StatusCreated
		if res.Outcome == instrument.PlaceOutcomeRejected {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, resp)
	}
}

// placeBracketRequest is the JSON body accepted by POST /orders/bracket.
type placeBracketRequest struct {
	Token       string  `json:"token"`
	Side        string  `json:"side"` // "buy" | "sell" -- the ENTRY's side
	Qty         int64   `json:"qty"`
	EntryType   string  `json:"entry_type"`            // "market" | "limit"
	EntryPrice  float64 `json:"entry_price,omitempty"` // required when entry_type == "limit"
	TargetPrice float64 `json:"target_price"`          // take-profit limit price, opposite side of entry
	StopPrice   float64 `json:"stop_price"`            // stop-loss trigger price, opposite side of entry
}

type placeBracketResponse struct {
	Outcome string        `json:"outcome"` // describes the entry leg: "filled" | "resting" | "rejected"
	Entry   orderResponse `json:"entry"`
	Target  orderResponse `json:"target"`
	Stop    orderResponse `json:"stop"`
	Fill    *fillResponse `json:"fill,omitempty"`
	Error   string        `json:"error,omitempty"`
}

// PlaceBracketHandler returns the handler for POST /orders/bracket. It
// builds the entry order plus its linked target (take-profit, a Limit
// order) and stop (stop-loss, a Stop order) children -- both on the
// opposite side of the entry, per bracket semantics -- and submits all
// three to the token's instrument actor as one atomic PlaceBracket call.
func PlaceBracketHandler(reg *instrument.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req placeBracketRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
			return
		}

		entryID, err := newOrderID()
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to generate order id")
			return
		}
		targetID, err := newOrderID()
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to generate order id")
			return
		}
		stopID, err := newOrderID()
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to generate order id")
			return
		}

		entrySide := order.Side(req.Side)
		closingSide := order.Sell
		if entrySide == order.Sell {
			closingSide = order.Buy
		}

		entry := order.Order{
			ID: entryID, Token: req.Token, Side: entrySide, Type: order.Type(req.EntryType),
			Qty: req.Qty, LimitPrice: req.EntryPrice,
		}
		target := order.Order{
			ID: targetID, Token: req.Token, Side: closingSide, Type: order.Limit,
			Qty: req.Qty, LimitPrice: req.TargetPrice,
		}
		stop := order.Order{
			ID: stopID, Token: req.Token, Side: closingSide, Type: order.Stop,
			Qty: req.Qty, LimitPrice: req.StopPrice,
		}

		act := reg.Actor(req.Token)
		res, err := act.PlaceBracket(r.Context(), entry, target, stop)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to place bracket order: "+err.Error())
			return
		}

		if res.Outcome != instrument.PlaceOutcomeRejected {
			reg.IndexOrder(entryID, req.Token)
			reg.IndexOrder(targetID, req.Token)
			reg.IndexOrder(stopID, req.Token)
		}

		resp := placeBracketResponse{
			Outcome: string(res.Outcome),
			Entry:   toOrderResponse(res.Entry),
			Target:  toOrderResponse(res.Target),
			Stop:    toOrderResponse(res.Stop),
			Error:   res.Error,
		}
		if res.Fill != nil {
			resp.Fill = &fillResponse{Price: res.Fill.Price, Qty: res.Fill.Qty}
		}

		status := http.StatusCreated
		if res.Outcome == instrument.PlaceOutcomeRejected {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, resp)
	}
}

type cancelOrderResponse struct {
	Outcome string        `json:"outcome"`
	Order   orderResponse `json:"order,omitempty"`
}

// CancelOrderHandler returns the handler for DELETE /orders/{id}. It looks
// up which instrument the order belongs to (via the registry's order index)
// and forwards the cancellation to that instrument's actor. The response's
// Outcome is the actor's definitive, race-safe answer -- see
// instrument.CancelOrderOutcome.
func CancelOrderHandler(reg *instrument.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orderID := chi.URLParam(r, "id")
		if orderID == "" {
			writeJSONError(w, http.StatusBadRequest, "order id is required")
			return
		}

		token, ok := reg.TokenForOrder(orderID)
		if !ok {
			writeJSON(w, http.StatusNotFound, cancelOrderResponse{Outcome: string(instrument.CancelOutcomeNotFound)})
			return
		}

		// Use Actor (not Peek) here: a persistent registry indexes orders
		// from a previous process's lifetime immediately on construction
		// (see Registry.NewPersistentRegistry), before any actor has been
		// spawned for that token. A cancel request for such an order must
		// still resolve to a live actor -- one gets created here, seeded
		// from the same persisted state that indexed the order in the
		// first place -- rather than being reported as not found.
		act := reg.Actor(token)

		res, err := act.CancelOrder(r.Context(), orderID)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to cancel order: "+err.Error())
			return
		}

		status := http.StatusOK
		if res.Outcome == instrument.CancelOutcomeNotFound {
			status = http.StatusNotFound
		}
		writeJSON(w, status, cancelOrderResponse{Outcome: string(res.Outcome), Order: toOrderResponse(res.Order)})
	}
}

// writeJSON writes v as a JSON response body with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
