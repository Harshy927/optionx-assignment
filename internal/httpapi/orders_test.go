package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/optionx/backend-assignment/internal/instrument"
)

// newTestRouter builds a router backed by a fresh registry (no DB needed for
// these tests -- order/position routes don't touch storage in this task;
// persistence write-through is added in Task 8). health_test.go's DB-backed
// tests are unaffected since they call HealthHandler directly, not through
// this router.
func newTestRouter(t *testing.T) (http.Handler, *instrument.Registry, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	reg := instrument.NewRegistry(ctx)
	return NewRouter(nil, reg, nil), reg, ctx
}

func doJSON(t *testing.T, r http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reqBody *bytes.Buffer
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = bytes.NewBuffer(b)
	} else {
		reqBody = bytes.NewBuffer(nil)
	}
	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func decodeJSON[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var out T
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	return out
}

func TestPlaceOrder_Market_FillsImmediately(t *testing.T) {
	r, reg, ctx := newTestRouter(t)

	// Seed a known price for the instrument before placing a market order,
	// same as how cmd/server delivers ticks to actors as they arrive.
	require.NoError(t, reg.Actor("NIFTY26AUG24800CE").Tick(ctx, 132.55))

	rec := doJSON(t, r, http.MethodPost, "/orders", placeOrderRequest{
		Token: "NIFTY26AUG24800CE", Side: "buy", Type: "market", Qty: 10,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	resp := decodeJSON[placeOrderResponse](t, rec)
	require.Equal(t, "filled", resp.Outcome)
	require.NotNil(t, resp.Fill)
	require.Equal(t, 132.55, resp.Fill.Price)
	require.Equal(t, "filled", resp.Order.Status)
	require.NotEmpty(t, resp.Order.ID)
}

func TestPlaceOrder_Limit_RestsThenFillsOnCrossingTick(t *testing.T) {
	r, reg, ctx := newTestRouter(t)
	require.NoError(t, reg.Actor("X").Tick(ctx, 100.0))

	rec := doJSON(t, r, http.MethodPost, "/orders", placeOrderRequest{
		Token: "X", Side: "buy", Type: "limit", Qty: 5, LimitPrice: 90.0,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	resp := decodeJSON[placeOrderResponse](t, rec)
	require.Equal(t, "resting", resp.Outcome)
	orderID := resp.Order.ID

	// A crossing tick, delivered the same way cmd/server delivers real feed
	// ticks, must fill it.
	require.NoError(t, reg.Actor("X").Tick(ctx, 88.0))

	posRec := doJSON(t, r, http.MethodGet, "/positions/X", nil)
	require.Equal(t, http.StatusOK, posRec.Code)
	pos := decodeJSON[positionResponse](t, posRec)
	require.Equal(t, int64(5), pos.Qty)
	require.Equal(t, 88.0, pos.AvgPrice)

	var found bool
	for _, o := range pos.Orders {
		if o.ID == orderID {
			found = true
			require.Equal(t, "filled", o.Status)
		}
	}
	require.True(t, found, "placed order must appear in the position's order list")
}

func TestPlaceOrder_InvalidBody_Rejected(t *testing.T) {
	r, _, _ := newTestRouter(t)

	rec := doJSON(t, r, http.MethodPost, "/orders", placeOrderRequest{
		Token: "X", Side: "sideways", Type: "market", Qty: 1,
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	resp := decodeJSON[placeOrderResponse](t, rec)
	require.NotEmpty(t, resp.Error)
}

func TestCancelOrder_RestingOrder_Succeeds(t *testing.T) {
	r, reg, ctx := newTestRouter(t)
	require.NoError(t, reg.Actor("X").Tick(ctx, 100.0))

	placeRec := doJSON(t, r, http.MethodPost, "/orders", placeOrderRequest{
		Token: "X", Side: "buy", Type: "limit", Qty: 5, LimitPrice: 50.0,
	})
	placed := decodeJSON[placeOrderResponse](t, placeRec)
	require.Equal(t, "resting", placed.Outcome)

	cancelRec := doJSON(t, r, http.MethodDelete, "/orders/"+placed.Order.ID, nil)
	require.Equal(t, http.StatusOK, cancelRec.Code)
	cancelResp := decodeJSON[cancelOrderResponse](t, cancelRec)
	require.Equal(t, "cancelled", cancelResp.Outcome)

	// A later crossing tick must NOT fill the cancelled order.
	require.NoError(t, reg.Actor("X").Tick(ctx, 10.0))
	posRec := doJSON(t, r, http.MethodGet, "/positions/X", nil)
	pos := decodeJSON[positionResponse](t, posRec)
	require.Equal(t, int64(0), pos.Qty, "cancelled order must never fill")
}

func TestCancelOrder_UnknownID_NotFound(t *testing.T) {
	r, _, _ := newTestRouter(t)

	rec := doJSON(t, r, http.MethodDelete, "/orders/does-not-exist", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
	resp := decodeJSON[cancelOrderResponse](t, rec)
	require.Equal(t, "not_found", resp.Outcome)
}

func TestGetPosition_UnknownInstrument_ReturnsFlat(t *testing.T) {
	r, _, _ := newTestRouter(t)

	rec := doJSON(t, r, http.MethodGet, "/positions/NEVER-SEEN", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	pos := decodeJSON[positionResponse](t, rec)
	require.Equal(t, int64(0), pos.Qty)
	require.Empty(t, pos.Orders)
}
