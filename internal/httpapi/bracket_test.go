package httpapi

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlaceBracket_EntryFillsImmediately_ChildrenActivateAndOneCancelsOther(t *testing.T) {
	r, reg, ctx := newTestRouter(t)
	require.NoError(t, reg.Actor("X").Tick(ctx, 100.0))

	rec := doJSON(t, r, http.MethodPost, "/orders/bracket", placeBracketRequest{
		Token: "X", Side: "buy", Qty: 10, EntryType: "market",
		TargetPrice: 110.0, StopPrice: 90.0,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	resp := decodeJSON[placeBracketResponse](t, rec)
	require.Equal(t, "filled", resp.Outcome)
	require.NotNil(t, resp.Fill)
	require.Equal(t, "resting", resp.Target.Status)
	require.Equal(t, "resting", resp.Stop.Status)

	// Price rises to the target -- target fills, stop auto-cancels.
	require.NoError(t, reg.Actor("X").Tick(ctx, 110.0))

	posRec := doJSON(t, r, http.MethodGet, "/positions/X", nil)
	pos := decodeJSON[positionResponse](t, posRec)
	require.Equal(t, int64(0), pos.Qty, "entry (long 10) + target fill (sell 10) nets flat")

	var targetStatus, stopStatus string
	for _, o := range pos.Orders {
		switch o.ID {
		case resp.Target.ID:
			targetStatus = o.Status
		case resp.Stop.ID:
			stopStatus = o.Status
		}
	}
	require.Equal(t, "filled", targetStatus)
	require.Equal(t, "cancelled", stopStatus)
}

func TestPlaceBracket_RestingEntry_ChildrenPending(t *testing.T) {
	r, reg, ctx := newTestRouter(t)
	require.NoError(t, reg.Actor("X").Tick(ctx, 100.0))

	rec := doJSON(t, r, http.MethodPost, "/orders/bracket", placeBracketRequest{
		Token: "X", Side: "buy", Qty: 10, EntryType: "limit", EntryPrice: 50.0,
		TargetPrice: 110.0, StopPrice: 40.0,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	resp := decodeJSON[placeBracketResponse](t, rec)
	require.Equal(t, "resting", resp.Outcome)
	require.Equal(t, "pending", resp.Target.Status)
	require.Equal(t, "pending", resp.Stop.Status)
}

func TestPlaceBracket_InvalidRequest_Rejected(t *testing.T) {
	r, _, _ := newTestRouter(t)

	// Mismatched side handling: an invalid side string fails order.Validate
	// inside the actor.
	rec := doJSON(t, r, http.MethodPost, "/orders/bracket", placeBracketRequest{
		Token: "X", Side: "sideways", Qty: 10, EntryType: "market",
		TargetPrice: 110.0, StopPrice: 90.0,
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	resp := decodeJSON[placeBracketResponse](t, rec)
	require.NotEmpty(t, resp.Error)
}

// TestCancelOrder_BracketChild_ResolvesViaOrderIndex verifies that a child
// order created via the bracket endpoint is discoverable by the standalone
// DELETE /orders/{id} cancel endpoint, exactly like a plain order.
func TestCancelOrder_BracketChild_ResolvesViaOrderIndex(t *testing.T) {
	r, reg, ctx := newTestRouter(t)
	require.NoError(t, reg.Actor("X").Tick(ctx, 100.0))

	rec := doJSON(t, r, http.MethodPost, "/orders/bracket", placeBracketRequest{
		Token: "X", Side: "buy", Qty: 10, EntryType: "market",
		TargetPrice: 110.0, StopPrice: 90.0,
	})
	resp := decodeJSON[placeBracketResponse](t, rec)
	require.Equal(t, "filled", resp.Outcome)

	cancelRec := doJSON(t, r, http.MethodDelete, "/orders/"+resp.Target.ID, nil)
	require.Equal(t, http.StatusOK, cancelRec.Code)
	cancelResp := decodeJSON[cancelOrderResponse](t, cancelRec)
	require.Equal(t, "cancelled", cancelResp.Outcome)
}
