package instrument

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/optionx/backend-assignment/internal/order"
)

func newRunningActor(t *testing.T, token string) (*Actor, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a := NewActor(token)
	a.Start(ctx)
	return a, ctx
}

func TestActor_MarketOrder_FillsImmediatelyAgainstKnownPrice(t *testing.T) {
	a, ctx := newRunningActor(t, "X")

	require.NoError(t, a.Tick(ctx, 100.0))

	res, err := a.PlaceOrder(ctx, order.Order{ID: "o1", Token: "X", Side: order.Buy, Type: order.Market, Qty: 10})
	require.NoError(t, err)
	require.Equal(t, PlaceOutcomeFilled, res.Outcome)
	require.NotNil(t, res.Fill)
	assert.Equal(t, 100.0, res.Fill.Price)
	assert.Equal(t, order.StatusFilled, res.Order.Status)

	snap, err := a.Snapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(10), snap.Position.Qty)
	assert.Equal(t, 100.0, snap.Position.AvgPrice)
}

func TestActor_MarketOrder_RestsIfNoPriceYet(t *testing.T) {
	a, ctx := newRunningActor(t, "X")

	res, err := a.PlaceOrder(ctx, order.Order{ID: "o1", Token: "X", Side: order.Buy, Type: order.Market, Qty: 10})
	require.NoError(t, err)
	require.Equal(t, PlaceOutcomeResting, res.Outcome, "market order placed before any tick must rest until a price is known")

	// The first tick should fill it.
	require.NoError(t, a.Tick(ctx, 55.0))
	snap, err := a.Snapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Orders, 1)
	assert.Equal(t, order.StatusFilled, snap.Orders[0].Status)
	assert.Equal(t, int64(10), snap.Position.Qty)
}

func TestActor_LimitOrder_RestsThenFillsOnCrossingTick(t *testing.T) {
	a, ctx := newRunningActor(t, "X")
	require.NoError(t, a.Tick(ctx, 100.0))

	res, err := a.PlaceOrder(ctx, order.Order{
		ID: "o1", Token: "X", Side: order.Buy, Type: order.Limit, Qty: 10, LimitPrice: 90.0,
	})
	require.NoError(t, err)
	require.Equal(t, PlaceOutcomeResting, res.Outcome, "limit buy above current price must rest, not fill immediately")

	// A tick that doesn't cross must not fill it.
	require.NoError(t, a.Tick(ctx, 95.0))
	snap, err := a.Snapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, order.StatusResting, snap.Orders[0].Status)

	// A tick that crosses (ltp <= limit) fills it.
	require.NoError(t, a.Tick(ctx, 90.0))
	snap, err = a.Snapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, order.StatusFilled, snap.Orders[0].Status)
	assert.Equal(t, int64(10), snap.Position.Qty)
	assert.Equal(t, 90.0, snap.Position.AvgPrice)
}

func TestActor_CancelOrder_Outcomes(t *testing.T) {
	a, ctx := newRunningActor(t, "X")
	require.NoError(t, a.Tick(ctx, 100.0))

	// Not found.
	res, err := a.CancelOrder(ctx, "does-not-exist")
	require.NoError(t, err)
	assert.Equal(t, CancelOutcomeNotFound, res.Outcome)

	// Cancel a resting order successfully.
	_, err = a.PlaceOrder(ctx, order.Order{
		ID: "o1", Token: "X", Side: order.Buy, Type: order.Limit, Qty: 10, LimitPrice: 50.0,
	})
	require.NoError(t, err)

	res, err = a.CancelOrder(ctx, "o1")
	require.NoError(t, err)
	assert.Equal(t, CancelOutcomeCancelled, res.Outcome)

	// A subsequent crossing tick must NOT fill the now-cancelled order.
	require.NoError(t, a.Tick(ctx, 10.0))
	snap, err := a.Snapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, order.StatusCancelled, snap.Orders[0].Status)
	assert.Equal(t, int64(0), snap.Position.Qty, "cancelled order must never fill")

	// Double-cancel is idempotent.
	res, err = a.CancelOrder(ctx, "o1")
	require.NoError(t, err)
	assert.Equal(t, CancelOutcomeAlreadyCancelled, res.Outcome)

	// Cancelling an already-filled order reports AlreadyFilled.
	require.NoError(t, a.Tick(ctx, 100.0))
	_, err = a.PlaceOrder(ctx, order.Order{ID: "o2", Token: "X", Side: order.Buy, Type: order.Market, Qty: 5})
	require.NoError(t, err)
	res, err = a.CancelOrder(ctx, "o2")
	require.NoError(t, err)
	assert.Equal(t, CancelOutcomeAlreadyFilled, res.Outcome)
}

func TestActor_PlaceOrder_RejectsInvalidOrder(t *testing.T) {
	a, ctx := newRunningActor(t, "X")

	res, err := a.PlaceOrder(ctx, order.Order{ID: "bad", Token: "X", Side: "sideways", Type: order.Market, Qty: 1})
	require.NoError(t, err)
	assert.Equal(t, PlaceOutcomeRejected, res.Outcome)
	assert.NotEmpty(t, res.Error)
}

func TestActor_PlaceOrder_RejectsDuplicateID(t *testing.T) {
	a, ctx := newRunningActor(t, "X")
	require.NoError(t, a.Tick(ctx, 100.0))

	_, err := a.PlaceOrder(ctx, order.Order{ID: "o1", Token: "X", Side: order.Buy, Type: order.Market, Qty: 1})
	require.NoError(t, err)

	res, err := a.PlaceOrder(ctx, order.Order{ID: "o1", Token: "X", Side: order.Buy, Type: order.Market, Qty: 1})
	require.NoError(t, err)
	assert.Equal(t, PlaceOutcomeRejected, res.Outcome)
}

func TestActor_PlaceOrder_RejectsWrongToken(t *testing.T) {
	a, ctx := newRunningActor(t, "X")
	res, err := a.PlaceOrder(ctx, order.Order{ID: "o1", Token: "Y", Side: order.Buy, Type: order.Market, Qty: 1})
	require.NoError(t, err)
	assert.Equal(t, PlaceOutcomeRejected, res.Outcome)
}

// --- Graded scenario 1: cancel-vs-trigger race -----------------------------
//
// A cancel request and a crossing tick arrive "at the same instant" for a
// resting limit order. Exactly one outcome must win, and the outcome
// reported to the API caller (the cancel's result) must always agree with
// the actor's final ledger state. This is run many times under -race to
// surface any ordering bug.

func TestActor_CancelVsTrigger_Race_SingleDeterministicWinner(t *testing.T) {
	const trials = 200

	for trial := 0; trial < trials; trial++ {
		trial := trial
		t.Run(fmt.Sprintf("trial_%d", trial), func(t *testing.T) {
			a, ctx := newRunningActor(t, "X")
			require.NoError(t, a.Tick(ctx, 100.0))

			orderID := "race-order"
			placeRes, err := a.PlaceOrder(ctx, order.Order{
				ID: orderID, Token: "X", Side: order.Buy, Type: order.Limit, Qty: 10, LimitPrice: 90.0,
			})
			require.NoError(t, err)
			require.Equal(t, PlaceOutcomeResting, placeRes.Outcome)

			// Fire the cancel and the crossing tick concurrently from two
			// separate goroutines, racing to be "next" in the actor's inbox.
			var wg sync.WaitGroup
			var cancelRes CancelOrderResult
			var cancelErr error

			wg.Add(2)
			go func() {
				defer wg.Done()
				cancelRes, cancelErr = a.CancelOrder(ctx, orderID)
			}()
			go func() {
				defer wg.Done()
				_ = a.Tick(ctx, 90.0) // crosses the limit buy at 90
			}()
			wg.Wait()

			require.NoError(t, cancelErr)

			snap, err := a.Snapshot(ctx)
			require.NoError(t, err)
			require.Len(t, snap.Orders, 1)
			finalOrder := snap.Orders[0]

			// Exactly one outcome must have won, and the cancel's reported
			// outcome must agree with the actor's final ledger state -- no
			// scenario where the caller is told "cancelled" but the order is
			// actually Filled, or vice versa.
			switch cancelRes.Outcome {
			case CancelOutcomeCancelled:
				assert.Equal(t, order.StatusCancelled, finalOrder.Status,
					"cancel reported success but order is not Cancelled in the ledger")
				assert.Equal(t, int64(0), snap.Position.Qty,
					"cancel won the race, so the order must never have filled the position")
			case CancelOutcomeAlreadyFilled:
				assert.Equal(t, order.StatusFilled, finalOrder.Status,
					"cancel reported AlreadyFilled but order is not Filled in the ledger")
				assert.Equal(t, int64(10), snap.Position.Qty,
					"tick won the race, so the position must reflect the fill")
			default:
				t.Fatalf("unexpected cancel outcome: %v", cancelRes.Outcome)
			}
		})
	}
}

// TestActor_ManyConcurrentTicksAndOrders_NoDataRace exercises the actor under
// heavy concurrent load (many goroutines placing orders, cancelling, and
// ticking simultaneously) purely to let `go test -race` catch any data race
// in the actor/state design -- correctness of individual outcomes is not
// asserted here (that's covered by the more targeted tests above), only that
// nothing panics or is flagged by the race detector.
func TestActor_ManyConcurrentTicksAndOrders_NoDataRace(t *testing.T) {
	a, ctx := newRunningActor(t, "X")
	require.NoError(t, a.Tick(ctx, 100.0))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		i := i
		wg.Add(3)
		go func() {
			defer wg.Done()
			_, _ = a.PlaceOrder(ctx, order.Order{
				ID: fmt.Sprintf("order-%d", i), Token: "X", Side: order.Buy, Type: order.Limit,
				Qty: 1, LimitPrice: 50.0 + float64(i%10),
			})
		}()
		go func() {
			defer wg.Done()
			_, _ = a.CancelOrder(ctx, fmt.Sprintf("order-%d", i))
		}()
		go func() {
			defer wg.Done()
			_ = a.Tick(ctx, 50.0+float64(i%20))
		}()
	}
	wg.Wait()

	// Just confirm the actor is still responsive and consistent afterward.
	snap, err := a.Snapshot(ctx)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(snap.Orders), 50)
}

func TestActor_Start_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	a := NewActor("X")
	a.Start(ctx)

	require.NoError(t, a.Tick(ctx, 100.0))
	cancel()

	// Give the run loop a moment to observe cancellation.
	time.Sleep(50 * time.Millisecond)

	_, err := a.Snapshot(context.Background())
	assert.Error(t, err, "actor should reject new requests after its context is cancelled")
}
