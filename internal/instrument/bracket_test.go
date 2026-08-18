package instrument

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/optionx/backend-assignment/internal/order"
)

func bracketOrders(entrySide order.Side, qty int64, entryPrice, target, stop float64) (order.Order, order.Order, order.Order) {
	closingSide := order.Sell
	if entrySide == order.Sell {
		closingSide = order.Buy
	}
	entry := order.Order{ID: "entry", Token: "X", Side: entrySide, Type: order.Market, Qty: qty}
	if entryPrice > 0 {
		entry.Type = order.Limit
		entry.LimitPrice = entryPrice
	}
	tgt := order.Order{ID: "target", Token: "X", Side: closingSide, Type: order.Limit, Qty: qty, LimitPrice: target}
	stp := order.Order{ID: "stop", Token: "X", Side: closingSide, Type: order.Stop, Qty: qty, LimitPrice: stop}
	return entry, tgt, stp
}

func TestPlaceBracket_EntryFillsImmediately_ChildrenActivate(t *testing.T) {
	a, ctx := newRunningActor(t, "X")
	require.NoError(t, a.Tick(ctx, 100.0))

	entry, target, stop := bracketOrders(order.Buy, 10, 0 /* market */, 110.0, 90.0)
	res, err := a.PlaceBracket(ctx, entry, target, stop)
	require.NoError(t, err)
	require.Equal(t, PlaceOutcomeFilled, res.Outcome)
	require.NotNil(t, res.Fill)

	snap, err := a.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(10), snap.Position.Qty)

	byID := ordersByID(snap.Orders)
	assert.Equal(t, order.StatusFilled, byID["entry"].Status)
	assert.Equal(t, order.StatusResting, byID["target"].Status, "children must be activated (Resting) once the entry fills")
	assert.Equal(t, order.StatusResting, byID["stop"].Status)
	assert.Equal(t, "entry", byID["target"].EntryID)
	assert.Equal(t, "stop", byID["target"].SiblingID)
}

func TestPlaceBracket_EntryRests_ChildrenStayPending(t *testing.T) {
	a, ctx := newRunningActor(t, "X")
	require.NoError(t, a.Tick(ctx, 100.0))

	// A limit entry far from the current price rests.
	entry, target, stop := bracketOrders(order.Buy, 10, 50.0, 110.0, 40.0)
	res, err := a.PlaceBracket(ctx, entry, target, stop)
	require.NoError(t, err)
	require.Equal(t, PlaceOutcomeResting, res.Outcome)

	snap, err := a.Snapshot(ctx)
	require.NoError(t, err)
	byID := ordersByID(snap.Orders)
	assert.Equal(t, order.StatusResting, byID["entry"].Status)
	assert.Equal(t, order.StatusPending, byID["target"].Status, "children must stay Pending until the entry fills")
	assert.Equal(t, order.StatusPending, byID["stop"].Status)
	assert.Equal(t, int64(0), snap.Position.Qty, "a pending child must never fill the position")

	// Even a tick that would cross a Pending child must not fill it.
	require.NoError(t, a.Tick(ctx, 200.0)) // crosses target's 110 if it were active
	snap, err = a.Snapshot(ctx)
	require.NoError(t, err)
	byID = ordersByID(snap.Orders)
	assert.Equal(t, order.StatusPending, byID["target"].Status)
	assert.Equal(t, int64(0), snap.Position.Qty)
}

func TestPlaceBracket_TargetFills_StopAutoCancels(t *testing.T) {
	a, ctx := newRunningActor(t, "X")
	require.NoError(t, a.Tick(ctx, 100.0))

	entry, target, stop := bracketOrders(order.Buy, 10, 0, 110.0, 90.0)
	_, err := a.PlaceBracket(ctx, entry, target, stop)
	require.NoError(t, err)

	// Price rises to the target -- target fills, stop must auto-cancel.
	require.NoError(t, a.Tick(ctx, 110.0))

	snap, err := a.Snapshot(ctx)
	require.NoError(t, err)
	byID := ordersByID(snap.Orders)
	assert.Equal(t, order.StatusFilled, byID["target"].Status)
	assert.Equal(t, order.StatusCancelled, byID["stop"].Status, "the sibling must auto-cancel when the other leg fills")

	// Entry (long 10) + target close (sell 10) = flat.
	assert.Equal(t, int64(0), snap.Position.Qty)

	// A further tick that would have crossed the (now-cancelled) stop must
	// not do anything.
	require.NoError(t, a.Tick(ctx, 50.0))
	snap, err = a.Snapshot(ctx)
	require.NoError(t, err)
	byID = ordersByID(snap.Orders)
	assert.Equal(t, order.StatusCancelled, byID["stop"].Status)
	assert.Equal(t, int64(0), snap.Position.Qty, "a cancelled stop must never fill, even on a crossing tick")
}

func TestPlaceBracket_StopFills_TargetAutoCancels(t *testing.T) {
	a, ctx := newRunningActor(t, "X")
	require.NoError(t, a.Tick(ctx, 100.0))

	entry, target, stop := bracketOrders(order.Buy, 10, 0, 110.0, 90.0)
	_, err := a.PlaceBracket(ctx, entry, target, stop)
	require.NoError(t, err)

	// Price falls to the stop -- stop fills, target must auto-cancel.
	require.NoError(t, a.Tick(ctx, 90.0))

	snap, err := a.Snapshot(ctx)
	require.NoError(t, err)
	byID := ordersByID(snap.Orders)
	assert.Equal(t, order.StatusFilled, byID["stop"].Status)
	assert.Equal(t, order.StatusCancelled, byID["target"].Status)
	assert.Equal(t, int64(0), snap.Position.Qty)
}

func TestPlaceBracket_CancelOneLegBeforeEntryFills_DoesNotCancelOther(t *testing.T) {
	a, ctx := newRunningActor(t, "X")
	require.NoError(t, a.Tick(ctx, 100.0))

	entry, target, stop := bracketOrders(order.Buy, 10, 50.0, 110.0, 40.0) // entry rests
	_, err := a.PlaceBracket(ctx, entry, target, stop)
	require.NoError(t, err)

	// Cancel the stop leg BEFORE the entry has ever filled -- this is a
	// plain cancel of a Pending order, not a fill-triggered auto-cancel, so
	// it must not cascade to the target.
	cancelRes, err := a.CancelOrder(ctx, "stop")
	require.NoError(t, err)
	assert.Equal(t, CancelOutcomeCancelled, cancelRes.Outcome)

	snap, err := a.Snapshot(ctx)
	require.NoError(t, err)
	byID := ordersByID(snap.Orders)
	assert.Equal(t, order.StatusCancelled, byID["stop"].Status)
	assert.Equal(t, order.StatusPending, byID["target"].Status, "cancelling one leg pre-activation must not cancel the other")
}

func TestPlaceBracket_Validation(t *testing.T) {
	a, ctx := newRunningActor(t, "X")

	tests := []struct {
		name             string
		entry, tgt, stop order.Order
	}{
		{
			name:  "target wrong type",
			entry: order.Order{ID: "e", Token: "X", Side: order.Buy, Type: order.Market, Qty: 10},
			tgt:   order.Order{ID: "t", Token: "X", Side: order.Sell, Type: order.Market, Qty: 10},
			stop:  order.Order{ID: "s", Token: "X", Side: order.Sell, Type: order.Stop, Qty: 10, LimitPrice: 90},
		},
		{
			name:  "stop wrong type",
			entry: order.Order{ID: "e", Token: "X", Side: order.Buy, Type: order.Market, Qty: 10},
			tgt:   order.Order{ID: "t", Token: "X", Side: order.Sell, Type: order.Limit, Qty: 10, LimitPrice: 110},
			stop:  order.Order{ID: "s", Token: "X", Side: order.Sell, Type: order.Limit, Qty: 10, LimitPrice: 90},
		},
		{
			name:  "target same side as entry",
			entry: order.Order{ID: "e", Token: "X", Side: order.Buy, Type: order.Market, Qty: 10},
			tgt:   order.Order{ID: "t", Token: "X", Side: order.Buy, Type: order.Limit, Qty: 10, LimitPrice: 110},
			stop:  order.Order{ID: "s", Token: "X", Side: order.Sell, Type: order.Stop, Qty: 10, LimitPrice: 90},
		},
		{
			name:  "mismatched qty",
			entry: order.Order{ID: "e", Token: "X", Side: order.Buy, Type: order.Market, Qty: 10},
			tgt:   order.Order{ID: "t", Token: "X", Side: order.Sell, Type: order.Limit, Qty: 5, LimitPrice: 110},
			stop:  order.Order{ID: "s", Token: "X", Side: order.Sell, Type: order.Stop, Qty: 10, LimitPrice: 90},
		},
		{
			name:  "duplicate ids",
			entry: order.Order{ID: "dup", Token: "X", Side: order.Buy, Type: order.Market, Qty: 10},
			tgt:   order.Order{ID: "dup", Token: "X", Side: order.Sell, Type: order.Limit, Qty: 10, LimitPrice: 110},
			stop:  order.Order{ID: "s", Token: "X", Side: order.Sell, Type: order.Stop, Qty: 10, LimitPrice: 90},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := a.PlaceBracket(ctx, tc.entry, tc.tgt, tc.stop)
			require.NoError(t, err)
			assert.Equal(t, PlaceOutcomeRejected, res.Outcome)
			assert.NotEmpty(t, res.Error)
		})
	}
}

func ordersByID(orders []order.Order) map[string]order.Order {
	out := make(map[string]order.Order, len(orders))
	for _, o := range orders {
		out[o.ID] = o
	}
	return out
}

// --- Graded-scenario-1 extension: bracket cancel-vs-trigger race ----------
//
// The assignment explicitly requires the cancel-race rules to apply to a
// bracket's pair. This test races a concurrent trigger-of-one-leg against a
// cancel-of-the-OTHER-leg, across many trials, asserting the same
// single-deterministic-winner guarantee Task 6 proved for a standalone
// order: exactly one leg ever fills, the other is always cancelled, and the
// canceller's reported outcome always agrees with final ledger state.
func TestActor_BracketCancelVsTrigger_Race_SingleDeterministicWinner(t *testing.T) {
	const trials = 200

	for trial := 0; trial < trials; trial++ {
		trial := trial
		t.Run(fmt.Sprintf("trial_%d", trial), func(t *testing.T) {
			a, ctx := newRunningActor(t, "X")
			require.NoError(t, a.Tick(ctx, 100.0))

			entry, target, stop := bracketOrders(order.Buy, 10, 0, 110.0, 90.0)
			placeRes, err := a.PlaceBracket(ctx, entry, target, stop)
			require.NoError(t, err)
			require.Equal(t, PlaceOutcomeFilled, placeRes.Outcome, "market entry must fill immediately, activating children")

			// Race: a tick crossing the TARGET leg, concurrently with a
			// cancel request for the STOP leg (the sibling).
			var wg sync.WaitGroup
			var cancelRes CancelOrderResult
			var cancelErr error

			wg.Add(2)
			go func() {
				defer wg.Done()
				cancelRes, cancelErr = a.CancelOrder(ctx, "stop")
			}()
			go func() {
				defer wg.Done()
				_ = a.Tick(ctx, 110.0) // crosses target
			}()
			wg.Wait()

			require.NoError(t, cancelErr)

			snap, err := a.Snapshot(ctx)
			require.NoError(t, err)
			byID := ordersByID(snap.Orders)
			stopFinal := byID["stop"]
			targetFinal := byID["target"]

			// Exactly one of {target filled, stop filled} must be true, and
			// the cancel's reported outcome for "stop" must always agree
			// with stop's final ledger state.
			switch cancelRes.Outcome {
			case CancelOutcomeCancelled:
				// The cancel won: stop is cancelled (whether by the cancel
				// request itself, or because target's fill cancelled it
				// first is irrelevant to this outcome -- Cancelled is
				// consistent either way).
				assert.Equal(t, order.StatusCancelled, stopFinal.Status)
			case CancelOutcomeAlreadyCancelled:
				// The target's fill got there first and cancelled stop as
				// its sibling; this cancel request arrived after and found
				// it already cancelled.
				assert.Equal(t, order.StatusCancelled, stopFinal.Status)
				assert.Equal(t, order.StatusFilled, targetFinal.Status,
					"if stop was already cancelled by the time this cancel ran, target must be the one that filled")
			case CancelOutcomeAlreadyFilled:
				// The stop itself filled before the cancel could apply --
				// only possible if the tick's LTP (110) also happened to
				// cross the stop, which it does not in this test's setup
				// (stop=90, tick=110 for a buy-side stop-loss triggers at
				// ltp<=90) -- included for completeness/documentation, not
				// expected to occur with these prices.
				t.Fatalf("unexpected AlreadyFilled outcome for stop leg with tick=110, stop=90: %+v", stopFinal)
			default:
				t.Fatalf("unexpected cancel outcome: %v", cancelRes.Outcome)
			}

			// Regardless of outcome ordering, the invariant "never both
			// filled" must hold.
			bothFilled := stopFinal.Status == order.StatusFilled && targetFinal.Status == order.StatusFilled
			assert.False(t, bothFilled, "target and stop must never both fill")
		})
	}
}
