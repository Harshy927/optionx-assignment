package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/optionx/backend-assignment/internal/instrument"
	"github.com/optionx/backend-assignment/internal/order"
)

// TestRestartUnderReplay_RestingOrder_FillsExactlyOnce is the graded
// scenario 2 test: "Kill the process mid-burst, restart it, replay the feed
// from an earlier seq. No duplicate fills, no distorted candles, no phantom
// positions." This test drives the full stack -- a real Postgres-backed
// instrument.Store, actor persistence write-through, and boot-time seeding
// via LoadAllInstrumentState -- across a simulated process restart:
//
//  1. "Process A": places a resting limit order, then applies a tick that
//     does NOT cross it yet.
//  2. Simulated crash: process A's actor is simply discarded (its
//     goroutine's context is cancelled) without any special shutdown
//     handling, exactly like a kill -9.
//  3. "Process B" (the restart): loads instrument state fresh from
//     Postgres via LoadAllInstrumentState (exactly as cmd/server does on
//     boot) and constructs a brand new actor seeded from it.
//  4. Replay: process B re-applies the SAME non-crossing tick again (the
//     replayed portion of the feed) and then a NEW crossing tick.
//
// Assertions: the order fills exactly once (not on the replayed tick, only
// on the genuinely new crossing tick), the position reflects exactly one
// fill's worth of quantity, and Postgres agrees with the in-memory actor's
// final state.
func TestRestartUnderReplay_RestingOrder_FillsExactlyOnce(t *testing.T) {
	db := openOrderTestDB(t)
	store := NewOrderStore(db)
	token := "RESTART-TEST-TOK"

	// --- Process A ---
	ctxA, cancelA := context.WithCancel(context.Background())
	actorA := instrument.NewPersistentActor(token, store, instrument.InitialState{})
	actorA.Start(ctxA)

	placeRes, err := actorA.PlaceOrder(ctxA, order.Order{
		ID: "restart-order-1", Token: token, Side: order.Buy, Type: order.Limit, Qty: 10, LimitPrice: 90.0,
	})
	require.NoError(t, err)
	require.Equal(t, instrument.PlaceOutcomeResting, placeRes.Outcome)

	// A tick that does NOT cross (order rests, must not fill).
	require.NoError(t, actorA.Tick(ctxA, 100.0))

	// Give the actor's single goroutine a moment to process both messages
	// and their persistence write-throughs before we "crash" it -- in real
	// usage the HTTP handler's PlaceOrder call already waits for the reply,
	// so this sleep only accounts for Tick's fire-and-forget send needing a
	// moment to be processed and persisted.
	time.Sleep(100 * time.Millisecond)

	// Verify persisted state mid-flight, before the "crash".
	preOrders, err := LoadOrdersForToken(context.Background(), db, token)
	require.NoError(t, err)
	require.Len(t, preOrders, 1)
	require.Equal(t, order.StatusResting, preOrders[0].Status, "order must still be resting in Postgres before the crash")

	// --- Simulated crash: no graceful shutdown, just stop the goroutine. ---
	cancelA()

	// --- Process B: the restart. ---
	seeds, err := LoadAllInstrumentState(context.Background(), db)
	require.NoError(t, err)
	seed, ok := seeds[token]
	require.True(t, ok, "restarted process must find the pre-crash order in Postgres")
	require.Len(t, seed.Orders, 1)
	require.Equal(t, order.StatusResting, seed.Orders[0].Status)
	require.Equal(t, int64(0), seed.Position.Qty, "no fill happened yet, so no position row exists")

	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	actorB := instrument.NewPersistentActor(token, store, seed)
	actorB.Start(ctxB)

	// --- Replay: the same non-crossing tick arrives again (from -from
	// pointing before it), then a genuinely new crossing tick. ---
	require.NoError(t, actorB.Tick(ctxB, 100.0)) // replayed, must not fill
	require.NoError(t, actorB.Tick(ctxB, 85.0))  // new, crosses the limit buy at 90

	snap, err := actorB.Snapshot(ctxB)
	require.NoError(t, err)

	require.Len(t, snap.Orders, 1)
	assert.Equal(t, order.StatusFilled, snap.Orders[0].Status)
	assert.Equal(t, int64(10), snap.Position.Qty, "the order must fill exactly once, not be double-counted")
	assert.Equal(t, 85.0, snap.Position.AvgPrice)

	// Postgres must agree with the in-memory actor -- no drift between the
	// durable record and the live state.
	finalOrders, err := LoadOrdersForToken(context.Background(), db, token)
	require.NoError(t, err)
	require.Len(t, finalOrders, 1)
	assert.Equal(t, order.StatusFilled, finalOrders[0].Status)

	finalPos, err := LoadPosition(context.Background(), db, token)
	require.NoError(t, err)
	assert.Equal(t, int64(10), finalPos.Qty)
	assert.Equal(t, 85.0, finalPos.AvgPrice)
}

// TestRestartUnderReplay_AlreadyFilledOrder_NeverRefills covers the other
// half of "no duplicate fills": an order that filled BEFORE the crash must
// not be re-evaluated at all after restart, even if a replayed tick would
// have crossed it.
func TestRestartUnderReplay_AlreadyFilledOrder_NeverRefills(t *testing.T) {
	db := openOrderTestDB(t)
	store := NewOrderStore(db)
	token := "RESTART-TEST-TOK-2"

	ctxA, cancelA := context.WithCancel(context.Background())
	actorA := instrument.NewPersistentActor(token, store, instrument.InitialState{})
	actorA.Start(ctxA)

	require.NoError(t, actorA.Tick(ctxA, 100.0))
	res, err := actorA.PlaceOrder(ctxA, order.Order{
		ID: "already-filled-1", Token: token, Side: order.Buy, Type: order.Market, Qty: 7,
	})
	require.NoError(t, err)
	require.Equal(t, instrument.PlaceOutcomeFilled, res.Outcome)

	time.Sleep(100 * time.Millisecond)
	cancelA() // simulated crash, after the fill was already persisted

	seeds, err := LoadAllInstrumentState(context.Background(), db)
	require.NoError(t, err)
	seed := seeds[token]
	require.Equal(t, int64(7), seed.Position.Qty)

	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	actorB := instrument.NewPersistentActor(token, store, seed)
	actorB.Start(ctxB)

	// Replay several ticks, including ones that would have "crossed" if the
	// order were somehow still resting. It must have no effect: the order
	// is loaded as StatusFilled, and handleTick only evaluates
	// StatusResting orders.
	for _, ltp := range []float64{100.0, 50.0, 200.0, 1.0} {
		require.NoError(t, actorB.Tick(ctxB, ltp))
	}

	snap, err := actorB.Snapshot(ctxB)
	require.NoError(t, err)
	require.Len(t, snap.Orders, 1)
	assert.Equal(t, order.StatusFilled, snap.Orders[0].Status)
	assert.Equal(t, int64(7), snap.Position.Qty, "position must be unaffected by replayed ticks after restart")
}
