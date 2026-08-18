package storage

import (
	"context"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/optionx/backend-assignment/internal/instrument"
	"github.com/optionx/backend-assignment/internal/order"
)

// openOrderTestDB is like openTestDB (candle_store_test.go) but also
// truncates orders and positions, so tests in this file don't see leftover
// state from other test files sharing the same local database.
func openOrderTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db := openTestDB(t)
	_, err := db.ExecContext(context.Background(), `TRUNCATE orders, positions`)
	require.NoError(t, err)
	return db
}

func TestOrderStore_SaveOrder_PersistsAndUpserts(t *testing.T) {
	db := openOrderTestDB(t)
	store := NewOrderStore(db)
	ctx := context.Background()

	o := order.Order{ID: "o1", Token: "X", Side: order.Buy, Type: order.Limit, Qty: 10, LimitPrice: 90, Status: order.StatusResting}
	require.NoError(t, store.SaveOrder(ctx, o))

	loaded, err := LoadOrdersForToken(ctx, db, "X")
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Equal(t, o, loaded[0])

	// Cancel it -- SaveOrder again with a new status must update in place,
	// not create a second row.
	o.Status = order.StatusCancelled
	require.NoError(t, store.SaveOrder(ctx, o))

	loaded, err = LoadOrdersForToken(ctx, db, "X")
	require.NoError(t, err)
	require.Len(t, loaded, 1, "SaveOrder must upsert, not duplicate, a row for the same order ID")
	assert.Equal(t, order.StatusCancelled, loaded[0].Status)
}

func TestOrderStore_SaveFillTransition_PersistsOrderAndPositionTogether(t *testing.T) {
	db := openOrderTestDB(t)
	store := NewOrderStore(db)
	ctx := context.Background()

	o := order.Order{ID: "o1", Token: "X", Side: order.Buy, Type: order.Market, Qty: 10, Status: order.StatusFilled}
	pos := instrument.Position{Token: "X", Qty: 10, AvgPrice: 100.0, RealizedPnL: 0}

	require.NoError(t, store.SaveFillTransition(ctx, o, pos))

	loadedOrders, err := LoadOrdersForToken(ctx, db, "X")
	require.NoError(t, err)
	require.Len(t, loadedOrders, 1)
	assert.Equal(t, order.StatusFilled, loadedOrders[0].Status)

	loadedPos, err := LoadPosition(ctx, db, "X")
	require.NoError(t, err)
	assert.Equal(t, pos, loadedPos)
}

func TestLoadPosition_UnknownToken_ReturnsZeroValue(t *testing.T) {
	db := openOrderTestDB(t)
	ctx := context.Background()

	pos, err := LoadPosition(ctx, db, "NEVER-SEEN")
	require.NoError(t, err)
	assert.Equal(t, instrument.Position{Token: "NEVER-SEEN"}, pos)
}

func TestLoadAllInstrumentState_CombinesOrdersAndPositions(t *testing.T) {
	db := openOrderTestDB(t)
	store := NewOrderStore(db)
	ctx := context.Background()

	// Instrument A: one filled order + a position.
	require.NoError(t, store.SaveFillTransition(ctx,
		order.Order{ID: "a1", Token: "A", Side: order.Buy, Type: order.Market, Qty: 10, Status: order.StatusFilled},
		instrument.Position{Token: "A", Qty: 10, AvgPrice: 50.0}))

	// Instrument B: one still-resting order, no position yet (never filled).
	require.NoError(t, store.SaveOrder(ctx,
		order.Order{ID: "b1", Token: "B", Side: order.Sell, Type: order.Limit, Qty: 5, LimitPrice: 200, Status: order.StatusResting}))

	all, err := LoadAllInstrumentState(ctx, db)
	require.NoError(t, err)

	require.Contains(t, all, "A")
	assert.Equal(t, int64(10), all["A"].Position.Qty)
	require.Len(t, all["A"].Orders, 1)
	assert.Equal(t, order.StatusFilled, all["A"].Orders[0].Status)

	require.Contains(t, all, "B")
	assert.Equal(t, int64(0), all["B"].Position.Qty, "instrument B never filled, so it has no position row")
	require.Len(t, all["B"].Orders, 1)
	assert.Equal(t, order.StatusResting, all["B"].Orders[0].Status)
}
