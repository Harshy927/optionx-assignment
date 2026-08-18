package instrument

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/optionx/backend-assignment/internal/order"
)

func fill(side order.Side, qty int64, price float64) order.Fill {
	return order.Fill{Token: "X", Side: side, Qty: qty, Price: price}
}

func TestApplyFill_OpensFromFlat(t *testing.T) {
	flat := Position{Token: "X"}

	longPos := ApplyFill(flat, fill(order.Buy, 10, 100.0))
	assert.Equal(t, int64(10), longPos.Qty)
	assert.Equal(t, 100.0, longPos.AvgPrice)
	assert.Equal(t, 0.0, longPos.RealizedPnL)

	shortPos := ApplyFill(flat, fill(order.Sell, 10, 100.0))
	assert.Equal(t, int64(-10), shortPos.Qty)
	assert.Equal(t, 100.0, shortPos.AvgPrice)
	assert.Equal(t, 0.0, shortPos.RealizedPnL)
}

func TestApplyFill_AddingToLong_RecomputesAveragePrice(t *testing.T) {
	pos := Position{Token: "X", Qty: 10, AvgPrice: 100.0}

	// Buy 10 more at 120 -> avg = (10*100 + 10*120) / 20 = 110.
	pos = ApplyFill(pos, fill(order.Buy, 10, 120.0))
	assert.Equal(t, int64(20), pos.Qty)
	assert.Equal(t, 110.0, pos.AvgPrice)
	assert.Equal(t, 0.0, pos.RealizedPnL, "adding to a position must not realize any P&L")
}

func TestApplyFill_AddingToShort_RecomputesAveragePrice(t *testing.T) {
	pos := Position{Token: "X", Qty: -10, AvgPrice: 100.0}

	// Sell 10 more at 80 -> avg = (10*100 + 10*80) / 20 = 90.
	pos = ApplyFill(pos, fill(order.Sell, 10, 80.0))
	assert.Equal(t, int64(-20), pos.Qty)
	assert.Equal(t, 90.0, pos.AvgPrice)
	assert.Equal(t, 0.0, pos.RealizedPnL)
}

func TestApplyFill_PartialClose_Long(t *testing.T) {
	pos := Position{Token: "X", Qty: 10, AvgPrice: 100.0}

	// Sell 4 at 120 -> realize 4*(120-100)=80, remaining 6 @ avg 100 (unchanged).
	pos = ApplyFill(pos, fill(order.Sell, 4, 120.0))
	assert.Equal(t, int64(6), pos.Qty)
	assert.Equal(t, 100.0, pos.AvgPrice, "avg price of the remaining open qty is unchanged by a partial close")
	assert.Equal(t, 80.0, pos.RealizedPnL)
}

func TestApplyFill_PartialClose_Short(t *testing.T) {
	pos := Position{Token: "X", Qty: -10, AvgPrice: 100.0}

	// Buy 4 at 80 -> realize 4*(100-80)=80, remaining -6 @ avg 100 (unchanged).
	pos = ApplyFill(pos, fill(order.Buy, 4, 80.0))
	assert.Equal(t, int64(-6), pos.Qty)
	assert.Equal(t, 100.0, pos.AvgPrice)
	assert.Equal(t, 80.0, pos.RealizedPnL)
}

func TestApplyFill_ExactClose_GoesFlat(t *testing.T) {
	pos := Position{Token: "X", Qty: 10, AvgPrice: 100.0}

	// Sell exactly 10 at 130 -> realize 10*(130-100)=300, flat.
	pos = ApplyFill(pos, fill(order.Sell, 10, 130.0))
	assert.Equal(t, int64(0), pos.Qty)
	assert.Equal(t, 0.0, pos.AvgPrice)
	assert.Equal(t, 300.0, pos.RealizedPnL)
}

func TestApplyFill_Flip_LongToShort(t *testing.T) {
	pos := Position{Token: "X", Qty: 10, AvgPrice: 100.0}

	// Sell 15 at 130 -> closes the 10 long (realize 10*(130-100)=300), opens
	// a fresh 5 short at 130.
	pos = ApplyFill(pos, fill(order.Sell, 15, 130.0))
	assert.Equal(t, int64(-5), pos.Qty)
	assert.Equal(t, 130.0, pos.AvgPrice)
	assert.Equal(t, 300.0, pos.RealizedPnL)
}

func TestApplyFill_Flip_ShortToLong(t *testing.T) {
	pos := Position{Token: "X", Qty: -10, AvgPrice: 100.0}

	// Buy 15 at 90 -> closes the 10 short (realize 10*(100-90)=100), opens a
	// fresh 5 long at 90.
	pos = ApplyFill(pos, fill(order.Buy, 15, 90.0))
	assert.Equal(t, int64(5), pos.Qty)
	assert.Equal(t, 90.0, pos.AvgPrice)
	assert.Equal(t, 100.0, pos.RealizedPnL)
}

func TestApplyFill_RealizedPnL_AccumulatesAcrossMultipleFills(t *testing.T) {
	pos := Position{Token: "X"}

	pos = ApplyFill(pos, fill(order.Buy, 10, 100.0)) // open long 10 @ 100
	pos = ApplyFill(pos, fill(order.Sell, 5, 120.0)) // realize 5*20=100, remaining 5 @ 100
	pos = ApplyFill(pos, fill(order.Sell, 5, 110.0)) // realize 5*10=50, now flat, cumulative 150

	assert.Equal(t, int64(0), pos.Qty)
	assert.Equal(t, 150.0, pos.RealizedPnL)
}

func TestApplyFill_ZeroQtyFill_IsNoOp(t *testing.T) {
	// Not reachable via order.Validate() (qty must be positive there), but
	// ApplyFill itself should not panic or corrupt state if ever called with
	// a zero-qty fill directly.
	pos := Position{Token: "X", Qty: 10, AvgPrice: 100.0, RealizedPnL: 50.0}
	out := ApplyFill(pos, fill(order.Buy, 0, 999.0))
	assert.Equal(t, pos.Qty, out.Qty)
	assert.Equal(t, pos.AvgPrice, out.AvgPrice)
	assert.Equal(t, pos.RealizedPnL, out.RealizedPnL)
}

func TestUnrealizedPnL_Long(t *testing.T) {
	pos := Position{Token: "X", Qty: 10, AvgPrice: 100.0}

	assert.Equal(t, 200.0, UnrealizedPnL(pos, 120.0), "long profits as price rises")
	assert.Equal(t, -100.0, UnrealizedPnL(pos, 90.0), "long loses as price falls")
	assert.Equal(t, 0.0, UnrealizedPnL(pos, 100.0))
}

func TestUnrealizedPnL_Short(t *testing.T) {
	pos := Position{Token: "X", Qty: -10, AvgPrice: 100.0}

	assert.Equal(t, 200.0, UnrealizedPnL(pos, 80.0), "short profits as price falls")
	assert.Equal(t, -100.0, UnrealizedPnL(pos, 110.0), "short loses as price rises")
	assert.Equal(t, 0.0, UnrealizedPnL(pos, 100.0))
}

func TestUnrealizedPnL_Flat(t *testing.T) {
	pos := Position{Token: "X", Qty: 0, AvgPrice: 0}
	assert.Equal(t, 0.0, UnrealizedPnL(pos, 12345.0))
}

func TestApplyFill_DoesNotMutateInput(t *testing.T) {
	pos := Position{Token: "X", Qty: 10, AvgPrice: 100.0, RealizedPnL: 5.0}
	original := pos

	_ = ApplyFill(pos, fill(order.Sell, 10, 200.0))

	assert.Equal(t, original, pos, "ApplyFill must not mutate its input Position")
}
