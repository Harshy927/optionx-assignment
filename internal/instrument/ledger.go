// Package instrument holds the position ledger domain logic -- pure,
// dependency-free math for turning fills into a running position with
// average price, realized P&L, and unrealized P&L. The concurrency wrapper
// (one actor goroutine per instrument, serializing ticks/orders/cancels) is
// added in Task 6; this file has no goroutines, channels, or I/O.
package instrument

import "github.com/optionx/backend-assignment/internal/order"

// Position is the running ledger for one instrument. Qty is signed: positive
// means long, negative means short, zero means flat. This sign convention is
// what keeps the unrealized P&L formula uniform for both directions (see
// UnrealizedPnL).
type Position struct {
	Token       string
	Qty         int64
	AvgPrice    float64 // volume-weighted average entry price of the current open Qty; meaningless when Qty == 0
	RealizedPnL float64 // cumulative realized P&L from all fills so far
}

// signedQty converts a fill's (Side, Qty) into a signed delta: positive for
// a buy, negative for a sell.
func signedQty(f order.Fill) int64 {
	if f.Side == order.Sell {
		return -f.Qty
	}
	return f.Qty
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

func sign64(n int64) int64 {
	switch {
	case n > 0:
		return 1
	case n < 0:
		return -1
	default:
		return 0
	}
}

// ApplyFill returns the position that results from applying fill to pos. It
// does not mutate pos.
//
// Three cases, based on how the fill's direction relates to the existing
// position:
//
//  1. Flat or same-direction (adding to a long with a buy, or to a short
//     with a sell): the average price is recomputed as the size-weighted
//     average of the old position and the new fill; realized P&L is
//     untouched, since nothing was closed.
//  2. Opposite-direction, partial or exact close (a sell that reduces but
//     doesn't flip a long, or vice versa): the closed portion realizes P&L
//     at (fill price - avg price) per unit for a long close, or (avg price -
//     fill price) per unit for a short close; the average price of the
//     remaining (still open) quantity is unchanged, matching standard
//     position-accounting behavior.
//  3. Opposite-direction, flip (the fill's quantity exceeds the existing
//     position, reversing its sign): the existing position is fully closed
//     (realizing P&L as in case 2) and a brand new position is opened for
//     the excess quantity at the fill price.
func ApplyFill(pos Position, fill order.Fill) Position {
	out := pos
	out.Token = fill.Token

	delta := signedQty(fill)
	if delta == 0 {
		return out
	}

	switch {
	case pos.Qty == 0:
		// Case 1 (opening from flat).
		out.Qty = delta
		out.AvgPrice = fill.Price

	case sign64(pos.Qty) == sign64(delta):
		// Case 1 (adding to an existing position in the same direction).
		totalQty := abs64(pos.Qty) + abs64(delta)
		out.AvgPrice = (float64(abs64(pos.Qty))*pos.AvgPrice + float64(abs64(delta))*fill.Price) / float64(totalQty)
		out.Qty = pos.Qty + delta

	default:
		// Opposite direction: closing, possibly flipping.
		closingQty := min(abs64(pos.Qty), abs64(delta))
		var pnlPerUnit float64
		if pos.Qty > 0 {
			// Long position being reduced by a sell.
			pnlPerUnit = fill.Price - pos.AvgPrice
		} else {
			// Short position being reduced by a buy.
			pnlPerUnit = pos.AvgPrice - fill.Price
		}
		out.RealizedPnL = pos.RealizedPnL + float64(closingQty)*pnlPerUnit

		remaining := abs64(delta) - abs64(pos.Qty)
		switch {
		case remaining > 0:
			// Case 3: flipped to the opposite side, opened fresh at the
			// fill price for the excess quantity.
			out.Qty = remaining * sign64(delta)
			out.AvgPrice = fill.Price
		case remaining == 0:
			// Exact close: flat.
			out.Qty = 0
			out.AvgPrice = 0
		default:
			// Case 2: partial close; remaining open quantity keeps its
			// original average price.
			out.Qty = pos.Qty + delta // same sign as pos.Qty, smaller magnitude
			out.AvgPrice = pos.AvgPrice
		}
	}

	return out
}

// UnrealizedPnL returns the mark-to-market P&L of the position's open
// quantity at ltp (the latest traded price). It is zero when the position is
// flat. The signed Qty convention makes one formula correct for both long
// and short: for a long (Qty > 0), profit increases as ltp rises above
// AvgPrice; for a short (Qty < 0), profit increases as ltp falls below
// AvgPrice, and the sign of Qty flips the arithmetic accordingly.
func UnrealizedPnL(pos Position, ltp float64) float64 {
	return float64(pos.Qty) * (ltp - pos.AvgPrice)
}
