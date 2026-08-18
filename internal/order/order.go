// Package order defines order/fill domain types and the pure trigger logic
// for deciding whether an order fills against a given last-traded price. It
// has no dependency on the feed, storage, or concurrency layers -- those are
// wired in by internal/instrument (the actor, Task 6) and internal/storage
// (persistence, Task 8).
package order

import "fmt"

// Side is the direction of an order or fill.
type Side string

const (
	Buy  Side = "buy"
	Sell Side = "sell"
)

// Type distinguishes market orders (fill immediately at the last traded
// price), limit orders (rest until a tick reaches a price at least as good
// as LimitPrice), and stop orders (rest until a tick reaches a price at
// least as BAD as LimitPrice -- used for the stop-loss leg of a bracket
// order, see internal/bracket: protecting a long means selling once price
// falls to or below the stop, the opposite trigger direction from a limit
// sell).
type Type string

const (
	Market Type = "market"
	Limit  Type = "limit"
	Stop   Type = "stop"
)

// Status is the lifecycle state of an order.
type Status string

const (
	StatusPending   Status = "pending"   // bracket child, not yet active until its entry fills
	StatusResting   Status = "resting"   // limit/stop order waiting for a crossing tick
	StatusFilled    Status = "filled"    // fully executed
	StatusCancelled Status = "cancelled" // cancelled before it could fill
)

// Order is a single order for one instrument. This model intentionally has
// no partial fills: an order is filled in full the moment it is marketable,
// which matches the assignment's scope (market fills immediately, limit
// rests then fills whole when crossed).
//
// EntryID and SiblingID support bracket (OCO) orders (internal/instrument's
// actor, stretch goal): EntryID is set on a bracket's target/stop children
// to the ID of the entry order that spawns them (empty for a standalone
// order or an entry itself); SiblingID links a bracket's two children to
// each other so that filling one can cancel the other. Both are plain
// string fields on Order (rather than a separate bracket-specific type) so
// the existing Store.SaveOrder persistence path needs no interface change
// to carry this linkage durably across a restart.
type Order struct {
	ID         string
	Token      string
	Side       Side
	Type       Type
	Qty        int64
	LimitPrice float64 // meaningful only when Type == Limit or Type == Stop
	Status     Status
	EntryID    string // non-empty only for a bracket's target/stop children
	SiblingID  string // non-empty only for a bracket's target/stop children
}

// Fill is one execution against an order.
type Fill struct {
	OrderID string
	Token   string
	Side    Side
	Qty     int64
	Price   float64
}

// Validate checks that an order's fields are internally consistent before it
// is accepted (e.g. via the REST API in Task 7).
func (o Order) Validate() error {
	if o.Token == "" {
		return fmt.Errorf("order: token is required")
	}
	if o.Side != Buy && o.Side != Sell {
		return fmt.Errorf("order: invalid side %q", o.Side)
	}
	if o.Type != Market && o.Type != Limit && o.Type != Stop {
		return fmt.Errorf("order: invalid type %q", o.Type)
	}
	if o.Qty <= 0 {
		return fmt.Errorf("order: qty must be positive, got %d", o.Qty)
	}
	if (o.Type == Limit || o.Type == Stop) && o.LimitPrice <= 0 {
		return fmt.Errorf("order: limit/stop orders require a positive limit price, got %v", o.LimitPrice)
	}
	return nil
}

// WouldFill reports whether the order is marketable against ltp (the latest
// tick price for its instrument):
//
//   - Market orders are always marketable -- they fill at the current LTP
//     the instant they are placed (or the instant the next tick arrives, if
//     placed between ticks).
//   - Limit buy orders are marketable when ltp <= LimitPrice (you're willing
//     to buy at LimitPrice or better, i.e. lower or equal).
//   - Limit sell orders are marketable when ltp >= LimitPrice (you're willing
//     to sell at LimitPrice or better, i.e. higher or equal).
//   - Stop orders trigger in the OPPOSITE direction from a limit at the same
//     side: a stop sell (protecting a long) triggers when ltp <= LimitPrice
//     (price fell to-or-below the stop, time to cut the loss); a stop buy
//     (protecting a short) triggers when ltp >= LimitPrice. This is what
//     makes a stop useful as the loss-limiting leg of a bracket order
//     (internal/bracket) paired with a limit take-profit leg on the same
//     side: whichever price level is reached first, in whichever direction,
//     triggers its corresponding order.
func WouldFill(o Order, ltp float64) bool {
	switch o.Type {
	case Market:
		return true
	case Limit:
		if o.Side == Buy {
			return ltp <= o.LimitPrice
		}
		return ltp >= o.LimitPrice
	case Stop:
		if o.Side == Sell {
			return ltp <= o.LimitPrice
		}
		return ltp >= o.LimitPrice
	default:
		return false
	}
}

// FillPrice returns the price an order fills at, given that WouldFill has
// already returned true for ltp. Both market and limit orders fill at the
// observed LTP (the price of the tick that made them marketable), rather
// than at the limit price itself -- this reflects that the limit price is a
// worst-acceptable-price bound, not a guaranteed execution price, and keeps
// the fill price always traceable to an actual tick. This choice is noted in
// the README as a design decision.
func FillPrice(ltp float64) float64 {
	return ltp
}
