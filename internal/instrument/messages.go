package instrument

import "github.com/optionx/backend-assignment/internal/order"

// message is the tagged-union of everything an Actor's single run loop can
// receive. Only the message types below implement it (via the unexported
// isMessage marker), so the compiler enforces that state.handle can be a
// closed switch.
type message interface {
	isMessage()
}

// PlaceOrderMsg asks the actor to accept a new order. Reply receives exactly
// one PlaceOrderResult.
type PlaceOrderMsg struct {
	Order order.Order
	Reply chan<- PlaceOrderResult
}

func (PlaceOrderMsg) isMessage() {}

// CancelOrderMsg asks the actor to cancel a resting order by ID. Reply
// receives exactly one CancelOrderResult.
type CancelOrderMsg struct {
	OrderID string
	Reply   chan<- CancelOrderResult
}

func (CancelOrderMsg) isMessage() {}

// TickMsg delivers a new last-traded-price for the actor's instrument.
type TickMsg struct {
	LTP float64
}

func (TickMsg) isMessage() {}

// SnapshotMsg asks for a copy of the actor's current position + all orders.
type SnapshotMsg struct {
	Reply chan<- Snapshot
}

func (SnapshotMsg) isMessage() {}

// PlaceBracketMsg asks the actor to accept a bracket order: an entry plus a
// linked take-profit (Target, a Limit order) and stop-loss (Stop, a Stop
// order) pair on the opposite side of Entry. The whole triple is validated
// and stored atomically in one message-handling step -- there is no window
// where the entry exists without its children (or vice versa) that a
// concurrently-arriving cancel or tick could observe. See
// internal/instrument's bracket.go for the full handling logic.
type PlaceBracketMsg struct {
	Entry  order.Order
	Target order.Order
	Stop   order.Order
	Reply  chan<- PlaceBracketResult
}

func (PlaceBracketMsg) isMessage() {}

// PlaceBracketResult is the actor's reply to a PlaceBracketMsg.
type PlaceBracketResult struct {
	Outcome PlaceOrderOutcome // Filled/Resting describes the ENTRY leg; children are always Pending or Resting once Outcome != Rejected
	Entry   order.Order
	Target  order.Order
	Stop    order.Order
	Fill    *order.Fill // non-nil iff the entry filled immediately
	Error   string
}

// PlaceOrderOutcome describes what happened to a newly-placed order.
type PlaceOrderOutcome string

const (
	PlaceOutcomeFilled   PlaceOrderOutcome = "filled"
	PlaceOutcomeResting  PlaceOrderOutcome = "resting"
	PlaceOutcomeRejected PlaceOrderOutcome = "rejected"
)

// PlaceOrderResult is the actor's reply to a PlaceOrderMsg.
type PlaceOrderResult struct {
	Outcome PlaceOrderOutcome
	Order   order.Order // the stored order, with Status set to match Outcome
	Fill    *order.Fill // non-nil iff Outcome == PlaceOutcomeFilled
	Error   string      // non-empty iff Outcome == PlaceOutcomeRejected
}

// CancelOrderOutcome definitively states what happened to a cancel request.
// Exactly one of these is ever returned, and it always agrees with the
// actor's internal ledger state -- there is no scenario where the API
// caller is told "cancelled" while the ledger shows a fill, or vice versa,
// because both are decided by the same actor goroutine in the same message
// handling step.
type CancelOrderOutcome string

const (
	// CancelOutcomeCancelled means the order was resting and is now
	// cancelled; it will never fill.
	CancelOutcomeCancelled CancelOrderOutcome = "cancelled"
	// CancelOutcomeAlreadyFilled means a tick filled the order before this
	// cancel request was processed -- the cancel lost the race.
	CancelOutcomeAlreadyFilled CancelOrderOutcome = "already_filled"
	// CancelOutcomeAlreadyCancelled means the order had already been
	// cancelled by an earlier request (idempotent double-cancel).
	CancelOutcomeAlreadyCancelled CancelOrderOutcome = "already_cancelled"
	// CancelOutcomeNotFound means no order with that ID is known to this
	// actor.
	CancelOutcomeNotFound CancelOrderOutcome = "not_found"
)

// CancelOrderResult is the actor's reply to a CancelOrderMsg.
type CancelOrderResult struct {
	Outcome CancelOrderOutcome
	Order   order.Order // the order's final state; zero value if NotFound
}

// Snapshot is a point-in-time copy of an actor's state.
type Snapshot struct {
	Token    string
	Position Position
	LastLTP  float64
	Orders   []order.Order // all orders the actor has ever seen (resting, filled, cancelled)
}
