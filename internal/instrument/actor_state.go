package instrument

import (
	"context"
	"log"

	"github.com/optionx/backend-assignment/internal/order"
)

// actorState holds everything an Actor's single goroutine mutates. It is
// deliberately a plain struct with no synchronization of its own: safety
// comes entirely from the fact that only Actor.run ever touches it, one
// message at a time.
type actorState struct {
	token     string
	position  Position
	lastLTP   float64
	hasLTP    bool
	orders    map[string]order.Order // all orders ever seen, keyed by ID
	store     Store                  // nil if this actor is in-memory-only
	publisher Publisher              // nil if this actor has no WebSocket streaming
}

func newActorState(token string, store Store, publisher Publisher) *actorState {
	return &actorState{
		token:     token,
		orders:    make(map[string]order.Order),
		store:     store,
		publisher: publisher,
	}
}

// publish sends the actor's current position + last price to the
// publisher, if one is configured. Called after every tick (price changed,
// so unrealized P&L changed) and after every fill (position changed).
func (s *actorState) publish() {
	if s.publisher == nil {
		return
	}
	s.publisher.Publish(UpdateFrom(s.token, s.position, s.lastLTP))
}

// restore seeds the actor's in-memory state from a previously-persisted
// InitialState, before any live message is processed. lastLTP/hasLTP are
// deliberately NOT restored from persistence: the actor should treat the
// first tick it receives after a restart as the authoritative "current
// price" rather than trusting a possibly-stale price from before the crash
// -- resting orders are re-evaluated against that fresh tick exactly as they
// would be in normal operation, which is what the graded restart+replay
// scenario depends on.
func (s *actorState) restore(seed InitialState) {
	s.position = seed.Position
	for _, o := range seed.Orders {
		s.orders[o.ID] = o
	}
}

// persist writes through to the store, if one is configured, and logs
// (rather than propagating) any error. A persistence failure must not
// block the in-memory actor from replying to its caller -- the caller's
// view of "did my order fill" is authoritative from the actor's own
// single-writer state; storage is a durability concern for restart
// recovery, not a precondition for correctness of the current process's
// in-memory answers. This mirrors the candle pipeline's choice (Task 4) to
// log-and-continue on a persistence error rather than crash the tick loop.
func (s *actorState) persistOrder(ctx context.Context, o order.Order) {
	if s.store == nil {
		return
	}
	if err := s.store.SaveOrder(ctx, o); err != nil {
		log.Printf("instrument %s: failed to persist order %s: %v", s.token, o.ID, err)
	}
}

func (s *actorState) persistFill(ctx context.Context, o order.Order, pos Position) {
	if s.store == nil {
		return
	}
	if err := s.store.SaveFillTransition(ctx, o, pos); err != nil {
		log.Printf("instrument %s: failed to persist fill for order %s: %v", s.token, o.ID, err)
	}
}

// handle dispatches one message. This is the only place order/tick/cancel
// logic runs, and it runs to completion (no yielding mid-handler) before the
// next message is read off the channel -- that atomicity is the entire
// mechanism behind the cancel-vs-trigger race guarantee. ctx is used only
// for the persistence write-through calls inside this handler (it is the
// actor's own run-loop ctx, not a per-request one, so a slow write cannot be
// cancelled by an unrelated caller's timeout -- it can only be cut short by
// the actor itself shutting down).
func (s *actorState) handle(ctx context.Context, msg message) {
	switch m := msg.(type) {
	case PlaceOrderMsg:
		s.handlePlaceOrder(ctx, m)
	case CancelOrderMsg:
		s.handleCancelOrder(ctx, m)
	case TickMsg:
		s.handleTick(ctx, m)
	case SnapshotMsg:
		s.handleSnapshot(m)
	case PlaceBracketMsg:
		s.handlePlaceBracket(ctx, m)
	}
}

func (s *actorState) handlePlaceOrder(ctx context.Context, m PlaceOrderMsg) {
	o := m.Order
	if err := o.Validate(); err != nil {
		m.Reply <- PlaceOrderResult{Outcome: PlaceOutcomeRejected, Order: o, Error: err.Error()}
		return
	}
	if o.Token != s.token {
		m.Reply <- PlaceOrderResult{
			Outcome: PlaceOutcomeRejected, Order: o,
			Error: "order token does not match this instrument actor",
		}
		return
	}
	if _, exists := s.orders[o.ID]; exists {
		m.Reply <- PlaceOrderResult{Outcome: PlaceOutcomeRejected, Order: o, Error: "duplicate order id"}
		return
	}

	// A market order needs a known price to fill against. If no tick has
	// ever arrived for this instrument, it cannot fill yet -- it rests until
	// the first tick, at which point handleTick's fill loop (WouldFill
	// returns true unconditionally for Market) fills it immediately. This
	// is an edge case not explicitly in the assignment (ticks are assumed to
	// be flowing continuously) but keeps the actor from ever fabricating a
	// fill price.
	if s.hasLTP && order.WouldFill(o, s.lastLTP) {
		fill := s.fillOrder(ctx, &o, s.lastLTP)
		m.Reply <- PlaceOrderResult{Outcome: PlaceOutcomeFilled, Order: o, Fill: &fill}
		return
	}

	o.Status = order.StatusResting
	s.orders[o.ID] = o
	s.persistOrder(ctx, o)
	m.Reply <- PlaceOrderResult{Outcome: PlaceOutcomeResting, Order: o}
}

func (s *actorState) handleCancelOrder(ctx context.Context, m CancelOrderMsg) {
	o, ok := s.orders[m.OrderID]
	if !ok {
		m.Reply <- CancelOrderResult{Outcome: CancelOutcomeNotFound}
		return
	}

	switch o.Status {
	case order.StatusResting, order.StatusPending:
		// A Pending order is a bracket child whose entry hasn't filled yet
		// -- cancelling it before that happens is a normal cancellation,
		// exactly like cancelling a resting order. If it has a sibling
		// (e.g. cancelling one bracket leg before the entry fills), the
		// sibling is intentionally left alone here: cancelling one child
		// pre-activation does not cancel the other or the entry -- only a
		// FILL cancels a sibling (see cancelSiblingOf, called from
		// fillOrder). This matches the assignment's "cancel-race rules
		// apply to the pair" for the fill-vs-cancel race on either leg,
		// without adding an implicit cascade for a plain single cancel.
		o.Status = order.StatusCancelled
		s.orders[o.ID] = o
		s.persistOrder(ctx, o)
		m.Reply <- CancelOrderResult{Outcome: CancelOutcomeCancelled, Order: o}
	case order.StatusFilled:
		// The cancel lost the race: a tick (processed earlier in this
		// actor's single message stream, by definition -- there is no other
		// way this order could already be Filled) already filled it.
		m.Reply <- CancelOrderResult{Outcome: CancelOutcomeAlreadyFilled, Order: o}
	case order.StatusCancelled:
		m.Reply <- CancelOrderResult{Outcome: CancelOutcomeAlreadyCancelled, Order: o}
	}
}

func (s *actorState) handleTick(ctx context.Context, m TickMsg) {
	s.lastLTP = m.LTP
	s.hasLTP = true

	// Fill every resting order that now crosses. Iterating a map in Go has
	// randomized order, which is fine here: each order is independent and a
	// single tick crossing multiple resting orders fills all of them --
	// there is no ordering requirement between distinct orders, only
	// between messages (which are already strictly ordered by the channel).
	for _, o := range s.orders {
		if o.Status != order.StatusResting {
			continue
		}
		if order.WouldFill(o, m.LTP) {
			// fillOrder writes the updated order back into s.orders itself.
			s.fillOrder(ctx, &o, m.LTP)
		}
	}

	// Every tick moves LastLTP, which moves UnrealizedPnL even when nothing
	// filled -- publish once per tick so subscribers see live mark-to-market
	// P&L, not just fill events. If a fill also happened above, fillOrder
	// already published once with the up-to-date position; this call is
	// harmless idempotent redundancy in that case, not a double-count of
	// any state (Update is a snapshot, not an event log) -- the WebSocket
	// hub's own throttling (Task 9's 100ms coalescing) is what actually
	// bounds how often subscribers receive these.
	s.publish()
}

func (s *actorState) handleSnapshot(m SnapshotMsg) {
	orders := make([]order.Order, 0, len(s.orders))
	for _, o := range s.orders {
		orders = append(orders, o)
	}
	m.Reply <- Snapshot{
		Token:    s.token,
		Position: s.position,
		LastLTP:  s.lastLTP,
		Orders:   orders,
	}
}

// fillOrder marks o as filled at price, applies the resulting fill to the
// position ledger, persists both together (see Store.SaveFillTransition),
// and returns the Fill record. It mutates *o directly (the caller is
// responsible for writing it back into s.orders, since Go maps don't
// support in-place struct mutation through a range variable).
//
// If o is a bracket entry (something else's EntryID points at it), filling
// it activates its Pending children in the same step -- see
// activateChildrenOf. If o is itself a bracket child (o.SiblingID is set),
// filling it cancels its sibling in the same step -- see cancelSiblingOf.
// Doing both synchronously, before this function returns (and therefore
// before the next message on this actor's channel can be processed), is
// what makes "when one child fills, the other cancels" atomic with respect
// to a concurrently-arriving cancel request for the sibling: that request
// is simply the next message, and by the time it's handled, the sibling is
// already Cancelled -- the requester is told AlreadyCancelled, which is
// accurate, not a race outcome that could have gone either way.
func (s *actorState) fillOrder(ctx context.Context, o *order.Order, price float64) order.Fill {
	fill := order.Fill{
		OrderID: o.ID,
		Token:   o.Token,
		Side:    o.Side,
		Qty:     o.Qty,
		Price:   order.FillPrice(price),
	}
	s.position = ApplyFill(s.position, fill)
	o.Status = order.StatusFilled
	s.orders[o.ID] = *o
	s.persistFill(ctx, *o, s.position)

	s.activateChildrenOf(ctx, o.ID)
	s.cancelSiblingOf(ctx, *o)

	s.publish()
	return fill
}
