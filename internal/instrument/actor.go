package instrument

import (
	"context"
	"fmt"

	"github.com/optionx/backend-assignment/internal/order"
)

// Actor owns exclusive, single-writer access to one instrument's resting
// orders and position ledger. All mutations -- ticks, order placement, order
// cancellation -- are delivered as messages over a single inbound channel
// and processed strictly in the order they are received by exactly one
// goroutine (run, in actor_state.go). This is what makes the cancel-vs-
// trigger race deterministic: a cancel and a crossing tick for the same
// order can never be evaluated concurrently against each other, because
// only one of the two messages can ever be "next" in the channel.
//
// Actor has no knowledge of candles -- those run independently (see
// cmd/server), driven off the same tick stream but through a separate
// consumer. It does have optional persistence write-through (Store) and
// optional restart recovery (Seed); both are nil-safe, so every test written
// before this capability existed continues to exercise a pure-in-memory
// actor unaffected by this addition.
type Actor struct {
	Token string
	inbox chan message
	done  chan struct{}

	store     Store        // nil means "no persistence" (used by tests and pre-Task-8 code)
	seed      InitialState // applied once, at the start of run, before the first message is processed
	publisher Publisher    // nil means "no WebSocket streaming" (used by tests and pre-Task-9 code)
}

// NewActor creates an in-memory-only actor for token (no persistence, no
// WebSocket publishing). Call Start to begin processing messages; until
// Start is called, sends to the actor will block forever.
func NewActor(token string) *Actor {
	return &Actor{
		Token: token,
		inbox: make(chan message, 128),
		done:  make(chan struct{}),
	}
}

// NewPersistentActor creates an actor for token that writes every order and
// position mutation through to store, and whose starting state is seed
// (typically loaded from Postgres by the caller before constructing the
// actor -- see internal/instrument.LoadInitialState in Task 8's storage
// integration, wired up in cmd/server). It has no WebSocket publisher; use
// WithPublisher to add one.
func NewPersistentActor(token string, store Store, seed InitialState) *Actor {
	return &Actor{
		Token: token,
		inbox: make(chan message, 128),
		done:  make(chan struct{}),
		store: store,
		seed:  seed,
	}
}

// WithPublisher sets the actor's WebSocket publisher and returns the actor,
// for chaining after either constructor. It must be called before Start.
func (a *Actor) WithPublisher(p Publisher) *Actor {
	a.publisher = p
	return a
}

// Start begins the actor's single processing goroutine. It returns
// immediately; the goroutine runs until ctx is cancelled, after which the
// actor stops accepting further progress (pending and future calls to
// PlaceOrder/CancelOrder/Tick/Snapshot will return ctx's error or a
// "stopped" error rather than hang).
func (a *Actor) Start(ctx context.Context) {
	go a.run(ctx)
}

func (a *Actor) run(ctx context.Context) {
	defer close(a.done)
	state := newActorState(a.Token, a.store, a.publisher)
	state.restore(a.seed)
	for {
		select {
		case msg := <-a.inbox:
			state.handle(ctx, msg)
		case <-ctx.Done():
			return
		}
	}
}

// errStopped is returned when a caller tries to interact with an actor whose
// run loop has already exited (ctx cancelled), distinguishing it from the
// caller's own ctx being cancelled.
var errStopped = fmt.Errorf("instrument actor: stopped")

// send delivers msg to the actor's inbox, respecting both the caller's ctx
// and the actor's own lifecycle (a.done).
func (a *Actor) send(ctx context.Context, msg message) error {
	select {
	case a.inbox <- msg:
		return nil
	case <-a.done:
		return errStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PlaceOrder submits a new order to the actor and waits for the outcome: the
// order fills immediately (market, or a limit that already crosses the last
// known price) or begins resting.
func (a *Actor) PlaceOrder(ctx context.Context, o order.Order) (PlaceOrderResult, error) {
	reply := make(chan PlaceOrderResult, 1)
	if err := a.send(ctx, PlaceOrderMsg{Order: o, Reply: reply}); err != nil {
		return PlaceOrderResult{}, err
	}
	select {
	case res := <-reply:
		return res, nil
	case <-a.done:
		return PlaceOrderResult{}, errStopped
	case <-ctx.Done():
		return PlaceOrderResult{}, ctx.Err()
	}
}

// CancelOrder requests cancellation of a resting order and waits for the
// outcome. The returned CancelOrderResult.Outcome definitively states what
// happened -- Cancelled, AlreadyFilled, AlreadyCancelled, or NotFound --
// and is guaranteed to agree with the actor's final ledger state, because
// both are decided by the same single-writer goroutine handling this
// request and any competing tick in one strict order.
func (a *Actor) CancelOrder(ctx context.Context, orderID string) (CancelOrderResult, error) {
	reply := make(chan CancelOrderResult, 1)
	if err := a.send(ctx, CancelOrderMsg{OrderID: orderID, Reply: reply}); err != nil {
		return CancelOrderResult{}, err
	}
	select {
	case res := <-reply:
		return res, nil
	case <-a.done:
		return CancelOrderResult{}, errStopped
	case <-ctx.Done():
		return CancelOrderResult{}, ctx.Err()
	}
}

// Tick delivers a new last-traded-price observation to the actor. It is
// fire-and-forget from the caller's perspective (no reply): the actor
// updates its notion of the current price and fills any resting order that
// now crosses it.
func (a *Actor) Tick(ctx context.Context, ltp float64) error {
	return a.send(ctx, TickMsg{LTP: ltp})
}

// PlaceBracket submits a bracket order (entry + linked target/stop children)
// to the actor and waits for the outcome. See PlaceBracketMsg and bracket.go
// for the full semantics -- notably that the entry, target, and stop are
// validated and stored as a single atomic step, so no concurrently-arriving
// cancel or tick can ever observe the triple half-constructed.
func (a *Actor) PlaceBracket(ctx context.Context, entry, target, stop order.Order) (PlaceBracketResult, error) {
	reply := make(chan PlaceBracketResult, 1)
	if err := a.send(ctx, PlaceBracketMsg{Entry: entry, Target: target, Stop: stop, Reply: reply}); err != nil {
		return PlaceBracketResult{}, err
	}
	select {
	case res := <-reply:
		return res, nil
	case <-a.done:
		return PlaceBracketResult{}, errStopped
	case <-ctx.Done():
		return PlaceBracketResult{}, ctx.Err()
	}
}

// Snapshot returns a point-in-time copy of the actor's position and all
// known orders. Because the snapshot itself is produced by a message
// processed through the same single-writer channel, it always reflects a
// fully-settled state -- never a state "in between" a tick and a
// concurrently-arriving cancel.
func (a *Actor) Snapshot(ctx context.Context) (Snapshot, error) {
	reply := make(chan Snapshot, 1)
	if err := a.send(ctx, SnapshotMsg{Reply: reply}); err != nil {
		return Snapshot{}, err
	}
	select {
	case res := <-reply:
		return res, nil
	case <-a.done:
		return Snapshot{}, errStopped
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	}
}
