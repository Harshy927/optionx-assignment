package instrument

import (
	"context"

	"github.com/optionx/backend-assignment/internal/order"
)

// Store is implemented by the persistence layer (internal/storage) to give
// an Actor durable write-through for orders and positions. A nil Store is
// valid: an Actor with no store simply skips persistence, which is what
// every test prior to this task relies on (in-memory-only actors).
//
// Both methods are called synchronously, inside the actor's single
// processing goroutine, immediately after the corresponding in-memory
// mutation and before the actor replies to the caller. This ordering is
// deliberate: it means a crash between "decided the outcome" and "told the
// caller" is impossible to observe as a state where the API response and
// Postgres disagree -- either the write commits before the reply is sent (so
// a crash right after can only lose the reply, not the durable fact), or the
// process dies before the write commits, in which case the reply is also
// never sent, and the pre-crash truth (as reloaded from Postgres on restart)
// is "still resting" -- exactly the state a replayed tick can correctly
// re-evaluate without any double effect.
type Store interface {
	// SaveOrder persists o's current status (used for a newly-resting order
	// or a cancellation -- any transition that does not itself change the
	// position ledger).
	SaveOrder(ctx context.Context, o order.Order) error

	// SaveFillTransition persists o (now Filled) and pos (the position after
	// applying that fill) as a single atomic unit, so a crash can never
	// observe "order marked filled but position not updated" or vice versa.
	SaveFillTransition(ctx context.Context, o order.Order, pos Position) error
}

// InitialState is what a Registry (or a caller constructing an Actor
// directly) feeds into Actor.Seed to restore an instrument's state from
// durable storage before the actor starts accepting live messages. This is
// the restart-recovery analog of candle.Aggregator.Seed (Task 4): it makes a
// freshly-created, in-memory actor resume exactly where a previous process
// left off, so that replaying the tick feed from an earlier point can never
// double-fill a resting order (it is only ever evaluated once more, and
// fillOrder is a no-op for anything not still in StatusResting) or fabricate
// a phantom position (the ledger starts from the last durably-saved value,
// not from zero).
type InitialState struct {
	Position Position
	Orders   []order.Order // every order previously known for this instrument, any status
}
