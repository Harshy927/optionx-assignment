package instrument

import (
	"context"
	"sync"
)

// Registry owns one Actor per instrument token, creating and starting them
// lazily on first use, and keeps a lightweight index of which token owns
// which order ID -- so a caller cancelling an order (e.g. the REST API's
// DELETE /orders/{id}) doesn't need to already know the order's instrument.
//
// Registry itself is safe for concurrent use from many goroutines (e.g.
// concurrent HTTP requests): its own state (the actors map and the order
// index) is protected by a mutex. This is a coarser-grained lock than the
// per-instrument actor design, but it only ever guards fast, in-memory
// map operations (never a channel send/receive, DB call, or anything that
// blocks for a meaningful duration), so it does not reintroduce the kind of
// contention the actor model is designed to avoid.
type Registry struct {
	ctx       context.Context         // parent lifecycle context; every actor is started with this
	store     Store                   // nil means actors created by this registry are in-memory-only
	seeds     map[string]InitialState // per-token restart-recovery state, consumed once on first Actor() call
	publisher Publisher               // nil means actors created by this registry don't stream updates

	mu      sync.Mutex
	actors  map[string]*Actor
	orderIx map[string]string // orderID -> token
}

// NewRegistry creates an empty, in-memory-only registry (no persistence).
// ctx governs the lifetime of every actor the registry ever creates:
// cancelling ctx stops all of them.
func NewRegistry(ctx context.Context) *Registry {
	return &Registry{
		ctx:     ctx,
		actors:  make(map[string]*Actor),
		orderIx: make(map[string]string),
	}
}

// NewPersistentRegistry creates a registry whose actors write through to
// store and are seeded from seeds (typically loaded from Postgres on boot --
// see internal/storage.LoadAllInstrumentState) the first time each token's
// actor is created. Instruments with no entry in seeds start from a zero
// InitialState, exactly like a brand new instrument.
//
// Every order in seeds is indexed into orderIx immediately (not lazily, when
// its actor is first created): a DELETE /orders/{id} for a pre-restart order
// must resolve to the right token even if no tick or new order has touched
// that instrument since boot, since Peek (used by the cancel path to avoid
// spawning actors as a side effect of a lookup) will not have created the
// actor yet.
func NewPersistentRegistry(ctx context.Context, store Store, seeds map[string]InitialState) *Registry {
	r := &Registry{
		ctx:     ctx,
		store:   store,
		seeds:   seeds,
		actors:  make(map[string]*Actor),
		orderIx: make(map[string]string),
	}
	for token, seed := range seeds {
		for _, o := range seed.Orders {
			r.orderIx[o.ID] = token
		}
	}
	return r
}

// SetPublisher configures the Publisher every future actor created by this
// registry (via Actor) will stream Updates to. It has no effect on actors
// already created. This is a separate setter (rather than a constructor
// parameter) so the WebSocket hub, the registry, and the store can all be
// constructed in whichever order is most convenient in cmd/server without
// a circular-construction dependency between the hub and the registry.
func (r *Registry) SetPublisher(p Publisher) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.publisher = p
}

// Actor returns the actor for token, creating and starting one if this is
// the first time token has been seen. If the registry was constructed with
// NewPersistentRegistry, the new actor writes through to the configured
// store and is seeded from seeds[token] (or a zero InitialState if token has
// no prior recorded state). If SetPublisher has been called, the new actor
// streams Updates to that publisher.
func (r *Registry) Actor(token string) *Actor {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.actors[token]
	if !ok {
		if r.store != nil {
			seed := r.seeds[token]
			a = NewPersistentActor(token, r.store, seed)
			// Re-index every order this instrument had before the restart,
			// so DELETE /orders/{id} can find the right actor for an order
			// placed in a previous process's lifetime, not just ones placed
			// after this boot.
			for _, o := range seed.Orders {
				r.orderIx[o.ID] = token
			}
		} else {
			a = NewActor(token)
		}
		if r.publisher != nil {
			a.WithPublisher(r.publisher)
		}
		a.Start(r.ctx)
		r.actors[token] = a
	}
	return a
}

// Peek returns the actor for token without creating one, so read-only
// callers (e.g. GET /positions/{token}) don't have the side effect of
// spawning a goroutine just to answer "this instrument has no history yet".
func (r *Registry) Peek(token string) (*Actor, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.actors[token]
	return a, ok
}

// IndexOrder records that orderID belongs to token, so a later TokenForOrder
// lookup (used to route a cancel request to the right actor) can find it.
func (r *Registry) IndexOrder(orderID, token string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.orderIx[orderID] = token
}

// TokenForOrder returns which token owns orderID, if the registry has seen
// it (via IndexOrder).
func (r *Registry) TokenForOrder(orderID string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	tok, ok := r.orderIx[orderID]
	return tok, ok
}

// Tokens returns every instrument token that currently has an actor.
func (r *Registry) Tokens() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.actors))
	for tok := range r.actors {
		out = append(out, tok)
	}
	return out
}
