package ws

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/optionx/backend-assignment/internal/instrument"
)

// fakeSink stands in for a Client's socket-writing side in tests that don't
// need a real network connection: it lets us drive Client.receive directly
// and inspect what would have been buffered/flushed, without gorilla's
// websocket.Conn requiring an actual HTTP upgrade.
//
// Rather than reimplement Client with an interface seam (a larger change
// than this task warrants), these tests exercise the coalescing logic via a
// minimal standalone type that mirrors Client's buffer semantics exactly --
// this is the same non-blocking-write, latest-value-wins buffer described
// in the package doc comment, tested in isolation from the actual socket
// I/O (which is covered by the live end-to-end demo instead, since a real
// stalled TCP client is impractical to construct deterministically in a
// unit test).
type fakeSink struct {
	mu  sync.Mutex
	buf map[string]instrument.Update
}

func newFakeSink() *fakeSink {
	return &fakeSink{buf: make(map[string]instrument.Update)}
}

func (s *fakeSink) receive(u instrument.Update) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf[u.Token] = u // latest value wins, never queues
}

func (s *fakeSink) drain() map[string]instrument.Update {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.buf
	s.buf = make(map[string]instrument.Update)
	return out
}

func TestCoalescing_LatestValueWinsWithinWindow(t *testing.T) {
	sink := newFakeSink()

	// Three updates for the same instrument arrive before any flush.
	sink.receive(instrument.Update{Token: "X", LastLTP: 100})
	sink.receive(instrument.Update{Token: "X", LastLTP: 101})
	sink.receive(instrument.Update{Token: "X", LastLTP: 102})

	drained := sink.drain()
	require.Len(t, drained, 1, "multiple updates for the same instrument within a window must coalesce into one")
	assert.Equal(t, 102.0, drained["X"].LastLTP, "the coalesced update must be the most recent one, not the first or an average")
}

func TestCoalescing_DifferentInstruments_BothRetained(t *testing.T) {
	sink := newFakeSink()

	sink.receive(instrument.Update{Token: "X", LastLTP: 100})
	sink.receive(instrument.Update{Token: "Y", LastLTP: 200})

	drained := sink.drain()
	require.Len(t, drained, 2)
	assert.Equal(t, 100.0, drained["X"].LastLTP)
	assert.Equal(t, 200.0, drained["Y"].LastLTP)
}

// TestHub_Publish_NeverBlocks_EvenWithManySlowClients is the core
// backpressure guarantee: Hub.Publish (called synchronously by an
// instrument actor's single-writer goroutine) must return quickly regardless
// of how many clients are registered or how slow/stalled any of them are.
// This test registers many real *Client values (via the exported Client
// type, using its actual receive/flush machinery through the hub) and
// floods Publish calls from a single goroutine (simulating the tick loop),
// asserting the whole flood completes well within a bound that would be
// impossible if any client's slowness could block a Publish call.
func TestHub_Publish_NeverBlocks_EvenWithManySlowClients(t *testing.T) {
	hub := NewHub()

	const numClients = 50
	clients := make([]*Client, numClients)
	for i := range clients {
		// A Client with a nil conn is fine here: Publish only reaches
		// receive() (buffer write), never flush() (which would dereference
		// conn) -- flush is driven by Run's ticker, which we never start in
		// this test, so no nil-conn write is attempted.
		c := &Client{
			hub:  hub,
			subs: make(map[string]struct{}),
			buf:  make(map[string]instrument.Update),
			done: make(chan struct{}),
		}
		hub.register(c)
		clients[i] = c
	}

	const numUpdates = 2000
	start := time.Now()
	for i := 0; i < numUpdates; i++ {
		hub.Publish(instrument.Update{Token: "X", LastLTP: float64(i)})
	}
	elapsed := time.Since(start)

	// 2000 publishes x 50 clients = 100,000 non-blocking map writes. This
	// should take well under a second on any reasonable machine; a
	// generous 2s bound avoids flakiness on a loaded CI box while still
	// catching any accidental blocking behavior (e.g. an unbuffered channel
	// send instead of a direct buffer write).
	assert.Less(t, elapsed, 2*time.Second, "Publish must not block on client count or client speed")

	// Every client should have coalesced down to exactly the latest value.
	for _, c := range clients {
		c.bufMu.Lock()
		u, ok := c.buf["X"]
		c.bufMu.Unlock()
		require.True(t, ok)
		assert.Equal(t, float64(numUpdates-1), u.LastLTP)
	}
}

func TestClient_Receive_RespectsSubscriptionFilter(t *testing.T) {
	c := &Client{
		subs: map[string]struct{}{"X": {}},
		buf:  make(map[string]instrument.Update),
	}

	c.receive(instrument.Update{Token: "X", LastLTP: 1})
	c.receive(instrument.Update{Token: "Y", LastLTP: 2}) // not subscribed, must be dropped

	assert.Len(t, c.buf, 1)
	_, hasX := c.buf["X"]
	_, hasY := c.buf["Y"]
	assert.True(t, hasX)
	assert.False(t, hasY, "an update for an instrument not in the subscription filter must not be buffered")
}

func TestClient_Receive_NoFilterMeansEverything(t *testing.T) {
	c := &Client{
		subs: make(map[string]struct{}), // empty = no filter
		buf:  make(map[string]instrument.Update),
	}

	c.receive(instrument.Update{Token: "X", LastLTP: 1})
	c.receive(instrument.Update{Token: "Y", LastLTP: 2})

	assert.Len(t, c.buf, 2, "a client with no subscription filter must receive every instrument's updates")
}

func TestHub_RegisterUnregister_UpdatesClientCount(t *testing.T) {
	hub := NewHub()
	assert.Equal(t, 0, hub.ClientCount())

	c := &Client{buf: make(map[string]instrument.Update)}
	hub.register(c)
	assert.Equal(t, 1, hub.ClientCount())

	hub.unregister(c)
	assert.Equal(t, 0, hub.ClientCount())
}
