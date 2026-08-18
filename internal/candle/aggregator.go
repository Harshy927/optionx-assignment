package candle

import (
	"github.com/optionx/backend-assignment/internal/feed"
)

// instrumentState tracks the in-progress candle and dedup watermark for one
// instrument.
type instrumentState struct {
	lastSeq int64 // highest seq applied so far; 0 means "none yet"
	hasSeen bool  // distinguishes "no ticks yet" from "last tick had seq 0"
	current Candle
	hasOpen bool // whether `current` holds an in-progress candle
}

// ApplyResult describes the effect of applying one tick to the aggregator.
type ApplyResult struct {
	// Duplicate is true if the tick's seq was <= the last-seen seq for its
	// instrument, meaning it was ignored entirely (no candle mutated).
	Duplicate bool

	// Current is the (possibly just-updated) in-progress candle for the
	// tick's own bucket. Zero value if Duplicate is true.
	Current Candle

	// Closed holds the previous bucket's finished candle if this tick's
	// bucket rolled the instrument over to a new minute. Nil otherwise (or
	// if Duplicate is true).
	Closed *Candle
}

// Aggregator maintains one in-progress 1-minute candle and one seq watermark
// per instrument. It is not safe for concurrent use from multiple goroutines
// without external synchronization -- in this project it is always driven by
// a single goroutine per instrument (see internal/instrument), so no internal
// locking is needed.
type Aggregator struct {
	states map[string]*instrumentState
}

// NewAggregator creates an empty in-memory aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{states: make(map[string]*instrumentState)}
}

// Apply processes one tick. A tick whose seq has already been seen for its
// instrument (seq <= lastSeq) is a no-op: it must never distort a candle,
// per the assignment's replay-safety requirement.
func (a *Aggregator) Apply(t feed.Tick) ApplyResult {
	st, ok := a.states[t.Token]
	if !ok {
		st = &instrumentState{}
		a.states[t.Token] = st
	}

	if st.hasSeen && t.Seq <= st.lastSeq {
		return ApplyResult{Duplicate: true}
	}
	st.lastSeq = t.Seq
	st.hasSeen = true

	bucket := BucketFor(t.Ts)

	var closed *Candle
	if !st.hasOpen {
		st.current = Candle{
			Token:     t.Token,
			BucketTS:  bucket,
			Open:      t.LTP,
			High:      t.LTP,
			Low:       t.LTP,
			Close:     t.LTP,
			TickCount: 1,
		}
		st.hasOpen = true
	} else if bucket.After(st.current.BucketTS) {
		// New minute: close out the old candle, start a fresh one.
		finished := st.current
		closed = &finished

		st.current = Candle{
			Token:     t.Token,
			BucketTS:  bucket,
			Open:      t.LTP,
			High:      t.LTP,
			Low:       t.LTP,
			Close:     t.LTP,
			TickCount: 1,
		}
	} else {
		// Same bucket (or, defensively, an out-of-order-but-not-duplicate
		// tick landing in an already-closed-in-the-past bucket -- treated as
		// still belonging to the current open bucket since ticks only ever
		// replay from an earlier *seq*, not an earlier *time*, per feed
		// semantics).
		c := &st.current
		c.High = max(c.High, t.LTP)
		c.Low = min(c.Low, t.LTP)
		c.Close = t.LTP
		c.TickCount++
	}

	return ApplyResult{Current: st.current, Closed: closed}
}

// Snapshot returns the current in-progress candle for token, if any.
func (a *Aggregator) Snapshot(token string) (Candle, bool) {
	st, ok := a.states[token]
	if !ok || !st.hasOpen {
		return Candle{}, false
	}
	return st.current, true
}

// Seed restores an instrument's state from durable storage (see
// internal/storage) so a freshly-created, in-memory Aggregator can resume
// exactly where a previous process left off: subsequent ticks with seq <=
// lastSeq are still treated as duplicates, and a tick landing in the same
// bucket as `current` continues accumulating into it rather than restarting
// the candle from scratch.
//
// current is nil when no candle has been persisted yet for this instrument
// (lastSeq may still be > 0, e.g. an instrument seen before but whose only
// candle has already rolled over and been superseded -- callers should only
// pass the most recently persisted candle row, if any).
//
// This is what makes restart-under-replay safe for candles: on boot, the
// caller loads the last-seen seq and the most recently persisted candle for
// each instrument and calls Seed before consuming any ticks.
func (a *Aggregator) Seed(token string, lastSeq int64, current *Candle) {
	st := &instrumentState{lastSeq: lastSeq, hasSeen: true}
	if current != nil {
		st.current = *current
		st.hasOpen = true
	}
	a.states[token] = st
}
