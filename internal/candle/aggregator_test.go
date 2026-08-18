package candle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/optionx/backend-assignment/internal/feed"
)

// t0 is an arbitrary fixed minute boundary (UTC) used to build deterministic
// timestamps across tests.
var t0 = time.Date(2026, 8, 18, 10, 30, 0, 0, time.UTC)

func tickAt(seq int64, token string, ltp float64, offset time.Duration) feed.Tick {
	return feed.Tick{
		Seq:   seq,
		Token: token,
		LTP:   ltp,
		Ts:    t0.Add(offset).UnixMilli(),
	}
}

func TestAggregator_SingleMinute_OHLC(t *testing.T) {
	agg := NewAggregator()
	tok := "NIFTY26AUG24800CE"

	// Ticks within the same minute: open=100, then up to 110 (high), down to
	// 90 (low), close at 105.
	res := agg.Apply(tickAt(1, tok, 100, 0))
	require.False(t, res.Duplicate)
	require.Nil(t, res.Closed)
	assert.Equal(t, 100.0, res.Current.Open)

	res = agg.Apply(tickAt(2, tok, 110, 5*time.Second))
	require.Nil(t, res.Closed)
	assert.Equal(t, 110.0, res.Current.High)

	res = agg.Apply(tickAt(3, tok, 90, 10*time.Second))
	require.Nil(t, res.Closed)
	assert.Equal(t, 90.0, res.Current.Low)

	res = agg.Apply(tickAt(4, tok, 105, 15*time.Second))
	require.Nil(t, res.Closed)

	final := res.Current
	assert.Equal(t, 100.0, final.Open)
	assert.Equal(t, 110.0, final.High)
	assert.Equal(t, 90.0, final.Low)
	assert.Equal(t, 105.0, final.Close)
	assert.Equal(t, int64(4), final.TickCount)
	assert.Equal(t, t0, final.BucketTS)
}

func TestAggregator_MinuteBoundary_RollsOverCandle(t *testing.T) {
	agg := NewAggregator()
	tok := "NIFTY26AUG24800CE"

	agg.Apply(tickAt(1, tok, 100, 0))
	agg.Apply(tickAt(2, tok, 105, 30*time.Second))

	// This tick lands in the next minute -- must close the old candle and
	// open a new one.
	res := agg.Apply(tickAt(3, tok, 108, 61*time.Second))

	require.NotNil(t, res.Closed, "crossing a minute boundary must close the previous candle")
	closed := *res.Closed
	assert.Equal(t, t0, closed.BucketTS)
	assert.Equal(t, 100.0, closed.Open)
	assert.Equal(t, 105.0, closed.Close)
	assert.Equal(t, int64(2), closed.TickCount)

	newCandle := res.Current
	assert.Equal(t, t0.Add(time.Minute), newCandle.BucketTS)
	assert.Equal(t, 108.0, newCandle.Open)
	assert.Equal(t, 108.0, newCandle.High)
	assert.Equal(t, 108.0, newCandle.Low)
	assert.Equal(t, 108.0, newCandle.Close)
	assert.Equal(t, int64(1), newCandle.TickCount)
}

func TestAggregator_DuplicateSeq_IsNoOp(t *testing.T) {
	agg := NewAggregator()
	tok := "NIFTY26AUG24800CE"

	agg.Apply(tickAt(1, tok, 100, 0))
	before := agg.Apply(tickAt(2, tok, 110, 5*time.Second)).Current

	// Replay seq=2 (already applied) with a *different* price -- this must
	// be ignored. If it weren't, high/close would change to 999.
	dup := agg.Apply(tickAt(2, tok, 999, 6*time.Second))
	assert.True(t, dup.Duplicate)

	after, ok := agg.Snapshot(tok)
	require.True(t, ok)
	assert.Equal(t, before, after, "candle must be byte-for-byte unchanged after a replayed duplicate seq")

	// Also replay an even older seq to confirm it's not just "!= latest".
	dup2 := agg.Apply(tickAt(1, tok, 555, 7*time.Second))
	assert.True(t, dup2.Duplicate)
	after2, _ := agg.Snapshot(tok)
	assert.Equal(t, before, after2)
}

func TestAggregator_SeqZero_IsValidFirstTick(t *testing.T) {
	// tickgen's seq is 1-based in practice (first tick for an instrument has
	// seq=1, per the pacer incrementing before emit), but the aggregator must
	// not special-case seq=0 as "no tick seen" -- hasSeen tracks that
	// explicitly. This guards against an off-by-one dedup bug.
	agg := NewAggregator()
	tok := "X"

	res := agg.Apply(tickAt(0, tok, 50, 0))
	require.False(t, res.Duplicate)

	// A second tick with the same seq=0 must now be treated as a duplicate.
	dup := agg.Apply(tickAt(0, tok, 999, time.Second))
	assert.True(t, dup.Duplicate)
}

func TestAggregator_MultipleInstruments_AreIndependent(t *testing.T) {
	agg := NewAggregator()

	agg.Apply(tickAt(1, "A", 100, 0))
	agg.Apply(tickAt(1, "B", 200, 0))
	agg.Apply(tickAt(2, "A", 150, time.Second))

	a, ok := agg.Snapshot("A")
	require.True(t, ok)
	assert.Equal(t, 150.0, a.Close)
	assert.Equal(t, int64(2), a.TickCount)

	b, ok := agg.Snapshot("B")
	require.True(t, ok)
	assert.Equal(t, 200.0, b.Close)
	assert.Equal(t, int64(1), b.TickCount)

	// Replaying B's seq=1 must not affect A.
	agg.Apply(tickAt(1, "B", 999, 2*time.Second))
	b2, _ := agg.Snapshot("B")
	assert.Equal(t, b, b2)
}

func TestAggregator_Snapshot_UnknownToken(t *testing.T) {
	agg := NewAggregator()
	_, ok := agg.Snapshot("does-not-exist")
	assert.False(t, ok)
}

func TestBucketFor_TruncatesToMinute(t *testing.T) {
	ts := time.Date(2026, 8, 18, 10, 30, 45, 123456789, time.UTC)
	got := BucketFor(ts.UnixMilli())
	want := time.Date(2026, 8, 18, 10, 30, 0, 0, time.UTC)
	assert.True(t, got.Equal(want), "got %v want %v", got, want)
}
