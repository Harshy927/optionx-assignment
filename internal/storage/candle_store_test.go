package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/optionx/backend-assignment/internal/candle"
	"github.com/optionx/backend-assignment/internal/feed"
)

// openTestDB connects to the local Postgres instance used for development
// and testing, applies migrations, and truncates the tables this test suite
// touches so each test starts from a clean slate. Requires a running local
// Postgres per README setup.
func openTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	cfg := ConfigFromEnv()
	db, err := Open(cfg)
	require.NoError(t, err, "failed to connect to local postgres; is it running? see README")
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()
	require.NoError(t, Migrate(ctx, db, "../../migrations"))
	_, err = db.ExecContext(ctx, `TRUNCATE candles, instrument_seq_watermark`)
	require.NoError(t, err)

	return db
}

func TestApplyTick_PersistsCandleAndWatermark(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	tok := "TESTTOK1"

	agg := candle.NewAggregator()
	t0 := time.Date(2026, 8, 18, 10, 30, 0, 0, time.UTC)

	tick := feed.Tick{Seq: 1, Token: tok, LTP: 100.0, Ts: t0.UnixMilli()}
	result := agg.Apply(tick)

	dup, err := ApplyTick(ctx, db, tok, tick.Seq, result)
	require.NoError(t, err)
	assert.False(t, dup)

	lastSeq, found, err := GetWatermark(ctx, db, tok)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, int64(1), lastSeq)

	c, found, err := LatestCandle(ctx, db, tok)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, 100.0, c.Open)
	assert.Equal(t, 100.0, c.Close)
	assert.Equal(t, int64(1), c.TickCount)
}

func TestApplyTick_ReplayedSeq_NeverDistortsCandle(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	tok := "TESTTOK2"

	agg := candle.NewAggregator()
	t0 := time.Date(2026, 8, 18, 10, 30, 0, 0, time.UTC)

	// Apply seq=1 and seq=2 normally.
	for _, tick := range []feed.Tick{
		{Seq: 1, Token: tok, LTP: 100.0, Ts: t0.UnixMilli()},
		{Seq: 2, Token: tok, LTP: 110.0, Ts: t0.Add(5 * time.Second).UnixMilli()},
	} {
		result := agg.Apply(tick)
		dup, err := ApplyTick(ctx, db, tok, tick.Seq, result)
		require.NoError(t, err)
		require.False(t, dup)
	}

	before, found, err := LatestCandle(ctx, db, tok)
	require.NoError(t, err)
	require.True(t, found)

	// Replay seq=2 (already applied), with a wildly different price. This
	// must be rejected as a duplicate and leave the persisted candle
	// byte-for-byte unchanged -- the core replay-safety guarantee.
	replay := feed.Tick{Seq: 2, Token: tok, LTP: 9999.0, Ts: t0.Add(6 * time.Second).UnixMilli()}
	replayResult := agg.Apply(replay)
	assert.True(t, replayResult.Duplicate, "in-memory aggregator must also flag this as duplicate")

	dup, err := ApplyTick(ctx, db, tok, replay.Seq, replayResult)
	require.NoError(t, err)
	assert.True(t, dup)

	after, found, err := LatestCandle(ctx, db, tok)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, before, after, "replayed duplicate seq must not change the persisted candle")
}

func TestApplyTick_MinuteBoundary_PersistsBothCandles(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	tok := "TESTTOK3"

	agg := candle.NewAggregator()
	t0 := time.Date(2026, 8, 18, 10, 30, 0, 0, time.UTC)

	tick1 := feed.Tick{Seq: 1, Token: tok, LTP: 100.0, Ts: t0.UnixMilli()}
	res1 := agg.Apply(tick1)
	_, err := ApplyTick(ctx, db, tok, tick1.Seq, res1)
	require.NoError(t, err)

	// Cross into the next minute.
	tick2 := feed.Tick{Seq: 2, Token: tok, LTP: 108.0, Ts: t0.Add(61 * time.Second).UnixMilli()}
	res2 := agg.Apply(tick2)
	require.NotNil(t, res2.Closed)
	_, err = ApplyTick(ctx, db, tok, tick2.Seq, res2)
	require.NoError(t, err)

	// Both the closed (first minute) and current (second minute) candles
	// must be persisted as distinct rows.
	var rows []CandleRow
	require.NoError(t, db.SelectContext(ctx, &rows,
		`SELECT token, bucket_ts, open, high, low, close, tick_count FROM candles WHERE token = $1 ORDER BY bucket_ts`, tok))
	require.Len(t, rows, 2)
	assert.Equal(t, 100.0, rows[0].Close)
	assert.Equal(t, 108.0, rows[1].Open)
}

func TestApplyTick_RestartAndReplay_NoDoubleCount(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	tok := "TESTTOK4"
	t0 := time.Date(2026, 8, 18, 10, 30, 0, 0, time.UTC)

	// "Process A" applies ticks 1, 2, 3.
	aggA := candle.NewAggregator()
	for _, tick := range []feed.Tick{
		{Seq: 1, Token: tok, LTP: 100.0, Ts: t0.UnixMilli()},
		{Seq: 2, Token: tok, LTP: 105.0, Ts: t0.Add(5 * time.Second).UnixMilli()},
		{Seq: 3, Token: tok, LTP: 110.0, Ts: t0.Add(10 * time.Second).UnixMilli()},
	} {
		res := aggA.Apply(tick)
		_, err := ApplyTick(ctx, db, tok, tick.Seq, res)
		require.NoError(t, err)
	}

	// "Restart": a brand new Aggregator, seeded from Postgres exactly as
	// cmd/server would do on boot.
	lastSeq, found, err := GetWatermark(ctx, db, tok)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(3), lastSeq)

	persisted, found, err := LatestCandle(ctx, db, tok)
	require.NoError(t, err)
	require.True(t, found)

	aggB := candle.NewAggregator()
	aggB.Seed(tok, lastSeq, &persisted)

	// Replay from an earlier seq (1 and 2 are replays; 3 is a replay too;
	// then a genuinely new tick 4 arrives).
	replayed := []feed.Tick{
		{Seq: 1, Token: tok, LTP: 999.0, Ts: t0.Add(11 * time.Second).UnixMilli()},
		{Seq: 2, Token: tok, LTP: 999.0, Ts: t0.Add(12 * time.Second).UnixMilli()},
		{Seq: 3, Token: tok, LTP: 999.0, Ts: t0.Add(13 * time.Second).UnixMilli()},
		{Seq: 4, Token: tok, LTP: 120.0, Ts: t0.Add(14 * time.Second).UnixMilli()},
	}
	for _, tick := range replayed {
		res := aggB.Apply(tick)
		_, err := ApplyTick(ctx, db, tok, tick.Seq, res)
		require.NoError(t, err)
	}

	final, found, err := LatestCandle(ctx, db, tok)
	require.NoError(t, err)
	require.True(t, found)

	// Only seq=4 (genuinely new) should have affected the candle: high must
	// reflect max(110, 120) = 120, tick_count must be 4 (3 real + 1 new),
	// NOT 3 + 4 = 7 (which would indicate double-counting from replay).
	assert.Equal(t, 100.0, final.Open)
	assert.Equal(t, 120.0, final.High)
	assert.Equal(t, 120.0, final.Close)
	assert.Equal(t, int64(4), final.TickCount, "replayed seqs 1-3 must not be double-counted after restart")

	finalSeq, _, err := GetWatermark(ctx, db, tok)
	require.NoError(t, err)
	assert.Equal(t, int64(4), finalSeq)
}

func TestAllWatermarks_And_AllLatestCandles(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	t0 := time.Date(2026, 8, 18, 10, 30, 0, 0, time.UTC)

	agg := candle.NewAggregator()
	for _, tick := range []feed.Tick{
		{Seq: 1, Token: "AAA", LTP: 10.0, Ts: t0.UnixMilli()},
		{Seq: 1, Token: "BBB", LTP: 20.0, Ts: t0.UnixMilli()},
		{Seq: 2, Token: "AAA", LTP: 15.0, Ts: t0.Add(time.Second).UnixMilli()},
	} {
		res := agg.Apply(tick)
		_, err := ApplyTick(ctx, db, tick.Token, tick.Seq, res)
		require.NoError(t, err)
	}

	watermarks, err := AllWatermarks(ctx, db)
	require.NoError(t, err)
	assert.Equal(t, int64(2), watermarks["AAA"])
	assert.Equal(t, int64(1), watermarks["BBB"])

	candles, err := AllLatestCandles(ctx, db)
	require.NoError(t, err)
	assert.Equal(t, 15.0, candles["AAA"].Close)
	assert.Equal(t, 20.0, candles["BBB"].Close)
}
