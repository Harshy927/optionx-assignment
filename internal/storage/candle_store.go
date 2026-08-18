package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/optionx/backend-assignment/internal/candle"
)

// CandleRow mirrors the candles table for scanning query results.
type CandleRow struct {
	Token     string    `db:"token"`
	BucketTS  time.Time `db:"bucket_ts"`
	Open      float64   `db:"open"`
	High      float64   `db:"high"`
	Low       float64   `db:"low"`
	Close     float64   `db:"close"`
	TickCount int64     `db:"tick_count"`
}

func (r CandleRow) toCandle() candle.Candle {
	return candle.Candle{
		Token:     r.Token,
		BucketTS:  r.BucketTS,
		Open:      r.Open,
		High:      r.High,
		Low:       r.Low,
		Close:     r.Close,
		TickCount: r.TickCount,
	}
}

// GetWatermark returns the last-seen seq for token, and false if the
// instrument has never been recorded.
func GetWatermark(ctx context.Context, q sqlx.QueryerContext, token string) (int64, bool, error) {
	var lastSeq int64
	err := sqlx.GetContext(ctx, q, &lastSeq,
		`SELECT last_seq FROM instrument_seq_watermark WHERE token = $1`, token)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("get watermark for %s: %w", token, err)
	}
	return lastSeq, true, nil
}

// LatestCandle returns the most recently persisted candle for token (highest
// bucket_ts), and false if none exists. Used on boot to seed the in-memory
// Aggregator's in-progress candle for an instrument.
func LatestCandle(ctx context.Context, q sqlx.QueryerContext, token string) (candle.Candle, bool, error) {
	var row CandleRow
	err := sqlx.GetContext(ctx, q, &row,
		`SELECT token, bucket_ts, open, high, low, close, tick_count
		 FROM candles WHERE token = $1 ORDER BY bucket_ts DESC LIMIT 1`, token)
	if err == sql.ErrNoRows {
		return candle.Candle{}, false, nil
	}
	if err != nil {
		return candle.Candle{}, false, fmt.Errorf("get latest candle for %s: %w", token, err)
	}
	return row.toCandle(), true, nil
}

// AllWatermarks returns every persisted (token -> last_seq) pair. Used on
// boot to seed all instruments at once, rather than one query per token.
func AllWatermarks(ctx context.Context, q sqlx.QueryerContext) (map[string]int64, error) {
	rows, err := q.QueryxContext(ctx, `SELECT token, last_seq FROM instrument_seq_watermark`)
	if err != nil {
		return nil, fmt.Errorf("list watermarks: %w", err)
	}
	defer rows.Close()

	out := make(map[string]int64)
	for rows.Next() {
		var token string
		var lastSeq int64
		if err := rows.Scan(&token, &lastSeq); err != nil {
			return nil, fmt.Errorf("scan watermark row: %w", err)
		}
		out[token] = lastSeq
	}
	return out, rows.Err()
}

// AllLatestCandles returns the most recently persisted candle for every
// instrument that has one, keyed by token.
func AllLatestCandles(ctx context.Context, q sqlx.QueryerContext) (map[string]candle.Candle, error) {
	rows, err := q.QueryxContext(ctx, `
		SELECT DISTINCT ON (token) token, bucket_ts, open, high, low, close, tick_count
		FROM candles
		ORDER BY token, bucket_ts DESC`)
	if err != nil {
		return nil, fmt.Errorf("list latest candles: %w", err)
	}
	defer rows.Close()

	out := make(map[string]candle.Candle)
	for rows.Next() {
		var row CandleRow
		if err := rows.StructScan(&row); err != nil {
			return nil, fmt.Errorf("scan candle row: %w", err)
		}
		out[row.Token] = row.toCandle()
	}
	return out, rows.Err()
}

// ApplyTick durably applies one tick's effect on an instrument's candle and
// seq watermark, atomically:
//
//  1. Reads the current watermark for the token (0 / "unseen" if absent).
//  2. If tick.Seq <= watermark, the transaction is rolled back and (0, true)
//     is returned to signal "this was a duplicate, nothing changed" --
//     this is what guarantees a replayed tick can never distort a candle,
//     even across process restarts.
//  3. Otherwise, upserts the candle row for result.Current (or, if the tick
//     rolled over a minute boundary, first finalizes result.Closed as its
//     own completed row) and bumps the watermark, all in the same
//     transaction, so a crash between these two writes is impossible to
//     observe as "candle updated but watermark not bumped" (or vice versa).
//
// The caller supplies result (from candle.Aggregator.Apply, called with the
// SAME tick) so this function does the persistence half of what the
// in-memory aggregator already computed, rather than recomputing OHLC logic
// in SQL.
func ApplyTick(ctx context.Context, db *sqlx.DB, token string, seq int64, result candle.ApplyResult) (duplicate bool, err error) {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() // no-op if committed

	lastSeq, found, err := GetWatermark(ctx, tx, token)
	if err != nil {
		return false, err
	}
	if found && seq <= lastSeq {
		// Duplicate -- roll back (via deferred Rollback) and report it.
		return true, nil
	}

	if result.Closed != nil {
		if err := upsertCandle(ctx, tx, *result.Closed); err != nil {
			return false, err
		}
	}
	if err := upsertCandle(ctx, tx, result.Current); err != nil {
		return false, err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO instrument_seq_watermark (token, last_seq) VALUES ($1, $2)
		ON CONFLICT (token) DO UPDATE SET last_seq = EXCLUDED.last_seq`,
		token, seq); err != nil {
		return false, fmt.Errorf("bump watermark for %s: %w", token, err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit tick for %s: %w", token, err)
	}
	return false, nil
}

func upsertCandle(ctx context.Context, tx *sqlx.Tx, c candle.Candle) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO candles (token, bucket_ts, open, high, low, close, tick_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (token, bucket_ts) DO UPDATE SET
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			tick_count = EXCLUDED.tick_count`,
		c.Token, c.BucketTS, c.Open, c.High, c.Low, c.Close, c.TickCount)
	if err != nil {
		return fmt.Errorf("upsert candle %s@%s: %w", c.Token, c.BucketTS, err)
	}
	return nil
}
