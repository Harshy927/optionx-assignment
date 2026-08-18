// Package candle aggregates a stream of ticks into 1-minute OHLC candles,
// per instrument. The Aggregator in this package is pure and in-memory: it
// has no DB or network dependency, which keeps the OHLC math easy to test in
// isolation. Persistence (Task 4) wraps this aggregator with a Postgres
// write-through and a durable seq watermark; this package only guards against
// duplicate ticks *within a single process's lifetime* using an in-memory
// watermark -- durability across restarts is layered on top, not here.
package candle

import "time"

// Candle is one instrument's OHLC bar for a single 1-minute bucket.
type Candle struct {
	Token     string
	BucketTS  time.Time // start of the 1-minute bucket, UTC
	Open      float64
	High      float64
	Low       float64
	Close     float64
	TickCount int64
}

// BucketFor truncates a tick's millisecond epoch timestamp down to the start
// of its 1-minute UTC bucket.
func BucketFor(tsMillis int64) time.Time {
	return time.UnixMilli(tsMillis).UTC().Truncate(time.Minute)
}
