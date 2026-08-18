// Package feed connects to a running tickgen instance over TCP and parses its
// newline-delimited JSON tick stream into typed Tick values.
package feed

// Tick is one parsed tick from the feed. Seq is monotonically increasing
// per-instrument (per Token) -- NOT globally -- and is the field idempotency
// checks are built around: a replayed tick has a seq the consumer has already
// applied for that instrument.
type Tick struct {
	Seq   int64   `json:"seq"`
	Token string  `json:"token"`
	LTP   float64 `json:"ltp"`
	Ts    int64   `json:"ts"`
}
