package feed

import (
	"encoding/json"
	"fmt"
)

// ParseLine parses a single NDJSON line (as emitted by tickgen) into a Tick.
// A malformed line returns a descriptive error rather than a zero-value Tick,
// so callers can decide whether to skip-and-log or abort.
func ParseLine(line []byte) (Tick, error) {
	var t Tick
	if err := json.Unmarshal(line, &t); err != nil {
		return Tick{}, fmt.Errorf("parse tick line %q: %w", string(line), err)
	}
	if t.Token == "" {
		return Tick{}, fmt.Errorf("parse tick line %q: missing token", string(line))
	}
	return t, nil
}
