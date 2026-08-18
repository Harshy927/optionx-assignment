package instrument

// Update is the position/P&L snapshot an Actor pushes toward the WebSocket
// layer (internal/ws) whenever an instrument's price or position changes.
// It intentionally carries only position + P&L fields (not the order list)
// -- the assignment's streaming requirement is "position + P&L updates",
// and keeping this payload small matters given it may be emitted once per
// tick per instrument during a burst.
type Update struct {
	Token         string
	Qty           int64
	AvgPrice      float64
	RealizedPnL   float64
	UnrealizedPnL float64
	LastLTP       float64
}

// Publisher is implemented by the WebSocket hub (internal/ws) to receive
// Updates from instrument actors. A nil Publisher is valid -- exactly like
// Store, an Actor with no publisher configured simply skips the call, which
// is what every actor created before this capability existed continues to
// do.
//
// Publish must not block the calling actor for any meaningful duration: the
// actor's single-writer goroutine calls it synchronously, in-line with tick
// processing, so a slow implementation here would directly throttle the
// instrument's ability to keep up with a bursty feed. internal/ws.Hub's
// implementation honors this by doing only a non-blocking channel send.
type Publisher interface {
	Publish(u Update)
}

// UpdateFrom builds an Update from the current position and last price.
// token is taken explicitly rather than from pos.Token, because Position's
// Token field is only ever set once a fill has occurred (see ApplyFill) --
// an instrument that has received ticks but never traded would otherwise
// produce an Update with an empty Token.
func UpdateFrom(token string, pos Position, lastLTP float64) Update {
	return Update{
		Token:         token,
		Qty:           pos.Qty,
		AvgPrice:      pos.AvgPrice,
		RealizedPnL:   pos.RealizedPnL,
		UnrealizedPnL: UnrealizedPnL(pos, lastLTP),
		LastLTP:       lastLTP,
	}
}
