package ws

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/optionx/backend-assignment/internal/instrument"
)

const (
	// flushInterval is the throttle window: at most one update per
	// instrument per client is sent within this window, per the
	// assignment's requirement.
	flushInterval = 100 * time.Millisecond

	// writeDeadline bounds how long a single WriteJSON call may block on a
	// client that has stopped reading (TCP send buffer full). Without this,
	// a truly stuck client's flush loop could hang indefinitely; with it,
	// the write fails fast and the client is disconnected and cleaned up.
	writeDeadline = 5 * time.Second
)

// subscribeMessage is the JSON shape a client sends to change which
// instruments it receives updates for.
//
//	{"action": "subscribe", "tokens": ["NIFTY26AUG24800CE", "..."]}
//	{"action": "unsubscribe", "tokens": ["NIFTY26AUG24800CE"]}
//
// A client that never sends a subscribe message receives updates for every
// instrument (empty subs set means "no filter") -- this keeps a quick
// `curl`-less manual test (or the simplest possible client) useful without
// requiring a subscribe handshake first.
type subscribeMessage struct {
	Action string   `json:"action"`
	Tokens []string `json:"tokens"`
}

// Client represents one connected WebSocket subscriber. Its buffer holds at
// most one pending instrument.Update per token -- a newer update for a
// token a client hasn't been sent yet simply overwrites the older one, per
// the coalescing backpressure policy described in package ws's doc comment.
type Client struct {
	hub  *Hub
	conn *websocket.Conn

	subMu sync.Mutex
	subs  map[string]struct{} // empty means "no filter, everything"

	bufMu sync.Mutex
	buf   map[string]instrument.Update

	done      chan struct{}
	closeOnce sync.Once
}

// NewClient wraps an already-upgraded WebSocket connection as a Client.
func NewClient(hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		hub:  hub,
		conn: conn,
		subs: make(map[string]struct{}),
		buf:  make(map[string]instrument.Update),
		done: make(chan struct{}),
	}
}

// Run registers the client with the hub, starts its read loop (for
// subscribe/unsubscribe messages) in the background, and drives the
// 100ms flush ticker until the connection closes or ctx is cancelled. Run
// blocks until the client disconnects; callers should invoke it in its own
// goroutine per accepted connection.
func (c *Client) Run(ctx context.Context) {
	c.hub.register(c)
	defer c.hub.unregister(c)
	defer c.conn.Close()

	go c.readLoop()

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !c.flush() {
				return
			}
		case <-c.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

// receive is called by the Hub (via Publish) from an instrument actor's
// single-writer goroutine. It must never block for a meaningful duration:
// it only ever takes two short-held mutexes to check the subscription
// filter and write into the coalescing buffer -- there is no channel send,
// no I/O, and no wait on the client's own read/write pace here. This is
// what guarantees a slow or stalled client cannot backpressure the tick
// loop or any other client.
func (c *Client) receive(u instrument.Update) {
	c.subMu.Lock()
	_, filtered := c.subs[u.Token]
	noFilter := len(c.subs) == 0
	c.subMu.Unlock()

	if !noFilter && !filtered {
		return
	}

	c.bufMu.Lock()
	c.buf[u.Token] = u
	c.bufMu.Unlock()
}

// flush drains the coalescing buffer and writes each pending update to the
// socket. It returns false if the write failed (connection is dead), so Run
// can stop and clean up.
func (c *Client) flush() bool {
	c.bufMu.Lock()
	if len(c.buf) == 0 {
		c.bufMu.Unlock()
		return true
	}
	pending := make([]instrument.Update, 0, len(c.buf))
	for _, u := range c.buf {
		pending = append(pending, u)
	}
	c.buf = make(map[string]instrument.Update)
	c.bufMu.Unlock()

	if err := c.conn.SetWriteDeadline(time.Now().Add(writeDeadline)); err != nil {
		return false
	}
	for _, u := range pending {
		if err := c.conn.WriteJSON(u); err != nil {
			return false
		}
	}
	return true
}

// readLoop reads subscribe/unsubscribe control messages from the client
// until the connection closes or an unrecoverable read error occurs, then
// signals Run to stop via c.done. It is the only goroutine that reads from
// c.conn, per gorilla/websocket's single-reader requirement.
func (c *Client) readLoop() {
	defer c.closeDone()
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		var msg subscribeMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("ws client: ignoring malformed control message: %v", err)
			continue
		}

		c.subMu.Lock()
		switch msg.Action {
		case "subscribe":
			for _, t := range msg.Tokens {
				c.subs[t] = struct{}{}
			}
		case "unsubscribe":
			for _, t := range msg.Tokens {
				delete(c.subs, t)
			}
		}
		c.subMu.Unlock()
	}
}

func (c *Client) closeDone() {
	c.closeOnce.Do(func() { close(c.done) })
}
