package feed

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"time"
)

// Consumer connects to a tickgen TCP endpoint and streams parsed Ticks onto a
// channel. It does not dedupe or reorder anything -- it is a thin transport
// layer; idempotency (via the seq watermark) is handled by consumers of the
// channel (candle aggregator, instrument actors), per the architecture.
type Consumer struct {
	Addr        string
	DialTimeout time.Duration
}

// NewConsumer builds a Consumer for the given tickgen address, e.g. "localhost:9001".
func NewConsumer(addr string) *Consumer {
	return &Consumer{Addr: addr, DialTimeout: 5 * time.Second}
}

// Run dials Addr, reads NDJSON lines until ctx is cancelled or the connection
// is closed by the peer, and sends each successfully parsed Tick on out. A
// line that fails to parse is logged and skipped rather than aborting the
// stream -- a single malformed line should not take down the feed.
//
// Run blocks until ctx is done or an unrecoverable connection error occurs. It
// does not reconnect on its own; the caller (e.g. cmd/server) may loop on Run
// if automatic reconnect is desired -- noted as a possible follow-up, not
// required for this assignment.
func (c *Consumer) Run(ctx context.Context, out chan<- Tick) error {
	dialer := net.Dialer{Timeout: c.DialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.Addr)
	if err != nil {
		return fmt.Errorf("dial tickgen at %s: %w", c.Addr, err)
	}
	defer conn.Close()

	// Close the connection promptly if the context is cancelled, so the
	// blocking Scanner.Scan() below unblocks instead of hanging forever.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	scanner := bufio.NewScanner(conn)
	// tickgen lines are short JSON objects; the default 64KB scanner buffer is
	// more than sufficient, so no buffer resize is needed here.

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		tick, err := ParseLine(line)
		if err != nil {
			log.Printf("feed: skipping malformed line: %v", err)
			continue
		}

		select {
		case out <- tick:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := scanner.Err(); err != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return fmt.Errorf("read from tickgen: %w", err)
		}
	}

	// Peer closed the connection cleanly (EOF) without an error.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("tickgen connection closed by peer")
}
