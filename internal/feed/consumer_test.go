package feed

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startFakeTickgen starts a minimal TCP server that writes the given lines to
// the first client that connects, then closes the connection. It returns the
// listener address.
func startFakeTickgen(t *testing.T, lines []string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		w := bufio.NewWriter(conn)
		for _, l := range lines {
			w.WriteString(l + "\n")
		}
		w.Flush()
	}()

	return ln.Addr().String()
}

func TestConsumer_Run_DeliversParsedTicks(t *testing.T) {
	addr := startFakeTickgen(t, []string{
		`{"seq": 1, "token": "NIFTY26AUG24800CE", "ltp": 100.0, "ts": 1000}`,
		`{"seq": 2, "token": "NIFTY26AUG24800CE", "ltp": 100.5, "ts": 1001}`,
		`not valid json, should be skipped`,
		`{"seq": 3, "token": "NIFTY26AUG24800CE", "ltp": 101.0, "ts": 1002}`,
	})

	c := NewConsumer(addr)
	out := make(chan Tick, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.Run(ctx, out)
	// The fake server closes the connection after writing, so Run should
	// return a "closed by peer" error rather than a context error.
	require.Error(t, err)

	close(out)
	var got []Tick
	for tick := range out {
		got = append(got, tick)
	}

	require.Len(t, got, 3, "malformed line must be skipped, not delivered or fatal")
	require.Equal(t, int64(1), got[0].Seq)
	require.Equal(t, int64(2), got[1].Seq)
	require.Equal(t, int64(3), got[2].Seq)
	require.Equal(t, "NIFTY26AUG24800CE", got[0].Token)
	require.Equal(t, 101.0, got[2].LTP)
}

func TestConsumer_Run_RespectsContextCancellation(t *testing.T) {
	// Server that never sends anything and never closes, to force Run to
	// block until context cancellation is what unblocks it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// Hold the connection open, sending nothing, until the test ends.
		<-time.After(5 * time.Second)
		conn.Close()
	}()

	c := NewConsumer(ln.Addr().String())
	out := make(chan Tick)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Run(ctx, out)
	}()

	// Give Run a moment to dial and start scanning, then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return promptly after context cancellation")
	}
}

func TestConsumer_Run_DialError(t *testing.T) {
	c := NewConsumer("127.0.0.1:1") // nothing listens on privileged port 1
	c.DialTimeout = 500 * time.Millisecond
	out := make(chan Tick)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.Run(ctx, out)
	require.Error(t, err)
}
