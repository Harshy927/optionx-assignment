package ws

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/optionx/backend-assignment/internal/instrument"
)

// dialWS connects a real WebSocket client to the given httptest server URL.
func dialWS(t *testing.T, serverURL string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// TestEndToEnd_SlowClientDoesNotBlockFastClient is a real-socket version of
// the backpressure requirement: a fast, actively-reading client must
// continue receiving timely updates even while a second, deliberately
// non-reading client is connected and falling behind. This exercises the
// actual Client.flush/receive/readLoop machinery over real TCP, not just
// the in-memory buffer logic covered by hub_test.go.
func TestEndToEnd_SlowClientDoesNotBlockFastClient(t *testing.T) {
	hub := NewHub()
	srv := httptest.NewServer(Handler(hub))
	defer srv.Close()

	fast := dialWS(t, srv.URL)
	slow := dialWS(t, srv.URL)
	_ = slow // deliberately never read from -- simulates a stalled client

	// Give both clients a moment to register with the hub.
	require.Eventually(t, func() bool { return hub.ClientCount() == 2 }, time.Second, 10*time.Millisecond)

	// Publish a burst of updates, as a real actor's tick loop would.
	for i := 0; i < 20; i++ {
		hub.Publish(instrument.Update{Token: "X", LastLTP: float64(100 + i)})
		time.Sleep(5 * time.Millisecond)
	}

	// The fast client must receive at least one update within a couple of
	// flush windows -- proving the slow, non-reading peer did not block
	// delivery to it.
	require.NoError(t, fast.SetReadDeadline(time.Now().Add(2*time.Second)))
	var got instrument.Update
	require.NoError(t, fast.ReadJSON(&got))
	require.Equal(t, "X", got.Token)
}

// TestEndToEnd_ThrottleCoalescesBurstIntoFewMessages verifies the 100ms
// throttle: many rapid updates for the same instrument within one flush
// window must arrive at the client as a small number of messages (one per
// window), not one message per publish.
func TestEndToEnd_ThrottleCoalescesBurstIntoFewMessages(t *testing.T) {
	hub := NewHub()
	srv := httptest.NewServer(Handler(hub))
	defer srv.Close()

	client := dialWS(t, srv.URL)
	require.Eventually(t, func() bool { return hub.ClientCount() == 1 }, time.Second, 10*time.Millisecond)

	// Fire 50 updates for the same instrument well within a single 100ms
	// flush window.
	for i := 0; i < 50; i++ {
		hub.Publish(instrument.Update{Token: "X", LastLTP: float64(i)})
	}

	// Read whatever arrives over the next ~250ms (roughly 2 flush windows).
	require.NoError(t, client.SetReadDeadline(time.Now().Add(400*time.Millisecond)))
	var messages []instrument.Update
	for {
		var u instrument.Update
		if err := client.ReadJSON(&u); err != nil {
			break
		}
		messages = append(messages, u)
	}

	require.NotEmpty(t, messages, "client must receive at least one coalesced update")
	require.Less(t, len(messages), 50, "50 rapid same-instrument updates must coalesce into far fewer than 50 messages")
	// The very last message received must reflect the latest published
	// value -- coalescing must never drop the newest state.
	last := messages[len(messages)-1]
	require.Equal(t, 49.0, last.LastLTP)
}
