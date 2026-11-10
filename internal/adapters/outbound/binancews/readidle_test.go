package binancews

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestReadIdleRenewedByPing proves the read deadline follows the venue's ping: a
// connection whose only traffic is control pings must stay open beyond the deadline.
func TestReadIdleRenewedByPing(t *testing.T) {
	server := newWSServer(t, func(conn *websocket.Conn, _ *http.Request) {
		// Let the client finish installing its handlers before the first ping; otherwise
		// the earliest ping can land in the window between dial and Connect's setup.
		time.Sleep(20 * time.Millisecond)
		for i := 0; i < 10; i++ {
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second)); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		<-time.After(200 * time.Millisecond)
	})

	conn := NewConnection(0, wsURL(server), WithReadIdle(60*time.Millisecond))
	if err := conn.Connect(context.Background(), map[string]Handler{"btcusdt@kline_5m": func([]byte) {}}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	select {
	case err := <-conn.Errors():
		t.Fatalf("the connection died although server pings renewed the read deadline: %v", err)
	case <-conn.Done():
		t.Fatal("the connection died although server pings renewed the read deadline")
	case <-time.After(250 * time.Millisecond):
	}
}
