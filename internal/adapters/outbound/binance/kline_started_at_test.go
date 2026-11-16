package binancesdk

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/candle"
)

// startedAt used to be written before it was read, so `restarted` was always true: the first dial
// of every process counted as a reconnect and the gauge opened claiming a recovery that never
// happened. The second Start IS a restart and must still count.
func TestFirstStartIsNotCountedAsAReconnect(t *testing.T) {
	server, _ := newMarketWSServer(t, func(_ *websocket.Conn, _ *http.Request, _ int) {
		<-time.After(2 * time.Second)
	})
	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {})
	feed.wsBasePath = wsServerURL(server)
	t.Cleanup(feed.Stop)

	if err := feed.Start([]string{"BTC/USDT"}); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	sum := func() int64 {
		feed.mu.Lock()
		defer feed.mu.Unlock()
		var total int64
		for _, p := range feed.parts {
			total += p.reconnects.Load()
		}
		if len(feed.parts) == 0 {
			t.Fatal("no partition was dialed")
		}
		return total
	}
	if got := sum(); got != 0 {
		t.Fatalf("first dial counted as %d reconnect(s)", got)
	}

	if err := feed.Start([]string{"BTC/USDT"}); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if got := sum(); got == 0 {
		t.Fatal("a real restart must still be counted as a reconnect")
	}
}
