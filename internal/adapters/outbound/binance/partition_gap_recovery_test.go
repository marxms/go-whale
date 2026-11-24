package binancesdk

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/ingestion/sourcetest"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// The partition-scoped reconnect used to call triggerGapRecovery without ever recording the
// disconnect instant (only the fleet path did), so the FIRST socket failure after boot skipped
// gap recovery entirely. The window must start when the failure was observed, not when the dial
// happens to run.
func TestPartitionReconnectRecordsTheGapWindow(t *testing.T) {
	rec := sourcetest.NewReconnectRecorder()
	server, _ := newMarketWSServer(t, func(conn *websocket.Conn, _ *http.Request, _ int) {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	c := NewSDKClient(streaming.ClientConfig{TradeBufferSize: 10, MarketType: "swap", GapRecovery: rec})
	c.wsBasePath = wsServerURL(server)
	t.Cleanup(func() { _ = c.Close() })

	symbols := make([]string, 0, 340)
	for i := 0; i < 340; i++ {
		symbols = append(symbols, fmt.Sprintf("GAP%04d/USDT", i))
	}
	c.symbolsMu.Lock()
	for _, s := range symbols {
		c.symbols[s] = true
	}
	c.symbolsMu.Unlock()

	c.connectMu.Lock()
	if _, err := c.connectAndResubscribeCombined(symbols); err != nil {
		c.connectMu.Unlock()
		t.Fatalf("combined connect: %v", err)
	}
	c.connectMu.Unlock()

	observed := time.Now()
	// delayed skips the stability wait; the production path passes the same request through the
	// timer before landing here.
	c.reconnectAndResubscribe(reconnectRequest{partition: 0, reason: "test", at: observed, delayed: true})

	batches := rec.Batches()
	if len(batches) != 1 {
		t.Fatalf("gap recovery batches = %d, want 1 (the partition path skipped it)", len(batches))
	}
	got := batches[0]
	if got.DisconnectTime.IsZero() {
		t.Fatal("the gap window has no disconnect time")
	}
	if delta := got.DisconnectTime.Sub(observed); delta < -time.Millisecond || delta > time.Millisecond {
		t.Fatalf("disconnect time = %v, want the observation instant %v (delta %v)", got.DisconnectTime, observed, delta)
	}
	if len(got.Symbols) == 0 || !got.ReconnectTime.After(got.DisconnectTime) {
		t.Fatalf("batch = %+v, want symbols and a reconnect after the disconnect", got)
	}
}
