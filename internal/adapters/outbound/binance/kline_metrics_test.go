package binancesdk

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/candle"
)

func TestAttachLogIncludesRoute(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	var routed string

	server, _ := newMarketWSServer(t, func(_ *websocket.Conn, _ *http.Request, _ int) {
		<-time.After(100 * time.Millisecond)
	})
	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {}, log)
	feed.SetMetrics(Metrics{KlineRoute: func(_, _, route string) { routed = route }})
	feed.wsBasePath = wsServerURL(server)
	if err := feed.Start([]string{"BTC/USDT"}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer feed.Stop()

	if !strings.Contains(logs.String(), "route=/market") {
		t.Fatalf("attach log does not name the route: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "stream_class=kline") {
		t.Fatalf("attach log does not name the stream class: %s", logs.String())
	}
	if routed != "/market" {
		t.Fatalf("route metric = %q, want /market", routed)
	}
}

func TestPartitionMetrics(t *testing.T) {
	server, _ := newMarketWSServer(t, func(conn *websocket.Conn, _ *http.Request, _ int) {
		frame := `{"stream":"btcusdt@kline_5m","data":` + capturedKlineData + `}`
		for i := 0; i < 2; i++ {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
				return
			}
		}
		<-time.After(200 * time.Millisecond)
	})

	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {})
	feed.wsBasePath = wsServerURL(server)
	if err := feed.Start([]string{"BTC/USDT"}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer feed.Stop()

	if !waitFor(t, 2*time.Second, func() bool {
		stats := feed.PartitionStats()
		return len(stats) == 1 && stats[0].Frames == 2
	}) {
		t.Fatalf("partition stats never reached two frames: %+v", feed.PartitionStats())
	}
	stats := feed.PartitionStats()
	if stats[0].Symbols != 1 {
		t.Errorf("symbols = %d, want 1", stats[0].Symbols)
	}
	if stats[0].LastFrameAge < 0 || stats[0].LastFrameAge > time.Second {
		t.Errorf("last frame age = %v, want within one second", stats[0].LastFrameAge)
	}

	if err := feed.AddSymbols([]string{"ETH/USDT"}); err != nil {
		t.Fatalf("AddSymbols: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		for _, p := range feed.PartitionStats() {
			if p.Symbols == 1 && p.Restores == 1 {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("no partition recorded the restore: %+v", feed.PartitionStats())
	}

	if err := feed.Start([]string{"BTC/USDT"}); err != nil {
		t.Fatalf("Start again: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		for _, p := range feed.PartitionStats() {
			if p.Reconnects >= 1 {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("no partition recorded the reconnect: %+v", feed.PartitionStats())
	}
}
