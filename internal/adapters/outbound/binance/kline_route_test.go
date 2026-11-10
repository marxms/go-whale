package binancesdk

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/candle"
)

func TestKlineFeedDialsMarketRoute(t *testing.T) {
	paths := make(chan string, 1)
	server, _ := newMarketWSServer(t, func(conn *websocket.Conn, r *http.Request, _ int) {
		paths <- r.URL.Path
		frame := `{"stream":"btcusdt@kline_5m","data":` + capturedKlineData + `}`
		_ = conn.WriteMessage(websocket.TextMessage, []byte(frame))
		<-time.After(100 * time.Millisecond)
	})

	var delivered atomic.Int64
	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {
		delivered.Add(1)
	})
	feed.wsBasePath = wsServerURL(server)
	if err := feed.Start([]string{"BTC/USDT"}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer feed.Stop()

	select {
	case path := <-paths:
		if path != "/market/stream" {
			t.Fatalf("kline feed dialled %q, want /market/stream", path)
		}
	case <-time.After(time.Second):
		t.Fatal("the kline feed never reached the server")
	}
	if !waitFor(t, time.Second, func() bool { return delivered.Load() == 1 }) {
		t.Fatalf("candles delivered = %d, want 1: the routed feed dropped the frame", delivered.Load())
	}
}

func TestStreamNameIsDocumented(t *testing.T) {
	for _, class := range []string{"trade", "kline"} {
		if got := routeFor("binance", "future", class); got == "" {
			t.Errorf("stream class %q has no documented route in the policy", class)
		}
	}
	if got := routeFor("binance", "spot", "kline"); got != "" {
		t.Errorf("binance spot route = %q, want none", got)
	}

	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {})
	streams := feed.klineStreams([]string{"BTC/USDT"})
	if len(streams) != 1 || streams[0] != "btcusdt@kline_5m" {
		t.Fatalf("kline streams = %v, want [btcusdt@kline_5m]", streams)
	}
}
