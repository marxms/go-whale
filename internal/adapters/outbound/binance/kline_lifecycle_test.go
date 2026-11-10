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

func TestTransportFaultsEscalateAfterThree(t *testing.T) {
	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {})
	defer feed.Stop()

	feed.transportFaults.Add(1)
	feed.transportFaults.Add(1)
	if feed.NeedsRestart() {
		t.Fatal("two consecutive faults must not restart the feed")
	}
	feed.transportFaults.Add(1)
	if !feed.NeedsRestart() {
		t.Fatal("three consecutive faults must restart the feed")
	}
}

func TestFrameResetsFaults(t *testing.T) {
	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {})
	defer feed.Stop()

	feed.transportFaults.Store(2)
	feed.noteFrame()
	if got := feed.transportFaults.Load(); got != 0 {
		t.Fatalf("faults after a frame = %d, want 0", got)
	}
	if feed.NeedsRestart() {
		t.Fatal("a delivered frame must clear the restart verdict")
	}
}

func TestQuietSocketDoesNotReconnect(t *testing.T) {
	var connections atomic.Int64
	server, _ := newMarketWSServer(t, func(_ *websocket.Conn, _ *http.Request, _ int) {
		connections.Add(1)
		<-time.After(300 * time.Millisecond)
	})

	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {})
	feed.wsBasePath = wsServerURL(server)
	if err := feed.Start([]string{"BTC/USDT"}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer feed.Stop()

	time.Sleep(150 * time.Millisecond)
	if feed.NeedsRestart() {
		t.Fatal("a quiet but connected socket must not request a restart")
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("connections = %d, want 1 (no self-reconnect)", got)
	}
}

func TestZeroFramesIsConfigFailure(t *testing.T) {
	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {})
	defer feed.Stop()

	now := time.Now()
	feed.startedAt.Store(now.Add(-time.Second).UnixMilli())
	if feed.ConfigFault(now) {
		t.Fatal("inside the first-frame deadline a quiet feed is not a config failure")
	}
	feed.startedAt.Store(now.Add(-(klineFirstFrameDeadline + time.Second)).UnixMilli())
	if !feed.ConfigFault(now) {
		t.Fatal("past the first-frame deadline with zero frames must be a config failure")
	}
	feed.noteFrame()
	if feed.ConfigFault(now) {
		t.Fatal("a delivered frame clears the config fault")
	}
}

func TestTransportErrorIncrementsFaults(t *testing.T) {
	server, _ := newMarketWSServer(t, func(conn *websocket.Conn, _ *http.Request, _ int) {
		_ = conn.Close()
	})
	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {})
	feed.wsBasePath = wsServerURL(server)
	if err := feed.Start([]string{"BTC/USDT"}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer feed.Stop()

	if !waitFor(t, 2*time.Second, func() bool { return feed.transportFaults.Load() >= 1 }) {
		t.Fatal("a real transport error did not increment the consecutive-fault counter")
	}
}
