package binancesdk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

func newMarketWSServer(
	t *testing.T,
	handle func(conn *websocket.Conn, r *http.Request, connection int),
) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var connections atomic.Int64
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		handle(conn, r, int(connections.Add(1)))
	}))
	t.Cleanup(server.Close)
	return server, &connections
}

func wsServerURL(server *httptest.Server) string {
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func waitFor(t *testing.T, timeout time.Duration, check func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func aggTradeFrame(symbol string, price, qty string) string {
	return `{"stream":"` + strings.ToLower(strings.ReplaceAll(symbol, "/", "")) + `@trade","data":` +
		`{"e":"trade","s":"` + symbol + `","t":42,"p":"` + price + `","q":"` + qty + `","T":1793882096789,"m":false}}`
}

func TestAggTradeCallbackParsesWirePayload(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, buffers: NewBufferManager(10)}
	payload := []byte(`{"e":"aggTrade","s":"BTC/USDT","a":1,"p":"101.5","q":"2","T":1793882096789,"m":false}`)
	var decoded tradeEvent
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	c.tradeCallback("BTC/USDT")(payload)
	if got := c.buffers.GetOrCreate("BTC/USDT").Len(); got != 1 {
		t.Fatalf("buffered = %d, want 1 (messagesIn=%d dropped=%d)", got, c.messagesIn.Load(), c.tradesDropped.Load())
	}
}

func TestFuturesSubscribeIngestsTradeOverOwnedTransport(t *testing.T) {
	server, _ := newMarketWSServer(t, func(conn *websocket.Conn, _ *http.Request, _ int) {
		_, control, err := conn.ReadMessage()
		if err != nil { // SUBSCRIBE frame
			t.Logf("read control: %v", err)
			return
		}
		t.Logf("control: %s", control)
		if err := conn.WriteMessage(websocket.TextMessage, []byte(aggTradeFrame("BTC/USDT", "101.5", "2"))); err != nil {
			t.Errorf("write trade: %v", err)
		}
		<-time.After(300 * time.Millisecond)
	})
	c := NewSDKClient(streaming.ClientConfig{TradeBufferSize: 10, MarketType: "swap"})
	c.wsBasePath = wsServerURL(server)
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Subscribe(context.Background(), "BTC/USDT"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if !waitFor(t, 3*time.Second, func() bool {
		return c.buffers.GetOrCreate("BTC/USDT").Len() == 1
	}) {
		t.Fatalf("trade was not buffered: messagesIn=%d tradesIn=%d dropped=%d",
			c.messagesIn.Load(), c.tradesIn.Load(), c.tradesDropped.Load())
	}
	trades, err := c.GetTrades(context.Background(), "BTC/USDT")
	if err != nil || len(trades) != 1 {
		t.Fatalf("GetTrades = %d, %v", len(trades), err)
	}
	if trades[0].Price != 101.5 || trades[0].Amount != 2 || trades[0].Side != "buy" {
		t.Fatalf("trade = %+v", trades[0])
	}
}

func TestSpotSubscribeIngestsTrade(t *testing.T) {
	server, _ := newMarketWSServer(t, func(conn *websocket.Conn, _ *http.Request, _ int) {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(aggTradeFrame("ETH/USDT", "2000", "0.5")))
		<-time.After(300 * time.Millisecond)
	})
	c := NewSpotSDKClient(streaming.ClientConfig{TradeBufferSize: 10, MarketType: "spot"})
	c.wsBasePath = wsServerURL(server)
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Subscribe(context.Background(), "ETH/USDT"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if !waitFor(t, 3*time.Second, func() bool {
		return c.buffers.GetOrCreate("ETH/USDT").Len() == 1
	}) {
		t.Fatal("spot trade was not buffered")
	}
}

func TestKlineFeedIngestsVenueCandle(t *testing.T) {
	candlesCh := make(chan candle.Candle, 1)
	server, _ := newMarketWSServer(t, func(conn *websocket.Conn, r *http.Request, _ int) {
		if got := r.URL.Query().Get("streams"); !strings.Contains(got, "btcusdt@kline_5m") {
			t.Errorf("streams query = %q", got)
		}
		frame := `{"stream":"btcusdt@kline_5m","data":{"E":1793882097000,"k":{"t":1793881800000,"o":"100","h":"102","l":"99","c":"101","v":"10","V":"6","x":true}}}`
		_ = conn.WriteMessage(websocket.TextMessage, []byte(frame))
		<-time.After(300 * time.Millisecond)
	})
	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(symbol string, candle candle.Candle) {
		if symbol == "BTC/USDT" {
			candlesCh <- candle
		}
	})
	feed.wsBasePath = wsServerURL(server)
	t.Cleanup(feed.Stop)
	if err := feed.Start([]string{"BTC/USDT"}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case candle := <-candlesCh:
		if candle.Open != 100 || candle.Close != 101 || candle.Volume != 10 ||
			candle.TakerBuyVolume != 6 || candle.TakerSellVolume != 4 ||
			candle.KlineEventAt != 1793882097000 {
			t.Fatalf("candle = %+v", candle)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("venue candle was not delivered")
	}
}

func TestFuturesReconnectsAfterSocketTermination(t *testing.T) {
	server, connections := newMarketWSServer(t, func(conn *websocket.Conn, _ *http.Request, connection int) {
		if connection == 1 {
			_, _, _ = conn.ReadMessage()
			// Let the SUBSCRIBE write complete before terminating the socket.
			// Closing in the same instant races the write and turns the test
			// into a writer error instead of a reconnect scenario.
			time.Sleep(50 * time.Millisecond)
			_ = conn.Close()
			return
		}
		<-time.After(time.Second)
	})
	c := NewSDKClient(streaming.ClientConfig{TradeBufferSize: 10, MarketType: "swap"})
	c.wsBasePath = wsServerURL(server)
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Subscribe(context.Background(), "BTC/USDT"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if !waitFor(t, 8*time.Second, func() bool {
		return c.reconnectCount.Load() > 0 && connections.Load() >= 2
	}) {
		t.Fatalf("reconnect did not run: reconnects=%d connections=%d",
			c.reconnectCount.Load(), connections.Load())
	}
}

func TestAdapterCloseIsIdempotent(t *testing.T) {
	server, _ := newMarketWSServer(t, func(_ *websocket.Conn, _ *http.Request, _ int) {
		<-time.After(500 * time.Millisecond)
	})
	c := NewSDKClient(streaming.ClientConfig{TradeBufferSize: 10, MarketType: "swap"})
	c.wsBasePath = wsServerURL(server)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStreamNameUsesIndividualTradeStream(t *testing.T) {
	if got := streamName("BTC/USDT"); got != "btcusdt@trade" {
		t.Fatalf("streamName = %q, want btcusdt@trade", got)
	}
}

func TestTradeCallbackKeepsIndividualTradeID(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, buffers: NewBufferManager(10)}
	payload := []byte(`{"e":"trade","s":"BTC/USDT","t":987654321,"p":"101.5","q":"2","T":1793882096789,"m":false}`)
	c.tradeCallback("BTC/USDT")(payload)
	trades := c.buffers.GetOrCreate("BTC/USDT").Flush()
	if len(trades) != 1 {
		t.Fatalf("buffered trades = %d, want 1", len(trades))
	}
	if trades[0].ID != "987654321" {
		t.Fatalf("trade id = %q, want 987654321 — dedup depends on the venue's own id", trades[0].ID)
	}
}
