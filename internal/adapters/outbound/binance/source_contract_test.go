package binancesdk

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/ingestion/sourcetest"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/trade"
)

// binanceSourceHarness drives a real SDKClient against an in-process combined-stream
// server. No network is involved: the server plays the venue, the adapter owns its real
// parser, buffers, canonical errors and reconnect path.
type binanceSourceHarness struct {
	t       *testing.T
	source  *SDKClient
	rec     *sourcetest.ReconnectRecorder
	mu      sync.Mutex
	active  *activeBinanceConn
	arrived chan struct{} // buffered(1): a new connection was accepted
	dropped chan struct{} // test asks the current handler to close its conn
	stopped chan struct{} // whole harness teardown
}

type activeBinanceConn struct {
	conn *websocket.Conn
	drop chan struct{}
}

type binanceTestTradeFrame struct {
	Stream string                `json:"stream"`
	Data   binanceTestTradeEvent `json:"data"`
}

type binanceTestTradeEvent struct {
	Event        string `json:"e"`
	Symbol       string `json:"s"`
	AggregateID  int64  `json:"a"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	TradeTime    int64  `json:"T"`
	BuyerIsMaker bool   `json:"m"`
}

func newBinanceSourceHarness(t *testing.T) *binanceSourceHarness {
	t.Helper()
	h := &binanceSourceHarness{
		t:       t,
		rec:     sourcetest.NewReconnectRecorder(),
		arrived: make(chan struct{}, 1),
		dropped: make(chan struct{}),
		stopped: make(chan struct{}),
	}

	server, _ := newMarketWSServer(t, h.serveWS)
	// Exercise the combined-URL reconnect path (the one production uses when enabled),
	// so a reconnect reattaches every symbol on the handshake instead of paced frames.
	h.source = NewSDKClient(streaming.ClientConfig{
		MarketType:      "swap",
		TradeBufferSize: 8,
		GapRecovery:     h.rec,
	})
	h.source.wsBasePath = wsServerURL(server)
	t.Cleanup(func() { _ = h.source.Close() })
	t.Cleanup(func() { close(h.stopped) })
	return h
}

func (h *binanceSourceHarness) serveWS(conn *websocket.Conn, _ *http.Request, _ int) {
	active := &activeBinanceConn{conn: conn, drop: make(chan struct{})}
	h.mu.Lock()
	h.active = active
	h.mu.Unlock()
	select {
	case h.arrived <- struct{}{}:
	default:
	}

	// The adapter sends SUBSCRIBE/UNSUBSCRIBE frames on the client side; consume them so
	// its writes never block. The server itself only writes trade frames.
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	select {
	case <-active.drop:
	case <-h.stopped:
	case <-readDone:
	}
	_ = conn.Close()
}

func (h *binanceSourceHarness) current() *activeBinanceConn {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.active
}

func (h *binanceSourceHarness) deliver(t *testing.T, symbol string, trades ...trade.Trade) {
	t.Helper()
	before := h.source.ConnectionStats().TradesIn
	deadline := time.Now().Add(5 * time.Second)
	var active *activeBinanceConn
	for active = h.current(); active == nil; active = h.current() {
		if time.Now().After(deadline) {
			t.Fatal("no live Binance websocket to deliver on")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, tr := range trades {
		id, _ := strconv.ParseInt(tr.ID, 10, 64)
		frame, err := json.Marshal(binanceTestTradeFrame{
			// Combined-stream envelope: the adapter's router keys on `stream`, which is
			// exactly the name it registered for this symbol.
			Stream: strings.ToLower(strings.ReplaceAll(symbol, "/", "")) + "@trade",
			Data: binanceTestTradeEvent{
				Event:       "trade",
				Symbol:      symbol,
				AggregateID: id,
				Price:       strconv.FormatFloat(tr.Price, 'f', -1, 64),
				Quantity:    strconv.FormatFloat(tr.Amount, 'f', -1, 64),
				TradeTime:   tr.Timestamp,
			},
		})
		if err != nil {
			t.Fatalf("marshal trade frame: %v", err)
		}
		if err := active.conn.WriteMessage(websocket.TextMessage, frame); err != nil {
			t.Fatalf("write trade frame: %v", err)
		}
	}
	want := before + uint64(len(trades))
	for h.source.ConnectionStats().TradesIn < want {
		if time.Now().After(deadline) {
			t.Fatalf("trades never reached the adapter buffer: got %d, want %d", h.source.ConnectionStats().TradesIn, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (h *binanceSourceHarness) forceReconnect(t *testing.T, symbol string) bool {
	t.Helper()
	// Drain a stale arrival from the initial connect.
	select {
	case <-h.arrived:
	default:
	}
	active := h.current()
	if active == nil {
		return false
	}
	close(active.drop)
	select {
	case <-h.arrived:
		return true
	case <-time.After(15 * time.Second):
		t.Fatalf("Binance did not dial a replacement connection after %s was dropped", symbol)
		return false
	}
}

// TestBinanceSDKSourceContract runs the consumer-owned Source suite against the real
// Binance futures adapter, driven entirely by an in-process websocket server.
func TestBinanceSDKSourceContract(t *testing.T) {
	sourcetest.Run(t, func(t *testing.T) sourcetest.Harness {
		h := newBinanceSourceHarness(t)
		return sourcetest.Harness{
			Source:         h.source,
			Deliver:        h.deliver,
			Capacity:       8,
			ForceReconnect: h.forceReconnect,
			Reconnects:     h.rec.Batches,
		}
	})
}

// TestBinanceSDKBatchUnavailableIsCanonical proves the adapter translates its internal
// "combined URL disabled" signal onto the port vocabulary; ingestion must be able to
// route on it without importing this package.
func TestBinanceSDKBatchUnavailableIsCanonical(t *testing.T) {
	c := NewSDKClient(streaming.ClientConfig{MarketType: "swap", TradeBufferSize: 8})
	c.combinedURLConnectEnabled = false
	t.Cleanup(func() { _ = c.Close() })

	err := c.SubscribeAll([]string{"BTC/USDT"})
	if !IsBatchSubscribeUnavailable(err) {
		t.Fatalf("SubscribeAll = %v, want the batch-unavailable signal", err)
	}
	if !errors.Is(err, streaming.ErrBatchUnsupported) {
		t.Fatalf("SubscribeAll = %v does not match the canonical ErrBatchUnsupported", err)
	}
}
