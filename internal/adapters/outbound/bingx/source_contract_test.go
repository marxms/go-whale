package bingx

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/ingestion/sourcetest"
	"github.com/marxms/go-whale/internal/trade"
)

// bingxSourceHarness drives a real BingXClient against an in-process market server. The
// adapter owns its real parser, buffers, canonical errors, heartbeat and reconnect
// path; the server only accepts connections, consumes the client's control frames and
// receives the trade frames the harness injects.
type bingxSourceHarness struct {
	t       *testing.T
	source  *BingXClient
	rec     *sourcetest.ReconnectRecorder
	mu      sync.Mutex
	active  *websocket.Conn
	stopped chan struct{}
}

type bingxTestFrame struct {
	DataType string           `json:"dataType"`
	Data     []bingxTestTrade `json:"data"`
}

type bingxTestTrade struct {
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	Time         int64  `json:"T"`
	BuyerIsMaker bool   `json:"m"`
	ID           int64  `json:"t"`
}

func newBingXSourceHarness(t *testing.T) *bingxSourceHarness {
	t.Helper()
	h := &bingxSourceHarness{
		t:       t,
		rec:     sourcetest.NewReconnectRecorder(),
		stopped: make(chan struct{}),
	}

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		h.mu.Lock()
		h.active = conn
		h.mu.Unlock()

		// Consume SUBSCRIBE/UNSUBSCRIBE frames so the client's writes never block.
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
		case <-h.stopped:
		case <-readDone:
		}
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	h.source = NewBingXClient(ClientConfig{
		WSURL:           wsURL,
		ExchangeID:      "bingx",
		MarketType:      "swap",
		TradeBufferSize: 8,
		GapRecovery:     h.rec,
	})
	t.Cleanup(func() { _ = h.source.Close() })
	t.Cleanup(func() { close(h.stopped) })

	if err := h.source.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return h
}

func (h *bingxSourceHarness) current() *websocket.Conn {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.active
}

func (h *bingxSourceHarness) deliver(t *testing.T, symbol string, trades ...trade.Trade) {
	t.Helper()
	before := h.source.ConnectionStats().TradesIn

	deadline := time.Now().Add(5 * time.Second)
	var conn *websocket.Conn
	for conn = h.current(); conn == nil; conn = h.current() {
		if time.Now().After(deadline) {
			t.Fatal("no live BingX websocket to deliver on")
		}
		time.Sleep(5 * time.Millisecond)
	}

	data := make([]bingxTestTrade, 0, len(trades))
	for _, tr := range trades {
		id, _ := strconv.ParseInt(tr.ID, 10, 64)
		data = append(data, bingxTestTrade{
			Price:    strconv.FormatFloat(tr.Price, 'f', -1, 64),
			Quantity: strconv.FormatFloat(tr.Amount, 'f', -1, 64),
			Time:     tr.Timestamp,
			ID:       id,
		})
	}
	frame, err := json.Marshal(bingxTestFrame{
		DataType: strings.ReplaceAll(symbol, "/", "-") + "@trade",
		Data:     data,
	})
	if err != nil {
		t.Fatalf("marshal trade frame: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
		t.Fatalf("write trade frame: %v", err)
	}

	want := before + uint64(len(trades))
	for h.source.ConnectionStats().TradesIn < want {
		if time.Now().After(deadline) {
			t.Fatalf("trades never reached the adapter buffer: got %d, want %d", h.source.ConnectionStats().TradesIn, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (h *bingxSourceHarness) forceReconnect(t *testing.T, _ string) bool {
	t.Helper()
	conn := h.current()
	if conn == nil {
		return false
	}
	// Closing the server end is a real transport drop from the adapter's point of view:
	// readMessages fails, handleDisconnect runs, and reconnect resubscribes every symbol.
	if err := conn.Close(); err != nil {
		t.Logf("close server conn: %v", err)
	}
	return true
}

// TestBingXSourceContract runs the consumer-owned Source suite against the real BingX
// adapter, driven entirely by an in-process websocket server.
func TestBingXSourceContract(t *testing.T) {
	sourcetest.Run(t, func(t *testing.T) sourcetest.Harness {
		h := newBingXSourceHarness(t)
		return sourcetest.Harness{
			Source:         h.source,
			Deliver:        h.deliver,
			Capacity:       8,
			ForceReconnect: h.forceReconnect,
			Reconnects:     h.rec.Batches,
		}
	})
}

// bingxHandshakeBodySpy proves the response body is actually released. gorilla returns a
// live *http.Response on success AND on ErrBadHandshake; the body must be closed on both.
type bingxHandshakeBodySpy struct{ closed bool }

func (s *bingxHandshakeBodySpy) Read([]byte) (int, error) { return 0, io.EOF }
func (s *bingxHandshakeBodySpy) Close() error             { s.closed = true; return nil }

func TestBingXCloseHandshakeResponseClosesBody(t *testing.T) {
	body := &bingxHandshakeBodySpy{}
	closeHandshakeResponse(&http.Response{Body: body})
	if !body.closed {
		t.Fatal("handshake response body was not closed")
	}
	closeHandshakeResponse(nil)
	closeHandshakeResponse(&http.Response{})
}

// TestBingXConnectContextOwnsConnectionGoroutines proves the connection-scoped context
// inherits the CONNECT caller's context. Before the fix it was derived from the client
// lifetime context, so canceling the caller left the connection goroutines running.
func TestBingXConnectContextOwnsConnectionGoroutines(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	client := NewBingXClient(ClientConfig{
		WSURL:      "ws" + strings.TrimPrefix(server.URL, "http"),
		ExchangeID: "bingx",
		MarketType: "swap",
	})
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	client.connMu.RLock()
	connCtx := client.connCtx
	client.connMu.RUnlock()
	if connCtx == nil {
		t.Fatal("Connect left connCtx nil")
	}
	select {
	case <-connCtx.Done():
		t.Fatal("connCtx was done before the caller canceled")
	default:
	}

	cancel()
	select {
	case <-connCtx.Done():
		// The caller's cancellation reached the connection-scoped context.
	case <-time.After(time.Second):
		t.Fatal("canceling the Connect context did not tear down connCtx")
	}
}
