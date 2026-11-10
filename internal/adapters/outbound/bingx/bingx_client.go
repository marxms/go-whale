// Package bingx adapts the BingX WebSocket market-data API to the streaming client
// contracts.
package bingx

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/platform/netdiag"
	"github.com/marxms/go-whale/internal/trade"
)

// BingX endpoint, buffer and timing defaults.
const (
	// BingXSpotWSEndpoint and BingXSwapWSEndpoint are the public market-data hosts.
	BingXSpotWSEndpoint = "wss://open-api-ws.bingx.com/market"
	BingXSwapWSEndpoint = "wss://open-api-swap.bingx.com/swap-market" //nolint:gosec // G101: public WebSocket endpoint, not a credential.

	// DefaultTradeBufferSize is the per-symbol trade buffer used when the
	// configured TradeBufferSize is zero.
	DefaultTradeBufferSize = 5000

	// InitialReconnectDelay sets the first reconnect wait; MaxReconnectDelay caps it,
	// ReconnectBackoffRate multiplies it, and MaxReconnectAttempts zero means infinite
	// retries.
	InitialReconnectDelay = 1 * time.Second
	MaxReconnectDelay     = 30 * time.Second
	ReconnectBackoffRate  = 2.0
	MaxReconnectAttempts  = 0 // 0 = infinite

	// HeartbeatTimeout is how long without an inbound "Ping" counts as a dead
	// connection. BingX documents a server ping every 5 seconds; 90 seconds is the
	// operational margin (18 missed pings), and it is the SAME threshold HealthCheck and
	// the reconnect path use. HeartbeatCheckPeriod is how often the client checks.
	HeartbeatTimeout     = 90 * time.Second
	HeartbeatCheckPeriod = 10 * time.Second
	// heartbeatWarningAfter starts the warning log before the timeout. Derived from
	// HeartbeatTimeout so the warning cannot drift from the reconnect threshold.
	heartbeatWarningAfter = HeartbeatTimeout * 2 / 3

	// BingXWriteDeadline prevents a blocked write from holding writeMu forever.
	BingXWriteDeadline = 10 * time.Second

	// BingXReadDeadline is extended on every successful read.
	BingXReadDeadline = 5 * time.Minute
)

// BingXClient implements StreamingClient for the BingX exchange.
//
// Every connection-tied goroutine is scoped to connCtx and exits on disconnect.
//
// Deadline and Close errors on the websocket are discarded EXPLICITLY throughout this
// file. SetRead/WriteDeadline only fails on an already-closed connection, and the Read or
// Write that follows surfaces that as a real error which IS handled; Close during teardown
// is unactionable by definition. The `_ =` makes the decision visible instead of leaving a
// reader unsure whether the error was considered or forgotten.
type BingXClient struct {
	config          ClientConfig
	log             *slog.Logger
	tradeBufferSize int

	conn      *websocket.Conn
	connMu    sync.RWMutex
	connected bool

	// Connection-scoped context - canceled on EVERY disconnect. All connection-tied
	// goroutines (readMessages, heartbeatMonitor, pingTicker) use this context and exit
	// cleanly when canceled.
	connCtx    context.Context
	connCancel context.CancelFunc
	connWg     sync.WaitGroup // Only tracks connection-scoped goroutines

	// Internal per-symbol trade buffering
	tradeBuffers  map[string][]trade.Trade
	subscriptions map[string]bool // symbol -> subscribed
	subsMu        sync.RWMutex

	// Write mutex to prevent concurrent writes on the WebSocket
	writeMu sync.Mutex

	// Client lifecycle context - only canceled on Close()
	ctx    context.Context
	cancel context.CancelFunc

	// Reconnection state
	reconnecting   bool
	reconnectCount int
	reconnectDelay time.Duration
	reconnectMu    sync.RWMutex

	// Heartbeat tracking - CRITICAL for detecting silent disconnections
	// lastPong is updated every time we receive a Ping from the server
	// If lastPong is older than HeartbeatTimeout, the connection is considered dead
	lastPong   time.Time
	lastPongMu sync.RWMutex

	// Gap recovery support.
	// disconnectTime stores a time.Time recording when the last disconnect occurred.
	// It is an atomic.Value (parity with the Binance SDK clients) because it is written
	// from handleDisconnect (under connMu) and read from triggerGapRecovery/handleDisconnect
	// without holding connMu — a plain time.Time (24 bytes) is not atomically readable, so
	// the cross-lock access raced. atomic.Value gives a lock-free, race-free seam.
	disconnectTime   atomic.Value     // stores time.Time
	recoveryCallback RecoveryCallback // DEPRECATED: use gapRecovery
	gapRecovery      GapRecoveryQueue // bounded worker pool

	// Monitoring counters (lock-free).
	messagesIn atomic.Uint64
	tradesIn   atomic.Uint64

	// Overflow counter for dropped trades.
	tradesDropped atomic.Uint64

	// netdiag correlates a disconnect with drops on other venues; the composition root
	// injects the process-wide instance.
	netdiag *netdiag.Correlator
}

// NewBingXClient creates new BingX streaming client.
func NewBingXClient(config ClientConfig, logs ...*slog.Logger) *BingXClient {
	// Set WSURL based on market type if not provided
	if config.WSURL == "" {
		if config.MarketType == "swap" {
			config.WSURL = BingXSwapWSEndpoint
		} else {
			config.WSURL = BingXSpotWSEndpoint
		}
	}

	// BingX: protocol-level ping as a safety net. The server already sends an application
	// "Ping" every 5 s (text on swap, JSON on spot), which is the primary heartbeat; this
	// low-frequency ping only prevents write starvation.
	if config.PingInterval == 0 {
		config.PingInterval = 1800 // 30 minutes, matching the reference implementation
	}

	bufSize := config.TradeBufferSize
	if bufSize == 0 {
		bufSize = DefaultTradeBufferSize
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &BingXClient{
		config:           config,
		log:              logger.OrDiscard(logs...),
		tradeBufferSize:  bufSize,
		tradeBuffers:     make(map[string][]trade.Trade),
		subscriptions:    make(map[string]bool),
		ctx:              ctx,
		cancel:           cancel,
		reconnectDelay:   InitialReconnectDelay,
		recoveryCallback: config.RecoveryCallback,
		gapRecovery:      config.GapRecovery,
		netdiag:          netdiag.New(),
	}

	return client
}

// SetNetdiag injects the cross-venue disconnect correlator.
func (c *BingXClient) SetNetdiag(correlator *netdiag.Correlator) { c.netdiag = correlator }

// closed reports whether Close canceled the client lifecycle. A zero-value fixture
// built in a test without the constructor has a nil ctx; treat that as open rather than
// panicking on the nil interface.
func (c *BingXClient) closed() bool { return c.ctx != nil && c.ctx.Err() != nil }

// closeHandshakeResponse releases the HTTP response that gorilla returns with every websocket
// handshake. It is non-nil on success and on ErrBadHandshake, and the library never closes it,
// so both paths own a body that must be released.
func closeHandshakeResponse(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}

// Connect establishes WebSocket connection.
func (c *BingXClient) Connect(ctx context.Context) error {
	return c.doConnect(ctx)
}

// doConnect performs the actual connection with a connection-scoped lifecycle: all
// goroutines use connCtx and are cleaned up on disconnect.
//
// resubscribeAll is NOT called here. doConnect holds connMu, and sendSubscribe takes
// connMu.RLock — Lock then RLock on the same goroutine deadlocks. It is called from
// reconnect after doConnect returns and the lock is released.
func (c *BingXClient) doConnect(ctx context.Context) error {
	c.connMu.Lock()

	// Step 1: Cancel previous connection context (stops ALL old goroutines)
	if c.connCancel != nil {
		c.connCancel()
	}

	// Step 2: Wait for old connection goroutines to exit cleanly
	// Release lock while waiting (goroutines may need it to exit)
	c.connMu.Unlock()
	c.connWg.Wait()
	c.connMu.Lock()
	defer c.connMu.Unlock()

	// Step 3: Close existing connection if still open
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}

	// Step 4: Create new connection-scoped context. It inherits the caller's context so a
	// canceled dial (shutdown, caller timeout) tears down the connection goroutines too.
	connCtx, connCancel := context.WithCancel(ctx)
	c.connCtx, c.connCancel = connCtx, connCancel

	logger.OrDiscard(c.log).Warn("bingx: connecting websocket", "url", c.config.WSURL)

	// Create a LOCAL Dialer (do not mutate the global DefaultDialer).
	// Compression disabled — reduces CPU overhead and avoids gorilla/websocket
	// compression-related bugs.
	// Explicit net.Dialer with TCP KeepAlive (parity with Binance).
	dialer := &websocket.Dialer{
		HandshakeTimeout:  45 * time.Second,
		EnableCompression: false,
		NetDialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	conn, resp, err := dialer.DialContext(ctx, c.config.WSURL, nil)
	closeHandshakeResponse(resp)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.conn = conn
	c.connected = true

	// Set the initial read deadline.
	_ = conn.SetReadDeadline(time.Now().Add(BingXReadDeadline))

	// CRITICAL: Register PongHandler for protocol-level pong responses
	// Note: BingX also sends text "Ping" which we handle separately in handleMessage
	conn.SetPongHandler(func(_ string) error {
		c.lastPongMu.Lock()
		c.lastPong = time.Now()
		c.lastPongMu.Unlock()

		// Extend read deadline
		c.connMu.RLock()
		if c.conn != nil {
			_ = c.conn.SetReadDeadline(time.Now().Add(BingXReadDeadline))
		}
		c.connMu.RUnlock()

		logger.OrDiscard(c.log).Debug("bingx: protocol pong received; heartbeat updated")
		return nil
	})

	// Reset the pong watchdog on every SUCCESSFUL (re)connect, co-located with
	// `c.connected = true` above (the single connect/reconnect success site). The freshly
	// established connection IS the implicit first heartbeat, so seed lastPong = now.
	// Without this, HealthCheck() (false when time.Since(lastPong) > HeartbeatTimeout) would
	// keep returning Healthy=false for up to HeartbeatTimeout after each reconnect even though
	// Connected=true, flipping the expected-scope /health endpoint to 503 (a false alarm) after
	// every BingX reconnect. reconnect() also re-stamps lastPong before resubscribing as
	// defense in depth.
	c.lastPongMu.Lock()
	c.lastPong = time.Now()
	c.lastPongMu.Unlock()

	logger.OrDiscard(c.log).Warn("bingx: connected to websocket")

	// Start connection-scoped goroutines (all use connCtx)
	c.connWg.Add(3)
	go c.readMessages(connCtx)
	go c.startHeartbeatMonitor(connCtx)
	go c.startPingTicker(connCtx)

	// Do NOT call resubscribeAll() here! doConnect() holds connMu.Lock via defer.
	// resubscribeAll() -> sendSubscribe() -> connMu.RLock() creates Lock() -> RLock() on the
	// same goroutine = DEADLOCK in Go. resubscribeAll() is called from reconnect() AFTER
	// doConnect() returns and the lock is released.

	return nil
}

// Subscribe to symbol trade stream (implements StreamingClient).
func (c *BingXClient) Subscribe(ctx context.Context, symbol string) error {
	// Canonical source contract: a closed client accepts no work, and an already
	// canceled caller must see the cancellation rather than a successful attach.
	if c.closed() {
		return fmt.Errorf("%w: bingx subscribe %s", streaming.ErrClosed, symbol)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("bingx subscribe %s: %w", symbol, err)
	}
	c.subsMu.Lock()

	// Check if already subscribed
	if c.subscriptions[symbol] {
		c.subsMu.Unlock()
		logger.OrDiscard(c.log).Debug("bingx: symbol already subscribed", "symbol", symbol)
		return nil
	}

	// Mark as subscribed and initialize buffer
	c.subscriptions[symbol] = true
	c.tradeBuffers[symbol] = make([]trade.Trade, 0, c.tradeBufferSize)
	c.subsMu.Unlock()

	// Send subscribe message
	if err := c.sendSubscribe(symbol); err != nil {
		c.subsMu.Lock()
		delete(c.subscriptions, symbol)
		delete(c.tradeBuffers, symbol)
		c.subsMu.Unlock()
		return fmt.Errorf("failed to subscribe %s: %w", symbol, err)
	}

	logger.OrDiscard(c.log).Debug("bingx: subscribed", "symbol", symbol)
	return nil
}

// Unsubscribe from symbol trade stream (implements StreamingClient).
func (c *BingXClient) Unsubscribe(symbol string) error {
	if c.closed() {
		return nil
	}
	c.subsMu.Lock()

	if !c.subscriptions[symbol] {
		c.subsMu.Unlock()
		return nil
	}

	delete(c.subscriptions, symbol)
	delete(c.tradeBuffers, symbol)
	c.subsMu.Unlock()

	// Send unsubscribe message
	return c.sendUnsubscribe(symbol)
}

// GetBufferFillRatio returns the fill ratio of the buffer for a symbol (0.0 to 1.0).
func (c *BingXClient) GetBufferFillRatio(symbol string) float64 {
	c.subsMu.RLock()
	defer c.subsMu.RUnlock()

	buffer, ok := c.tradeBuffers[symbol]
	if !ok {
		return 0.0
	}

	return float64(len(buffer)) / float64(c.tradeBufferSize)
}

// GetTrades returns buffered trades and clears the buffer (implements StreamingClient).
func (c *BingXClient) GetTrades(ctx context.Context, symbol string) ([]trade.Trade, error) {
	if c.closed() {
		return nil, fmt.Errorf("%w: bingx get trades %s", streaming.ErrClosed, symbol)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.subsMu.RLock()
	if !c.subscriptions[symbol] {
		c.subsMu.RUnlock()
		return nil, fmt.Errorf("%w: bingx %s", streaming.ErrNotSubscribed, symbol)
	}
	c.subsMu.RUnlock()

	c.subsMu.Lock()
	buf := c.tradeBuffers[symbol]
	if len(buf) == 0 {
		c.subsMu.Unlock()
		return nil, nil
	}
	// Return an ISOLATED COPY. Returning the aliased slice header (same backing array) and
	// resetting the map slot to buf[:0] lets the next handleTradeMessage append overwrite
	// buf[0..] — the very trades the consumer is iterating (data race + silent
	// corruption/duplication of BingX volume/CVD). Copy here, exactly like the Binance
	// buffer.go Flush already does, so the consumer owns its slice.
	out := make([]trade.Trade, len(buf))
	copy(out, buf)
	c.tradeBuffers[symbol] = buf[:0]
	c.subsMu.Unlock()

	logger.OrDiscard(c.log).Debug("bingx: get trades",
		"exchange", c.config.ExchangeID,
		"market_type", c.config.MarketType,
		"trades", len(out),
		"symbol", symbol)
	return out, nil
}

// sendSubscribe sends the subscribe message to the WebSocket. Every write operation sets a
// write deadline.
func (c *BingXClient) sendSubscribe(symbol string) error {
	c.connMu.RLock()
	if !c.connected || c.conn == nil {
		c.connMu.RUnlock()
		return fmt.Errorf("%w: bingx subscribe frame", streaming.ErrNotConnected)
	}
	conn := c.conn
	c.connMu.RUnlock()

	// Convert format: BTC/USDT -> BTC-USDT
	bingxSymbol := strings.ReplaceAll(symbol, "/", "-")

	logger.OrDiscard(c.log).Debug("bingx: subscribe symbol translated",
		"symbol", symbol, "bingx_symbol", bingxSymbol, "market_type", c.config.MarketType)

	// BingX subscribe format
	msg := map[string]interface{}{
		"id":       fmt.Sprintf("sub_%s_%d", bingxSymbol, time.Now().Unix()),
		"reqType":  "sub",
		"dataType": fmt.Sprintf("%s@trade", bingxSymbol),
	}

	logger.OrDiscard(c.log).Debug("bingx: sending subscribe", "message", msg)

	c.writeMu.Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(BingXWriteDeadline))
	err := conn.WriteJSON(msg)
	_ = conn.SetWriteDeadline(time.Time{})
	c.writeMu.Unlock()
	return err
}

// sendUnsubscribe sends unsubscribe message to WebSocket.
func (c *BingXClient) sendUnsubscribe(symbol string) error {
	c.connMu.RLock()
	if !c.connected || c.conn == nil {
		c.connMu.RUnlock()
		return nil // Already disconnected
	}
	conn := c.conn
	c.connMu.RUnlock()

	// Convert format: BTC/USDT -> BTC-USDT
	bingxSymbol := strings.ReplaceAll(symbol, "/", "-")

	msg := map[string]interface{}{
		"id":       fmt.Sprintf("unsub_%s_%d", bingxSymbol, time.Now().Unix()),
		"reqType":  "unsub",
		"dataType": fmt.Sprintf("%s@trade", bingxSymbol),
	}

	c.writeMu.Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(BingXWriteDeadline))
	err := conn.WriteJSON(msg)
	_ = conn.SetWriteDeadline(time.Time{})
	c.writeMu.Unlock()
	return err
}

// resubscribeAll resubscribes to all symbols after reconnection.
func (c *BingXClient) resubscribeAll() {
	c.subsMu.RLock()
	symbols := make([]string, 0, len(c.subscriptions))
	for symbol := range c.subscriptions {
		symbols = append(symbols, symbol)
	}
	c.subsMu.RUnlock()

	if len(symbols) == 0 {
		logger.OrDiscard(c.log).Debug("bingx: no symbols to resubscribe")
		return
	}

	logger.OrDiscard(c.log).Warn("bingx: resubscribing", "symbols", len(symbols))

	successful := 0
	failed := 0

	for _, symbol := range symbols {
		if err := c.sendSubscribe(symbol); err != nil {
			logger.OrDiscard(c.log).Error("bingx: resubscription failed", "symbol", symbol, "error", err)
			failed++
		} else {
			logger.OrDiscard(c.log).Debug("bingx: resubscribed", "symbol", symbol)
			successful++
		}
	}

	if failed > 0 {
		logger.OrDiscard(c.log).Warn("bingx: resubscription complete with failures",
			"successful", successful, "failed", failed, "total", len(symbols))
	} else {
		logger.OrDiscard(c.log).Warn("bingx: resubscription complete", "successful", successful)
	}
}

// startPingTicker sends periodic pings to keep the connection alive. It uses connCtx to exit
// cleanly on disconnect (no goroutine leak).
func (c *BingXClient) startPingTicker(connCtx context.Context) {
	defer c.connWg.Done()

	if c.config.PingInterval <= 0 {
		logger.OrDiscard(c.log).Debug("bingx: ping ticker disabled")
		return
	}

	logger.OrDiscard(c.log).Warn("bingx: ping ticker started", "interval_seconds", c.config.PingInterval)

	// Send an immediate ping on startup before the first ticker fires.
	// Without this, the 120s heartbeat timeout window starts immediately after reconnect
	// but no pong arrives for PingInterval seconds (BingX server may not ping proactively),
	// causing a spurious HEARTBEAT TIMEOUT → reconnect cascade.
	select {
	case <-connCtx.Done():
		return
	default:
		c.sendPing()
	}

	ticker := time.NewTicker(time.Duration(c.config.PingInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-connCtx.Done():
			return
		case <-ticker.C:
			c.sendPing()
		}
	}
}

// sendPing sends a WebSocket protocol-level ping (not JSON). It uses WriteControl for
// concurrent safety; no writeMu is needed.
func (c *BingXClient) sendPing() {
	c.connMu.RLock()
	if !c.connected || c.conn == nil {
		c.connMu.RUnlock()
		return
	}
	conn := c.conn
	c.connMu.RUnlock()

	err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(BingXWriteDeadline))
	if err != nil {
		logger.OrDiscard(c.log).Error("bingx: ping failed; triggering reconnection", "error", err)
		go c.handleDisconnect("ping_send_failure")
		return
	}

	logger.OrDiscard(c.log).Debug("bingx: ping sent")
}

// sendPongText sends a PONG response to the server's text "Ping" message (BingX Swap
// protocol). It updates lastPong AFTER a successful send (not before): if the pong send
// fails, the connection is broken, and updating lastPong before would mask the failure and
// delay reconnection.
func (c *BingXClient) sendPongText() {
	c.connMu.RLock()
	if !c.connected || c.conn == nil {
		c.connMu.RUnlock()
		return
	}
	conn := c.conn
	c.connMu.RUnlock()

	// Write deadline on the pong response.
	c.writeMu.Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(BingXWriteDeadline))
	err := conn.WriteMessage(websocket.TextMessage, []byte("Pong"))
	_ = conn.SetWriteDeadline(time.Time{})
	c.writeMu.Unlock()

	if err != nil {
		// Pong failure = connection is broken
		logger.OrDiscard(c.log).Error("bingx: text pong failed; triggering reconnection", "error", err)
		go c.handleDisconnect("pong_text_send_failure")
		return
	}

	// Update lastPong ONLY after a successful send.
	c.lastPongMu.Lock()
	c.lastPong = time.Now()
	c.lastPongMu.Unlock()

	logger.OrDiscard(c.log).Debug("bingx: text pong sent; heartbeat updated")
}

// sendPongJSON sends PONG response to server's JSON ping message (BingX Spot protocol)
// Spot uses: {"ping": uuid, "time": timestamp} → {"pong": uuid, "time": timestamp}.
// On failure the caller resets the client for a clean reconnection.
func (c *BingXClient) sendPongJSON(pingID, pingTime interface{}) {
	c.connMu.RLock()
	if !c.connected || c.conn == nil {
		c.connMu.RUnlock()
		return
	}
	conn := c.conn
	c.connMu.RUnlock()

	pongMsg := map[string]interface{}{
		"pong": pingID,
		"time": pingTime,
	}

	// Write deadline on the pong response.
	c.writeMu.Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(BingXWriteDeadline))
	err := conn.WriteJSON(pongMsg)
	_ = conn.SetWriteDeadline(time.Time{})
	c.writeMu.Unlock()

	if err != nil {
		// Pong failure = connection is broken
		logger.OrDiscard(c.log).Error("bingx: JSON pong failed; triggering reconnection", "error", err)
		go c.handleDisconnect("pong_json_send_failure")
		return
	}

	// Update lastPong ONLY after successful send
	c.lastPongMu.Lock()
	c.lastPong = time.Now()
	c.lastPongMu.Unlock()

	logger.OrDiscard(c.log).Debug("bingx: JSON pong sent; heartbeat updated")
}

// readMessages reads messages from the WebSocket. No busy-loop: it exits on disconnect or
// context cancel. It uses connCtx, preventing duplicate readers across reconnections.
func (c *BingXClient) readMessages(connCtx context.Context) {
	defer c.connWg.Done()

	for {
		select {
		case <-connCtx.Done():
			logger.OrDiscard(c.log).Debug("bingx: read messages exiting; connection context canceled")
			return
		default:
		}

		c.connMu.RLock()
		conn := c.conn
		c.connMu.RUnlock()

		if conn == nil {
			// Connection gone - exit immediately (no busy-loop).
			logger.OrDiscard(c.log).Debug("bingx: read messages exiting; connection is nil")
			return
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-connCtx.Done():
				logger.OrDiscard(c.log).Debug("bingx: read messages exiting; context canceled during read")
				return
			default:
			}
			logger.OrDiscard(c.log).Error("bingx: error reading message", "error", err)
			c.handleDisconnect("read_loop_error")
			return
		}

		// Extend read deadline on every successful read
		_ = conn.SetReadDeadline(time.Now().Add(BingXReadDeadline))

		c.messagesIn.Add(1)
		c.handleMessage(message)
	}
}

// handleMessage processes incoming WebSocket message.
func (c *BingXClient) handleMessage(data []byte) {
	// Check if data is GZIP compressed (magic number 0x1f 0x8b)
	if len(data) > 2 && data[0] == 0x1f && data[1] == 0x8b {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			logger.OrDiscard(c.log).Warn("bingx: gzip decompression failed", "error", err)
			return
		}
		defer func() { _ = reader.Close() }()

		decompressed, err := io.ReadAll(reader)
		if err != nil {
			logger.OrDiscard(c.log).Warn("bingx: reading decompressed data failed", "error", err)
			return
		}
		data = decompressed
	}

	// Handle text PING message from server (BingX Swap protocol: "Ping" → "Pong")
	if string(data) == "Ping" || string(data) == "ping" {
		c.sendPongText()
		return
	}

	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		logger.OrDiscard(c.log).Debug("bingx: non-JSON message ignored", "sample", string(data[:min(len(data), 50)]))
		return
	}

	// Handle the JSON PING message (BingX Spot protocol:
	// {ping:..., time:...} → {pong:..., time:...}).
	if pingID, hasPing := msg["ping"]; hasPing {
		pingTime := msg["time"] // may be nil, that's OK
		c.sendPongJSON(pingID, pingTime)
		return
	}

	// Handle pong response (when WE sent a ping and server responds)
	if pong, ok := msg["pong"]; ok {
		logger.OrDiscard(c.log).Debug("bingx: pong received", "pong", pong)
		return
	}

	// Handle trade data
	if dataType, ok := msg["dataType"].(string); ok && dataType != "" {
		c.handleTradeMessage(msg)
		return
	}

	// Handle subscription response
	if code, ok := msg["code"].(float64); ok {
		if code == 0 {
			logger.OrDiscard(c.log).Debug("bingx: subscription confirmed")
		} else {
			logger.OrDiscard(c.log).Warn("bingx: subscription error", "message", msg)
		}
	}
}

// handleTradeMessage parses and distributes trade data.
func (c *BingXClient) handleTradeMessage(msg map[string]interface{}) {
	dataType, _ := msg["dataType"].(string)

	// BingX SPOT returns a single object, SWAP returns an array
	var data []interface{}

	// Try as array first (SWAP)
	if dataArray, ok := msg["data"].([]interface{}); ok {
		data = dataArray
	} else if dataObj, ok := msg["data"].(map[string]interface{}); ok {
		// SPOT returns single object, convert to 1-element array
		data = []interface{}{dataObj}
	} else {
		logger.OrDiscard(c.log).Debug("bingx: data is neither array nor object", "type", fmt.Sprintf("%T", msg["data"]))
		return
	}

	if len(data) == 0 {
		return
	}

	// Extract symbol from dataType (e.g., "BTC-USDT@trade" -> "BTC-USDT")
	bingxSymbol := dataType
	if idx := len(dataType) - 6; idx > 0 && dataType[idx:] == "@trade" {
		bingxSymbol = dataType[:idx]
	}

	// Convert BTC-USDT -> BTC/USDT (internal format)
	symbol := strings.ReplaceAll(bingxSymbol, "-", "/")

	logger.OrDiscard(c.log).Debug("bingx: trades received",
		"exchange", c.config.ExchangeID,
		"market_type", c.config.MarketType,
		"trades", len(data),
		"symbol", symbol)

	c.subsMu.Lock()
	if !c.subscriptions[symbol] {
		c.subsMu.Unlock()
		logger.OrDiscard(c.log).Debug("bingx: symbol not subscribed; ignoring",
			"exchange", c.config.ExchangeID, "market_type", c.config.MarketType, "symbol", symbol)
		return
	}

	// Parse trades and add to buffer
	for _, item := range data {
		trade, err := c.parseTrade(symbol, item)
		if err != nil {
			logger.OrDiscard(c.log).Warn("bingx: trade parse failed", "error", err)
			continue
		}

		// Add to buffer (limit size)
		if len(c.tradeBuffers[symbol]) < c.tradeBufferSize {
			c.tradeBuffers[symbol] = append(c.tradeBuffers[symbol], trade)
			c.tradesIn.Add(1)
		} else {
			c.tradesDropped.Add(1)
			logger.OrDiscard(c.log).Warn("bingx: trade buffer full; dropping trade", "symbol", symbol)
		}
	}
	c.subsMu.Unlock()
}

// parseTrade parses BingX trade message.
func (c *BingXClient) parseTrade(symbol string, data interface{}) (trade.Trade, error) {
	tradeData, ok := data.(map[string]interface{})
	if !ok {
		return trade.Trade{}, fmt.Errorf("invalid trade data")
	}

	priceStr, _ := tradeData["p"].(string)
	qtyStr, _ := tradeData["q"].(string)
	timeFloat, _ := tradeData["T"].(float64)
	isBuyerMaker, _ := tradeData["m"].(bool)

	price, err := strconv.ParseFloat(priceStr, 64)
	if err != nil {
		return trade.Trade{}, fmt.Errorf("invalid price: %s", priceStr)
	}

	amount, err := strconv.ParseFloat(qtyStr, 64)
	if err != nil {
		return trade.Trade{}, fmt.Errorf("invalid amount: %s", qtyStr)
	}

	side := trade.Buy
	if isBuyerMaker {
		side = trade.Sell
	}

	// BingX sends trade ID ("t" field) on spot and inverse swap,
	// but NOT on linear swap. Extract if available.
	tradeID := ""
	if tVal, ok := tradeData["t"]; ok {
		switch v := tVal.(type) {
		case string:
			tradeID = v
		case float64:
			tradeID = fmt.Sprintf("%d", int64(v))
		}
	}

	timestamp := int64(timeFloat)

	// Generate a composite key for dedup when the exchange provides no trade ID.
	if tradeID == "" {
		tradeID = fmt.Sprintf("bingx:%s:%.8f:%.8f:%d", symbol, price, amount, int64(timeFloat))
	}

	return trade.Trade{
		ID:        tradeID, // composite key for linear swap if exchange provides no ID
		Symbol:    symbol,
		Price:     price,
		Amount:    amount,
		Side:      side,
		Timestamp: timestamp,
	}, nil
}

// cleanupOnError performs cleanup when an error occurs
// Clears stale data while preserving subscription state for resubscription.
func (c *BingXClient) cleanupOnError(err error) {
	logger.OrDiscard(c.log).Warn("bingx: cleanup initiated", "reason", err)

	c.subsMu.Lock()
	subscriptionCount := len(c.subscriptions)
	bufferCount := len(c.tradeBuffers)

	// Log buffered trades being discarded for monitoring.
	var totalBufferedTrades int
	for symbol, buffer := range c.tradeBuffers {
		if len(buffer) > 0 {
			totalBufferedTrades += len(buffer)
			logger.OrDiscard(c.log).Warn("bingx: discarding buffered trades due to disconnect",
				"trades", len(buffer), "symbol", symbol)
		}
	}

	// Clear trade buffers to prevent stale data — reset slices but preserve map keys.
	// Destroying the map would cause trade loss if handleTradeMessage runs before
	// buffers are recreated (BingX handles this via direct map access, but this is safer).
	for symbol := range c.tradeBuffers {
		c.tradeBuffers[symbol] = make([]trade.Trade, 0, c.tradeBufferSize)
	}
	// Ensure subscriptions without buffers get buffers (defensive)
	for symbol := range c.subscriptions {
		if _, exists := c.tradeBuffers[symbol]; !exists {
			c.tradeBuffers[symbol] = make([]trade.Trade, 0, c.tradeBufferSize)
		}
	}
	// IMPORTANT: We do NOT clear the subscriptions map
	// It's our "desired state" used for resubscription
	c.subsMu.Unlock()

	if totalBufferedTrades > 0 {
		logger.OrDiscard(c.log).Warn("bingx: cleanup complete",
			"discarded_trades", totalBufferedTrades,
			"cleared_buffers", bufferCount,
			"preserved_subscriptions", subscriptionCount)
	} else {
		logger.OrDiscard(c.log).Warn("bingx: cleanup complete",
			"cleared_buffers", bufferCount, "preserved_subscriptions", subscriptionCount)
	}
}

// handleDisconnect handles connection loss and triggers reconnection. It is the single entry
// point, with the connected check as guard.
func (c *BingXClient) handleDisconnect(trigger string) {
	logger.OrDiscard(c.log).Warn("bingx: disconnect triggered", "exchange", c.config.ExchangeID, "trigger", trigger)
	c.connMu.Lock()
	if !c.connected {
		c.connMu.Unlock()
		return // Already disconnected
	}

	c.connected = false
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}

	// Record disconnect time for gap recovery (atomic store — read lock-free elsewhere)
	c.disconnectTime.Store(time.Now())

	// Cancel connection context INSIDE lock - all connection goroutines will exit
	if c.connCancel != nil {
		c.connCancel()
	}
	c.connMu.Unlock()

	// Cross-exchange disconnect correlation (netdiag): reaching here means a genuine
	// disconnect transition (the guard above returns early if already disconnected).
	// Recording it lets a concurrent Binance drop within the window be classified as
	// a local-network event rather than exchange-specific. Called outside connMu.
	if c.netdiag != nil {
		c.netdiag.Record(c.config.ExchangeID, time.Now().UnixMilli())
	}

	// Get diagnostic information
	c.subsMu.RLock()
	symbolCount := len(c.subscriptions)
	c.subsMu.RUnlock()

	c.reconnectMu.RLock()
	reconnectAttempts := c.reconnectCount
	c.reconnectMu.RUnlock()

	logger.OrDiscard(c.log).Warn("bingx: websocket disconnected",
		"active_subscriptions", symbolCount, "reconnect_attempts", reconnectAttempts)

	// Cleanup stale data
	c.cleanupOnError(fmt.Errorf("connection lost"))

	// Start reconnection
	go c.reconnect()
}

// reconnect implements exponential backoff reconnection. resubscribeAll is called HERE
// (after doConnect returns), not inside doConnect where it would deadlock.
func (c *BingXClient) reconnect() {
	defer func() {
		if r := recover(); r != nil {
			logger.OrDiscard(c.log).Error("bingx: panic in reconnect goroutine", "panic", r)
		}
	}()

	c.reconnectMu.Lock()
	if c.reconnecting {
		c.reconnectMu.Unlock()
		logger.OrDiscard(c.log).Warn("bingx: reconnection already in progress; skipping duplicate attempt")
		return
	}
	c.reconnecting = true
	c.reconnectMu.Unlock()

	startTime := time.Now()
	logger.OrDiscard(c.log).Warn("bingx: reconnection started",
		"max_attempts", MaxReconnectAttempts, "initial_delay", InitialReconnectDelay)

	// Enqueue gap recovery on EVERY non-clean exit from this reconnect run, not only the
	// success path. cleanShutdown is set true exactly on the ctx.Done() return paths
	// (operator-driven Close); on every other exit — successful reconnect, a partial-
	// resubscribe early return, MaxReconnectAttempts exhaustion (today gated by
	// MaxReconnectAttempts==0/infinite, but a fragile silent-data-loss invariant), or a
	// recovered panic — we enqueue the disconnect->now window so missed trades still heal.
	// triggerGapRecovery is idempotent: it no-ops when no symbols are subscribed and skips
	// when disconnectTime was never recorded, and the coalescing queue dedups, so enqueuing
	// once here is safe and sufficient.
	cleanShutdown := false
	defer func() {
		if !cleanShutdown {
			c.triggerGapRecovery(time.Now())
		}
	}()

	defer func() {
		c.reconnectMu.Lock()
		c.reconnecting = false
		c.reconnectMu.Unlock()
		logger.OrDiscard(c.log).Warn("bingx: reconnection ended", "total_time", time.Since(startTime).Round(time.Second))
	}()

	for {
		select {
		case <-c.ctx.Done():
			logger.OrDiscard(c.log).Warn("bingx: reconnection aborted; context canceled")
			cleanShutdown = true // clean Close() — do NOT enqueue gap recovery
			return
		default:
		}

		if MaxReconnectAttempts > 0 && c.reconnectCount >= MaxReconnectAttempts {
			logger.OrDiscard(c.log).Error("bingx: max reconnect attempts reached; stopping reconnection",
				"max_attempts", MaxReconnectAttempts)
			return
		}

		c.reconnectCount++
		maxAttempts := "inf"
		if MaxReconnectAttempts > 0 {
			maxAttempts = fmt.Sprintf("%d", MaxReconnectAttempts)
		}

		// Add ±25% jitter to prevent a thundering herd when multiple BingX connections
		// reconnect in lockstep after a shared network event.
		jitter := time.Duration(rand.Int63n(int64(c.reconnectDelay)/2+1)) - c.reconnectDelay/4 //nolint:gosec // G404: reconnect jitter only; cryptographic randomness is not required.
		delay := c.reconnectDelay + jitter
		if delay < 0 {
			delay = c.reconnectDelay
		}
		logger.OrDiscard(c.log).Warn("bingx: reconnect attempt",
			"attempt", c.reconnectCount,
			"max_attempts", maxAttempts,
			"delay", delay,
			"base_delay", c.reconnectDelay)

		select {
		case <-time.After(delay):
		case <-c.ctx.Done():
			logger.OrDiscard(c.log).Warn("bingx: reconnection aborted during backoff")
			cleanShutdown = true // clean Close() — do NOT enqueue gap recovery
			return
		}

		if err := c.doConnect(c.ctx); err != nil {
			logger.OrDiscard(c.log).Error("bingx: reconnection attempt failed",
				"attempt", c.reconnectCount, "error", err)
			c.reconnectDelay = time.Duration(float64(c.reconnectDelay) * ReconnectBackoffRate)
			if c.reconnectDelay > MaxReconnectDelay {
				c.reconnectDelay = MaxReconnectDelay
			}
			continue
		}

		// Success!
		reconnectTime := time.Now()

		// Gap recovery for the success path is handled by the single top-of-function deferred
		// guard (cleanShutdown==false). It enqueues with time.Now() at return — after
		// resubscribeAll() below — which conservatively extends the recovery window to also
		// cover resubscribe latency. We deliberately do NOT register a second defer here to
		// avoid a redundant (though idempotent) enqueue.

		c.subsMu.RLock()
		symbolCount := len(c.subscriptions)
		c.subsMu.RUnlock()

		logger.OrDiscard(c.log).Warn("bingx: reconnected successfully",
			"attempts", c.reconnectCount,
			"total_time", time.Since(startTime).Round(time.Second),
			"subscriptions", symbolCount)

		// Defensively reset lastPong to the reconnect time.
		// The previous heartbeat goroutine may fire one final tick (reading stale lastPong)
		// inside the connWg.Wait() window before exiting. That stale value could still be
		// present when the new goroutines start accumulating elapsed time. Overwriting here
		// guarantees the new heartbeat monitor starts from a clean baseline.
		c.lastPongMu.Lock()
		c.lastPong = reconnectTime
		c.lastPongMu.Unlock()

		c.reconnectCount = 0
		c.reconnectDelay = InitialReconnectDelay

		// Resubscribe OUTSIDE of doConnect (no deadlock risk).
		// doConnect() has returned and released connMu, so sendSubscribe() can safely acquire RLock
		logger.OrDiscard(c.log).Warn("bingx: starting resubscription", "symbols", symbolCount)
		c.resubscribeAll()

		// Gap recovery fires on this return via the top-of-function deferred guard
		// (cleanShutdown remains false on the success path).
		return
	}
}

// triggerGapRecovery enqueues gap recovery for all subscribed symbols. It uses
// GapRecoveryQueue (bounded worker pool) when available and falls back to per-symbol
// RecoveryCallback goroutines when GapRecoveryQueue is nil.
func (c *BingXClient) triggerGapRecovery(reconnectTime time.Time) {
	c.subsMu.RLock()
	symbols := make([]string, 0, len(c.subscriptions))
	for symbol := range c.subscriptions {
		symbols = append(symbols, symbol)
	}
	c.subsMu.RUnlock()

	if len(symbols) == 0 {
		return
	}

	// disconnectTime is an atomic.Value — load it lock-free (no subsMu coupling). If a
	// disconnect was never recorded (defensive: e.g. triggered before any handleDisconnect
	// stored a value), skip rather than enqueue a bogus window.
	disconnectTime, ok := c.disconnectTime.Load().(time.Time)
	if !ok || disconnectTime.IsZero() {
		logger.OrDiscard(c.log).Warn("bingx: gap recovery skipped; disconnect time not recorded", "symbols", len(symbols))
		return
	}

	// Normalize the external config market type ("swap") to the internal canonical type
	// ("future") so this batch shares ONE coalescing-queue / circuit-breaker key with the
	// periodic reconciliation backstop (which enqueues with the internal type).
	// Without this, the queue key "bingx:swap|SYMBOL" never coalesces with the
	// reconciliation key "bingx:future|SYMBOL", defeating dedup. Storage is unaffected
	// (the gap-recovery worker re-normalizes via market.FromConfig anyway).
	marketType, err := market.FromConfig(c.config.MarketType)
	if err != nil {
		logger.OrDiscard(c.log).Error("bingx: gap recovery skipped; invalid market type",
			"market_type", c.config.MarketType, "error", err)
		return
	}
	internalMarketType := marketType.String()

	gapDuration := reconnectTime.Sub(disconnectTime)

	// Use the bounded worker pool (preferred).
	if c.gapRecovery != nil {
		logger.OrDiscard(c.log).Warn("bingx: gap recovery enqueueing symbols",
			"symbols", len(symbols),
			"gap", gapDuration.Round(time.Second),
			"disconnect", disconnectTime.Format("15:04:05"),
			"reconnect", reconnectTime.Format("15:04:05"))
		c.gapRecovery.EnqueueBatch(c.config.ExchangeID, internalMarketType, symbols, disconnectTime, reconnectTime)
		return
	}

	// Fallback: per-symbol goroutines with a semaphore.
	if c.recoveryCallback == nil {
		logger.OrDiscard(c.log).Warn("bingx: gap recovery not configured; skipping symbols",
			"symbols", len(symbols), "gap", gapDuration.Round(time.Second))
		return
	}

	logger.OrDiscard(c.log).Warn("bingx: triggering legacy gap recovery",
		"symbols", len(symbols),
		"gap", gapDuration.Round(time.Second),
		"disconnect", disconnectTime.Format("15:04:05"),
		"reconnect", reconnectTime.Format("15:04:05"))

	semaphore := make(chan struct{}, 10)

	for _, symbol := range symbols {
		semaphore <- struct{}{}

		go func(sym string) {
			defer func() {
				<-semaphore
				if r := recover(); r != nil {
					logger.OrDiscard(c.log).Error("bingx: panic in legacy gap recovery",
						"market_type", internalMarketType, "symbol", sym, "panic", r)
				}
			}()

			c.recoveryCallback(c.config.ExchangeID, internalMarketType, sym, disconnectTime, reconnectTime)
		}(symbol)
	}

	logger.OrDiscard(c.log).Warn("bingx: legacy gap recovery callbacks dispatched", "symbols", len(symbols))
}

// startHeartbeatMonitor monitors connection health via heartbeat tracking. It uses connCtx
// (immutable reference), so there is no race on channel reassignment.
func (c *BingXClient) startHeartbeatMonitor(connCtx context.Context) {
	defer c.connWg.Done()

	ticker := time.NewTicker(HeartbeatCheckPeriod)
	defer ticker.Stop()

	logger.OrDiscard(c.log).Warn("bingx: heartbeat monitor started",
		"timeout", HeartbeatTimeout, "check_period", HeartbeatCheckPeriod)

	for {
		select {
		case <-connCtx.Done():
			logger.OrDiscard(c.log).Info("bingx: heartbeat monitor stopped; connection context canceled")
			return

		case <-ticker.C:
			c.lastPongMu.RLock()
			lastPong := c.lastPong
			c.lastPongMu.RUnlock()

			timeSinceLastPong := time.Since(lastPong)

			c.connMu.RLock()
			connected := c.connected
			c.connMu.RUnlock()

			if !connected {
				logger.OrDiscard(c.log).Debug("bingx: heartbeat check skipped; not connected")
				continue
			}

			// Trust the server's heartbeat (application "Ping" every 5 s). No proactive
			// reconnect based on connection age; if the server stops sending pings or the
			// connection breaks, the timeout detects it.
			if timeSinceLastPong > heartbeatWarningAfter {
				logger.OrDiscard(c.log).Warn("bingx: heartbeat warning",
					"since_last_ping", timeSinceLastPong.Round(time.Second), "threshold", HeartbeatTimeout)
			} else {
				logger.OrDiscard(c.log).Debug("bingx: heartbeat ok", "since_last_ping", timeSinceLastPong.Round(time.Second))
			}

			if timeSinceLastPong > HeartbeatTimeout {
				logger.OrDiscard(c.log).Error("bingx: heartbeat timeout; triggering reconnection",
					"since_last_ping", timeSinceLastPong.Round(time.Second), "threshold", HeartbeatTimeout)
				c.handleDisconnect("heartbeat_timeout")
				return
			}
		}
	}
}

// Close terminates all connections (implements StreamingClient).
func (c *BingXClient) Close() error {
	logger.OrDiscard(c.log).Warn("bingx: closing websocket client")

	// Cancel client lifecycle context
	c.cancel()

	// Cancel connection context (stops all connection goroutines)
	if c.connCancel != nil {
		c.connCancel()
	}

	// Close connection
	c.connMu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.connected = false
	c.connMu.Unlock()

	// Clear subscriptions and buffers
	c.subsMu.Lock()
	c.subscriptions = make(map[string]bool)
	c.tradeBuffers = make(map[string][]trade.Trade)
	c.subsMu.Unlock()

	// Wait for connection goroutines
	c.connWg.Wait()

	logger.OrDiscard(c.log).Warn("bingx: websocket client closed")
	return nil
}

// Type returns the client type (implements StreamingClient).
func (c *BingXClient) Type() ClientType {
	return ClientTypeNative
}

// HealthCheck returns the health status (implements StreamingClient).
func (c *BingXClient) HealthCheck() bool {
	c.connMu.RLock()
	connected := c.connected
	c.connMu.RUnlock()

	if !connected {
		return false
	}

	c.lastPongMu.RLock()
	lastPong := c.lastPong
	c.lastPongMu.RUnlock()

	if time.Since(lastPong) > HeartbeatTimeout {
		logger.OrDiscard(c.log).Warn("bingx: health check unhealthy; no heartbeat",
			"since_last_ping", time.Since(lastPong).Round(time.Second), "threshold", HeartbeatTimeout)
		return false
	}

	return true
}

// IsConnected returns connection status.
func (c *BingXClient) IsConnected() bool {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.connected
}

// ConnectionStats returns runtime metrics (implements StatsProvider).
func (c *BingXClient) ConnectionStats() ConnectionStats {
	c.connMu.RLock()
	connected := c.connected
	c.connMu.RUnlock()

	c.reconnectMu.RLock()
	reconnecting := c.reconnecting
	reconnectCount := c.reconnectCount
	c.reconnectMu.RUnlock()

	c.lastPongMu.RLock()
	lastPong := c.lastPong
	c.lastPongMu.RUnlock()

	c.subsMu.RLock()
	numSymbols := len(c.subscriptions)
	c.subsMu.RUnlock()

	var lastPongAgo time.Duration
	if !lastPong.IsZero() {
		lastPongAgo = time.Since(lastPong)
	}

	// Surface the last disconnect (zero until the first disconnect).
	lastDisconnect, _ := c.disconnectTime.Load().(time.Time)

	return ConnectionStats{
		ExchangeID:     c.config.ExchangeID,
		MarketType:     c.config.MarketType,
		Connected:      connected,
		Reconnecting:   reconnecting,
		ReconnectCount: reconnectCount,
		Symbols:        numSymbols,
		MessagesIn:     c.messagesIn.Load(),
		TradesIn:       c.tradesIn.Load(),
		TradesDropped:  c.tradesDropped.Load(),
		LastPongAgo:    lastPongAgo,
		Healthy:        c.HealthCheck(),
		LastDisconnect: lastDisconnect,
	}
}
