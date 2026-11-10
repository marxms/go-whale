package binancesdk

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/binancews"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/platform/netdiag"
	"github.com/marxms/go-whale/internal/trade"
)

// defaultSpotWSBasePath is the WebSocket streams base path for the spot client. The client
// carries the resolved value as a field so tests can point it at a local server without
// mutating process state.
func defaultSpotWSBasePath() string {
	return wsBaseFromEnv("GOWHALE_BINANCE_SPOT_WS_URL", "wss://stream.binance.com:9443")
}

// SpotSDKClient implements StreamingClient for Binance SPOT.
type SpotSDKClient struct {
	config streaming.ClientConfig
	log    *slog.Logger

	// bufCap is the RESOLVED per-symbol trade buffer capacity (after the 0->30000
	// fallback in NewSpotSDKClient) — the divisor in GetBufferFillRatio. See SDKClient.bufCap.
	bufCap int

	wsConn *binancews.Connection
	// wsBasePath is resolved once at construction; tests may override it.
	wsBasePath string
	// combinedURLConnectEnabled and livenessLedgerEnabled are client tuning flags, kept on the
	// client so tests do not mutate process-wide state.
	combinedURLConnectEnabled bool
	livenessLedgerEnabled     bool
	netdiag                   *netdiag.Correlator
	buffers                   *BufferManager

	symbols   map[string]bool
	symbolsMu sync.RWMutex

	metrics       Metrics
	messagesIn    atomic.Uint64
	tradesIn      atomic.Uint64
	tradesDropped atomic.Uint64

	reconnectCount atomic.Uint64
	reconnecting   atomic.Bool
	reconnectMu    sync.Mutex
	reconnectCh    chan string // buffered(1); signals the reconnect worker (deduped)

	connected     atomic.Bool
	lastMessageAt atomic.Value
	connectMu     sync.Mutex
	connectTime   atomic.Value // stores time.Time

	// Connection-scoped lifecycle.
	connCtx    context.Context
	connCancel context.CancelFunc
	connWg     sync.WaitGroup

	// Gap recovery tracking
	disconnectTime atomic.Value // stores time.Time

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// SUBSCRIBE-frame rate limiter (prevents Binance 1008 disconnect storm).
	// Used ONLY on the dynamic add/remove (Subscribe) path; the combined-URL
	// reconnect path is frame-free and does not pace.
	pacer *subscribePacer

	// Combined-URL connect. partitions holds one connection per URL
	// partition; partitions[0] aliases wsConn. resubscribedAt arms the watchdog
	// grace (D2). Guarded by connectMu.
	partitions     []*binancews.Connection
	resubscribedAt atomic.Value // stores time.Time
}

// ensureConnected thread-safely connects if not already connected.
// Allows retry on failure (unlike sync.Once which permanently fails).
func (c *SpotSDKClient) ensureConnected(ctx context.Context) error {
	if c.connected.Load() {
		return nil
	}
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	if c.connected.Load() {
		return nil
	}
	return c.connectLocked(ctx)
}

// NewSpotSDKClient creates a new SpotSDKClient with the given config.
func NewSpotSDKClient(config streaming.ClientConfig, logs ...*slog.Logger) *SpotSDKClient {
	ctx, cancel := context.WithCancel(context.Background())

	bufferSize := config.TradeBufferSize
	if bufferSize == 0 {
		bufferSize = 30000
	}

	client := &SpotSDKClient{
		config:                    config,
		log:                       logger.OrDiscard(logs...),
		bufCap:                    bufferSize,
		buffers:                   NewBufferManager(bufferSize),
		symbols:                   make(map[string]bool),
		wsBasePath:                defaultSpotWSBasePath(),
		combinedURLConnectEnabled: true,
		livenessLedgerEnabled:     true,
		netdiag:                   netdiag.New(),
		reconnectCh:               make(chan string, 1),
		ctx:                       ctx,
		cancel:                    cancel,
		pacer:                     newSubscribePacer(subscribeMinInterval),
	}

	// Start the reconnect worker as a client-lifecycle goroutine. It is NOT a
	// connWg member, so the reconnect path (which calls Connect -> connWg.Wait)
	// can never self-join.
	client.wg.Add(1)
	go client.reconnectWorker()

	return client
}

// SetNetdiag injects the cross-venue disconnect correlator.
func (c *SpotSDKClient) SetNetdiag(correlator *netdiag.Correlator) { c.netdiag = correlator }

// SetMetrics injects the observability port. The composition root calls it before the
// client is used; a zero value keeps every metric a safe no-op.
func (c *SpotSDKClient) SetMetrics(m Metrics) {
	c.metrics = m
	m.initScope(venueBinance, marketTypeSpot)
}

// requestReconnect signals the reconnect worker without blocking (deduped by the buffered channel).
func (c *SpotSDKClient) requestReconnect(reason string) {
	select {
	case c.reconnectCh <- reason:
	default:
		// A reconnect is already queued/running — the signal was dropped.
		// Surface it as a metric so reconnect churn is observable.
		c.metrics.incReconnectSignalDropped(venueBinance, marketTypeSpot)
	}
}

// reconnectWorker runs reconnects OUTSIDE connWg so Connect()->connWg.Wait() never self-joins.
func (c *SpotSDKClient) reconnectWorker() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case reason := <-c.reconnectCh:
			c.reconnectAndResubscribe(reason)
		}
	}
}

// Connect establishes WebSocket connection to Binance spot market streams.
// Connection-scoped lifecycle: it cancels the previous monitor before creating a new one.
func (c *SpotSDKClient) Connect(ctx context.Context) error {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	return c.connectLocked(ctx)
}

// connectLocked performs the connection. Caller MUST hold connectMu.
// It must NOT lock connectMu itself — doing so would deadlock against the caller.
func (c *SpotSDKClient) connectLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	if c.connected.Load() {
		return nil
	}
	// Step 1: Cancel the previous connection context.
	if c.connCancel != nil {
		logger.OrDiscard(c.log).Info("binance sdk spot: canceling previous connection monitor")
		c.connCancel()
	}

	// Step 2: Wait for the old connection goroutines to exit.
	c.connWg.Wait()

	logger.OrDiscard(c.log).Info("binance spot ws: connecting to market streams")
	conn := binancews.NewConnection(0, c.wsBasePath, binancews.WithReadIdle(readIdleFor(venueBinance, marketTypeSpot)))
	if err := connectMarketStream(ctx, c.ctx, conn, nil); err != nil {
		return fmt.Errorf("binance spot websocket connect failed: %w", err)
	}
	c.wsConn = conn
	c.markConnectedLocked()

	// Emit event
	NewSDKEvent(EventConnected, venueBinance, marketTypeSpot).
		WithMetadata("goroutines", runtime.NumGoroutine()).
		Log()

	// Update metrics
	c.metrics.setGoroutines(venueBinance, marketTypeSpot, float64(runtime.NumGoroutine()))
	c.metrics.setHealthState(venueBinance, marketTypeSpot, float64(HealthHealthy))

	logger.OrDiscard(c.log).Info("binance sdk spot client connected; monitor started")
	return nil
}

// markConnectedLocked records the live-connection state and (re)starts the
// connection-scoped monitor. Caller MUST hold connectMu and have already
// dialed. Shared by the empty-connect path and the combined-URL path.
func (c *SpotSDKClient) markConnectedLocked() {
	c.connected.Store(true)
	c.lastMessageAt.Store(time.Now())
	c.connectTime.Store(time.Now())
	// Arm the resubscribe-grace anchor before (re)starting the monitor, for the combined,
	// empty-connect, and initial-connect paths uniformly.
	c.resubscribedAt.Store(time.Now())

	// Step 3: Create a new connection-scoped context.
	c.connCtx, c.connCancel = context.WithCancel(c.ctx)

	// Step 4: Start connection-scoped goroutines.
	c.startConnectionMonitor(c.connCtx)
	c.startSocketErrorWatchers(c.connCtx, c.connectionsLocked())
}

// connectionsLocked returns the current connection set. Caller MUST hold connectMu.
func (c *SpotSDKClient) connectionsLocked() []*binancews.Connection {
	if len(c.partitions) > 0 {
		return append([]*binancews.Connection(nil), c.partitions...)
	}
	if c.wsConn != nil {
		return []*binancews.Connection{c.wsConn}
	}
	return nil
}

// sortedSubscribedSymbols returns the subscribed symbols in stable sorted order
// so combined-URL partition membership is deterministic across reconnects (O7).
func (c *SpotSDKClient) sortedSubscribedSymbols() []string {
	c.symbolsMu.RLock()
	symbols := make([]string, 0, len(c.symbols))
	for sym := range c.symbols {
		symbols = append(symbols, sym)
	}
	c.symbolsMu.RUnlock()
	sort.Strings(symbols)
	return symbols
}

func (c *SpotSDKClient) newSpotWS(id int) *binancews.Connection {
	return binancews.NewConnection(id, c.wsBasePath, binancews.WithReadIdle(readIdleFor(venueBinance, marketTypeSpot)))
}

// connectAndResubscribeCombined performs the combined-URL reconnect for spot.
// Handlers are installed before the reader starts, so no frame lacks a route.
func (c *SpotSDKClient) connectAndResubscribeCombined(symbols []string) (int, error) {
	if c.connCancel != nil {
		logger.OrDiscard(c.log).Info("binance spot ws: canceling previous connection monitor")
		c.connCancel()
	}
	c.connWg.Wait()

	streams := aggTradeStreams(symbols)
	parts := partitionStreams(streams)
	logger.OrDiscard(c.log).Warn("binance spot ws: combined-URL connect",
		"symbols", len(symbols),
		"streams", len(streams),
		"partitions", len(parts),
		"stream_budget", maxStreamsPerConnection,
		"url_char_budget", maxCombinedURLLength)

	clients := make([]*binancews.Connection, 0, len(parts))
	resubscribed := 0
	off := 0
	for i, slice := range parts {
		conn := c.newSpotWS(i)
		part := symbols[off : off+len(slice)]
		off += len(part)
		handlers := make(map[string]binancews.Handler, len(part))
		for _, symbol := range part {
			handlers[streamName(symbol)] = c.tradeCallback(symbol)
		}
		if err := connectMarketStream(c.ctx, c.ctx, conn, handlers); err != nil {
			for _, prev := range clients {
				_ = prev.Close()
			}
			return 0, fmt.Errorf("combined-URL connect failed (partition %d/%d, %d streams): %w", i+1, len(parts), len(slice), err)
		}
		clients = append(clients, conn)
		resubscribed += len(part)
	}

	c.wsConn = clients[0]
	c.partitions = clients
	c.markConnectedLocked()

	return resubscribed, nil
}

// closed reports whether Close canceled the client lifecycle. A zero-value fixture
// built in a test without the constructor has a nil ctx; treat that as open.
func (c *SpotSDKClient) closed() bool { return c.ctx != nil && c.ctx.Err() != nil }

// Subscribe adds a symbol and starts streaming its aggTrade data.
func (c *SpotSDKClient) Subscribe(ctx context.Context, symbol string) error {
	if c.closed() {
		return fmt.Errorf("%w: binance spot subscribe %s", streaming.ErrClosed, symbol)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("binance spot subscribe %s: %w", symbol, err)
	}
	symbol = strings.ToUpper(symbol)

	c.symbolsMu.Lock()
	c.symbols[symbol] = true
	c.symbolsMu.Unlock()

	if err := c.ensureConnected(ctx); err != nil {
		return fmt.Errorf("spot sdk client not connected, cannot subscribe to %s: %w", symbol, err)
	}

	if err := c.pacer.wait(ctx); err != nil {
		return fmt.Errorf("subscribe pacing aborted for %s: %w", symbol, err)
	}

	c.connectMu.Lock()
	conn := c.wsConn
	c.connectMu.Unlock()
	if conn == nil {
		return fmt.Errorf("spot websocket not initialized")
	}
	if err := conn.Subscribe(ctx, streamName(symbol), c.tradeCallback(symbol)); err != nil {
		return fmt.Errorf("failed to subscribe to %s spot aggTrade: %w", symbol, err)
	}

	// Emit subscribe event
	NewSDKEvent(EventSubscribe, venueBinance, marketTypeSpot).
		WithSymbol(symbol).
		Log()

	logger.OrDiscard(c.log).Info("subscribed via spot sdk", "symbol", symbol, "stream", "@trade")
	return nil
}

// tradeCallback returns the per-symbol spot message handler. Shared by Subscribe
// (dynamic add) and the combined-URL resubscribe path so both record metrics,
// parse, and buffer identically.
func (c *SpotSDKClient) tradeCallback(symbol string) binancews.Handler {
	return func(data []byte) {
		defer func() {
			if r := recover(); r != nil {
				logger.OrDiscard(c.log).Error("panic in spot trade callback", "symbol", symbol, "panic", r)
				c.tradesDropped.Add(1)
				c.metrics.incTradesDropped(venueBinance, marketTypeSpot)
			}
		}()

		var msg tradeEvent
		if err := json.Unmarshal(data, &msg); err != nil {
			logger.OrDiscard(c.log).Warn("spot trade callback: malformed frame", "symbol", symbol, "error", err)
			c.tradesDropped.Add(1)
			c.metrics.incTradesDropped(venueBinance, marketTypeSpot)
			return
		}

		c.messagesIn.Add(1)
		c.lastMessageAt.Store(time.Now())

		c.metrics.incMessagesIn(venueBinance, marketTypeSpot)

		trade := c.toDomainTrade(&msg)
		if trade == nil {
			c.tradesDropped.Add(1)
			c.metrics.incTradesDropped(venueBinance, marketTypeSpot)
			return
		}

		// Same disjoint-branch contract as the futures callback: ingested means BUFFERED.
		buf := c.buffers.GetOrCreate(symbol)
		if buf.Append(*trade) {
			logger.OrDiscard(c.log).Warn("spot trade buffer overflow; trade dropped", "symbol", symbol, "cap", c.config.TradeBufferSize)
			c.tradesDropped.Add(1)
			c.metrics.incTradesDropped(venueBinance, marketTypeSpot)
			return
		}
		c.tradesIn.Add(1)
		c.metrics.incTradesIngested(venueBinance, marketTypeSpot)
	}
}

// Unsubscribe removes a symbol and stops its stream.
func (c *SpotSDKClient) Unsubscribe(symbol string) error {
	if c.closed() {
		return nil
	}
	symbol = strings.ToUpper(symbol)

	c.symbolsMu.Lock()
	delete(c.symbols, symbol)
	c.symbolsMu.Unlock()

	c.connectMu.Lock()
	conn := c.wsConn
	c.connectMu.Unlock()
	if conn != nil {
		_ = conn.Unsubscribe(context.Background(), streamName(symbol))
	}

	c.buffers.Remove(symbol)
	logger.OrDiscard(c.log).Info("unsubscribed", "symbol", symbol, "stream", "@trade", "market_type", marketTypeSpot)
	return nil
}

// GetTrades returns and clears all buffered trades for a symbol.
// Only returns trades for symbols that were subscribed; never creates phantom buffers.
func (c *SpotSDKClient) GetTrades(ctx context.Context, symbol string) ([]trade.Trade, error) {
	if c.closed() {
		return nil, fmt.Errorf("%w: binance spot get trades %s", streaming.ErrClosed, symbol)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	symbol = strings.ToUpper(symbol)
	c.symbolsMu.RLock()
	_, subscribed := c.symbols[symbol]
	c.symbolsMu.RUnlock()
	if !subscribed {
		return nil, fmt.Errorf("%w: binance spot %s", streaming.ErrNotSubscribed, symbol)
	}
	buf := c.buffers.GetOrCreate(symbol)
	return buf.Flush(), nil
}

// startConnectionMonitor monitors the connection health and triggers reconnect if needed.
// It uses connCtx so the monitor exits cleanly when the connection is canceled.
func (c *SpotSDKClient) startConnectionMonitor(connCtx context.Context) {
	monitoredConnection{
		log:              c.log,
		venue:            venueBinance,
		scope:            marketTypeSpot,
		clientCtx:        c.ctx,
		messagesIn:       &c.messagesIn,
		lastMessageAt:    &c.lastMessageAt,
		resubscribedAt:   &c.resubscribedAt,
		reconnecting:     &c.reconnecting,
		connected:        &c.connected,
		metrics:          c.metrics,
		requestReconnect: c.requestReconnect,
		shutdownLog:      "binance sdk spot monitor shutting down",
		disconnectedLog:  "binance sdk spot: monitor detects disconnected",
		stalledLog:       "binance sdk spot: no messages received",
		wg:               &c.connWg,
	}.run(connCtx)
}

// closeAllConnections closes every live WebSocket connection. Best-effort.
func (c *SpotSDKClient) closeAllConnections() {
	c.connectMu.Lock()
	clients := append([]*binancews.Connection(nil), c.partitions...)
	single := c.wsConn
	c.connectMu.Unlock()
	if len(clients) == 0 && single != nil {
		clients = []*binancews.Connection{single}
	}
	for i, conn := range clients {
		if conn == nil {
			continue
		}
		logger.OrDiscard(c.log).Info("binance spot ws: closing connection",
			"partition", i+1, "partitions", len(clients))
		if err := conn.Close(); err != nil {
			logger.OrDiscard(c.log).Error("binance spot ws: error closing connection during reconnect", "error", err)
		}
	}
}

// reconnectDial performs one connect attempt on the reconnect path. Combined-URL
// flag on: connect all partitions and attach handlers atomically, returning the
// live count. Flag off: empty Connect(), returns -1 (resubscribe
// per-symbol via the paced Subscribe()).
func (c *SpotSDKClient) reconnectDial(symbols []string) (int, error) {
	if c.combinedURLConnectEnabled {
		c.connectMu.Lock()
		defer c.connectMu.Unlock()
		return c.connectAndResubscribeCombined(symbols)
	}
	dialCtx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	if err := c.Connect(dialCtx); err != nil {
		return 0, err
	}
	return -1, nil
}

// reconnectAndResubscribe handles reconnection and re-subscription of all symbols.
func (c *SpotSDKClient) reconnectAndResubscribe(reason string) {
	if c.reconnecting.Load() {
		// Warn so the skip is visible under log_level=warn.
		logger.OrDiscard(c.log).Warn("binance sdk spot: reconnect already in progress; skipping")
		return
	}
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()

	if c.reconnecting.Load() {
		return
	}
	c.reconnecting.Store(true)
	defer c.reconnecting.Store(false)

	select {
	case <-c.ctx.Done():
		logger.OrDiscard(c.log).Info("binance sdk spot: reconnect aborted; client context canceled")
		return
	default:
	}

	c.symbolsMu.RLock()
	symbolCount := len(c.symbols)
	c.symbolsMu.RUnlock()

	logger.OrDiscard(c.log).Warn("binance sdk spot reconnect triggered",
		"reason", reason,
		"prior_reconnects", c.reconnectCount.Load(),
		"symbols", symbolCount)

	// Emit event and metric
	NewSDKEvent(EventReconnectTriggered, venueBinance, marketTypeSpot).
		WithReason(reason).
		WithMetadata("reconnect_count", c.reconnectCount.Load()).
		WithMetadata("symbols", symbolCount).
		Log()
	c.metrics.incReconnectAttempt(venueBinance, marketTypeSpot, reason)

	symbols := c.sortedSubscribedSymbols() // sorted -> deterministic partition membership

	if len(symbols) == 0 {
		// Warn so the skip is visible under log_level=warn.
		logger.OrDiscard(c.log).Warn("binance sdk spot: no symbols to resubscribe; skipping reconnect")
		return
	}

	// Mark the abandoned generation disconnected before opening its replacement.
	c.connected.Store(false)
	c.lastMessageAt.Store(time.Time{})
	c.disconnectTime.Store(time.Now())

	// Decouple gap-recovery enqueue from reconnect SUCCESS: enqueue the window on
	// EVERY exit path below (including a failed Connect() or partial resubscribe),
	// so the affected symbols always heal. The disconnect time is now recorded and
	// symbols is non-empty, so the deferred call has a valid window. Idempotency is
	// guaranteed by the coalescing queue (EnqueueBatch dedups).
	defer c.triggerGapRecovery(symbols)

	// Close existing connections (all combined-URL partitions or the empty-connect client).
	c.closeAllConnections()

	logger.OrDiscard(c.log).Info("binance sdk spot: waiting for socket cleanup", "delay", socketCleanupDelay)
	select {
	case <-time.After(socketCleanupDelay):
	case <-c.ctx.Done():
		return
	}

	// Re-dial in a RETRY LOOP with backoff+jitter instead of a single
	// attempt. connectLocked already canceled the old monitor before the failed
	// dial and only starts a new monitor on SUCCESS, so a single failed Connect()
	// would leave NO monitor running and reconnectCh empty — the client would be
	// permanently dead until process restart. Loop until Connect() succeeds or the
	// client context is canceled. This runs in the reconnectWorker goroutine (NOT a
	// connWg member), so connectLocked's connWg.Wait() never self-joins; on attempts
	// after a failed dial no monitor was started, so connCancel()+connWg.Wait() at the
	// top of connectLocked operate on an already-dead monitor and return immediately.
	delay := reconnectBackoffBase
	resubscribed := 0
	for attempt := 1; ; attempt++ {
		select {
		case <-c.ctx.Done():
			logger.OrDiscard(c.log).Warn("binance sdk spot: reconnect aborted during retry; client context canceled",
				"attempt", attempt)
			return
		default:
		}

		// reconnectDial connects (combined-URL or empty) and, on the
		// combined path, attaches all handlers atomically. n is the live count
		// (combined) or -1 (empty-connect path, resubscribe per-symbol below).
		n, err := c.reconnectDial(symbols)
		if err == nil {
			resubscribed = n
			break // connected; monitor restarted by markConnectedLocked
		}

		c.connected.Store(false)
		c.metrics.setHealthState(venueBinance, marketTypeSpot, float64(HealthDisconnected))
		c.metrics.incReconnectFailure(venueBinance, marketTypeSpot)
		logger.OrDiscard(c.log).Warn("binance sdk spot: reconnect attempt failed",
			"attempt", attempt, "error", err, "retry_in", delay)
		NewSDKEvent(EventReconnectFailed, venueBinance, marketTypeSpot).
			WithError(err).
			WithReason("connect_failed").
			WithMetadata("attempt", attempt).
			Log()

		jitter := time.Duration(rand.Int63n(int64(delay)/2+1)) - delay/4 //nolint:gosec // G404: reconnect jitter only; cryptographic randomness is not required.
		sleep := delay + jitter
		if sleep < 0 {
			sleep = delay
		}
		select {
		case <-time.After(sleep):
		case <-c.ctx.Done():
			logger.OrDiscard(c.log).Warn("binance sdk spot: reconnect aborted during backoff; client context canceled",
				"attempt", attempt)
			return
		}

		delay = time.Duration(float64(delay) * reconnectBackoffRate)
		if delay > reconnectBackoffMax {
			delay = reconnectBackoffMax
		}
	}

	c.reconnectCount.Add(1)

	if resubscribed < 0 {
		// Empty-connect path: re-subscribe per-symbol via the paced Subscribe().
		logger.OrDiscard(c.log).Info("binance sdk spot: resubscribing symbols", "symbols", len(symbols))
		resubscribed = 0
		for _, symbol := range symbols {
			if err := c.Subscribe(c.ctx, symbol); err != nil {
				logger.OrDiscard(c.log).Error("binance sdk spot: failed to resubscribe after reconnect",
					"symbol", symbol, "error", err)
			} else {
				resubscribed++
			}
		}
	}

	// Surface partial restore as a metric + explicit warning. On the combined-URL
	// path "resubscribed" means the handler is attached on the live conn (D3).
	recordResubscribeOutcome(c.metrics, marketTypeSpot, len(symbols), resubscribed, c.log)

	// Promote the reconnect-outcome log to Warn so it is visible under the
	// production log_level=warn (Info is suppressed there, hiding recovery from view).
	// Worded neutrally ("restored", not "completed") so a partial restore never reads
	// as success.
	logger.OrDiscard(c.log).Warn("binance sdk spot reconnect finished",
		"restored", resubscribed,
		"symbols", len(symbols),
		"total_reconnects", c.reconnectCount.Load())

	NewSDKEvent(EventReconnectSuccess, venueBinance, marketTypeSpot).
		WithMetadata("resubscribed", resubscribed).
		WithMetadata("total_symbols", len(symbols)).
		WithMetadata("total_reconnects", c.reconnectCount.Load()).
		Log()

	// Drain any reconnect signal queued WHILE this reconnect was running.
	// Otherwise a stale signal would immediately trigger a redundant reconnect that
	// tears down the connection we just rebuilt. Non-blocking single-slot drain.
	select {
	case <-c.reconnectCh:
	default:
	}

	// Gap recovery is enqueued via the deferred triggerGapRecovery registered after
	// the disconnect time was recorded — it fires on success and on every early return.
}

// HealthCheck returns true if connected and receiving messages within timeout.
// It uses granular health state instead of a simple boolean.
func (c *SpotSDKClient) HealthCheck() bool {
	state := c.GetHealthState()
	return state == HealthHealthy
}

// GetHealthState returns the current granular health state.
func (c *SpotSDKClient) GetHealthState() HealthState {
	if c.reconnecting.Load() {
		return HealthReconnecting
	}
	if !c.connected.Load() {
		return HealthDisconnected
	}

	lastMsg, ok := c.lastMessageAt.Load().(time.Time)
	if !ok {
		return HealthDisconnected
	}

	age := time.Since(lastMsg)
	switch {
	case age < 30*time.Second:
		return HealthHealthy
	case age < 2*time.Minute:
		return HealthStalled
	default:
		return HealthDisconnected
	}
}

// ConnectionStats returns current connection and buffer metrics.
func (c *SpotSDKClient) ConnectionStats() streaming.ConnectionStats {
	c.symbolsMu.RLock()
	symCount := len(c.symbols)
	c.symbolsMu.RUnlock()

	connTime, _ := c.connectTime.Load().(time.Time)
	uptime := time.Since(connTime)

	// Surface the last disconnect (zero until the first disconnect).
	lastDisconnect, _ := c.disconnectTime.Load().(time.Time)

	return streaming.ConnectionStats{
		ExchangeID:     venueBinance,
		MarketType:     marketTypeSpot,
		Connected:      c.connected.Load(),
		Reconnecting:   c.reconnecting.Load(),
		ReconnectCount: saturatingInt(c.reconnectCount.Load()),
		Symbols:        symCount,
		MessagesIn:     c.messagesIn.Load(),
		TradesIn:       c.tradesIn.Load(),
		TradesDropped:  c.tradesDropped.Load(),
		LastPongAgo:    0,
		Uptime:         uptime,
		Healthy:        c.HealthCheck(),
		HealthState:    c.GetHealthState().String(),
		LastDisconnect: lastDisconnect,
	}
}

// GetBufferFillRatio returns the fill ratio for a specific symbol's buffer.
func (c *SpotSDKClient) GetBufferFillRatio(symbol string) float64 {
	if c.bufCap <= 0 {
		return 0
	}
	buf := c.buffers.GetOrCreate(symbol)
	return float64(buf.Len()) / float64(c.bufCap)
}

// Type returns the client type.
func (c *SpotSDKClient) Type() streaming.ClientType {
	return streaming.ClientTypeNative
}

// IsSharedSubscriptionClient returns true.
func (c *SpotSDKClient) IsSharedSubscriptionClient() bool {
	return true
}

// Close disconnects all streams and cleans up resources.
// It cancels both the client and connection contexts and waits for all goroutines.
func (c *SpotSDKClient) Close() error {
	logger.OrDiscard(c.log).Info("binance sdk spot: closing client")

	// Step 1: Cancel client lifecycle → stops all goroutines
	c.cancel()

	// Step 2: Cancel connection context (redundant but defensive).
	// Read connCancel under connectMu — connectLocked writes it under the
	// same lock from the reconnect worker, so an unguarded read here is a data race.
	c.connectMu.Lock()
	c.connected.Store(false)
	cancel := c.connCancel
	c.connectMu.Unlock()
	if cancel != nil {
		cancel()
	}

	// Step 3: Interrupt in-flight I/O before joining connection goroutines.
	c.closeAllConnections()

	// Step 4: Wait for ALL goroutines to exit.
	c.connWg.Wait()
	c.wg.Wait()

	// Update metrics
	c.metrics.setGoroutines(venueBinance, marketTypeSpot, float64(runtime.NumGoroutine()))
	c.metrics.setHealthState(venueBinance, marketTypeSpot, float64(HealthDisconnected))

	NewSDKEvent(EventDisconnected, venueBinance, marketTypeSpot).
		WithReason("client_close").
		Log()

	logger.OrDiscard(c.log).Info("binance sdk spot client closed")
	return nil
}

// toDomainTrade converts a spot AggTradeResponse to trade.Trade.
func (c *SpotSDKClient) toDomainTrade(event *tradeEvent) *trade.Trade {
	if event == nil || event.Symbol == "" || event.Price == "" || event.Quantity == "" {
		return nil
	}
	price, err := strconv.ParseFloat(event.Price, 64)
	if err != nil {
		logger.OrDiscard(c.log).Error("failed to parse spot trade price", "value", event.Price, "error", err)
		return nil
	}
	qty, err := strconv.ParseFloat(event.Quantity, 64)
	if err != nil {
		logger.OrDiscard(c.log).Error("failed to parse spot trade quantity", "value", event.Quantity, "error", err)
		return nil
	}

	side := trade.Buy
	if event.BuyerIsMaker {
		side = trade.Sell
	}

	id := event.AggregateID
	if id == 0 {
		id = event.TradeID
	}
	return &trade.Trade{
		ID:        strconv.FormatInt(id, 10),
		Symbol:    event.Symbol,
		Price:     price,
		Amount:    qty,
		Timestamp: event.TradeTime,
		Side:      side,
	}
}

// triggerGapRecovery triggers gap recovery for all symbols after a reconnect.
// Uses EnqueueBatch for efficient batch recovery.
func (c *SpotSDKClient) triggerGapRecovery(symbols []string) {
	if c.config.GapRecovery == nil {
		return
	}

	dt, ok := c.disconnectTime.Load().(time.Time)
	if !ok || dt.IsZero() {
		logger.OrDiscard(c.log).Warn("binance sdk spot: gap recovery skipped; disconnect time not recorded")
		return
	}
	reconnectTime := time.Now()

	// Enqueue with the INTERNAL market type ("spot") so the gap-recovery
	// coalescing-queue key and circuit-breaker scope match the periodic reconciliation
	// and storage paths (which use market.FromConfig). The worker normalizes the value
	// for storage, so this only unifies the key — it does not change persistence.
	marketType, err := market.FromConfig(c.config.MarketType)
	if err != nil {
		logger.OrDiscard(c.log).Error("binance sdk spot: gap recovery skipped; invalid market type",
			"market_type", c.config.MarketType, "error", err)
		return
	}

	logger.OrDiscard(c.log).Info("binance sdk spot: triggering gap recovery",
		"symbols", len(symbols),
		"disconnect", dt.Format(time.RFC3339),
		"reconnect", reconnectTime.Format(time.RFC3339))

	c.config.GapRecovery.EnqueueBatch(venueBinance, marketType.String(), symbols, dt, reconnectTime)

	NewSDKEvent(EventGapRecoveryTriggered, venueBinance, marketTypeSpot).
		WithMetadata("symbols", len(symbols)).
		WithMetadata("disconnect_time", dt.Format(time.RFC3339)).
		WithMetadata("reconnect_time", reconnectTime.Format(time.RFC3339)).
		Log()
}
