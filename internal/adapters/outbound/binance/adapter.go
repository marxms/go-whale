// Package binancesdk adapts the Binance spot and USD-M futures WebSocket APIs to the
// streaming client contracts.
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
	"github.com/marxms/go-whale/internal/health"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/platform/netdiag"
	"github.com/marxms/go-whale/internal/trade"
)

// defaultFuturesWSBasePath is the WebSocket streams base path for the futures client. The
// client carries the resolved value as a field so tests can point it at a local server
// without mutating process state.
//
// The default is the PRODUCTION host. The legacy host is the futures testnet (see
// isTestnetWSBase) and must be selected explicitly, where the production stream is unreachable.
func defaultFuturesWSBasePath() string {
	return wsBaseFromEnv("GOWHALE_BINANCE_FUTURES_WS_URL", "wss://fstream.binance.com")
}

// SDKClient implements StreamingClient using the official Binance connector Go SDK.
type SDKClient struct {
	config streaming.ClientConfig
	log    *slog.Logger

	// bufCap is the RESOLVED per-symbol trade buffer capacity (after the 0->30000
	// fallback in NewSDKClient) — the divisor in GetBufferFillRatio. Dividing by
	// config.TradeBufferSize directly produced +Inf when unset (the resolved cap was
	// only stored inside BufferManager), pinning the adaptive scheduler to minProcessDelay.
	bufCap int

	// Binance WebSocket transport. Reconnection remains application-owned.
	wsConn *binancews.Connection
	// wsBasePath is resolved once at construction; tests may override it.
	wsBasePath string
	// combinedURLConnectEnabled and livenessLedgerEnabled are client tuning flags. They live
	// on the client so tests can flip them without mutating process-wide state.
	combinedURLConnectEnabled bool
	livenessLedgerEnabled     bool
	// netdiag correlates a disconnect with drops on other venues; the composition root
	// injects the process-wide instance.
	netdiag *netdiag.Correlator

	// Buffers
	buffers *BufferManager

	// Symbol management
	symbols   map[string]bool
	symbolsMu sync.RWMutex

	// Metrics
	metrics       Metrics
	messagesIn    atomic.Uint64
	tradesIn      atomic.Uint64
	tradesDropped atomic.Uint64

	// Reconnection
	reconnectCount atomic.Uint64
	reconnecting   atomic.Bool
	reconnectMu    sync.Mutex  // protects re-subscription
	reconnectCh    chan string // buffered(1); signals the reconnect worker (deduped)

	// State
	connected     atomic.Bool
	lastMessageAt atomic.Value // stores time.Time
	connectMu     sync.Mutex
	connectTime   atomic.Value // stores time.Time

	// Connection-scoped lifecycle: connCtx/connCancel/connWg are recreated on every
	// Connect() and canceled on every disconnect/reconnect.
	connCtx    context.Context
	connCancel context.CancelFunc
	connWg     sync.WaitGroup

	// Gap recovery tracking
	disconnectTime atomic.Value // stores time.Time

	// Client lifecycle — only canceled on Close()
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Gap recovery trigger

	// SUBSCRIBE-frame rate limiter (prevents Binance 1008 disconnect storm).
	// Used ONLY on the dynamic add/remove (Subscribe) path; the combined-URL
	// reconnect path is frame-free and does not pace.
	pacer *subscribePacer

	// Combined-URL connect. partitions holds one connection per URL
	// partition; partitions[0] aliases wsConn. resubscribedAt records the
	// resubscribe-complete instant that arms the watchdog grace (D2).
	// Guarded by connectMu.
	partitions     []*binancews.Connection
	resubscribedAt atomic.Value // stores time.Time

	// ledger is the per-symbol/per-partition freshness authority (D1), stamped on
	// the ingest hot path and scanned per-partition by the watchdog. Always non-nil;
	// only stamped/used when c.livenessLedgerEnabled.
	ledger *health.Ledger

	// heartbeat detects process-wide CPU starvation so the watchdog does not storm a
	// starved box with reconnects (D2). Client-scoped, started once.
	heartbeat     *starvationHeartbeat
	heartbeatOnce sync.Once

	// ledgerDeadPartition is published by the watchdog scan and read by
	// GetHealthState (2E): a dead partition degrades /health even when the aggregate
	// last-message is fresh (a live sibling would otherwise mask it — O1 at the
	// health surface).
	ledgerDeadPartition atomic.Bool
}

// NewSDKClient creates a new SDKClient with the given config.
func NewSDKClient(config streaming.ClientConfig, logs ...*slog.Logger) *SDKClient {
	ctx, cancel := context.WithCancel(context.Background())

	bufferSize := config.TradeBufferSize
	if bufferSize == 0 {
		bufferSize = 30000
	}

	client := &SDKClient{
		config:                    config,
		log:                       logger.OrDiscard(logs...),
		bufCap:                    bufferSize,
		buffers:                   NewBufferManager(bufferSize),
		ledger:                    health.New(),
		heartbeat:                 &starvationHeartbeat{},
		symbols:                   make(map[string]bool),
		wsBasePath:                defaultFuturesWSBasePath(),
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
func (c *SDKClient) SetNetdiag(correlator *netdiag.Correlator) { c.netdiag = correlator }

// SetMetrics injects the observability port. The composition root calls it before the
// client is used; a zero value keeps every metric a safe no-op.
func (c *SDKClient) SetMetrics(m Metrics) {
	c.metrics = m
	m.initScope(venueBinance, marketTypeFuture)
}

// closed reports whether Close canceled the client lifecycle. A zero-value fixture
// built in a test without the constructor has a nil ctx; treat that as open rather than
// panicking on the nil interface.
func (c *SDKClient) closed() bool { return c.ctx != nil && c.ctx.Err() != nil }

// requestReconnect signals the reconnect worker without blocking (deduped by the buffered channel).
func (c *SDKClient) requestReconnect(reason string) {
	select {
	case c.reconnectCh <- reason:
	default:
		// A reconnect is already queued/running — the signal was dropped.
		// Surface it as a metric so reconnect churn is observable.
		c.metrics.incReconnectSignalDropped(venueBinance, marketTypeFuture)
	}
}

// reconnectWorker runs reconnects OUTSIDE connWg so Connect()->connWg.Wait() never self-joins.
func (c *SDKClient) reconnectWorker() {
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

// ensureConnected thread-safely connects if not already connected.
// Allows retry on failure (unlike sync.Once which permanently fails).
func (c *SDKClient) ensureConnected(ctx context.Context) error {
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

// Connect establishes a WebSocket connection to Binance futures market streams.
// Connection-scoped lifecycle: it cancels the previous monitor before creating a new one.
func (c *SDKClient) Connect(ctx context.Context) error {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	return c.connectLocked(ctx)
}

// connectLocked performs the connection. Caller MUST hold connectMu.
// It must NOT lock connectMu itself — doing so would deadlock against the caller.
func (c *SDKClient) connectLocked(ctx context.Context) error {
	// A refused call must not dismantle the healthy generation's recovery owner.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	if c.connected.Load() {
		return nil
	}
	// Step 1: Cancel the previous connection context (stops ALL old goroutines).
	if c.connCancel != nil {
		logger.OrDiscard(c.log).Info("binance sdk: canceling previous connection monitor")
		c.connCancel()
	}

	// Step 2: Wait for the old connection goroutines to exit cleanly.
	c.connWg.Wait()

	logger.OrDiscard(c.log).Info("binance ws: connecting to market streams")
	conn := binancews.NewConnection(0, c.wsBasePath,
		binancews.WithRoute(routeFor(venueBinance, marketTypeFuture, streamClassTrade)),
		binancews.WithReadIdle(readIdleFor(venueBinance, marketTypeFuture)))
	if err := connectMarketStream(ctx, c.ctx, conn, nil); err != nil {
		return fmt.Errorf("binance websocket connect failed: %w", err)
	}
	c.wsConn = conn

	c.markConnectedLocked()

	// Emit event
	NewSDKEvent(EventConnected, venueBinance, marketTypeFuture, c.log).
		WithMetadata("goroutines", runtime.NumGoroutine()).
		Log()

	// Update metrics
	c.metrics.setGoroutines(venueBinance, marketTypeFuture, float64(runtime.NumGoroutine()))
	c.metrics.setHealthState(venueBinance, marketTypeFuture, float64(HealthHealthy))

	logger.OrDiscard(c.log).Info("binance sdk client connected; monitor started")
	return nil
}

// markConnectedLocked records the live-connection state and (re)starts the
// connection-scoped monitor. Caller MUST hold connectMu and have already
// dialed. Shared by the empty-connect path and the combined-URL path.
func (c *SDKClient) markConnectedLocked() {
	c.connected.Store(true)
	c.lastMessageAt.Store(time.Now())
	c.connectTime.Store(time.Now())
	// Arm the resubscribe-grace anchor before (re)starting the monitor so a tick right
	// after connect cannot fire a spurious stall. Set here (not only in the combined path)
	// so the empty-connect and initial-connect paths get the grace too.
	c.resubscribedAt.Store(time.Now())

	// Step 3: Create a new connection-scoped context.
	c.connCtx, c.connCancel = context.WithCancel(c.ctx)

	// Step 4: Start connection-scoped goroutines.
	c.startConnectionMonitor(c.connCtx)
	if c.livenessLedgerEnabled {
		c.startPartitionLivenessWatchdog(c.connCtx)
		// Client-scoped (survives reconnects), started once: detects process-wide
		// CPU starvation for the watchdog's D2 interlock.
		c.heartbeatOnce.Do(func() {
			c.wg.Add(1)
			go func() { defer c.wg.Done(); c.heartbeat.run(c.ctx) }()
		})
	}
	// The application is the sole reconnect owner for EOF, shutdown and renewal.
	c.startSocketErrorWatchers(c.connCtx, c.connectionsLocked())
}

// connectionsLocked returns the current connection set. Caller MUST hold connectMu.
func (c *SDKClient) connectionsLocked() []*binancews.Connection {
	if len(c.partitions) > 0 {
		return append([]*binancews.Connection(nil), c.partitions...)
	}
	if c.wsConn != nil {
		return []*binancews.Connection{c.wsConn}
	}
	return nil
}

// Liveness watchdog tuning (package vars so tests can shrink them). Sized so the
// worst-case detection latency (staleThreshold + one interval = 45s) plus a
// reconnect stays under the 60s RTO. The per-symbol threshold can be this short
// because the trigger is the stale FRACTION of a partition, not any one symbol's
// silence: a live partition's liquid majority keeps the fraction well under 50%
// even while a few illiquid symbols are quiet, so 35s does not flap the fleet.
const (
	livenessWatchdogInterval       = 10 * time.Second
	livenessStaleThresholdMs       = int64(35_000) // per-symbol silence tolerance
	livenessStalePartitionFraction = 0.5           // a partition is "dead" when >50% of its symbols are stale
	livenessMinPartitionSymbols    = 5             // ignore tiny partitions (noise)
)

// startPartitionLivenessWatchdog runs the per-partition liveness watchdog — the additive
// detector for the partition-death case the aggregate monitor is structurally blind to. It
// runs only when c.livenessLedgerEnabled and is
// connection-scoped (canceled and re-created on every reconnect, like the monitor).
func (c *SDKClient) startPartitionLivenessWatchdog(connCtx context.Context) {
	c.connWg.Add(1)
	go func() {
		defer c.connWg.Done()
		t := time.NewTicker(livenessWatchdogInterval)
		defer t.Stop()
		for {
			select {
			case <-connCtx.Done():
				return
			case <-t.C:
				c.checkPartitionLiveness()
			}
		}
	}()
}

// checkPartitionLiveness scans the ledger, exports per-partition stale fractions,
// and triggers a reconnect when a partition's symbols are collectively stale. It
// keys on the stale FRACTION (not any single symbol's silence) so an illiquid
// aggTrade symbol going quiet cannot flap the fleet — the lesson from the
// discarded per-symbol STEP2 watchdog.
func (c *SDKClient) checkPartitionLiveness() {
	if c.reconnecting.Load() {
		return // the resubscribe grace covers ramp-up; don't act mid-reconnect
	}
	nowMs := time.Now().UnixMilli()
	stats := c.ledger.PartitionStaleness(nowMs, livenessStaleThresholdMs)

	// A GAUGE THAT IS NEVER WRITTEN AGAIN KEEPS ASSERTING ITS LAST VALUE.
	//
	// The partition count is not fixed: partitionStreams derives it from the live symbol
	// count, so a fleet that shrinks from 4 partitions to 2 leaves partition="2" and
	// partition="3" in the vector forever, frozen at whatever they read on the last scan
	// that saw them. Frozen at 1.0 that is a permanent false alarm; frozen at 0.0 it is a
	// permanent false all-clear for a partition that does not exist. Either way the series
	// is describing a topology that is gone.
	//
	// Dropping the scope's children before the rescan makes the exported set exactly the
	// partitions the ledger currently knows about. It is O(children) on a handful of
	// series, on a 10s timer.
	c.metrics.resetPartitionStale(venueBinance, marketTypeFuture)

	deadPid, deadStat := -1, health.PartitionStat{}
	anyFresh := false
	for pid, s := range stats {
		c.metrics.setPartitionStale(venueBinance, marketTypeFuture, strconv.Itoa(pid), s.StaleFraction())
		if s.Total < livenessMinPartitionSymbols {
			continue
		}
		if s.StaleFraction() >= livenessStalePartitionFraction {
			if deadPid == -1 {
				deadPid, deadStat = pid, s
			}
		} else {
			anyFresh = true
		}
	}
	// Publish whether a partition is dead so GetHealthState degrades /health.
	c.ledgerDeadPartition.Store(deadPid != -1)
	if deadPid == -1 {
		return
	}

	// Starvation interlock: if NO partition is fresh AND the process was recently
	// descheduled, the staleness is process-wide CPU starvation, not stream death.
	// Reconnecting would only pile load on a starved box; suppress + alert. Streams recover
	// as the process catches up; a genuine death
	// is reconnected once the suppression window lapses. A fresh sibling partition
	// proves the process IS scheduled, so that path always reconnects.
	if !anyFresh && c.heartbeat != nil && c.heartbeat.recentlyStarved(nowMs) {
		c.metrics.incStarvationSuppressed(venueBinance, marketTypeFuture)
		logger.OrDiscard(c.log).Error("binance sdk: all partitions stale and process descheduled; suppressing reconnect",
			"cause", "cpu_starvation")
		return
	}

	logger.OrDiscard(c.log).Warn("binance sdk: partition liveness stall; triggering reconnect",
		"partition", deadPid,
		"stale_symbols", deadStat.Stale,
		"total_symbols", deadStat.Total,
		"stale_fraction", deadStat.StaleFraction())
	c.requestReconnect(fmt.Sprintf("partition %d liveness stall (%d/%d stale)", deadPid, deadStat.Stale, deadStat.Total))
}

// resubscribeVerifyWindow is how long after a resubscribe to wait before checking
// that streams are delivering data.
const resubscribeVerifyWindow = 10 * time.Second

// countVerifiedResubscribe reports how many of symbols have delivered data (ledger
// LastSeen at or after resubAtMs) — "restored" by data flow, not handler attach.
func countVerifiedResubscribe(ledger *health.Ledger, symbols []string, resubAtMs int64) (verified, unverified int) {
	for _, sym := range symbols {
		if last, ok := ledger.LastSeen(sym); ok && last >= resubAtMs {
			verified++
		}
	}
	return verified, len(symbols) - verified
}

// scheduleResubscribeVerification waits one verification window, then counts how many
// resubscribed symbols actually delivered data. The unverified remainder — attached but
// silent — is surfaced via resubscribe_unverified_total, exposing a
// silent partial resubscribe that handler-attach accounting would report as a full
// "restored N/N". Connection-scoped (canceled on teardown), so it cannot leak.
func (c *SDKClient) scheduleResubscribeVerification(connCtx context.Context, symbols []string, resubAtMs int64) {
	c.connWg.Add(1)
	go func() {
		defer c.connWg.Done()
		t := time.NewTimer(resubscribeVerifyWindow)
		defer t.Stop()
		select {
		case <-connCtx.Done():
			return
		case <-t.C:
		}
		verified, unverified := countVerifiedResubscribe(c.ledger, symbols, resubAtMs)
		if unverified > 0 {
			c.metrics.addResubscribeUnverified(venueBinance, marketTypeFuture, float64(unverified))
			logger.OrDiscard(c.log).Warn("binance sdk: resubscribe verification",
				"verified", verified,
				"symbols", len(symbols),
				"window", resubscribeVerifyWindow,
				"unverified", unverified)
		} else {
			logger.OrDiscard(c.log).Info("binance sdk: resubscribe verified",
				"symbols", len(symbols))
		}
	}()
}

// partitionLikelyLive reports whether partition pid is safe to write to: not while
// a reconnect is in progress, and not when the partition's symbols are collectively
// stale per the ledger (a dead partition). Optimistic when the partition has too little
// data to judge (startup), since a genuinely dead partition is reconnected by the watchdog
// anyway. Used by dynamic Subscribe to avoid writing to a dead socket.
func (c *SDKClient) partitionLikelyLive(pid int) bool {
	if c.reconnecting.Load() {
		return false
	}
	stats := c.ledger.PartitionStaleness(time.Now().UnixMilli(), livenessStaleThresholdMs)
	s, ok := stats[pid]
	if !ok || s.Total < livenessMinPartitionSymbols {
		return true
	}
	return s.StaleFraction() < livenessStalePartitionFraction
}

func (c *SDKClient) newFuturesWS(id int) *binancews.Connection {
	return binancews.NewConnection(id, c.wsBasePath,
		binancews.WithRoute(routeFor(venueBinance, marketTypeFuture, streamClassTrade)),
		binancews.WithReadIdle(readIdleFor(venueBinance, marketTypeFuture)))
}

// connectAndResubscribeCombined performs the combined-URL reconnect: it (re)builds one SDK
// stream-client per URL partition, Connect()s each with its full stream slice (frame-free;
// the server subscribes on the handshake), then attaches handlers per symbol via the
// frame-free Execute()+On() ordering mandated by the SDK. Caller MUST hold connectMu (called
// via connectLocked's peers on the reconnect path). Returns the number of symbols whose
// handler was attached on a live connection (the truthful resubscribe count).
//
// The ordering Connect(streams) -> Execute() -> On("message") is load-bearing:
// On() reads GlobalStreamConnectionMap (populated by Connect) and silently
// no-ops if the stream is absent, so handlers MUST be attached only after the
// matching partition's Connect(streams).
func (c *SDKClient) connectAndResubscribeCombined(symbols []string) (int, error) {
	// Steps 1 and 2: stop the old monitor and wait for its goroutines.
	if c.connCancel != nil {
		logger.OrDiscard(c.log).Info("binance sdk: canceling previous connection monitor")
		c.connCancel()
	}
	c.connWg.Wait()

	streams := aggTradeStreams(symbols)
	parts := partitionStreams(streams)
	logger.OrDiscard(c.log).Warn("binance sdk: combined-URL connect",
		"symbols", len(symbols),
		"streams", len(streams),
		"partitions", len(parts),
		"stream_budget", maxStreamsPerConnection,
		"url_char_budget", maxCombinedURLLength)

	// Build + connect one fresh stream-client per partition. partitions[0]
	// becomes c.wsConn so the Subscribe/Unsubscribe/Close paths keep
	// operating on a valid client.
	//
	// HANDLERS ARE ATTACHED PER PARTITION, IMMEDIATELY AFTER ITS OWN ConnectMarket.
	//
	// ConnectMarket makes the server start pushing for EVERY stream in the slice at once, and
	// the SDK DISCARDS a message whose stream has no callback yet — reporting it through a
	// stdlib log.Printf inside the vendored package ("No callback registered for stream …"),
	// which no counter of ours can see. Connecting every partition first and only then
	// attaching every handler leaves partition 0 live and unrouted for the whole duration of
	// each later dial PLUS each preceding attach. Measured in production: 142 dropped
	// aggTrade messages across 51 distinct streams, all inside a single attach — which is
	// also why resubscribe verification reported 406/500 delivering.
	//
	// Attaching inside the loop cannot REMOVE the window: the SDK mandates Connect -> Execute
	// -> On, because On() reads GlobalStreamConnectionMap and silently no-ops when the stream
	// is absent. It bounds it to one partition's own attaches, which at the production budget
	// (6000 URL chars ≈ 350 streams) is the difference between one partition waiting on the
	// other's dial and not.
	clients := make([]*binancews.Connection, 0, len(parts))
	resubscribed := 0
	// symbols[off:off+len(slice)] are this partition's symbols: partitionStreams preserves
	// order and aggTradeStreams is 1:1 with symbols.
	off := 0
	for i, slice := range parts {
		conn := c.newFuturesWS(i)
		part := symbols[off : off+len(slice)]
		off += len(part)
		handlers := make(map[string]binancews.Handler, len(part))
		for _, symbol := range part {
			if c.livenessLedgerEnabled {
				c.ledger.Assign(symbol, i)
			}
			handlers[streamName(symbol)] = c.tradeCallback(symbol)
		}
		logger.OrDiscard(c.log).Info("binance ws: combined-URL connect partition",
			"partition", i+1, "partitions", len(parts), "streams", len(slice))
		// Handlers are installed before the reader starts, closing the window between
		// Connect and On in which frames would otherwise be dropped.
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
	// AFTER the attaches, not before. This arms resubscribedAt, which the verification below
	// measures delivery against; arming it while handlers were still being wired counted
	// symbols as "not delivering" that did not yet have a callback to deliver into.
	c.markConnectedLocked() // arms resubscribedAt (N4) and (re)starts the monitor

	if c.livenessLedgerEnabled {
		// Verify the resubscribe restored DATA FLOW, not just handler attachment
		// — count symbols delivering after this instant within the window.
		c.scheduleResubscribeVerification(c.connCtx, symbols, time.Now().UnixMilli())
	}
	return resubscribed, nil
}

// Subscribe adds a symbol and starts streaming its aggTrade data.
func (c *SDKClient) Subscribe(ctx context.Context, symbol string) error {
	// Canonical source contract: a closed client accepts no work, and a caller that
	// already canceled must see the cancellation rather than a successful attach.
	if c.closed() {
		return fmt.Errorf("%w: binance subscribe %s", streaming.ErrClosed, symbol)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("binance subscribe %s: %w", symbol, err)
	}
	symbol = strings.ToUpper(symbol)

	c.symbolsMu.Lock()
	c.symbols[symbol] = true
	c.symbolsMu.Unlock()

	if err := c.ensureConnected(ctx); err != nil {
		return fmt.Errorf("sdk client not connected, cannot subscribe to %s: %w", symbol, err)
	}

	if err := c.pacer.wait(ctx); err != nil {
		return fmt.Errorf("subscribe pacing aborted for %s: %w", symbol, err)
	}

	// Capture the live client under connectMu so a concurrent reconnect that rebuilds
	// c.wsConn cannot be observed as a torn/stale pointer (the write happens under connectMu
	// on the reconnect path).
	c.connectMu.Lock()
	conn := c.wsConn
	c.connectMu.Unlock()
	if conn == nil {
		return fmt.Errorf("binance websocket not initialized")
	}

	// Don't write a SUBSCRIBE to a dead partition socket. Partition 0 is the
	// dynamic-attach target (c.wsConn); if it's not live, defer — the symbol is already in
	// c.symbols, so the next deterministic reconnect establishes it. A clean deferral
	// instead of a broken-pipe storm.
	if c.livenessLedgerEnabled && !c.partitionLikelyLive(0) {
		c.metrics.incDynamicSubscribeDeferred(venueBinance, marketTypeFuture)
		logger.OrDiscard(c.log).Warn("binance sdk: deferring dynamic subscribe; partition 0 not live",
			"symbol", symbol, "partition", 0)
		return nil
	}

	if err := conn.Subscribe(ctx, streamName(symbol), c.tradeCallback(symbol)); err != nil {
		return fmt.Errorf("failed to subscribe to %s aggTrade: %w", symbol, err)
	}
	if c.livenessLedgerEnabled {
		// Dynamic subscribes attach on partition 0 (c.wsConn); later routing can reach
		// the owning partition.
		c.ledger.Assign(symbol, 0)
	}

	// Emit subscribe event
	NewSDKEvent(EventSubscribe, venueBinance, marketTypeFuture, c.log).
		WithSymbol(symbol).
		Log()

	logger.OrDiscard(c.log).Info("subscribed via sdk", "symbol", symbol, "stream", "@trade")
	return nil
}

// tradeCallback returns the per-symbol message handler. Shared by Subscribe
// (dynamic add) and the combined-URL resubscribe path so both record metrics,
// parse, and buffer identically.
func (c *SDKClient) tradeCallback(symbol string) binancews.Handler {
	return func(data []byte) {
		defer func() {
			if r := recover(); r != nil {
				logger.OrDiscard(c.log).Error("panic in binance trade callback", "symbol", symbol, "panic", r)
				c.tradesDropped.Add(1)
				c.metrics.incTradesDropped(venueBinance, marketTypeFuture)
			}
		}()

		var event tradeEvent
		if err := json.Unmarshal(data, &event); err != nil {
			logger.OrDiscard(c.log).Warn("binance trade callback: malformed frame", "symbol", symbol, "error", err)
			c.tradesDropped.Add(1)
			c.metrics.incTradesDropped(venueBinance, marketTypeFuture)
			return
		}

		now := time.Now()
		c.messagesIn.Add(1)
		c.lastMessageAt.Store(now)
		// Stamp the per-symbol freshness ledger on message arrival (liveness = "this stream
		// is delivering"), reusing the now already computed. Gated so the hot path pays only
		// a bool check when the ledger is off.
		if c.livenessLedgerEnabled && c.ledger != nil {
			c.ledger.Stamp(symbol, now.UnixMilli())
		}

		// Do NOT set LastMessageAgeSeconds here. At ~50k trades/s this is wasted Prometheus
		// work in the hot path; the connection monitor already refreshes this gauge every 10s
		// with the real age computed from c.lastMessageAt.
		c.metrics.incMessagesIn(venueBinance, marketTypeFuture)

		trade := c.toDomainTrade(&event)
		if trade == nil {
			c.tradesDropped.Add(1)
			c.metrics.incTradesDropped(venueBinance, marketTypeFuture)
			return
		}

		// INGESTED means BUFFERED. If tradesIn advanced on every parsed trade, including the
		// ones the buffer refused, ingested and dropped would double-count the same trade and
		// "parsed - dropped" would not be the number that reached the aggregator. The two
		// branches are disjoint and their sum is the parsed count.
		buf := c.buffers.GetOrCreate(symbol)
		if buf.Append(*trade) {
			logger.OrDiscard(c.log).Warn("trade buffer overflow; trade dropped", "symbol", symbol, "cap", c.config.TradeBufferSize)
			c.tradesDropped.Add(1)
			c.metrics.incTradesDropped(venueBinance, marketTypeFuture)
			return
		}
		c.tradesIn.Add(1)
		c.metrics.incTradesIngested(venueBinance, marketTypeFuture)
	}
}

// Unsubscribe removes a symbol and stops its stream.
func (c *SDKClient) Unsubscribe(symbol string) error {
	// Teardown after Close is a no-op: the contract explicitly allows Unsubscribe to be
	// called after the client is gone (close-after-cancel), and the transport may already
	// be nil.
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
		_ = conn.Unsubscribe(context.Background(), strings.ToLower(strings.ReplaceAll(symbol, "/", ""))+"@trade")
	}

	c.buffers.Remove(symbol)
	if c.livenessLedgerEnabled {
		c.ledger.Forget(symbol)
	}
	logger.OrDiscard(c.log).Info("unsubscribed", "symbol", symbol, "stream", "@trade")
	return nil
}

// sortedSubscribedSymbols returns the subscribed symbols in stable sorted order.
// Sorting makes combined-URL partition membership DETERMINISTIC across reconnects
// (O7): without it the list came from map iteration, re-randomizing which symbols
// land in which partition on every reconnect — which makes targeted per-partition
// recovery and "restored" accounting meaningless and is why different symbols are
// hit in different incidents.
func (c *SDKClient) sortedSubscribedSymbols() []string {
	c.symbolsMu.RLock()
	symbols := make([]string, 0, len(c.symbols))
	for sym := range c.symbols {
		symbols = append(symbols, sym)
	}
	c.symbolsMu.RUnlock()
	sort.Strings(symbols)
	return symbols
}

// GetTrades returns and clears all buffered trades for a symbol.
// Only returns trades for symbols that were subscribed; never creates phantom buffers.
func (c *SDKClient) GetTrades(ctx context.Context, symbol string) ([]trade.Trade, error) {
	if c.closed() {
		return nil, fmt.Errorf("%w: binance get trades %s", streaming.ErrClosed, symbol)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	symbol = strings.ToUpper(symbol)
	c.symbolsMu.RLock()
	_, subscribed := c.symbols[symbol]
	c.symbolsMu.RUnlock()
	if !subscribed {
		return nil, fmt.Errorf("%w: binance %s", streaming.ErrNotSubscribed, symbol)
	}
	buf := c.buffers.GetOrCreate(symbol)
	return buf.Flush(), nil
}

// startConnectionMonitor monitors connection health and triggers a reconnect if needed. It
// uses connCtx so the monitor exits cleanly when the connection is canceled.
func (c *SDKClient) startConnectionMonitor(connCtx context.Context) {
	monitoredConnection{
		log:              c.log,
		venue:            venueBinance,
		scope:            marketTypeFuture,
		clientCtx:        c.ctx,
		messagesIn:       &c.messagesIn,
		lastMessageAt:    &c.lastMessageAt,
		resubscribedAt:   &c.resubscribedAt,
		reconnecting:     &c.reconnecting,
		connected:        &c.connected,
		metrics:          c.metrics,
		requestReconnect: c.requestReconnect,
		shutdownLog:      "binance sdk connection monitor shutting down",
		disconnectedLog:  "binance sdk: connection monitor detects disconnected state",
		stalledLog:       "binance sdk: no messages received",
		wg:               &c.connWg,
	}.run(connCtx)
}

// recordResubscribeOutcome surfaces partial reconnect recovery as a first-class
// metric. On the per-symbol resubscribe path, `resubscribed` counts symbols whose
// SUBSCRIBE frame write returned nil; any shortfall vs `total` means those symbols
// were NOT restored (e.g. the connection was RST mid-resubscribe). Counting the
// shortfall makes the mass-kill observable instead of hiding behind a "completed"
// log. `market` is the normalized market label ("future"/"spot").
func recordResubscribeOutcome(metrics Metrics, market string, total, resubscribed int, logs ...*slog.Logger) {
	log := logger.OrDiscard(logs...)
	if incomplete := total - resubscribed; incomplete > 0 {
		metrics.addResubscribeIncomplete(venueBinance, market, float64(incomplete))
		log.Warn("binance sdk: resubscribe incomplete; symbols not restored after reconnect",
			"missing", incomplete, "symbols", total, "market_type", market)
	}
}

// closeAllConnections closes every live WebSocket connection: all combined-URL
// partitions when present, otherwise the single client. Errors are logged, not
// propagated — teardown is best-effort.
func (c *SDKClient) closeAllConnections() {
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
		logger.OrDiscard(c.log).Info("binance ws: closing connection",
			"partition", i+1, "partitions", len(clients))
		if err := conn.Close(); err != nil {
			logger.OrDiscard(c.log).Error("binance ws: error closing connection during reconnect", "error", err)
		}
	}
}

// reconnectDial performs one connect attempt on the reconnect path. With the
// combined-URL flag on it connects all partitions and attaches every handler
// atomically under connectMu, returning the count of live (re)subscribed
// symbols. With the flag off it does the empty Connect() and returns -1, signaling the
// caller to resubscribe per-symbol via the paced Subscribe().
func (c *SDKClient) reconnectDial(symbols []string) (int, error) {
	if c.combinedURLConnectEnabled {
		c.connectMu.Lock()
		defer c.connectMu.Unlock()
		return c.connectAndResubscribeCombined(symbols)
	}
	// Empty-connect path: a bounded dial timeout parented on the client ctx.
	dialCtx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	if err := c.Connect(dialCtx); err != nil {
		return 0, err
	}
	return -1, nil
}

// reconnectAndResubscribe handles reconnection and re-subscription of all symbols.
// On the empty-connect path the Binance SDK v2 does NOT resubscribe to market streams after
// reconnect, so this closes everything, reconnects, and re-subscribes all tracked symbols.
// On the combined-URL path the server subscribes on the handshake (frame-free).
func (c *SDKClient) reconnectAndResubscribe(reason string) {
	if c.reconnecting.Load() {
		// Warn so the skip is visible under log_level=warn.
		logger.OrDiscard(c.log).Warn("binance sdk: reconnect already in progress; skipping")
		return
	}
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()

	// Double-check after acquiring lock
	if c.reconnecting.Load() {
		return
	}
	c.reconnecting.Store(true)
	defer c.reconnecting.Store(false)

	// Check if client is still active
	select {
	case <-c.ctx.Done():
		logger.OrDiscard(c.log).Info("binance sdk: reconnect aborted; client context canceled")
		NewSDKEvent(EventReconnectFailed, venueBinance, marketTypeFuture, c.log).
			WithReason("client_context_canceled").
			Log()
		return
	default:
	}

	c.symbolsMu.RLock()
	symbolCount := len(c.symbols)
	c.symbolsMu.RUnlock()

	logger.OrDiscard(c.log).Warn("binance sdk reconnect triggered",
		"reason", reason,
		"prior_reconnects", c.reconnectCount.Load(),
		"symbols", symbolCount)

	// Emit event and metric
	NewSDKEvent(EventReconnectTriggered, venueBinance, marketTypeFuture, c.log).
		WithReason(reason).
		WithMetadata("reconnect_count", c.reconnectCount.Load()).
		WithMetadata("symbols", symbolCount).
		Log()
	c.metrics.incReconnectAttempt(venueBinance, marketTypeFuture, reconnectReasonLabel(reason))

	// Get subscribed symbols (sorted → deterministic partition membership, O7).
	symbols := c.sortedSubscribedSymbols()

	if len(symbols) == 0 {
		// Warn so the skip is visible under log_level=warn.
		logger.OrDiscard(c.log).Warn("binance sdk: no symbols to resubscribe; skipping reconnect")
		return
	}

	// Mark disconnected before abandoning the transport and its handlers.
	c.connected.Store(false)
	c.lastMessageAt.Store(time.Time{}) // zero time to force health check detection
	c.disconnectTime.Store(time.Now())

	// Decouple gap-recovery enqueue from reconnect SUCCESS: enqueue the window on
	// EVERY exit path below (including a failed Connect() or partial resubscribe),
	// so the affected symbols always heal. The disconnect time is now recorded and
	// symbols is non-empty, so the deferred call has a valid window. Idempotency is
	// guaranteed by the coalescing queue (EnqueueBatch dedups).
	defer c.triggerGapRecovery(symbols)

	// Close existing WebSocket connections (all combined-URL partitions, or the single
	// empty-connect client).
	c.closeAllConnections()

	// Wait for OS cleanup and SDK goroutines to settle.
	logger.OrDiscard(c.log).Info("binance sdk: waiting for socket cleanup", "delay", socketCleanupDelay)
	select {
	case <-time.After(socketCleanupDelay):
	case <-c.ctx.Done():
		return
	}

	// Re-dial in a RETRY LOOP with backoff+jitter instead of a single attempt.
	// connectLocked already canceled the old monitor before the failed
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
			logger.OrDiscard(c.log).Warn("binance sdk: reconnect aborted during retry; client context canceled",
				"attempt", attempt)
			return
		default:
		}

		// reconnectDial connects (combined-URL or empty) and, on the combined path, attaches
		// all handlers atomically under connectMu. n is the number of symbols whose stream
		// is live (combined path); -1 on the empty-connect path, where resubscribe happens
		// per-symbol below.
		n, err := c.reconnectDial(symbols)
		if err == nil {
			resubscribed = n
			break // connected; monitor restarted by markConnectedLocked
		}

		c.connected.Store(false)
		c.metrics.setHealthState(venueBinance, marketTypeFuture, float64(HealthDisconnected))
		c.metrics.incReconnectFailure(venueBinance, marketTypeFuture)
		logger.OrDiscard(c.log).Warn("binance sdk: reconnect attempt failed",
			"attempt", attempt, "error", err, "retry_in", delay)
		NewSDKEvent(EventReconnectFailed, venueBinance, marketTypeFuture, c.log).
			WithError(err).
			WithReason("connect_failed").
			WithMetadata("attempt", attempt).
			Log()

		// ±25% jitter to avoid a thundering herd of reconnects in lockstep.
		jitter := time.Duration(rand.Int63n(int64(delay)/2+1)) - delay/4 //nolint:gosec // G404: reconnect jitter only; cryptographic randomness is not required.
		sleep := delay + jitter
		if sleep < 0 {
			sleep = delay
		}
		select {
		case <-time.After(sleep):
		case <-c.ctx.Done():
			logger.OrDiscard(c.log).Warn("binance sdk: reconnect aborted during backoff; client context canceled",
				"attempt", attempt)
			return
		}

		// Exponential backoff, capped.
		delay = time.Duration(float64(delay) * reconnectBackoffRate)
		if delay > reconnectBackoffMax {
			delay = reconnectBackoffMax
		}
	}

	c.reconnectCount.Add(1)

	if resubscribed < 0 {
		// Empty-connect path: re-subscribe per-symbol via the paced Subscribe(). A nil
		// Subscribe means the frame was written, not that the stream is confirmed
		// live, so this count is best-effort.
		logger.OrDiscard(c.log).Info("binance sdk: resubscribing symbols", "symbols", len(symbols))
		resubscribed = 0
		for _, symbol := range symbols {
			if err := c.Subscribe(c.ctx, symbol); err != nil {
				logger.OrDiscard(c.log).Error("binance sdk: failed to resubscribe after reconnect",
					"symbol", symbol, "error", err)
			} else {
				resubscribed++
			}
		}
	}

	// Surface partial restore as a metric + explicit warning. On the combined-URL
	// path "resubscribed" means the handler is attached on the live connection (D3).
	recordResubscribeOutcome(c.metrics, marketTypeFuture, len(symbols), resubscribed, c.log)

	// Promote the reconnect-outcome log to Warn so it is visible under the production
	// log_level=warn (Info is suppressed there, hiding recovery from view).
	// Worded neutrally ("restored", not "completed") so a partial restore never reads
	// as success.
	logger.OrDiscard(c.log).Warn("binance sdk reconnect finished",
		"restored", resubscribed,
		"symbols", len(symbols),
		"total_reconnects", c.reconnectCount.Load())

	NewSDKEvent(EventReconnectSuccess, venueBinance, marketTypeFuture, c.log).
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

// HealthCheck returns true if connected and receiving messages within the timeout. It uses
// the granular health state instead of a simple boolean.
func (c *SDKClient) HealthCheck() bool {
	state := c.GetHealthState()
	return state == HealthHealthy
}

// GetHealthState returns the current granular health state.
func (c *SDKClient) GetHealthState() HealthState {
	if c.reconnecting.Load() {
		return HealthReconnecting
	}
	if !c.connected.Load() {
		return HealthDisconnected
	}

	// A dead partition (per the ledger watchdog) degrades health even when the aggregate
	// last-message is fresh — a live sibling partition keeps that aggregate warm, which is
	// the "aggregation lie" at the health surface.
	if c.livenessLedgerEnabled && c.ledgerDeadPartition.Load() {
		return HealthStalled
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
func (c *SDKClient) ConnectionStats() streaming.ConnectionStats {
	c.symbolsMu.RLock()
	symCount := len(c.symbols)
	c.symbolsMu.RUnlock()

	connTime, _ := c.connectTime.Load().(time.Time)
	uptime := time.Since(connTime)

	// Surface the last disconnect so the periodic-backstop proof metric can exclude
	// disconnect-triggered enqueues. Zero time until the first disconnect.
	lastDisconnect, _ := c.disconnectTime.Load().(time.Time)

	var lastMessageAgo time.Duration
	c.connectMu.Lock()
	conn := c.wsConn
	c.connectMu.Unlock()
	if conn != nil {
		if last := conn.LastMessageAt(); !last.IsZero() {
			lastMessageAgo = time.Since(last)
		}
	}

	return streaming.ConnectionStats{
		ExchangeID:     venueBinance,
		MarketType:     marketTypeFuture,
		Connected:      c.connected.Load(),
		Reconnecting:   c.reconnecting.Load(),
		ReconnectCount: saturatingInt(c.reconnectCount.Load()),
		Symbols:        symCount,
		MessagesIn:     c.messagesIn.Load(),
		TradesIn:       c.tradesIn.Load(),
		TradesDropped:  c.tradesDropped.Load(),
		LastPongAgo:    lastMessageAgo,
		Uptime:         uptime,
		Healthy:        c.HealthCheck(),
		HealthState:    c.GetHealthState().String(),
		LastDisconnect: lastDisconnect,
	}
}

// GetBufferFillRatio returns the fill ratio for a specific symbol's buffer.
func (c *SDKClient) GetBufferFillRatio(symbol string) float64 {
	if c.bufCap <= 0 {
		return 0
	}
	buf := c.buffers.GetOrCreate(symbol)
	return float64(buf.Len()) / float64(c.bufCap)
}

// Type returns the client type.
func (c *SDKClient) Type() streaming.ClientType {
	return streaming.ClientTypeNative
}

// IsSharedSubscriptionClient returns true (single combined stream).
func (c *SDKClient) IsSharedSubscriptionClient() bool {
	return true
}

// Close disconnects all streams and cleans up resources. It cancels both the client and
// connection contexts and waits for all goroutines.
func (c *SDKClient) Close() error {
	logger.OrDiscard(c.log).Info("binance sdk: closing client")

	// Step 1: Cancel client lifecycle → stops all goroutines
	c.cancel()

	// Step 2: Cancel connection context (redundant but defensive).
	// Read connCancel under connectMu — connectLocked writes it under the same lock from the
	// reconnect worker, so an unguarded read here is a data race.
	c.connectMu.Lock()
	// A dial already publishing under this mutex must finish before the final
	// disconnected state. Cancellation prevents any subsequent dial admission.
	c.connected.Store(false)
	cancel := c.connCancel
	c.connectMu.Unlock()
	if cancel != nil {
		cancel()
	}

	// Step 3: Release raw transports before joining connection goroutines.
	c.closeAllConnections()

	// Step 4: Wait for ALL goroutines to exit.
	c.connWg.Wait()
	c.wg.Wait()

	// Update metrics
	c.metrics.setGoroutines(venueBinance, marketTypeFuture, float64(runtime.NumGoroutine()))
	c.metrics.setHealthState(venueBinance, marketTypeFuture, float64(HealthDisconnected))

	NewSDKEvent(EventDisconnected, venueBinance, marketTypeFuture, c.log).
		WithReason("client_close").
		Log()

	logger.OrDiscard(c.log).Info("binance sdk client closed")
	return nil
}

// toDomainTrade converts one aggregate-trade payload to a trade.Trade.
func (c *SDKClient) toDomainTrade(event *tradeEvent) *trade.Trade {
	if event == nil || event.Symbol == "" || event.Price == "" || event.Quantity == "" {
		return nil
	}

	price, err := strconv.ParseFloat(event.Price, 64)
	if err != nil {
		logger.OrDiscard(c.log).Error("failed to parse trade price", "value", event.Price, "error", err)
		return nil
	}
	amount, err := strconv.ParseFloat(event.Quantity, 64)
	if err != nil {
		logger.OrDiscard(c.log).Error("failed to parse trade quantity", "value", event.Quantity, "error", err)
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
		Timestamp: event.TradeTime,
		Price:     price,
		Amount:    amount,
		Side:      side,
	}
}

// triggerGapRecovery triggers gap recovery for all symbols after a reconnect.
// Uses EnqueueBatch for efficient batch recovery.
func (c *SDKClient) triggerGapRecovery(symbols []string) {
	if c.config.GapRecovery == nil {
		return
	}

	dt, ok := c.disconnectTime.Load().(time.Time)
	if !ok || dt.IsZero() {
		logger.OrDiscard(c.log).Warn("binance sdk: gap recovery skipped; disconnect time not recorded")
		return
	}
	reconnectTime := time.Now()

	// Enqueue with the INTERNAL market type ("future") so the gap-recovery
	// coalescing-queue key and circuit-breaker scope match the periodic reconciliation
	// and storage paths (which use market.FromConfig). The worker normalizes the value
	// for storage, so this only unifies the key — it does not change persistence.
	marketType, err := market.FromConfig(c.config.MarketType)
	if err != nil {
		logger.OrDiscard(c.log).Error("binance sdk: gap recovery skipped; invalid market type",
			"market_type", c.config.MarketType, "error", err)
		return
	}

	logger.OrDiscard(c.log).Info("binance sdk: triggering gap recovery",
		"symbols", len(symbols),
		"disconnect", dt.Format(time.RFC3339),
		"reconnect", reconnectTime.Format(time.RFC3339))

	c.config.GapRecovery.EnqueueBatch(venueBinance, marketType.String(), symbols, dt, reconnectTime)

	NewSDKEvent(EventGapRecoveryTriggered, venueBinance, marketTypeFuture, c.log).
		WithMetadata("symbols", len(symbols)).
		WithMetadata("disconnect_time", dt.Format(time.RFC3339)).
		WithMetadata("reconnect_time", reconnectTime.Format(time.RFC3339)).
		Log()
}
