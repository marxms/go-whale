package ingestion

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// HybridClientState tracks the state of each hybrid client.
type HybridClientState struct {
	Client     Source
	Symbol     string
	Exchange   string
	MarketType market.MarketType
	Key        string // exchange:marketType:symbol

	// inFlight is true while a worker is draining THIS client. It is the invariant that
	// makes trade deduplication sound.
	//
	// The dedup downstream is a monotonic WATERMARK ("drop any trade id <= lastID"). That
	// is only correct if a symbol's batches are processed IN ORDER. Re-enqueueing every client
	// on every tick regardless of whether one was already being processed, with several
	// workers draining the queue, lets two batches of the same symbol overlap: the batch
	// holding the HIGHER ids could commit its watermark first, and the batch holding the
	// LOWER ids was then discarded wholesale as "already seen" despite never having been seen.
	//
	// That is a silent, permanent VOLUME LOSS: no error, no log, no metric — the bucket
	// simply closes lighter than the exchange's (sparse, always a deficit, 0.0005%-0.5%
	// against Binance futures).
	//
	// One in-flight batch per client removes the interleave BY CONSTRUCTION, so the
	// watermark's precondition holds instead of being hoped for. It also stops redundant
	// GetTrades calls from a re-enqueue that ignored work already in flight.
	inFlight atomic.Bool
}

// HybridManager manages streaming through native WebSocket clients.
type HybridManager struct {
	handler       TradeHandler
	limitsByRoute map[string]StreamLimits

	clients map[string]*HybridClientState // key: exchange:marketType:symbol
	mu      sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Worker pool for processing trades
	numWorkers   int
	processQueue chan *HybridClientState

	// Adaptive polling configuration
	minProcessDelay time.Duration // Minimum delay (high activity / euforia)
	maxProcessDelay time.Duration // Maximum delay (low activity)
	currentDelay    time.Duration // Current adaptive delay
	delayMu         sync.RWMutex  // Protects currentDelay

	// Gap recovery callback (DEPRECATED — use gapRecovery)
	recoveryCallback streaming.RecoveryCallback

	// Bounded worker pool for gap recovery
	gapRecovery streaming.GapRecoveryQueue

	// Configurable trade buffer size (passed to native clients)
	tradeBufferSize int

	// factories is the native-client catalog assembled by the composition root
	// and handed in through the constructor. It is copied on construction and never
	// mutated afterwards, so there is no registration order and no package-level
	// state: a fake enters through the same argument as a real adapter.
	factories map[string]streaming.ClientFactory

	log *slog.Logger
}

func (m *HybridManager) hasNativeClient(exchangeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.factories[exchangeID]
	return ok
}

// StreamLimits bounds one exchange/market route's connections and symbols.
type StreamLimits struct {
	MaxSymbolsPerInstance int
	MaxConnectionsPerIP   int
}

const (
	// Adaptive polling thresholds.
	defaultMinProcessDelay = 50 * time.Millisecond  // Fast polling during market euphoria
	defaultMaxProcessDelay = 200 * time.Millisecond // Conservative idle-period polling
	bufferHighThreshold    = 0.7                    // 70% full = increase polling frequency
	bufferMedThreshold     = 0.3                    // 30% full = moderate polling
)

// NewHybridManager creates a manager that uses native clients when available.
//
// factories is the native-client catalog, owned by the composition root. It is copied
// here so later mutation by the caller cannot change which adapter a route resolves to.
// A nil catalog is valid: Subscribe then reports that no native client is available.
func NewHybridManager(handler TradeHandler, numWorkers int, factories map[string]streaming.ClientFactory, logs ...*slog.Logger) *HybridManager {
	ctx, cancel := context.WithCancel(context.Background())

	if numWorkers < 1 {
		numWorkers = 16
	}

	catalog := make(map[string]streaming.ClientFactory, len(factories))
	for exchange, factory := range factories {
		catalog[exchange] = factory
	}

	return &HybridManager{
		limitsByRoute:   make(map[string]StreamLimits),
		handler:         handler,
		clients:         make(map[string]*HybridClientState),
		ctx:             ctx,
		cancel:          cancel,
		numWorkers:      numWorkers,
		processQueue:    make(chan *HybridClientState, 100000),
		minProcessDelay: defaultMinProcessDelay, // 50ms - for market euphoria
		maxProcessDelay: defaultMaxProcessDelay, // 500ms - for idle periods
		currentDelay:    defaultMaxProcessDelay, // Start conservative
		factories:       catalog,
		log:             logger.OrDiscard(logs...),
	}
}

func routeKey(exchangeID string, marketType market.MarketType) string {
	return fmt.Sprintf("%s:%s", exchangeID, marketType)
}

// SetStreamLimits records the limits for one route.
func (m *HybridManager) SetStreamLimits(exchangeID string, marketType market.MarketType, limits StreamLimits) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.limitsByRoute[routeKey(exchangeID, marketType)] = limits
}

func (m *HybridManager) getLimitsForRoute(exchangeID string, marketType market.MarketType) StreamLimits {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if limits, ok := m.limitsByRoute[routeKey(exchangeID, marketType)]; ok {
		return limits
	}
	return StreamLimits{}
}

// Start launches the processing workers. ctx is the process lifetime context; Stop cancels
// the derived run context, so either cancellation terminates the workers.
func (m *HybridManager) Start(ctx context.Context) {
	logger.OrDiscard(m.log).Info("hybrid manager starting", "workers", m.numWorkers)

	runCtx, cancel := context.WithCancel(ctx)
	oldCancel := m.cancel
	m.ctx, m.cancel = runCtx, cancel
	oldCancel()

	// Start workers
	for i := 0; i < m.numWorkers; i++ {
		m.wg.Add(1)
		go m.worker(runCtx, i)
	}

	// Start the scheduler
	m.wg.Add(1)
	go m.scheduler(runCtx)
}

// SetRecoveryCallback configures the gap recovery callback that will be
// invoked when any streaming client successfully reconnects after a disconnection.
//
// This should be called during initialization, before subscribing to any symbols.
func (m *HybridManager) SetRecoveryCallback(callback streaming.RecoveryCallback) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recoveryCallback = callback
	logger.OrDiscard(m.log).Info("hybrid manager: gap recovery callback configured")
}

// SetGapRecovery configures the bounded worker pool for gap recovery.
// When set, this takes precedence over the legacy RecoveryCallback.
func (m *HybridManager) SetGapRecovery(gr streaming.GapRecoveryQueue) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gapRecovery = gr
	logger.OrDiscard(m.log).Info("hybrid manager: gap recovery manager configured")
}

// SetTradeBufferSize sets the per-symbol trade buffer size for native clients.
func (m *HybridManager) SetTradeBufferSize(size int) {
	m.tradeBufferSize = size
}

// Subscribe adds a symbol using the native client.
func (m *HybridManager) Subscribe(ctx context.Context, symbol, exchangeID string, marketType market.MarketType) error {
	key := fmt.Sprintf("%s:%s:%s", exchangeID, marketType, symbol)

	// Check if already subscribed
	m.mu.RLock()
	if _, exists := m.clients[key]; exists {
		m.mu.RUnlock()
		logger.OrDiscard(m.log).Debug("hybrid manager: symbol already subscribed", "key", key)
		return nil
	}
	m.mu.RUnlock()

	var client Source
	var err error

	if m.hasNativeClient(exchangeID) {
		client, err = m.createNativeClient(exchangeID, marketType)
		if err != nil {
			return fmt.Errorf("native client failed for %s: %w", exchangeID, err)
		}
		logger.OrDiscard(m.log).Info("hybrid manager: using native client", "key", key)
	} else {
		return fmt.Errorf("no native client available for %s", exchangeID)
	}

	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	// Subscribe on the client
	if err := client.Subscribe(ctx, symbol); err != nil {
		// Do NOT close the client on subscribe failure. Every client in this module is a SHARED
		// connection serving many symbols, so closing one on a single symbol's failure would
		// destroy every other subscription on that exchange.
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	// Double-check after network operations to avoid duplicate entries when concurrent subscribe happens.
	m.mu.Lock()
	if existingState, exists := m.clients[key]; exists {
		m.mu.Unlock()
		// If the new client is a different instance (e.g., a REST adapter), clean it up.
		// If it's the same instance (pooled native client), the pool Subscribe was a no-op.
		if client != existingState.Client {
			// Teardown: both errors are unactionable — we are discarding this client
			// either way, and a failing Close means the socket is already gone. Discarded
			// EXPLICITLY rather than implicitly so the decision is visible, not omitted.
			_ = client.Unsubscribe(symbol)
			_ = client.Close()
		}
		logger.OrDiscard(m.log).Debug("hybrid manager: symbol already subscribed (concurrent)", "key", key)
		return nil
	}

	// Save state
	state := &HybridClientState{
		Client:     client,
		Symbol:     symbol,
		Exchange:   exchangeID,
		MarketType: marketType,
		Key:        key,
	}
	m.clients[key] = state
	m.mu.Unlock()

	logger.OrDiscard(m.log).Info("hybrid manager: symbol subscribed", "key", key, "client_type", client.Type())
	return nil
}

// createNativeClient creates a native WebSocket client (BingX, Binance).
func (m *HybridManager) createNativeClient(exchangeID string, marketType market.MarketType) (Source, error) {
	m.mu.RLock()
	factory, ok := m.factories[exchangeID]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no native factory for %s", exchangeID)
	}

	limits := m.getLimitsForRoute(exchangeID, marketType)

	// Convert the market type to the external format (future → swap for binance/bingx)
	venue, err := market.ParseExchange(exchangeID)
	if err != nil {
		return nil, err
	}
	adapter := market.NewAdapter(venue)
	externalMarketType, err := adapter.ToExchange(marketType)
	if err != nil {
		return nil, err
	}

	config := streaming.ClientConfig{
		ExchangeID:              exchangeID,
		MarketType:              externalMarketType, // Use external format (spot/swap)
		MaxSymbolsPerConnection: limits.MaxSymbolsPerInstance,
		MaxConnectionsPerIP:     limits.MaxConnectionsPerIP,
		RecoveryCallback:        m.recoveryCallback, // Legacy gap recovery callback
		GapRecovery:             m.gapRecovery,      // Bounded worker pool
		TradeBufferSize:         m.tradeBufferSize,  // Configurable buffer per symbol
		// URLs and ping intervals are defined inside the clients
	}

	client, err := factory(config)
	if err != nil {
		return nil, err
	}

	// Native clients connect automatically on creation; the real Subscribe is called later in
	// HybridManager.Subscribe.
	return client, nil
}

// Unsubscribe removes a symbol.
func (m *HybridManager) Unsubscribe(symbol, exchangeID string, marketType market.MarketType) error {
	key := fmt.Sprintf("%s:%s:%s", exchangeID, marketType, symbol)

	m.mu.Lock()
	state, exists := m.clients[key]
	if !exists {
		m.mu.Unlock()
		return nil
	}
	delete(m.clients, key)

	// Check if any other subscription shares the same client (pooled connections)
	clientShared := false
	for _, other := range m.clients {
		if other.Client == state.Client {
			clientShared = true
			break
		}
	}
	m.mu.Unlock()

	// Unsubscribe the individual symbol from the client/pool
	// Teardown — see the note above: unactionable, discarded explicitly.
	_ = state.Client.Unsubscribe(symbol)

	// Only close the client if no other subscriptions reference it
	// and if it is not a shared subscription client (e.g. native connection pool)
	// whose lifecycle is managed externally.
	if !clientShared {
		if shared, ok := state.Client.(SharedSource); !ok || !shared.IsSharedSubscriptionClient() {
			_ = state.Client.Close()
		}
	}

	logger.OrDiscard(m.log).Info("hybrid manager: symbol removed", "key", key, "shared_client", clientShared)
	return nil
}

// scheduler feeds the processing queue with an adaptive delay.
func (m *HybridManager) scheduler(ctx context.Context) {
	defer m.wg.Done()

	// Separate goroutine to compute the adaptive delay every 500ms
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.adaptiveDelayCalculator(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Get current adaptive delay
		m.delayMu.RLock()
		delay := m.currentDelay
		m.delayMu.RUnlock()

		time.Sleep(delay)
		m.scheduleClients()
	}
}

// adaptiveDelayCalculator adjusts the delay based on the buffers' fill ratio.
func (m *HybridManager) adaptiveDelayCalculator(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			newDelay := m.calculateAdaptiveDelay()

			m.delayMu.Lock()
			oldDelay := m.currentDelay
			m.currentDelay = newDelay
			m.delayMu.Unlock()

			// Log significant delay changes
			if newDelay != oldDelay {
				logger.OrDiscard(m.log).Debug("hybrid manager: adaptive delay adjusted",
					"previous", oldDelay, "current", newDelay)
			}
		}
	}
}

// calculateAdaptiveDelay computes the delay from the buffer fill ratio.
func (m *HybridManager) calculateAdaptiveDelay() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.clients) == 0 {
		return m.maxProcessDelay
	}

	// Sample buffer fill ratios
	var totalFill float64
	var samples int

	for _, state := range m.clients {
		// Check whether the client implements BufferFillAware
		if fillAware, ok := state.Client.(BackpressureSource); ok {
			fill := fillAware.GetBufferFillRatio(state.Symbol)
			totalFill += fill
			samples++
		}
	}

	// If no client supports a fill ratio, use the conservative delay
	if samples == 0 {
		return m.maxProcessDelay
	}

	avgFill := totalFill / float64(samples)

	// Determine the delay from the fill ratio
	switch {
	case avgFill >= bufferHighThreshold:
		// Very full buffers (70%+) — market euphoria: use the minimum delay to drain quickly
		return m.minProcessDelay

	case avgFill >= bufferMedThreshold:
		// Partially full buffers (30-70%) — moderate activity: interpolate between min and max
		fillRange := bufferHighThreshold - bufferMedThreshold
		fillPosition := (avgFill - bufferMedThreshold) / fillRange
		delayRange := float64(m.maxProcessDelay - m.minProcessDelay)
		return m.maxProcessDelay - time.Duration(fillPosition*delayRange)

	default:
		// Empty buffers (<30%) — save CPU
		return m.maxProcessDelay
	}
}

func (m *HybridManager) scheduleClients() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, state := range m.clients {
		// Skip clients a worker is already draining. Without this, the same symbol could be
		// processed by two workers at once and the watermark dedup downstream would discard
		// legitimate trades — see the comment on HybridClientState.inFlight.
		if !state.inFlight.CompareAndSwap(false, true) {
			continue
		}
		select {
		case m.processQueue <- state:
		default:
			// Not enqueued, so release the claim — otherwise this client would be marked
			// in-flight forever and never processed again.
			state.inFlight.Store(false)
			// Queue full — log to detect sustained saturation
			logger.OrDiscard(m.log).Error("hybrid manager: process queue full; skipping client",
				"key", state.Key, "capacity", cap(m.processQueue))
		}
	}
}

// worker processes trades from the clients.
func (m *HybridManager) worker(ctx context.Context, id int) {
	defer m.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case state := <-m.processQueue:
			m.processClient(ctx, id, state)
		}
	}
}

func (m *HybridManager) processClient(ctx context.Context, _ int, state *HybridClientState) {
	// Release the in-flight claim on EVERY exit path, including the early returns below.
	// A leaked claim would silently stop this symbol from ever being drained again.
	defer state.inFlight.Store(false)

	// Get trades from client
	trades, err := state.Client.GetTrades(ctx, state.Symbol)
	if err != nil {
		if ctx.Err() == nil {
			logger.OrDiscard(m.log).Debug("hybrid manager: get trades failed", "key", state.Key, "error", err)
		}
		return
	}

	if len(trades) == 0 {
		return // No new trades
	}

	// Use HandleTradesWithContext to pass exchange and marketType
	if err := m.handler.HandleTradesWithContext(state.Exchange, string(state.MarketType), state.Symbol, trades); err != nil {
		logger.OrDiscard(m.log).Error("hybrid manager: handle trades failed", "key", state.Key, "error", err)
	}
}

// Stop stops the manager.
func (m *HybridManager) Stop() {
	logger.OrDiscard(m.log).Info("hybrid manager stopping")

	m.cancel()

	// Close all clients
	m.mu.Lock()
	for key, state := range m.clients {
		// Shutdown teardown — unactionable, discarded explicitly.
		_ = state.Client.Unsubscribe(state.Symbol)
		_ = state.Client.Close()
		delete(m.clients, key)
	}
	m.mu.Unlock()

	m.wg.Wait()
	logger.OrDiscard(m.log).Info("hybrid manager stopped")
}

// GetStats returns statistics.
func (m *HybridManager) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.delayMu.RLock()
	currentDelay := m.currentDelay
	m.delayMu.RUnlock()

	stats := map[string]interface{}{
		"total_clients":    len(m.clients),
		"workers":          m.numWorkers,
		"current_delay_ms": currentDelay.Milliseconds(),
		"min_delay_ms":     m.minProcessDelay.Milliseconds(),
		"max_delay_ms":     m.maxProcessDelay.Milliseconds(),
		"adaptive_polling": true,
	}

	// Count clients and calculate average fill ratio
	nativeCount := 0
	var totalFill float64
	var fillSamples int

	for _, state := range m.clients {
		nativeCount++
		// Get fill ratio if supported
		if fillAware, ok := state.Client.(BackpressureSource); ok {
			totalFill += fillAware.GetBufferFillRatio(state.Symbol)
			fillSamples++
		}
	}

	stats["native_clients"] = nativeCount

	if fillSamples > 0 {
		stats["avg_buffer_fill_ratio"] = totalFill / float64(fillSamples)
	}

	return stats
}

// ListSubscriptions lists every active subscription.
func (m *HybridManager) ListSubscriptions() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	subs := make([]string, 0, len(m.clients))
	for key := range m.clients {
		subs = append(subs, key)
	}
	return subs
}

// GetConnectionStats collects ConnectionStats from all unique native clients.
func (m *HybridManager) GetConnectionStats() []streaming.ConnectionStats {
	m.mu.RLock()
	clients := make([]Source, 0, len(m.clients))
	for _, state := range m.clients {
		clients = append(clients, state.Client)
	}
	m.mu.RUnlock()

	// Deduplicate: multiple symbols share the same underlying client (via pool).
	// Use pointer address as identity key.
	seen := make(map[uintptr]bool)
	var stats []streaming.ConnectionStats

	for _, client := range clients {
		if sp, ok := client.(StatsSource); ok {
			// Use interface pointer comparison via fmt trick
			key := reflect.ValueOf(client).Pointer()
			if seen[key] {
				continue
			}
			seen[key] = true
			stats = append(stats, sp.ConnectionStats())
		}
	}
	return stats
}
