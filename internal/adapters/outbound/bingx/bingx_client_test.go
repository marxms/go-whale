package bingx

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/market"
)

// TestBingXClientHeartbeatConfiguration verifies that heartbeat constants are correctly set.
func TestBingXClientHeartbeatConfiguration(t *testing.T) {
	expectedTimeout := 90 * time.Second
	expectedCheckPeriod := 10 * time.Second

	if HeartbeatTimeout != expectedTimeout {
		t.Errorf("HeartbeatTimeout = %v, expected %v", HeartbeatTimeout, expectedTimeout)
	}

	if HeartbeatCheckPeriod != expectedCheckPeriod {
		t.Errorf("HeartbeatCheckPeriod = %v, expected %v", HeartbeatCheckPeriod, expectedCheckPeriod)
	}
}

// TestBingXClientCreation verifies client is created with correct initial state.
// TestBingXTimersFromPolicy proves the health and reconnect timers derive from the
// documented venue policy instead of a second hardcoded 60 s. BingX documents a server
// ping every 5 s; 90 s is the operational margin (at least 3x the ping), and the health
// verdict must move with that threshold.
func TestBingXTimersFromPolicy(t *testing.T) {
	policy, ok := market.ConnPolicyFor("bingx", market.Future)
	if !ok {
		t.Fatal("no connection policy for bingx future")
	}
	if policy.ServerPing <= 0 {
		t.Fatalf("policy ServerPing = %v, want the documented 5 s server ping", policy.ServerPing)
	}
	if HeartbeatTimeout < 3*policy.ServerPing {
		t.Fatalf("HeartbeatTimeout = %v, want at least 3x the %v server ping", HeartbeatTimeout, policy.ServerPing)
	}

	client := NewBingXClient(ClientConfig{
		WSURL:      "wss://test.example.com",
		ExchangeID: "bingx",
		MarketType: "swap",
	})
	t.Cleanup(func() { client.cancel() })
	client.connMu.Lock()
	client.connected = true
	client.connMu.Unlock()

	// Just inside the threshold: healthy.
	client.lastPongMu.Lock()
	client.lastPong = time.Now().Add(-HeartbeatTimeout + time.Second)
	client.lastPongMu.Unlock()
	if !client.HealthCheck() {
		t.Fatalf("HealthCheck = false at HeartbeatTimeout-1s; the verdict must follow the policy threshold")
	}

	// Just past the threshold: unhealthy. A hardcoded 60 s answered healthy here on the
	// 90 s policy, masking a dead connection for up to 30 s.
	client.lastPongMu.Lock()
	client.lastPong = time.Now().Add(-HeartbeatTimeout - time.Second)
	client.lastPongMu.Unlock()
	if client.HealthCheck() {
		t.Fatalf("HealthCheck = true at HeartbeatTimeout+1s; the verdict must follow the policy threshold")
	}
}

func TestBingXClientCreation(t *testing.T) {
	config := ClientConfig{
		WSURL:      "wss://test.example.com",
		ExchangeID: "bingx",
		MarketType: "swap",
	}

	client := NewBingXClient(config)

	if client == nil {
		t.Fatal("NewBingXClient returned nil")
	}

	// connCancel should be nil before Connect (no connection-scoped context yet).
	if client.connCancel != nil {
		t.Error("connCancel should be nil before Connect")
	}

	// Check that lastPong is zero value before Connect
	if !client.lastPong.IsZero() {
		t.Error("lastPong should be zero before Connect")
	}

	// PingInterval should default to 1800 (30 minutes)
	if client.config.PingInterval != 1800 {
		t.Errorf("PingInterval = %d, expected 1800", client.config.PingInterval)
	}

	// Clean up
	client.cancel()
}

// TestBingXClientLastPongUpdate verifies that lastPong can be properly updated.
func TestBingXClientLastPongUpdate(t *testing.T) {
	config := ClientConfig{
		WSURL:      "wss://test.example.com",
		ExchangeID: "bingx",
		MarketType: "swap",
	}

	client := NewBingXClient(config)

	// Set initial lastPong to old time
	oldTime := time.Now().Add(-1 * time.Hour)
	client.lastPongMu.Lock()
	client.lastPong = oldTime
	client.lastPongMu.Unlock()

	// Verify initial state
	client.lastPongMu.RLock()
	initialLastPong := client.lastPong
	client.lastPongMu.RUnlock()

	if !initialLastPong.Equal(oldTime) {
		t.Errorf("Initial lastPong = %v, expected %v", initialLastPong, oldTime)
	}

	// Update lastPong
	beforeUpdate := time.Now()
	client.lastPongMu.Lock()
	client.lastPong = time.Now()
	client.lastPongMu.Unlock()
	afterUpdate := time.Now()

	client.lastPongMu.RLock()
	newLastPong := client.lastPong
	client.lastPongMu.RUnlock()

	if newLastPong.Before(beforeUpdate) || newLastPong.After(afterUpdate) {
		t.Errorf("lastPong update timing issue: got %v, expected between %v and %v", newLastPong, beforeUpdate, afterUpdate)
	}

	// Clean up
	client.cancel()
}

// TestBingXClientHealthCheckWithStaleHeartbeat verifies HealthCheck returns false when heartbeat is stale.
func TestBingXClientHealthCheckWithStaleHeartbeat(t *testing.T) {
	config := ClientConfig{
		WSURL:      "wss://test.example.com",
		ExchangeID: "bingx",
		MarketType: "swap",
	}

	client := NewBingXClient(config)

	// Simulate connected state
	client.connMu.Lock()
	client.connected = true
	client.connMu.Unlock()

	// Set recent lastPong - should be healthy
	client.lastPongMu.Lock()
	client.lastPong = time.Now()
	client.lastPongMu.Unlock()

	if !client.HealthCheck() {
		t.Error("HealthCheck should return true when connected with recent heartbeat")
	}

	// Set stale lastPong (>HeartbeatTimeout ago) - should be unhealthy
	client.lastPongMu.Lock()
	client.lastPong = time.Now().Add(-(HeartbeatTimeout + 5*time.Second))
	client.lastPongMu.Unlock()

	if client.HealthCheck() {
		t.Error("HealthCheck should return false when heartbeat is stale (>HeartbeatTimeout)")
	}

	// Clean up
	client.cancel()
}

// TestBingXClientHealthCheckDisconnected verifies HealthCheck returns false when disconnected.
func TestBingXClientHealthCheckDisconnected(t *testing.T) {
	config := ClientConfig{
		WSURL:      "wss://test.example.com",
		ExchangeID: "bingx",
		MarketType: "swap",
	}

	client := NewBingXClient(config)

	if client.HealthCheck() {
		t.Error("HealthCheck should return false when not connected")
	}

	// Clean up
	client.cancel()
}

// TestBingXClientConnectionScopedContext verifies the connCtx/connCancel lifecycle.
func TestBingXClientConnectionScopedContext(t *testing.T) {
	config := ClientConfig{
		WSURL:      "wss://test.example.com",
		ExchangeID: "bingx",
		MarketType: "swap",
	}

	client := NewBingXClient(config)

	// connCancel should be nil initially (no connection yet)
	if client.connCancel != nil {
		t.Error("connCancel should be nil before connection")
	}

	// Simulate creating a connection-scoped context (as doConnect does)
	client.connCtx, client.connCancel = context.WithCancel(client.ctx)

	// connCancel should not be nil
	if client.connCancel == nil {
		t.Error("connCancel should not be nil after creation")
	}

	// Context should not be done
	select {
	case <-client.connCtx.Done():
		t.Error("connCtx should not be done before cancel")
	default:
		// Expected
	}

	// Cancel connection context
	client.connCancel()

	// Context should be done
	select {
	case <-client.connCtx.Done():
		// Expected - connection goroutines would exit here
	case <-time.After(100 * time.Millisecond):
		t.Error("connCtx should be done after cancel")
	}

	// Clean up
	client.cancel()
}

// TestBingXClientReconnectionConstants verifies reconnection constants are reasonable.
func TestBingXClientReconnectionConstants(t *testing.T) {
	if InitialReconnectDelay != 1*time.Second {
		t.Errorf("InitialReconnectDelay = %v, expected 1s", InitialReconnectDelay)
	}

	if MaxReconnectDelay != 30*time.Second {
		t.Errorf("MaxReconnectDelay = %v, expected 30s", MaxReconnectDelay)
	}

	if ReconnectBackoffRate != 2.0 {
		t.Errorf("ReconnectBackoffRate = %v, expected 2.0", ReconnectBackoffRate)
	}

	if MaxReconnectAttempts != 0 {
		t.Errorf("MaxReconnectAttempts = %v, expected 0 (infinite)", MaxReconnectAttempts)
	}
}

// TestBingXClientContext verifies context cancellation.
func TestBingXClientContext(t *testing.T) {
	config := ClientConfig{
		WSURL:      "wss://test.example.com",
		ExchangeID: "bingx",
		MarketType: "swap",
	}

	client := NewBingXClient(config)

	select {
	case <-client.ctx.Done():
		t.Error("Context should not be done initially")
	default:
	}

	client.cancel()

	select {
	case <-client.ctx.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("Context should be done after cancel")
	}
}

// TestBingXClientDefaultWSURL verifies default WebSocket URLs are set correctly.
func TestBingXClientDefaultWSURL(t *testing.T) {
	tests := []struct {
		name       string
		marketType string
		expected   string
	}{
		{
			name:       "swap market",
			marketType: "swap",
			expected:   BingXSwapWSEndpoint,
		},
		{
			name:       "spot market",
			marketType: "spot",
			expected:   BingXSpotWSEndpoint,
		},
		{
			name:       "empty market type defaults to spot",
			marketType: "",
			expected:   BingXSpotWSEndpoint,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := ClientConfig{
				ExchangeID: "bingx",
				MarketType: tt.marketType,
			}

			client := NewBingXClient(config)

			if client.config.WSURL != tt.expected {
				t.Errorf("WSURL = %s, expected %s", client.config.WSURL, tt.expected)
			}

			client.cancel()
		})
	}
}

// TestBingXClientWriteDeadlineConstant verifies the write deadline constant.
func TestBingXClientWriteDeadlineConstant(t *testing.T) {
	if BingXWriteDeadline != 10*time.Second {
		t.Errorf("BingXWriteDeadline = %v, expected 10s", BingXWriteDeadline)
	}

	if BingXReadDeadline != 5*time.Minute {
		t.Errorf("BingXReadDeadline = %v, expected 5m", BingXReadDeadline)
	}
}

// BenchmarkLastPongUpdate benchmarks the lastPong update operation.
func BenchmarkLastPongUpdate(b *testing.B) {
	config := ClientConfig{
		WSURL:      "wss://test.example.com",
		ExchangeID: "bingx",
		MarketType: "swap",
	}

	client := NewBingXClient(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.lastPongMu.Lock()
		client.lastPong = time.Now()
		client.lastPongMu.Unlock()
	}

	client.cancel()
}

// BenchmarkHealthCheck benchmarks the HealthCheck operation.
func BenchmarkHealthCheck(b *testing.B) {
	config := ClientConfig{
		WSURL:      "wss://test.example.com",
		ExchangeID: "bingx",
		MarketType: "swap",
	}

	client := NewBingXClient(config)
	client.connMu.Lock()
	client.connected = true
	client.connMu.Unlock()
	client.lastPongMu.Lock()
	client.lastPong = time.Now()
	client.lastPongMu.Unlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = client.HealthCheck()
	}

	client.cancel()
}

// TestBingXDisconnectTimeStoreLoadRoundTrip verifies the atomic.Value seam: a stored
// time.Time round-trips via Load, and an unset value yields ok=false so triggerGapRecovery
// can defensively skip rather than enqueue a bogus window.
func TestBingXDisconnectTimeStoreLoadRoundTrip(t *testing.T) {
	config := ClientConfig{ExchangeID: "bingx", MarketType: "swap"}
	client := NewBingXClient(config)
	defer client.cancel()

	// Unset: Load returns nil interface -> type assertion ok is false.
	if v, ok := client.disconnectTime.Load().(time.Time); ok {
		t.Errorf("disconnectTime should be unset before any Store, got %v (ok=%v)", v, ok)
	}

	want := time.Now().Add(-30 * time.Second)
	client.disconnectTime.Store(want)

	got, ok := client.disconnectTime.Load().(time.Time)
	if !ok {
		t.Fatal("disconnectTime Load failed type assertion after Store")
	}
	if !got.Equal(want) {
		t.Errorf("disconnectTime round-trip = %v, want %v", got, want)
	}
}

// TestBingXDisconnectTimeNoRaceConcurrentAccess is the race regression test.
// disconnectTime is written under connMu (handleDisconnect) and read without that lock
// (triggerGapRecovery) — a plain 24-byte time.Time is not atomically readable, so that
// access shape is a data race `go test -race` flags. This test drives the SAME access
// shape concurrently (lock-free Store mirroring the write site, lock-free Load mirroring
// the read site, plus concurrent subsMu traffic) and must be clean under -race.
// Network-free.
func TestBingXDisconnectTimeNoRaceConcurrentAccess(_ *testing.T) {
	config := ClientConfig{ExchangeID: "bingx", MarketType: "swap"}
	client := NewBingXClient(config)
	defer client.cancel()

	const iterations = 2000
	var wg sync.WaitGroup

	// Writer: mirrors handleDisconnect's c.disconnectTime.Store(time.Now()).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			client.disconnectTime.Store(time.Now())
		}
	}()

	// Reader: mirrors triggerGapRecovery's lock-free dt, _ := Load().(time.Time).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if dt, ok := client.disconnectTime.Load().(time.Time); ok {
				_ = time.Since(dt)
			}
		}
	}()

	// Concurrent subsMu traffic: the race pairs a connMu write with a subsMu read, so
	// exercise subsMu alongside to approximate the real two-lock interleaving.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			client.subsMu.Lock()
			client.subscriptions["BTC/USDT"] = true
			client.subsMu.Unlock()
			client.subsMu.RLock()
			_ = len(client.subscriptions)
			client.subsMu.RUnlock()
		}
	}()

	wg.Wait()
}

// TestBingXTriggerGapRecoveryNormalizesMarketType verifies that triggerGapRecovery
// normalizes the external config market type ("swap") to the internal canonical type
// ("future") before enqueue, so the gap-recovery key matches the periodic reconciliation
// key and the two coalesce on one queue/circuit-breaker entry. We capture the marketType
// via the RecoveryCallback fallback path (gapRecovery == nil), which receives the same
// value.
func TestBingXTriggerGapRecoveryNormalizesMarketType(t *testing.T) {
	var (
		mu              sync.Mutex
		gotMarketTypes  []string
		gotExchangeID   string
		gotDisconnectTS time.Time
	)
	var wg sync.WaitGroup
	wg.Add(1) // one subscribed symbol -> one callback dispatch

	config := ClientConfig{
		ExchangeID: "bingx",
		MarketType: "swap", // external; internal canonical is "future"
		RecoveryCallback: func(exchangeID, marketType, _ string, disconnectTime, _ time.Time) {
			mu.Lock()
			gotMarketTypes = append(gotMarketTypes, marketType)
			gotExchangeID = exchangeID
			gotDisconnectTS = disconnectTime
			mu.Unlock()
			wg.Done()
		},
	}
	client := NewBingXClient(config)
	defer client.cancel()

	// Subscribe one symbol (desired state) and record a disconnect.
	client.subsMu.Lock()
	client.subscriptions["BTC/USDT"] = true
	client.subsMu.Unlock()

	disconnectAt := time.Now().Add(-10 * time.Second)
	client.disconnectTime.Store(disconnectAt)

	client.triggerGapRecovery(time.Now())

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(gotMarketTypes) != 1 {
		t.Fatalf("expected exactly 1 recovery callback, got %d", len(gotMarketTypes))
	}
	if gotMarketTypes[0] != "future" {
		t.Errorf("marketType passed to recovery = %q, want %q (canonical normalization)", gotMarketTypes[0], "future")
	}
	if gotExchangeID != "bingx" {
		t.Errorf("exchangeID = %q, want %q", gotExchangeID, "bingx")
	}
	if !gotDisconnectTS.Equal(disconnectAt) {
		t.Errorf("disconnect time = %v, want %v (loaded from atomic.Value)", gotDisconnectTS, disconnectAt)
	}
}

// TestBingXTriggerGapRecoverySkipsWhenDisconnectTimeUnset verifies the defensive guard: if
// no disconnect was ever recorded, triggerGapRecovery must NOT fire recovery (which would
// enqueue a bogus window) even when symbols are subscribed.
func TestBingXTriggerGapRecoverySkipsWhenDisconnectTimeUnset(t *testing.T) {
	called := make(chan struct{}, 1)
	config := ClientConfig{
		ExchangeID: "bingx",
		MarketType: "swap",
		RecoveryCallback: func(_, _, _ string, _, _ time.Time) {
			called <- struct{}{}
		},
	}
	client := NewBingXClient(config)
	defer client.cancel()

	client.subsMu.Lock()
	client.subscriptions["BTC/USDT"] = true
	client.subsMu.Unlock()

	// disconnectTime intentionally never stored.
	client.triggerGapRecovery(time.Now())

	select {
	case <-called:
		t.Error("recovery callback fired despite unrecorded disconnect time (guard violated)")
	case <-time.After(100 * time.Millisecond):
		// Expected: no dispatch.
	}
}

// TestBingXHealthyAfterReconnectLastPongReset verifies the intent: once a (re)connect
// success path stamps lastPong to "now" (as doConnect and reconnect do), HealthCheck()
// returns true immediately instead of false-flapping for up to 60s. We simulate the
// post-reconnect state (connected + freshly stamped lastPong) without any network I/O.
func TestBingXHealthyAfterReconnectLastPongReset(t *testing.T) {
	config := ClientConfig{ExchangeID: "bingx", MarketType: "swap"}
	client := NewBingXClient(config)
	defer client.cancel()

	// Pre-reconnect: connected but a stale lastPong from before the drop -> unhealthy.
	client.connMu.Lock()
	client.connected = true
	client.connMu.Unlock()
	client.lastPongMu.Lock()
	client.lastPong = time.Now().Add(-90 * time.Second)
	client.lastPongMu.Unlock()
	if client.HealthCheck() {
		t.Fatal("precondition: HealthCheck should be false with a 90s-stale lastPong")
	}

	// Simulate the reconnect success path resetting the pong watchdog.
	client.lastPongMu.Lock()
	client.lastPong = time.Now()
	client.lastPongMu.Unlock()

	if !client.HealthCheck() {
		t.Error("HealthCheck should be true immediately after reconnect resets lastPong")
	}
}

// TestBingXClientEndpoints verifies endpoint constants.
func TestBingXClientEndpoints(t *testing.T) {
	if BingXSpotWSEndpoint != "wss://open-api-ws.bingx.com/market" {
		t.Errorf("BingXSpotWSEndpoint = %s, unexpected", BingXSpotWSEndpoint)
	}

	if BingXSwapWSEndpoint != "wss://open-api-swap.bingx.com/swap-market" { //nolint:gosec // G101: test credential literal, not a secret.
		t.Errorf("BingXSwapWSEndpoint = %s, unexpected", BingXSwapWSEndpoint)
	}
}

// TestBingXClientClose verifies Close() properly cleans up resources.
func TestBingXClientClose(t *testing.T) {
	config := ClientConfig{
		WSURL:      "wss://test.example.com",
		ExchangeID: "bingx",
		MarketType: "swap",
	}

	client := NewBingXClient(config)

	// Close should not panic
	err := client.Close()
	if err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	// Should not be connected
	if client.IsConnected() {
		t.Error("Should not be connected after Close")
	}
}

// TestBingXClientPingInterval verifies ping interval defaults.
func TestBingXClientPingInterval(t *testing.T) {
	// Default should be 1800 seconds (30 minutes)
	config := ClientConfig{
		WSURL:      "wss://test.example.com",
		ExchangeID: "bingx",
		MarketType: "swap",
	}

	client := NewBingXClient(config)
	if client.config.PingInterval != 1800 {
		t.Errorf("Default PingInterval = %d, expected 1800", client.config.PingInterval)
	}
	client.cancel()

	// Custom value should be preserved
	config2 := ClientConfig{
		WSURL:        "wss://test.example.com",
		ExchangeID:   "bingx",
		MarketType:   "swap",
		PingInterval: 15,
	}

	client2 := NewBingXClient(config2)
	if client2.config.PingInterval != 15 {
		t.Errorf("Custom PingInterval = %d, expected 15", client2.config.PingInterval)
	}
	client2.cancel()
}
