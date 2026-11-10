package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/market"
)

type recordingOrchestrator struct {
	Orchestrator
	calls []reconcileCall
}

type reconcileCall struct {
	exchangeID string
	marketType string
	symbols    []string
	since      time.Time
	until      time.Time
}

func (r *recordingOrchestrator) EnqueueBatch(exchangeID, marketType string, symbols []string, disconnectTime, reconnectTime time.Time) {
	r.calls = append(r.calls, reconcileCall{
		exchangeID: exchangeID,
		marketType: marketType,
		symbols:    append([]string(nil), symbols...),
		since:      disconnectTime,
		until:      reconnectTime,
	})
}

func (r *recordingOrchestrator) EnqueueTargetedBucket(exchangeID, marketType, symbol, _ string, bucketOpenTime int64) {
	at := time.UnixMilli(bucketOpenTime)
	r.EnqueueBatch(exchangeID, marketType, []string{symbol}, at, at)
}

type fakeTimestampProvider struct {
	// latest maps "exchange|internalMarketType|internalSymbol|tf" -> open-time ms.
	latest map[string]int64
}

func (f *fakeTimestampProvider) key(exchange, marketType, symbol, tf string) string {
	return exchange + "|" + marketType + "|" + symbol + "|" + tf
}

func (f *fakeTimestampProvider) GetLatestTimestamp(_ context.Context, exchange, marketType, symbol, tf string) (int64, bool) {
	ts, ok := f.latest[f.key(exchange, marketType, symbol, tf)]
	return ts, ok
}

var _ LatestTimestampProvider = (*fakeTimestampProvider)(nil)

func TestReconcileGapsOnce(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	window := 10 * time.Minute

	tests := []struct {
		name      string
		scopes    []Scope
		subs      []string
		orchNil   bool
		wantCalls []reconcileCall
		wantCount int
	}{
		{
			name: "enqueues per scope filtered by prefix",
			scopes: []Scope{
				{ExchangeID: "binance", MarketType: market.Future},
				{ExchangeID: "bingx", MarketType: market.Spot},
			},
			subs: []string{
				"binance:future:BTC/USDT",
				"binance:future:ETH/USDT",
				"bingx:spot:SOL/USDT",
				"bybit:swap:XRP/USDT", // not an enabled scope -> ignored
			},
			wantCount: 2,
			wantCalls: []reconcileCall{
				{exchangeID: "binance", marketType: "future", symbols: []string{"BTC/USDT", "ETH/USDT"}, since: now.Add(-window), until: now},
				{exchangeID: "bingx", marketType: "spot", symbols: []string{"SOL/USDT"}, since: now.Add(-window), until: now},
			},
		},
		{
			name:      "skips scope with no active symbols",
			scopes:    []Scope{{ExchangeID: "binance", MarketType: market.Future}, {ExchangeID: "bingx", MarketType: market.Spot}},
			subs:      []string{"binance:future:BTC/USDT"}, // bingx has none
			wantCount: 1,
			wantCalls: []reconcileCall{
				{exchangeID: "binance", marketType: "future", symbols: []string{"BTC/USDT"}, since: now.Add(-window), until: now},
			},
		},
		{
			name:      "no subscriptions is a no-op",
			scopes:    []Scope{{ExchangeID: "binance", MarketType: market.Future}},
			subs:      nil,
			wantCount: 0,
			wantCalls: nil,
		},
		{
			name:      "nil orchestrator is a no-op",
			scopes:    []Scope{{ExchangeID: "binance", MarketType: market.Future}},
			subs:      []string{"binance:future:BTC/USDT"},
			orchNil:   true,
			wantCount: 0,
			wantCalls: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var orch Orchestrator
			var fake *recordingOrchestrator
			if !tt.orchNil {
				fake = &recordingOrchestrator{}
				orch = fake
			}

			backstop := NewBackstop(orch, nil, tt.scopes, nil, Metrics{})
			got := backstop.ReconcileOnce(t.Context(), tt.subs, now, window, nil)
			if got != tt.wantCount {
				t.Fatalf("ReconcileOnce returned %d, want %d", got, tt.wantCount)
			}
			if tt.orchNil {
				return
			}
			assertCallsEqual(t, fake.calls, tt.wantCalls)
		})
	}
}

// TestReconcileGapsOnce_StalenessGate verifies the periodic backstop only enqueues
// symbols whose most-recent PERSISTED candle on the fastest timeframe is stale,
// keys the provider the same way the worker does, and fails toward recovery.
func TestReconcileGapsOnce_StalenessGate(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	window := 30 * time.Minute

	timeframes := []string{"1m", "5m", "1h"}
	fastestTF := "1m"
	intervalMs := int64(60_000)
	staleLagMs := StalenessIntervals * intervalMs
	scope := Scope{ExchangeID: "binance", MarketType: market.Future}
	internalMarket := scope.MarketType.String()

	mkKey := func(sym string) string {
		p := &fakeTimestampProvider{}
		return p.key(scope.ExchangeID, internalMarket, market.NormalizeSymbol(sym), fastestTF)
	}

	tests := []struct {
		name        string
		latest      map[string]int64
		subs        []string
		wantSymbols []string
		wantCount   int
	}{
		{
			name: "fresh symbol is not enqueued",
			latest: map[string]int64{
				mkKey("BTC/USDT"): now.UnixMilli() - intervalMs,
			},
			subs:        []string{"binance:future:BTC/USDT"},
			wantSymbols: nil,
			wantCount:   0,
		},
		{
			name: "stale symbol is enqueued",
			latest: map[string]int64{
				mkKey("BTC/USDT"): now.UnixMilli() - staleLagMs - 1,
			},
			subs:        []string{"binance:future:BTC/USDT"},
			wantSymbols: []string{"BTC/USDT"},
			wantCount:   1,
		},
		{
			name:        "unknown timestamp fails toward recovery",
			latest:      map[string]int64{},
			subs:        []string{"binance:future:BTC/USDT"},
			wantSymbols: []string{"BTC/USDT"},
			wantCount:   1,
		},
		{
			name: "only the lagging subset is enqueued",
			latest: map[string]int64{
				mkKey("BTC/USDT"): now.UnixMilli() - intervalMs,
				mkKey("ETH/USDT"): now.UnixMilli() - staleLagMs - 1,
				mkKey("SOL/USDT"): now.UnixMilli() - (staleLagMs + 5),
			},
			subs:        []string{"binance:future:BTC/USDT", "binance:future:ETH/USDT", "binance:future:SOL/USDT"},
			wantSymbols: []string{"ETH/USDT", "SOL/USDT"},
			wantCount:   1,
		},
		{
			name: "all fresh produces zero scopes and no enqueue",
			latest: map[string]int64{
				mkKey("BTC/USDT"): now.UnixMilli() - intervalMs,
				mkKey("ETH/USDT"): now.UnixMilli() - 2*intervalMs,
			},
			subs:        []string{"binance:future:BTC/USDT", "binance:future:ETH/USDT"},
			wantSymbols: nil,
			wantCount:   0,
		},
		{
			name: "zero persisted timestamp is treated as stale",
			latest: map[string]int64{
				mkKey("BTC/USDT"): 0,
			},
			subs:        []string{"binance:future:BTC/USDT"},
			wantSymbols: []string{"BTC/USDT"},
			wantCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backstop := NewBackstop(&recordingOrchestrator{}, &fakeTimestampProvider{latest: tt.latest},
				[]Scope{scope}, timeframes, Metrics{})
			fake := backstop.Orchestrator.(*recordingOrchestrator)

			got := backstop.ReconcileOnce(t.Context(), tt.subs, now, window, nil)
			if got != tt.wantCount {
				t.Fatalf("ReconcileOnce returned %d, want %d", got, tt.wantCount)
			}

			if tt.wantSymbols == nil {
				if len(fake.calls) != 0 {
					t.Fatalf("expected no EnqueueBatch calls, got %+v", fake.calls)
				}
				return
			}

			if len(fake.calls) != 1 {
				t.Fatalf("expected exactly 1 EnqueueBatch call, got %d (%+v)", len(fake.calls), fake.calls)
			}
			if !sameStringSet(fake.calls[0].symbols, tt.wantSymbols) {
				t.Errorf("enqueued symbols = %v, want (as set) %v", fake.calls[0].symbols, tt.wantSymbols)
			}
		})
	}
}

func TestReconcileGapsOnce_NilProviderEnqueuesAll(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	backstop := NewBackstop(&recordingOrchestrator{}, nil,
		[]Scope{{ExchangeID: "binance", MarketType: market.Future}},
		[]string{"1m", "5m"}, Metrics{})
	fake := backstop.Orchestrator.(*recordingOrchestrator)

	got := backstop.ReconcileOnce(t.Context(), []string{"binance:future:BTC/USDT", "binance:future:ETH/USDT"}, now, 30*time.Minute, nil)
	if got != 1 {
		t.Fatalf("nil provider: want 1 scope, got %d", got)
	}
	if len(fake.calls) != 1 || len(fake.calls[0].symbols) != 2 {
		t.Fatalf("nil provider must enqueue all symbols, got %+v", fake.calls)
	}
}

// TestReconcileGapsOnce_ProofMetric verifies the proof counter increments only when
// the gated backstop enqueues stale symbols AND no preceding disconnect is reported
// for that scope.
func TestReconcileGapsOnce_ProofMetric(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	window := 30 * time.Minute

	timeframes := []string{"1m"}
	intervalMs := int64(60_000)
	staleLagMs := StalenessIntervals * intervalMs

	scope := Scope{ExchangeID: "binance", MarketType: market.Future}
	internalMarket := scope.MarketType.String()
	keyer := &fakeTimestampProvider{}
	staleKey := keyer.key(scope.ExchangeID, internalMarket, market.NormalizeSymbol("BTC/USDT"), "1m")
	freshKey := staleKey

	tests := []struct {
		name              string
		latest            map[string]int64
		disconnectedScope bool
		wantDelta         int
	}{
		{
			name:      "fresh-only tick does not increment",
			latest:    map[string]int64{freshKey: now.UnixMilli() - intervalMs},
			wantDelta: 0,
		},
		{
			name:              "stale symbol with preceding disconnect does not increment",
			latest:            map[string]int64{staleKey: now.UnixMilli() - staleLagMs - 1},
			disconnectedScope: true,
			wantDelta:         0,
		},
		{
			name:      "stale symbol without disconnect increments",
			latest:    map[string]int64{staleKey: now.UnixMilli() - staleLagMs - 1},
			wantDelta: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var backstopEnqueues int
			backstop := NewBackstop(&recordingOrchestrator{}, &fakeTimestampProvider{latest: tt.latest},
				[]Scope{scope}, timeframes, Metrics{
					PeriodicBackstopEnqueue: func(_, _ string) { backstopEnqueues++ },
				})

			backstop.ReconcileOnce(t.Context(), []string{"binance:future:BTC/USDT"}, now, window,
				func(_, _ string) bool { return tt.disconnectedScope })

			if backstopEnqueues != tt.wantDelta {
				t.Fatalf("proof metric delta = %d, want %d", backstopEnqueues, tt.wantDelta)
			}
		})
	}
}

// TestScopeDisconnectPredicate verifies that a scope-disconnect predicate built
// from connection stats reports "preceding disconnect" only when a client matching
// the (exchangeID, internalMarketType) scope recorded a disconnect at or after the
// tick's lookback boundary.
func TestScopeDisconnectPredicate(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	since := now.Add(-30 * time.Minute)
	internalFuture := market.Future.String()

	tests := []struct {
		name     string
		stats    []DisconnectStat
		exchange string
		market   string
		want     bool
	}{
		{name: "no disconnect ever -> false", stats: []DisconnectStat{{ExchangeID: "binance", MarketType: "future"}}, exchange: "binance", market: internalFuture, want: false},
		{name: "recent disconnect within window -> true", stats: []DisconnectStat{{ExchangeID: "binance", MarketType: "future", LastDisconnect: now.Add(-5 * time.Minute)}}, exchange: "binance", market: internalFuture, want: true},
		{name: "old disconnect before window -> false", stats: []DisconnectStat{{ExchangeID: "binance", MarketType: "future", LastDisconnect: now.Add(-90 * time.Minute)}}, exchange: "binance", market: internalFuture, want: false},
		{name: "disconnect on a different scope -> false", stats: []DisconnectStat{{ExchangeID: "bingx", MarketType: "swap", LastDisconnect: now.Add(-1 * time.Minute)}}, exchange: "binance", market: internalFuture, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred := ScopeDisconnectPredicate(tt.stats, since)
			if got := pred(tt.exchange, tt.market); got != tt.want {
				t.Fatalf("predicate(%s,%s) = %v, want %v", tt.exchange, tt.market, got, tt.want)
			}
		})
	}
}

func assertCallsEqual(t *testing.T, got, want []reconcileCall) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d EnqueueBatch calls, want %d (%+v)", len(got), len(want), got)
	}
	wantByScope := make(map[string]reconcileCall, len(want))
	for _, w := range want {
		wantByScope[w.exchangeID+":"+w.marketType] = w
	}
	for _, g := range got {
		w, ok := wantByScope[g.exchangeID+":"+g.marketType]
		if !ok {
			t.Fatalf("unexpected EnqueueBatch for scope %s:%s", g.exchangeID, g.marketType)
		}
		if !g.since.Equal(w.since) || !g.until.Equal(w.until) {
			t.Errorf("scope %s:%s window = [%s, %s], want [%s, %s]",
				g.exchangeID, g.marketType, g.since, g.until, w.since, w.until)
		}
		if !sameStringSet(g.symbols, w.symbols) {
			t.Errorf("scope %s:%s symbols = %v, want (as set) %v", g.exchangeID, g.marketType, g.symbols, w.symbols)
		}
	}
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]int, len(a))
	for _, s := range a {
		set[s]++
	}
	for _, s := range b {
		set[s]--
		if set[s] < 0 {
			return false
		}
	}
	return true
}
