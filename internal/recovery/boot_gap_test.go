package recovery

import (
	"context"
	"testing"
	"time"
)

type capturedBatch struct {
	exchangeID string
	marketType string
	symbols    []string
	from, to   time.Time
}

type batchRecorder struct {
	Orchestrator
	batches []capturedBatch
}

func (r *batchRecorder) EnqueueBatch(exchangeID, marketType string, symbols []string, disconnectTime, reconnectTime time.Time) {
	r.batches = append(r.batches, capturedBatch{exchangeID, marketType, symbols, disconnectTime, reconnectTime})
}

func (r *batchRecorder) EnqueueTargetedBucket(exchangeID, marketType, symbol, _ string, bucketOpenTime int64) {
	at := time.UnixMilli(bucketOpenTime)
	r.EnqueueBatch(exchangeID, marketType, []string{symbol}, at, at)
}

// fixedProvider reports the same "latest persisted candle" age for every symbol.
type fixedProvider struct {
	latestMs int64
	ok       bool
}

func (f fixedProvider) GetLatestTimestamp(_ context.Context, _, _, _, _ string) (int64, bool) {
	return f.latestMs, f.ok
}

// perSymbolProvider reports a different persisted head per symbol, which is the
// realistic shape: an illiquid asset can sit hours without a 5m trade while majors
// are current.
type perSymbolProvider struct {
	latestMs map[string]int64
}

func (p perSymbolProvider) GetLatestTimestamp(_ context.Context, _, _, symbol, _ string) (int64, bool) {
	ms, ok := p.latestMs[symbol]
	return ms, ok
}

// TestBootGap_EnqueuesTheDowntimeWindow is the guard for the RESTART SCAR: a restart
// leaves a hole the reconnect path cannot heal because the disconnect time died with
// the previous process.
func TestBootGap_EnqueuesTheDowntimeWindow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 28, 3, 47, 14, 0, time.UTC)
	provider := fixedProvider{latestMs: now.Add(-71 * time.Second).UnixMilli(), ok: true}

	orch := &batchRecorder{}
	EnqueueBootGap(t.Context(), orch, provider, "binance", "future", []string{"BTC/USDT", "ETH/USDT"}, now)

	if len(orch.batches) != 1 {
		t.Fatalf("expected exactly one batch, got %d — the restart hole goes unhealed", len(orch.batches))
	}
	b := orch.batches[0]
	if len(b.symbols) != 2 {
		t.Errorf("enqueued %d symbols, want 2", len(b.symbols))
	}
	if !b.to.Equal(now) {
		t.Errorf("reconnect time = %v, want %v", b.to, now)
	}
	if gap := b.to.Sub(b.from); gap < 71*time.Second {
		t.Errorf("window is %v, shorter than the %v hole it must cover", gap, 71*time.Second)
	}
}

// TestBootGap_RefusesALongOutage: a hole beyond this path's scope must stay visibly
// open rather than be papered over with a healed tail.
func TestBootGap_RefusesALongOutage(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 28, 3, 47, 14, 0, time.UTC)
	provider := fixedProvider{latestMs: now.Add(-72 * time.Hour).UnixMilli(), ok: true}

	orch := &batchRecorder{}
	EnqueueBootGap(t.Context(), orch, provider, "binance", "future", []string{"BTC/USDT"}, now)

	if len(orch.batches) != 0 {
		t.Fatalf("enqueued %d batches for a 72h outage", len(orch.batches))
	}
}

// TestBootGap_OneStaleSymbolDoesNotVetoTheFleet is the guard for the self-inflicted
// regression in the long-outage guard: the window is per symbol, not the fleet
// minimum.
func TestBootGap_OneStaleSymbolDoesNotVetoTheFleet(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	provider := perSymbolProvider{latestMs: map[string]int64{
		"BTC/USDT":      now.Add(-71 * time.Second).UnixMilli(),
		"ETH/USDT":      now.Add(-80 * time.Second).UnixMilli(),
		"SOL/USDT":      now.Add(-95 * time.Second).UnixMilli(),
		"DEADCOIN/USDT": now.Add(-9 * time.Hour).UnixMilli(),
	}}

	orch := &batchRecorder{}
	EnqueueBootGap(t.Context(), orch, provider, "binance", "future",
		[]string{"BTC/USDT", "ETH/USDT", "SOL/USDT", "DEADCOIN/USDT"}, now)

	if len(orch.batches) != 1 {
		t.Fatalf("expected 1 batch, got %d — a single illiquid symbol vetoed the restart heal", len(orch.batches))
	}

	b := orch.batches[0]
	if len(b.symbols) != 3 {
		t.Errorf("enqueued %d symbols, want 3 (the healable majors) — got %v", len(b.symbols), b.symbols)
	}
	for _, sym := range b.symbols {
		if sym == "DEADCOIN/USDT" {
			t.Error("enqueued the beyond-cap symbol: its hole must stay visibly OPEN")
		}
	}
	if gap := b.to.Sub(b.from); gap < 95*time.Second {
		t.Errorf("window %v is shorter than the widest healable hole (95s)", gap)
	}
	if gap := b.to.Sub(b.from); gap > BootGapMaxWindow {
		t.Errorf("window %v exceeds the %v cap", gap, BootGapMaxWindow)
	}
}

func TestBootGap_StillHealsWhatItIsFor(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 28, 3, 47, 14, 0, time.UTC)
	provider := fixedProvider{latestMs: now.Add(-10 * time.Minute).UnixMilli(), ok: true}

	orch := &batchRecorder{}
	EnqueueBootGap(t.Context(), orch, provider, "binance", "future", []string{"BTC/USDT"}, now)

	if len(orch.batches) != 1 {
		t.Fatalf("expected one batch for a 10min restart hole, got %d", len(orch.batches))
	}
}

func TestBootGap_SkipsWhenNothingWasPersisted(t *testing.T) {
	t.Parallel()

	orch := &batchRecorder{}
	EnqueueBootGap(t.Context(), orch, fixedProvider{ok: false}, "binance", "future", []string{"BTC/USDT"}, time.Now())

	if len(orch.batches) != 0 {
		t.Fatalf("enqueued %d batches for a cold start with no persisted data", len(orch.batches))
	}
}

func TestBootGap_SkipsWhenAlreadyFresh(t *testing.T) {
	t.Parallel()

	now := time.Now()
	provider := fixedProvider{latestMs: now.Add(-2 * time.Second).UnixMilli(), ok: true}

	orch := &batchRecorder{}
	EnqueueBootGap(t.Context(), orch, provider, "binance", "future", []string{"BTC/USDT"}, now)

	if len(orch.batches) != 0 {
		t.Fatalf("enqueued %d batches although persisted data was already current", len(orch.batches))
	}
}

func TestBootGap_NoOpWithoutOrchestratorOrSymbols(t *testing.T) {
	t.Parallel()

	now := time.Now()
	provider := fixedProvider{latestMs: now.Add(-71 * time.Second).UnixMilli(), ok: true}

	EnqueueBootGap(t.Context(), nil, provider, "binance", "future", []string{"BTC/USDT"}, now) // must not panic

	orch := &batchRecorder{}
	EnqueueBootGap(t.Context(), orch, provider, "binance", "future", nil, now)
	if len(orch.batches) != 0 {
		t.Fatalf("enqueued %d batches for an empty symbol list", len(orch.batches))
	}
}
