package recovery

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/market"
)

// --- test doubles -----------------------------------------------------------

// fakeClientPool returns a nil RestClient (the SDK/fake fillers ignore it).
type fakeClientPool struct{}

func (fakeClientPool) Get(string, string) (HistorySource, error) { return nil, nil }
func (fakeClientPool) CloseAll()                                 {}

// fakeFiller returns a fixed FillResult. When current is set it carries the
// open in-progress bucket, mirroring the  SDK behavior.
type fakeFiller struct {
	historical []candle.Candle
	current    *candle.Candle
}

func (f fakeFiller) Name() string { return "fake" }
func (f fakeFiller) Fill(_ context.Context, _ HistorySource, _ FillRequest) (FillResult, error) {
	return FillResult{Historical: f.historical, Current: f.current}, nil
}

// recordingWriter records seeded current-bucket candles per timeframe.
type recordingWriter struct {
	mu          sync.Mutex
	seededCur   map[string]candle.Candle
	seededBatch int
}

func newRecordingWriter() *recordingWriter {
	return &recordingWriter{seededCur: make(map[string]candle.Candle)}
}

func (w *recordingWriter) SeedCandlesBatch(_ context.Context, _, _, _, _ string, cs []candle.Candle) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seededBatch += len(cs)
	return nil
}

func (w *recordingWriter) SeedCurrentBucketCandle(_ context.Context, _, _, _, tf string, c candle.Candle) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seededCur[tf] = c
	return nil
}

func (w *recordingWriter) GetCandlesByScoreRange(context.Context, string, string, string, string, int64, int64) ([]candle.Candle, error) {
	return nil, nil
}

func (w *recordingWriter) GetCandlesCount(context.Context, string, string, string, string) (int64, error) {
	return 0, nil
}

// passValidator always reports the gap as filled.
type passValidator struct{}

func (passValidator) Validate(context.Context, string, string, string, string, int64, int64) (bool, int) {
	return true, 0
}

// staleProvider always reports the same stale latest-timestamp, so freshness
// never advances unless the cycle itself seeds the current bucket.
type staleProvider struct{ ts int64 }

func (p staleProvider) GetLatestTimestamp(context.Context, string, string, string, string) (int64, bool) {
	return p.ts, true
}

// newTestManager wires a Manager with all external deps faked.
func newTestManager(f GapFiller, w StorageWriter, p LatestTimestampProvider) *Manager {
	m := NewManager(w, 1, candle.NewTimeframeRegistry([]string{"1m"}))
	m.clientPool = fakeClientPool{}
	m.validator = passValidator{}
	m.SetFiller(f)
	if p != nil {
		m.SetTimestampProvider(p)
	}
	return m
}

// --- tests ------------------------------------------------------------------

// TestProcessSeedsCurrentBucketAdvancesFreshness verifies / acceptance:
// when the filler returns the open current bucket, process() seeds it (advancing
// persisted freshness) and counts the timeframe as a success — not 0/N.
func TestProcessSeedsCurrentBucketAdvancesFreshness(t *testing.T) {
	tfMs := int64(60_000)
	currentBucket := (time.Now().UnixMilli() / tfMs) * tfMs

	w := newRecordingWriter()
	f := fakeFiller{
		historical: []candle.Candle{{Timestamp: currentBucket - tfMs, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1}},
		current:    &candle.Candle{Timestamp: currentBucket, Open: 2, High: 2, Low: 2, Close: 2, Volume: 2},
	}
	m := newTestManager(f, w, nil)

	now := time.Now()
	m.process(t.Context(), 0, RecoveryJob{
		ExchangeID: "binance", MarketType: "swap", Symbol: "BTC/USDT",
		DisconnectTime: now.Add(-5 * time.Minute), ReconnectTime: now,
	})

	w.mu.Lock()
	defer w.mu.Unlock()
	seeded, ok := w.seededCur["1m"]
	if !ok {
		t.Fatal("expected current bucket to be seeded for 1m")
	}
	if seeded.Timestamp != currentBucket {
		t.Errorf("seeded current ts = %d, want %d (advanced to current bucket)", seeded.Timestamp, currentBucket)
	}
	if w.seededBatch != 1 {
		t.Errorf("expected 1 historical candle seeded, got %d", w.seededBatch)
	}
}

// TestBreakerSuspendsNoProgressSymbol verifies  acceptance: a symbol gap-
// recovered K consecutive cycles WITHOUT freshness advancing is suspended and
// the no-progress metric is emitted. With no current bucket and a fixed stale
// provider timestamp, freshness never advances.
func TestBreakerSuspendsNoProgressSymbol(t *testing.T) {
	const k = 3
	w := newRecordingWriter()
	// No current bucket -> nothing advances freshness; historical present so the
	// cycle still "does work" but freshness stays pinned to the stale provider ts.
	f := fakeFiller{historical: []candle.Candle{{Timestamp: 1000, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1}}}
	m := newTestManager(f, w, staleProvider{ts: 1000})
	m.breaker = newNoProgressBreaker(k, time.Hour, DefaultFreshnessGraceMs)

	job := RecoveryJob{ExchangeID: "binance", MarketType: "swap", Symbol: "BTC/USDT"}
	key := "binance:swap|" + market.NormalizeSymbol(job.Symbol)
	// The manager labels metrics with the internal market type (FromConfig maps
	// "swap" -> "future"), not the raw config string.
	const internalMarket = "future"

	noProgress := 0.0
	m.SetMetrics(Metrics{NoProgressTotal: func(exchange, marketType string) {
		if exchange == "binance" && marketType == internalMarket {
			noProgress++
		}
	}})

	// First cycle seeds the watermark; next k cycles stall, the k-th suspends.
	for i := 0; i < k+1; i++ {
		if m.breaker.isSuspended(key) {
			t.Fatalf("must not be suspended before K cycles complete (i=%d)", i)
		}
		m.process(t.Context(), 0, job)
	}

	if !m.breaker.isSuspended(key) {
		t.Fatal("symbol must be suspended after K no-progress cycles")
	}
	if got := noProgress; got != 1 {
		t.Errorf("gaprecovery_no_progress_total delta = %v, want 1", got)
	}

	// A suspended symbol is skipped: no further Fill work, no new seeds.
	batchBefore := w.seededBatch
	m.process(t.Context(), 0, job)
	if w.seededBatch != batchBefore {
		t.Error("suspended symbol must be skipped (no additional seeding)")
	}
}
