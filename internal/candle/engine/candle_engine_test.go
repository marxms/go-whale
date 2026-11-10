package engine

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

func newTestEngine() *CandleEngine {
	return NewCandleEngine(map[string]int{"default": 100, "5m": 200})
}

func TestUpdateCandle_FirstCandle(t *testing.T) {
	ce := newTestEngine()
	c := candle.Candle{Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50}
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", c)

	got := ce.GetCandlesDesc("binance", "future", "BTC/USDT", "5m", 1)
	if len(got) != 1 {
		t.Fatalf("expected 1 candle, got %d", len(got))
	}
	if got[0].Open != 100 || got[0].Close != 105 {
		t.Fatalf("unexpected candle: %+v", got[0])
	}
}

func TestUpdateCandle_MergeOpen(t *testing.T) {
	ce := newTestEngine()

	// First update.
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50,
	})

	// Second update same bucket — open candle merge.
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: 1000, Open: 101, High: 115, Low: 88, Close: 112, Volume: 30,
	})

	got := ce.GetCandlesDesc("binance", "future", "BTC/USDT", "5m", 1)
	if len(got) != 1 {
		t.Fatalf("expected 1, got %d", len(got))
	}
	g := got[0]

	// Open stays from first write.
	if g.Open != 100 {
		t.Errorf("Open: expected 100, got %f", g.Open)
	}
	// High = max(110, 115).
	if g.High != 115 {
		t.Errorf("High: expected 115, got %f", g.High)
	}
	// Low = min(90, 88).
	if g.Low != 88 {
		t.Errorf("Low: expected 88, got %f", g.Low)
	}
	// Close = latest.
	if g.Close != 112 {
		t.Errorf("Close: expected 112, got %f", g.Close)
	}
	// Volume = sum.
	if g.Volume != 80 {
		t.Errorf("Volume: expected 80, got %f", g.Volume)
	}
}

func TestUpdateCandle_MergeClosedMaxVolume(t *testing.T) {
	ce := newTestEngine()

	// Seed two candle.
	ce.SeedCandles("binance", "future", "BTC/USDT", "5m", []candle.Candle{
		{Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50},
		{Timestamp: 2000, Open: 106, High: 120, Low: 95, Close: 115, Volume: 60},
	})

	// Historical update with lower volume — should be ignored.
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: 1000, Open: 99, High: 111, Low: 89, Close: 104, Volume: 40,
	})
	got := ce.GetCandlesDesc("binance", "future", "BTC/USDT", "5m", 10)
	// Find the ts=1000 candle.
	var found candle.Candle
	for _, c := range got {
		if c.Timestamp == 1000 {
			found = c
			break
		}
	}
	if found.Volume != 50 {
		t.Errorf("closed candle should keep volume 50, got %f", found.Volume)
	}

	// Historical update with higher volume — should replace.
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: 1000, Open: 99, High: 111, Low: 89, Close: 104, Volume: 70,
	})
	got = ce.GetCandlesDesc("binance", "future", "BTC/USDT", "5m", 10)
	for _, c := range got {
		if c.Timestamp == 1000 {
			found = c
			break
		}
	}
	if found.Volume != 70 {
		t.Errorf("closed candle should now be replaced with volume 70, got %f", found.Volume)
	}
}

func TestOnCandleClose_FiresOnBucketTransition(t *testing.T) {
	ce := newTestEngine()

	var closedKey CompositeKey
	var closedCandle candle.Candle
	callCount := 0

	ce.OnCandleClose(func(key CompositeKey, c candle.Candle) {
		closedKey = key
		closedCandle = c
		callCount++
	})

	// Push first candle.
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50,
	})
	if callCount != 0 {
		t.Fatalf("expected 0 close calls, got %d", callCount)
	}

	// Push candle with new bucket timestamp — should fire close.
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: 2000, Open: 106, High: 120, Low: 95, Close: 115, Volume: 60,
	})
	if callCount != 1 {
		t.Fatalf("expected 1 close call, got %d", callCount)
	}
	if closedCandle.Timestamp != 1000 {
		t.Errorf("closed candle timestamp: expected 1000, got %d", closedCandle.Timestamp)
	}
	if closedKey.Symbol != "BTC/USDT" {
		t.Errorf("closed key symbol: expected BTC/USDT, got %s", closedKey.Symbol)
	}
}

func TestOnCandleClose_DoesNotFireOnSameBucket(t *testing.T) {
	ce := newTestEngine()
	callCount := 0
	ce.OnCandleClose(func(_ CompositeKey, _ candle.Candle) { callCount++ })

	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50,
	})
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: 1000, Open: 101, High: 115, Low: 88, Close: 112, Volume: 30,
	})
	if callCount != 0 {
		t.Fatalf("expected 0 close calls for same bucket, got %d", callCount)
	}
}

func TestSeedCandles(t *testing.T) {
	ce := newTestEngine()
	seed := []candle.Candle{
		{Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50},
		{Timestamp: 2000, Open: 106, High: 120, Low: 95, Close: 115, Volume: 60},
		{Timestamp: 3000, Open: 116, High: 130, Low: 105, Close: 125, Volume: 70},
	}
	ce.SeedCandles("binance", "future", "BTC/USDT", "5m", seed)

	got := ce.GetCandlesDesc("binance", "future", "BTC/USDT", "5m", 10)
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	// Newest first.
	if got[0].Timestamp != 3000 {
		t.Errorf("first should be 3000, got %d", got[0].Timestamp)
	}
	if got[2].Timestamp != 1000 {
		t.Errorf("last should be 1000, got %d", got[2].Timestamp)
	}
}

func TestGetCandlesDesc_Limit(t *testing.T) {
	ce := newTestEngine()
	ce.SeedCandles("binance", "future", "BTC/USDT", "5m", []candle.Candle{
		{Timestamp: 1000}, {Timestamp: 2000}, {Timestamp: 3000},
	})
	got := ce.GetCandlesDesc("binance", "future", "BTC/USDT", "5m", 2)
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestGetCandlesDesc_MissingKey(t *testing.T) {
	ce := newTestEngine()
	got := ce.GetCandlesDesc("binance", "future", "DOGE/USDT", "5m", 10)
	if got != nil {
		t.Fatalf("expected nil for missing key, got %v", got)
	}
}

func TestUpdateCandlesBatch(t *testing.T) {
	ce := newTestEngine()
	batch := []candle.Candle{
		{Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50},
		{Timestamp: 1000, Open: 101, High: 115, Low: 88, Close: 112, Volume: 30},
		{Timestamp: 2000, Open: 113, High: 125, Low: 100, Close: 120, Volume: 40},
	}
	ce.UpdateCandlesBatch("binance", "future", "BTC/USDT", "5m", batch)

	got := ce.GetCandlesDesc("binance", "future", "BTC/USDT", "5m", 10)
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
	// First one should be merged.
	latest := got[1] // oldest = ts:1000
	if latest.Volume != 80 {
		t.Errorf("merged volume: expected 80, got %f", latest.Volume)
	}
}

func TestStats(t *testing.T) {
	ce := newTestEngine()
	ce.SeedCandles("binance", "future", "BTC/USDT", "5m", []candle.Candle{
		{Timestamp: 1000}, {Timestamp: 2000},
	})
	ce.SeedCandles("binance", "future", "ETH/USDT", "1h", []candle.Candle{
		{Timestamp: 3000},
	})

	stats := ce.Stats()
	if stats.TotalBuffers != 2 {
		t.Errorf("TotalBuffers: expected 2, got %d", stats.TotalBuffers)
	}
	if stats.TotalCandles != 3 {
		t.Errorf("TotalCandles: expected 3, got %d", stats.TotalCandles)
	}
	if stats.BuffersByTF["5m"] != 1 || stats.BuffersByTF["1h"] != 1 {
		t.Errorf("BuffersByTF unexpected: %v", stats.BuffersByTF)
	}
}

func TestBufferSizeDefault(t *testing.T) {
	ce := NewCandleEngine(nil)
	sz := ce.bufferSize("5m")
	if sz != defaultBufferSize {
		t.Errorf("expected %d, got %d", defaultBufferSize, sz)
	}
}

func TestBufferSizePerTF(t *testing.T) {
	ce := NewCandleEngine(map[string]int{"5m": 300, "default": 100})
	if ce.bufferSize("5m") != 300 {
		t.Errorf("expected 300 for 5m, got %d", ce.bufferSize("5m"))
	}
	if ce.bufferSize("1h") != 100 {
		t.Errorf("expected 100 fallback for 1h, got %d", ce.bufferSize("1h"))
	}
}

func TestHistoricalInsert_NewCandle(t *testing.T) {
	ce := newTestEngine()
	// Seed with candle at 2000.
	ce.SeedCandles("binance", "future", "BTC/USDT", "5m", []candle.Candle{
		{Timestamp: 2000, Open: 106, High: 120, Low: 95, Close: 115, Volume: 60},
	})
	// Insert historical at 1000 (doesn't exist yet).
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50,
	})

	got := ce.GetCandlesDesc("binance", "future", "BTC/USDT", "5m", 10)
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
	if got[0].Timestamp != 2000 || got[1].Timestamp != 1000 {
		t.Errorf("order wrong: %d, %d", got[0].Timestamp, got[1].Timestamp)
	}
}

func TestConcurrentAccess(t *testing.T) {
	ce := newTestEngine()
	var wg sync.WaitGroup

	// Concurrent writers.
	for i := range 100 {
		wg.Add(1)
		go func(ts int64) {
			defer wg.Done()
			ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
				Timestamp: ts, Open: 100, High: 110, Low: 90, Close: 105, Volume: 1,
			})
		}(int64(i * 1000))
	}

	// Concurrent readers.
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = ce.GetCandlesDesc("binance", "future", "BTC/USDT", "5m", 10)
			_ = ce.Stats()
		}()
	}

	wg.Wait()

	// Just verify no panic and data exists.
	stats := ce.Stats()
	if stats.TotalCandles == 0 {
		t.Error("expected some candles after concurrent writes")
	}
}

func TestCapacityLimit(t *testing.T) {
	ce := NewCandleEngine(map[string]int{"5m": 5})
	for i := range 10 {
		ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
			Timestamp: int64(i * 1000), Open: 100, High: 110, Low: 90, Close: 105, Volume: 1,
		})
	}
	got := ce.GetCandlesDesc("binance", "future", "BTC/USDT", "5m", 100)
	if len(got) != 5 {
		t.Fatalf("expected 5 (capacity limit), got %d", len(got))
	}
	// Newest should be 9000.
	if got[0].Timestamp != 9000 {
		t.Errorf("newest: expected 9000, got %d", got[0].Timestamp)
	}
}

func TestMultipleCloseCallbacks(t *testing.T) {
	ce := newTestEngine()
	var closedTimestamps []int64
	ce.OnCandleClose(func(_ CompositeKey, c candle.Candle) {
		closedTimestamps = append(closedTimestamps, c.Timestamp)
	})

	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 10})
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{Timestamp: 2000, Open: 106, High: 120, Low: 95, Close: 115, Volume: 20})
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{Timestamp: 3000, Open: 116, High: 130, Low: 105, Close: 125, Volume: 30})

	if len(closedTimestamps) != 2 {
		t.Fatalf("expected 2 close callbacks, got %d", len(closedTimestamps))
	}
	if closedTimestamps[0] != 1000 || closedTimestamps[1] != 2000 {
		t.Errorf("unexpected closed timestamps: %v", closedTimestamps)
	}
}

// ============================================================================
// BUG-001 FIX: GetLatestTimestamp Redis fallback tests
// ============================================================================

// mockRedisStore is a minimal in-memory implementation of RedisLatestTimestampStore for testing.
type mockRedisStore struct {
	data map[string][]candle.Candle
}

func newMockRedisStore() *mockRedisStore {
	return &mockRedisStore{data: make(map[string][]candle.Candle)}
}

func (m *mockRedisStore) SeedCandle(exchange, marketType, symbol, tf string, c candle.Candle) {
	key := exchange + ":" + marketType + ":" + symbol + ":" + tf
	m.data[key] = append(m.data[key], c)
	// Sort descending by timestamp
	sort.Slice(m.data[key], func(i, j int) bool {
		return m.data[key][i].Timestamp > m.data[key][j].Timestamp
	})
}

func (m *mockRedisStore) GetCandlesDesc(_ context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]candle.Candle, error) {
	key := exchange + ":" + marketType + ":" + symbol + ":" + timeframe
	candles := m.data[key]
	if limit > 0 && limit < len(candles) {
		return candles[:limit], nil
	}
	return candles, nil
}

// TestGetLatestTimestamp_BufferHit verifies buffer returns latest timestamp.
func TestGetLatestTimestamp_BufferHit(t *testing.T) {
	ce := newTestEngine()
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{Timestamp: 1779058800000})

	ts, ok := ce.GetLatestTimestamp(t.Context(), "binance", "future", "BTC/USDT", "5m")
	if !ok {
		t.Fatal("expected timestamp from buffer")
	}
	if ts != 1779058800000 {
		t.Errorf("expected 1779058800000, got %d", ts)
	}
}

// TestGetLatestTimestamp_BufferMiss_RedisFallback verifies Redis fallback when buffer is empty.
func TestGetLatestTimestamp_BufferMiss_RedisFallback(t *testing.T) {
	mockStore := newMockRedisStore()
	mockStore.SeedCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{Timestamp: 1779058800000})

	ce := newTestEngine()
	ce.SetRedisStore(mockStore)

	ts, ok := ce.GetLatestTimestamp(t.Context(), "binance", "future", "BTC/USDT", "5m")
	if !ok {
		t.Fatal("expected timestamp from Redis fallback")
	}
	if ts != 1779058800000 {
		t.Errorf("expected 1779058800000, got %d", ts)
	}
}

// TestGetLatestTimestamp_BufferEmpty_RedisFallback verifies fallback when buffer exists but is empty.
func TestGetLatestTimestamp_BufferEmpty_RedisFallback(t *testing.T) {
	mockStore := newMockRedisStore()
	mockStore.SeedCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{Timestamp: 1779058500000})

	ce := newTestEngine()
	ce.SetRedisStore(mockStore)

	// Buffer is created on first UpdateCandle, but we don't call it — so no buffer
	ts, ok := ce.GetLatestTimestamp(t.Context(), "binance", "future", "BTC/USDT", "5m")
	if !ok {
		t.Fatal("expected timestamp from Redis fallback")
	}
	if ts != 1779058500000 {
		t.Errorf("expected 1779058500000, got %d", ts)
	}
}

// TestGetLatestTimestamp_NoBufferNoRedis verifies (0, false) when both are empty.
func TestGetLatestTimestamp_NoBufferNoRedis(t *testing.T) {
	ce := newTestEngine()
	// No Redis store configured

	_, ok := ce.GetLatestTimestamp(t.Context(), "binance", "future", "BTC/USDT", "5m")
	if ok {
		t.Error("expected false when no buffer and no Redis")
	}
}

// TestGetLatestTimestamp_RedisEmpty verifies graceful handling when Redis is empty.
func TestGetLatestTimestamp_RedisEmpty(t *testing.T) {
	mockStore := newMockRedisStore()

	ce := newTestEngine()
	ce.SetRedisStore(mockStore)

	_, ok := ce.GetLatestTimestamp(t.Context(), "binance", "future", "BTC/USDT", "5m")
	if ok {
		t.Error("expected false when Redis is empty")
	}
}

// cancelAwareRedisStore records the context its read actually receives. A store built on
// context.Background() would see a deadline error after the 3s fallback ceiling; the caller's
// canceled context must reach the read immediately instead.
type cancelAwareRedisStore struct {
	observed error
}

func (s *cancelAwareRedisStore) GetCandlesDesc(ctx context.Context, _, _, _, _ string, _ int) ([]candle.Candle, error) {
	s.observed = ctx.Err()
	if s.observed != nil {
		return nil, s.observed
	}
	<-ctx.Done()
	s.observed = ctx.Err()
	return nil, s.observed
}

func TestGetLatestTimestamp_CallerCancellationReachesRedisRead(t *testing.T) {
	store := &cancelAwareRedisStore{}
	ce := newTestEngine()
	ce.SetRedisStore(store)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, ok := ce.GetLatestTimestamp(ctx, "binance", "future", "BTC/USDT", "5m")
	if ok {
		t.Fatal("a canceled read must not report a timestamp")
	}
	if !errors.Is(store.observed, context.Canceled) {
		t.Fatalf("Redis read observed %v, want the caller's context.Canceled — the fallback did not inherit ctx", store.observed)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("GetLatestTimestamp took %v after cancellation; it waited on its own deadline", elapsed)
	}
}
