package engine

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

type warmSnapshotRead struct {
	symbol, timeframe string
	from, to          time.Time
	depth             int
}

type warmSnapshotCold struct {
	*scoreRangeColdStore
	mu    sync.Mutex
	reads []warmSnapshotRead
}

func (s *warmSnapshotCold) QueryRange(ctx context.Context, exchange, market, symbol, timeframe string, from, to time.Time, depth int) ([]candle.Candle, error) {
	s.mu.Lock()
	s.reads = append(s.reads, warmSnapshotRead{symbol, timeframe, from, to, depth})
	s.mu.Unlock()
	return s.scoreRangeColdStore.QueryRange(ctx, exchange, market, symbol, timeframe, from, to, depth)
}

// WARM-01: one boot has one snapshot, so each timeframe/depth has one reusable SQL
// window and every symbol loads its measured history without changing depth.
func TestWarmCandleEngine_SharedSnapshotPreservesHistory(t *testing.T) {
	rig := newScoreRangeRig(t.Context(), t, 20)
	ctx := context.Background()
	symbols := []string{"WARM-A/USDT", "WARM-B/USDT", "WARM-C/USDT"}
	for _, symbol := range symbols {
		if err := rig.redis.AddActiveSymbol(ctx, "binance", "future", symbol); err != nil {
			t.Fatal(err)
		}
	}
	newest := time.Now().Truncate(5 * time.Minute).Add(-5 * time.Minute).UnixMilli()
	history := []candle.Candle{
		{Timestamp: newest - 5*60_000, Open: 10, High: 13, Low: 9, Close: 12, Volume: 10, TakerBuyVolume: 4, TakerSellVolume: 6},
		{Timestamp: newest, Open: 12, High: 15, Low: 11, Close: 14, Volume: 20, TakerBuyVolume: 13, TakerSellVolume: 7},
	}
	cold := &warmSnapshotCold{scoreRangeColdStore: &scoreRangeColdStore{rows: history}}
	cfg := config.Config{CandleEngine: config.CandleEngineConfig{
		Timeframes: []string{"5m", "15m"}, BufferSizes: map[string]int{"5m": 4, "15m": 8},
	}}
	before := time.Now()
	WarmCandleEngine(t.Context(), rig.tiered.engine, cold, rig.redis, cfg)
	after := time.Now()
	if len(cold.reads) != len(symbols)*len(cfg.CandleEngine.Timeframes) {
		t.Fatalf("read scope incomplete: %d", len(cold.reads))
	}
	first := cold.reads[0].to
	if first.Before(before) || first.After(after) {
		t.Fatalf("snapshot outside this boot: %v", first)
	}
	queries := make(map[string]string)
	for _, read := range cold.reads {
		if !read.to.Equal(first) {
			t.Fatalf("different snapshot for %s/%s: %v != %v", read.symbol, read.timeframe, read.to, first)
		}
		depth := cfg.CandleEngine.BufferSizes[read.timeframe]
		wantFrom := first.Add(-candle.SeedWindow(candle.Timeframe(read.timeframe), depth))
		if read.depth != depth || !read.from.Equal(wantFrom) {
			t.Fatalf("wrong declared window/depth: %+v, want %v/%d", read, wantFrom, depth)
		}
		query := read.from.UTC().Format(time.RFC3339Nano) + "|" + read.to.UTC().Format(time.RFC3339Nano)
		if previous, ok := queries[read.timeframe]; ok && query != previous {
			t.Fatalf("declared read window differs within %s", read.timeframe)
		}
		queries[read.timeframe] = query
		got := rig.tiered.engine.GetCandlesAsc("binance", "future", read.symbol, read.timeframe, depth)
		if !reflect.DeepEqual(got, history) {
			t.Fatalf("history changed for %s/%s: got %+v, want %+v", read.symbol, read.timeframe, got, history)
		}
	}
	if queries["5m"] == queries["15m"] {
		t.Fatal("different declared windows must remain different SQL templates")
	}
}
