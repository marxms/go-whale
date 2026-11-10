package engine

import (
	"context"
	"reflect"
	"runtime"
	"slices"
	"testing"
	"time"
	"unsafe"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

func storedSeedFixture(n int) []candle.Candle {
	out := make([]candle.Candle, n)
	for i := range out {
		out[i] = candle.Candle{
			Timestamp: int64(i+1) * 300_000, Open: 10, High: 13, Low: 9,
			Close: 12, Volume: 10, TakerBuyVolume: 4, TakerSellVolume: 6, Source: "measured",
			AggregateMethod: "stream", CreatedAt: 123, LastUpdate: 456, LastTradeAt: 789,
		}
	}
	return out
}

// WARM-06: stored admission preserves financial and provenance fields while
// omitting only the persistence result. Expected candles are explicit fixtures.
func TestLoadStoredCandles_PreservesHistory(t *testing.T) {
	measured := storedSeedFixture(3)
	degraded := measured[1]
	degraded.TakerBuyVolume, degraded.TakerSellVolume = 0, 0
	for _, tc := range []struct {
		name     string
		capacity int
		resident []candle.Candle
		incoming []candle.Candle
		want     []candle.Candle
	}{
		{"empty buffer", 3, nil, measured, measured},
		{"overlap preserves taker", 3, measured, []candle.Candle{degraded}, measured},
		{"ring retains newest", 2, nil, measured, measured[1:]},
		{"empty input", 3, nil, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng := NewCandleEngine(map[string]int{"5m": tc.capacity})
			if len(tc.resident) > 0 {
				eng.SeedCandles("binance", "future", "LOAD/USDT", "5m", tc.resident)
			}
			inputBefore := slices.Clone(tc.incoming)
			before := time.Now().UnixMilli()
			eng.LoadStoredCandles("binance", "future", "LOAD/USDT", "5m", tc.incoming)
			after := time.Now().UnixMilli()
			if count := eng.CandleCount("binance", "future", "LOAD/USDT", "5m"); count != len(tc.want) {
				t.Fatalf("resident count = %d, want %d", count, len(tc.want))
			}
			got := eng.GetCandlesAsc("binance", "future", "LOAD/USDT", "5m", len(tc.resident)+len(tc.incoming)+1)
			want := slices.Clone(tc.want)
			if len(tc.resident) > 0 && len(tc.incoming) > 0 {
				if got[1].LastUpdate < before || got[1].LastUpdate > after {
					t.Fatal("overlap must retain the existing merge's update timestamp")
				}
				want[1].LastUpdate = got[1].LastUpdate
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("stored history changed: got %+v, want %+v", got, tc.want)
			}
			if !reflect.DeepEqual(tc.incoming, inputBefore) {
				t.Fatal("stored input mutated")
			}
		})
	}
}

type warmAllocationCold struct {
	*scoreRangeColdStore
	before uint64
}

func (s *warmAllocationCold) QueryRange(ctx context.Context, exchange, market, symbol, tf string, from, to time.Time, depth int) ([]candle.Candle, error) {
	rows, err := s.scoreRangeColdStore.QueryRange(ctx, exchange, market, symbol, tf, from, to, depth)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	s.before = mem.TotalAlloc
	return rows, err
}

// WARM-06: exercise the real boot caller, so restoring SeedCandles there cannot
// hide behind the cheaper admission API. Two ring-sized arrays bound this phase.
func TestWarmCandleEngine_AdmissionAllocationBudget(t *testing.T) {
	rig := newScoreRangeRig(t.Context(), t, 800)
	if err := rig.redis.AddActiveSymbol(context.Background(), "binance", "future", "LOAD/USDT"); err != nil {
		t.Fatal(err)
	}
	history := storedSeedFixture(800)
	offset := time.Now().Truncate(5*time.Minute).UnixMilli() - 801*300_000
	for i := range history {
		history[i].Timestamp += offset
	}
	cold := &warmAllocationCold{scoreRangeColdStore: &scoreRangeColdStore{rows: history}}
	cfg := config.Config{CandleEngine: config.CandleEngineConfig{Timeframes: []string{"5m"}, BufferSizes: map[string]int{"5m": 800}}}
	WarmCandleEngine(t.Context(), rig.tiered.engine, cold, rig.redis, cfg)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	allocated := mem.TotalAlloc - cold.before
	budget := uint64(2 * 800 * unsafe.Sizeof(candle.Candle{}))
	if allocated > budget {
		t.Fatalf("warm admission allocated %d bytes, exceeds %d (two resident arrays)", allocated, budget)
	}
	if got := rig.tiered.engine.CandleCount("binance", "future", "LOAD/USDT", "5m"); got != 800 {
		t.Fatalf("allocation reduction dropped history: got %d, want 800", got)
	}
	if got := rig.tiered.engine.GetCandlesAsc("binance", "future", "LOAD/USDT", "5m", 800); !reflect.DeepEqual(got, history) {
		t.Fatal("warm admission changed measured financial history or provenance")
	}
}

func TestLoadStoredCandles_AvoidsDiscardedBatchAllocations(t *testing.T) {
	data := storedSeedFixture(800)
	sizes := map[string]int{"5m": 800}
	persistAllocs := testing.AllocsPerRun(20, func() {
		eng := NewCandleEngine(sizes)
		eng.SeedCandles("binance", "future", "LOAD/USDT", "5m", data)
	})
	loadAllocs := testing.AllocsPerRun(20, func() {
		eng := NewCandleEngine(sizes)
		eng.LoadStoredCandles("binance", "future", "LOAD/USDT", "5m", data)
	})
	if persistAllocs-loadAllocs < 2 {
		t.Fatalf("discarded batch still allocated: persistence %.0f, stored load %.0f; want at least two fewer", persistAllocs, loadAllocs)
	}
}

func BenchmarkStoredSeedAdmission(b *testing.B) {
	data := storedSeedFixture(800)
	sizes := map[string]int{"5m": 800}
	for _, persist := range []bool{true, false} {
		name := "stored_load"
		if persist {
			name = "persistence_result"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				eng := NewCandleEngine(sizes)
				if persist {
					eng.SeedCandles("binance", "future", "LOAD/USDT", "5m", data)
				} else {
					eng.LoadStoredCandles("binance", "future", "LOAD/USDT", "5m", data)
				}
			}
		})
	}
}
