package engine

import (
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// TestMergeHistorical_LateDeltaIsNotDiscarded is the RED guard for the silent-discard
// defect.
//
// mergeHistorical compared the INCOMING candle's Volume against the STORED accumulated
// Volume and replaced only when greater. An incoming ws_delta carries only the trades of
// the current batch, so its Volume is a fraction of the bucket total and the condition is
// essentially never true — the late trades were dropped with no error, no log and no
// metric. Measured effect: ETH/USDT 15m closed buckets sitting 0.09%-0.13% below the
// exchange.
//
// The merge branch MUST be chosen by the declared Source, never by comparing magnitudes.
func TestMergeHistorical_LateDeltaIsNotDiscarded(t *testing.T) {
	t.Parallel()

	const (
		bucketTS = int64(1_700_000_000_000)
		nextTS   = bucketTS + 300_000 // +5m, rolls the bucket
	)

	tests := []struct {
		name            string
		late            candle.Candle
		wantVolume      float64
		wantTakerBuy    float64
		wantLateCounted bool
		wantExplanation string
	}{
		{
			name: "late ws_delta accumulates onto the closed bucket",
			late: candle.Candle{
				Timestamp:      bucketTS,
				Close:          101,
				Volume:         2,
				TakerBuyVolume: 1.5,
				Source:         string(sourceWSDelta),
			},
			// stored 100 + late 2 — the late trades belong to this bucket and are additive
			wantVolume:      102,
			wantTakerBuy:    61.5,
			wantLateCounted: true,
			wantExplanation: "a ws_delta is an increment; discarding it leaves the bucket short of the exchange",
		},
		{
			name: "late rest_total MAX-merges instead of accumulating",
			late: candle.Candle{
				Timestamp:      bucketTS,
				Close:          101,
				Volume:         140,
				TakerBuyVolume: 80,
				Source:         string(sourceRESTTotal),
			},
			// REST carries the bucket TOTAL: MAX(100, 140), never 100+140
			wantVolume:      140,
			wantTakerBuy:    80,
			wantLateCounted: true,
			wantExplanation: "a rest_total is the whole bucket; summing it would double-count",
		},
		{
			name: "stale rest_total below the accumulated value never regresses the bucket",
			late: candle.Candle{
				Timestamp:      bucketTS,
				Close:          99,
				Volume:         40,
				TakerBuyVolume: 20,
				Source:         string(sourceRESTTotal),
			},
			wantVolume:      100,
			wantTakerBuy:    60,
			wantLateCounted: true,
			wantExplanation: "MAX protects against a late/stale REST snapshot regressing a good value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ce := NewCandleEngine(map[string]int{"default": 16})
			rec := newEngineTestMetrics()
			ce.SetMetrics(rec.port())

			// Bucket accumulates live, then rolls over.
			ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", candle.Candle{
				Timestamp:      bucketTS,
				Open:           100,
				High:           105,
				Low:            95,
				Close:          100,
				Volume:         100,
				TakerBuyVolume: 60,
				Source:         string(sourceWSDelta),
			})
			ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", candle.Candle{
				Timestamp: nextTS,
				Open:      100,
				High:      100,
				Low:       100,
				Close:     100,
				Volume:    1,
				Source:    string(sourceWSDelta),
			})

			before := rec.lateTotal()

			// The late candle arrives for the bucket that already rolled.
			ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", tt.late)

			got := ce.GetCandleByTimestamp("binance", "future", "ETH/USDT", "5m", bucketTS)
			if got == nil {
				t.Fatalf("closed bucket %d disappeared from the buffer", bucketTS)
			}
			if got.Volume != tt.wantVolume {
				t.Errorf("volume = %v, want %v — %s", got.Volume, tt.wantVolume, tt.wantExplanation)
			}
			if got.TakerBuyVolume != tt.wantTakerBuy {
				t.Errorf("takerBuyVolume = %v, want %v", got.TakerBuyVolume, tt.wantTakerBuy)
			}
			if tt.wantLateCounted && rec.lateTotal() <= before {
				t.Error("late update was not counted: a silent discard is indistinguishable from no late update at all")
			}
		})
	}
}

// TestMergeHistorical_NeverDecidesByMagnitude pins the invariant that motivated the fix:
// the merge branch is a function of the declared Source alone. A ws_delta smaller than
// the stored value must still be applied — the very case the old code dropped.
func TestMergeHistorical_NeverDecidesByMagnitude(t *testing.T) {
	t.Parallel()

	const bucketTS = int64(1_700_000_000_000)

	ce := NewCandleEngine(map[string]int{"default": 16})
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: bucketTS, Open: 1, High: 1, Low: 1, Close: 1,
		Volume: 1000, Source: string(sourceWSDelta),
	})
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: bucketTS + 300_000, Open: 1, High: 1, Low: 1, Close: 1,
		Volume: 1, Source: string(sourceWSDelta),
	})

	// A tiny late delta: 0.5 against a stored 1000. A magnitude comparison would drop it.
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: bucketTS, Close: 1, Volume: 0.5, Source: string(sourceWSDelta),
	})

	got := ce.GetCandleByTimestamp("binance", "future", "BTC/USDT", "5m", bucketTS)
	if got == nil {
		t.Fatal("closed bucket disappeared")
	}
	if got.Volume != 1000.5 {
		t.Errorf("volume = %v, want 1000.5 — a delta smaller than the accumulated total is still part of the bucket", got.Volume)
	}
}

// TestMergeHistorical_UnlabelledIsTreatedAsTotal pins the deliberate ASYMMETRY of the
// dispatch: only an explicit ws_delta accumulates; anything unlabelled MAX-merges.
//
// The choice is a safety trade, not an oversight. MAX-merging a delta under-counts by that
// delta and is recoverable; accumulating a total inflates the bucket by the whole total and
// corrupts the taker/CVD the signal engine consumes. In a gap-recovery storm, 30 unlabelled REST
// re-writes of a closed bucket would sum to 31x the true volume. Removing this default silently
// reintroduces that failure, so it is pinned here.
func TestMergeHistorical_UnlabelledIsTreatedAsTotal(t *testing.T) {
	t.Parallel()

	const bucketTS = int64(1_700_000_000_000)

	ce := NewCandleEngine(map[string]int{"default": 16})
	ce.UpdateCandle("binance", "future", "SOL/USDT", "5m", candle.Candle{
		Timestamp: bucketTS, Open: 1, High: 1, Low: 1, Close: 1,
		Volume: 100, Source: string(sourceWSDelta),
	})
	ce.UpdateCandle("binance", "future", "SOL/USDT", "5m", candle.Candle{
		Timestamp: bucketTS + 300_000, Open: 1, High: 1, Low: 1, Close: 1,
		Volume: 1, Source: string(sourceWSDelta),
	})

	// Storm tail: 30 unlabelled re-writes carrying the bucket TOTAL.
	for range 30 {
		ce.UpdateCandle("binance", "future", "SOL/USDT", "5m", candle.Candle{
			Timestamp: bucketTS, Open: 1, High: 1, Low: 1, Close: 1, Volume: 100,
		})
	}

	got := ce.GetCandleByTimestamp("binance", "future", "SOL/USDT", "5m", bucketTS)
	if got == nil {
		t.Fatal("closed bucket disappeared")
	}
	if got.Volume != 100 {
		t.Errorf("volume = %v, want 100 — unlabelled updates must MAX-merge; accumulating them would give 3100", got.Volume)
	}
}
