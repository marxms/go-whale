package engine

import (
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// TestUpdateCandle_GapRecoveryStorm_NoVolumeAccumulation reproduces a fleet-reconnect storm
// that inflated COPPER/USDT 1h volume ~61x (6,284,779 vs the correct 102,711). During a
// reconnect, gap recovery re-ran the SAME open bucket many times while live WS deltas kept
// arriving. The REST kline total is the AUTHORITATIVE volume for a bucket, so repeated
// gap-recovery seeds plus interleaved live deltas MUST converge to MAX (the REST total) — never
// accumulate. This is the regression guard: a volume far above the REST total means the engine
// is double-counting under the storm.
func TestUpdateCandle_GapRecoveryStorm_NoVolumeAccumulation(t *testing.T) {
	ce := newTestEngine()
	const (
		ex, mt, sym, tf = "binance", "future", "COPPER/USDT", "1h"
		ts              = int64(1000)
		restTotal       = 102_711.0
	)
	base := candle.Candle{Timestamp: ts, Open: 6.5, High: 6.6, Low: 6.5, Close: 6.55}

	// Live aggregation builds the open bucket from WS deltas (sums to the total).
	live1 := base
	live1.Volume = 60_000
	ce.UpdateCandle(ex, mt, sym, tf, live1)
	live2 := base
	live2.Volume = 42_711
	ce.UpdateCandle(ex, mt, sym, tf, live2)

	// Fleet reconnect → gap-recovery retry storm: the same bucket is re-seeded with
	// the REST total ~60 times, with live deltas still trickling in between each.
	for i := 0; i < 60; i++ {
		seed := base
		seed.Volume = restTotal
		ce.SeedCurrentBucketCandle(ex, mt, sym, tf, seed)

		lateDelta := base
		lateDelta.Volume = 500
		ce.UpdateCandle(ex, mt, sym, tf, lateDelta)
	}

	got := ce.GetCandleByTimestamp(ex, mt, sym, tf, ts)
	if got == nil {
		t.Fatal("expected the open bucket to exist")
	}
	// Volume must be the REST total (MAX-merged), NOT ~60×restTotal.
	//
	// THE MARGIN IS 5%, NOT 50%. At 1.5x this assertion was VACUOUS: deleting the
	// gap-recovered marker outright leaves 60 accumulated deltas of 500 on top of the total
	// (132.711) — comfortably under the 154.066 the old threshold allowed — so the test
	// passed with the very protection it exists to guard removed. A guard for an incident
	// has to fail when the guard is gone.
	if got.Volume > restTotal*1.05 {
		t.Fatalf("gap-recovery storm accumulated volume: got %.0f, want ~%.0f (MAX). "+
			"This is the COPPER/USDT 61x inflation — the open-candle merge double-counted.",
			got.Volume, restTotal)
	}
	if got.Volume < restTotal*0.9 {
		t.Fatalf("volume below the REST total: got %.0f, want ~%.0f", got.Volume, restTotal)
	}
}

// TestUpdateCandle_LateWritesAfterClose_NoAccumulation closes the marker-deletion
// race the in-memory gapRecoveredBuckets map could otherwise leave open:
// once a newer bucket opens, the closed bucket's gap-recovered marker is dropped,
// but late re-writes on that closed bucket (the storm tail) must still go
// through mergeHistorical (replace-if-higher = MAX), never SUM.
func TestUpdateCandle_LateWritesAfterClose_NoAccumulation(t *testing.T) {
	ce := newTestEngine()
	const ex, mt, sym, tf = "binance", "future", "X/USDT", "1h"
	base := candle.Candle{Open: 1, High: 1, Low: 1, Close: 1}

	// Bucket 1000 open + gap-recovered, then bucket 2000 opens (1000 closes,
	// its marker is deleted).
	c1 := base
	c1.Timestamp = 1000
	c1.Volume = 100
	ce.UpdateCandle(ex, mt, sym, tf, c1)
	ce.SeedCurrentBucketCandle(ex, mt, sym, tf, c1)

	c2 := base
	c2.Timestamp = 2000
	c2.Volume = 50
	ce.UpdateCandle(ex, mt, sym, tf, c2)

	// Late re-writes on the closed bucket 1000 (storm tail) — must MAX, not sum.
	for i := 0; i < 30; i++ {
		late := base
		late.Timestamp = 1000
		late.Volume = 100
		ce.UpdateCandle(ex, mt, sym, tf, late)
	}

	got := ce.GetCandleByTimestamp(ex, mt, sym, tf, 1000)
	if got == nil {
		t.Fatal("expected closed bucket 1000 to exist")
	}
	if got.Volume > 150 {
		t.Fatalf("late writes on a closed bucket accumulated: got %.0f, want ~100 (MAX)", got.Volume)
	}
}
