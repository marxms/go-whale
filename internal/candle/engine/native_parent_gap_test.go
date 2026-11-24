package engine

import (
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// A parent the VENUE produced keeps its values when a child is marked: the exchange stated them,
// and a derived rebuild may not replace them. What it cannot keep is COMPLETENESS. A child we know
// we heard partially makes the parent's coverage unproven, and leaving HasGap=false makes the
// coarser levels fold it as clean.
func TestRepairAncestorsMarksANativeParentWithoutOverwritingItsValues(t *testing.T) {
	const ex, mt, sym = "binance", "future", "NATIVEPARENT/USDT"
	ce := NewCandleEngine(map[string]int{"default": 100})
	ce.RegisterDerivedScope(ex, mt, "5m")

	base := candle.BucketTimestamp(time.Now().Add(-2*time.Hour).UnixMilli(), "30m")
	// The venue's own 30m: values a derived rebuild must never replace.
	ce.UpdateCandle(ex, mt, sym, "30m", candle.Candle{
		Timestamp: base, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 99,
		Source: string(redisstore.CandleMergeSourceWSKline),
	})

	const childMs = int64(5 * 60 * 1000)
	for i := int64(0); i < 6; i++ {
		ce.UpdateCandle(ex, mt, sym, "5m", candle.Candle{
			Timestamp: base + i*childMs, Open: 100, High: 110, Low: 90, Close: 105,
			Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4,
			Source: string(redisstore.CandleMergeSourceWSKline),
		})
		ce.FoldUp(ex, mt, sym, "5m", base+i*childMs)
	}

	before := ce.GetCandleByTimestamp(ex, mt, sym, "30m", base)
	if before == nil {
		t.Fatal("precondition: the native 30m parent is missing")
	}
	if before.Origin != candle.OriginNative {
		t.Fatalf("precondition: parent origin = %v, want native", before.Origin)
	}
	if before.Volume != 99 {
		t.Fatalf("precondition: the derived fold already replaced the venue's volume: %v", before.Volume)
	}

	marked := ce.MarkOpenBucketGapped(ex, mt, "5m", []string{sym})
	if len(marked) != 1 {
		t.Fatalf("marked %d buckets, want 1", len(marked))
	}
	ce.RepairAncestors(ex, mt, "5m", marked)

	after := ce.GetCandleByTimestamp(ex, mt, sym, "30m", base)
	if after == nil {
		t.Fatal("the native parent disappeared during the repair")
	}
	if !after.HasGap {
		t.Fatal("the native parent kept HasGap=false after a child was marked incomplete")
	}
	// The child closed during the hole, so its mark is provisional; the parent inherits the
	// WEAKER promise, never the optimistic one it was marked with before.
	if after.GapKind != candle.GapProvisional {
		t.Fatalf("GapKind = %v, want GapProvisional — the cause must travel with the mark",
			after.GapKind)
	}
	if after.Volume != 99 || after.High != 2 || after.Low != 0.5 {
		t.Fatalf("the mark overwrote the venue's values: H%v L%v V%v, want H2 L0.5 V99",
			after.High, after.Low, after.Volume)
	}
}
