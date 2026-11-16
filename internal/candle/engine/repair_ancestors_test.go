package engine

import (
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// A CLOSED parent receives no future frame, so nothing rebuilds it after a gap mark unless the
// repair is invoked explicitly. Without RepairAncestors the parent keeps the value it held before
// the mark AND keeps claiming HasGap=false: a frozen bucket published as clean.
func TestRepairAncestorsRebuildsAClosedParentAfterAGapMark(t *testing.T) {
	const ex, mt, sym = "binance", "future", "REPAIRANCESTOR/USDT"
	ce := NewCandleEngine(map[string]int{"default": 100})
	ce.RegisterDerivedScope(ex, mt, "5m")

	// A complete 30m bucket in the past: six 5m children, every one of them already closed.
	base := candle.BucketTimestamp(time.Now().Add(-2*time.Hour).UnixMilli(), "30m")
	const childMs = int64(5 * 60 * 1000)
	for i := int64(0); i < 6; i++ {
		ce.UpdateCandle(ex, mt, sym, "5m", candle.Candle{
			Timestamp: base + i*childMs, Open: 100, High: 110, Low: 90, Close: 105,
			Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4,
			Source: string(redisstore.CandleMergeSourceWSKline),
		})
		// The live path folds on every frame, not once at the end: each step needs the coarser
		// levels its predecessors produced.
		ce.FoldUp(ex, mt, sym, "5m", base+i*childMs)
	}

	before := ce.GetCandleByTimestamp(ex, mt, sym, "30m", base)
	if before == nil {
		t.Fatal("precondition: the 30m parent was never folded")
	}
	if before.HasGap {
		t.Fatalf("precondition: parent already marked: %+v", before)
	}
	if before.Volume != 60 {
		t.Fatalf("precondition: parent volume = %v, want 60", before.Volume)
	}

	marked := ce.MarkOpenBucketGapped(ex, mt, "5m", []string{sym})
	if len(marked) != 1 {
		t.Fatalf("MarkOpenBucketGapped marked %d buckets, want 1", len(marked))
	}
	ce.RepairAncestors(ex, mt, "5m", marked)

	repaired := ce.GetCandleByTimestamp(ex, mt, sym, "30m", base)
	if repaired == nil {
		t.Fatal("the 30m parent disappeared during the repair")
	}
	if !repaired.HasGap {
		t.Fatal("the closed parent kept HasGap=false after its child was marked incomplete")
	}
	if repaired.Volume != 60 {
		t.Fatalf("parent volume = %v after the repair, want 60 (the children's sum)", repaired.Volume)
	}
}
