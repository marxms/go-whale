package engine

import (
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// A fold that cannot be true is never published as CLEAN: a parent whose low sits above the market
// price must carry a mark. Refusing the write would re-freeze the bucket, so
// the bar is published and MARKED instead.
func TestFoldMarksProvisionalWhenTheResultCannotBeTrue(t *testing.T) {
	const ex, mt, sym = "binance", "future", "SANITY/USDT"
	ce := NewCandleEngine(map[string]int{"default": 100})
	ce.RegisterDerivedScope(ex, mt, "5m")
	var violations []string
	ce.SetMetrics(Metrics{
		CandleSanityViolationTotal: func(_, _, _, violation string) { violations = append(violations, violation) },
	})

	base := candle.BucketTimestamp(time.Now().Add(-2*time.Hour).UnixMilli(), "15m")
	const childMs = int64(5 * 60 * 1000)
	for i := int64(0); i < 3; i++ {
		// Every child carries its high BELOW its low: no fold of these can be a real bar.
		ce.UpdateCandle(ex, mt, sym, "5m", candle.Candle{
			Timestamp: base + i*childMs, Open: 95, High: 90, Low: 95, Close: 95,
			Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4,
			Source: string(redisstore.CandleMergeSourceWSKline),
		})
		ce.FoldUp(ex, mt, sym, "5m", base+i*childMs)
	}

	parent := ce.GetCandleByTimestamp(ex, mt, sym, "15m", base)
	if parent == nil {
		t.Fatal("the 15m parent was never folded")
	}
	if !parent.HasGap {
		t.Fatal("an impossible bar was published as clean")
	}
	if parent.GapKind != candle.GapProvisional {
		t.Fatalf("GapKind = %v, want GapProvisional — a bar that contradicts its children must not "+
			"be folded onward as settled", parent.GapKind)
	}
	if len(violations) == 0 {
		t.Fatal("no sanity violation was counted: the check ran without leaving evidence")
	}
}
