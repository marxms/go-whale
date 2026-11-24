package engine

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// A frozen DERIVED timeframe is invisible to a per-scope aggregate: the 5m keeps advancing while
// the 30m stops and /health reports healthy. This watermark is what makes the freeze nameable.
func TestTimeframeFreshnessTracksEachTimeframeIndependently(t *testing.T) {
	ce := NewCandleEngine(map[string]int{"default": 16})
	now := time.Now().UnixMilli()

	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: now - 60_000, Volume: 1, TakerBuyVolume: 1, Source: "ws_kline",
	})
	ce.UpdateCandle("binance", "future", "BTC/USDT", "30m", candle.Candle{
		Timestamp: now - 3*3_600_000, Volume: 1, TakerBuyVolume: 1, Source: "ws_kline",
	})

	rows := ce.TimeframeFreshness("binance", "future", []string{"5m", "30m", "1h"}, now)
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want one per requested timeframe", len(rows))
	}
	byTF := make(map[string]TimeframeFreshness, len(rows))
	for _, row := range rows {
		byTF[row.Timeframe] = row
	}

	if got := byTF["5m"]; !got.HaveData || got.NewestAgeMs < 55_000 || got.NewestAgeMs > 65_000 {
		t.Fatalf("5m = %+v, want an age near 60s", got)
	}
	if got := byTF["30m"]; !got.HaveData || got.NewestAgeMs < 10_700_000 {
		t.Fatalf("30m = %+v, want an age near three hours — the freeze must be visible", got)
	}
	if got := byTF["1h"]; got.HaveData {
		t.Fatalf("1h = %+v, want no data: a timeframe nobody wrote is not frozen", got)
	}
}

// The watermark only moves forward. A historical repair (an older bucket arriving late) must not
// make a healthy timeframe look frozen.
func TestTimeframeFreshnessIgnoresAnOlderRepair(t *testing.T) {
	ce := NewCandleEngine(map[string]int{"default": 16})
	now := time.Now().UnixMilli()

	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: now - 30_000, Volume: 1, TakerBuyVolume: 1, Source: "ws_kline",
	})
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
		Timestamp: now - 3_600_000, Volume: 1, TakerBuyVolume: 1, Source: "rest_total",
	})

	row := ce.TimeframeFreshness("binance", "future", []string{"5m"}, now)[0]
	if row.NewestAgeMs > 60_000 {
		t.Fatalf("newest age = %d, want the newest bucket to keep the watermark", row.NewestAgeMs)
	}
}

// The fold is the ONLY writer of a derived timeframe. A watermark fed exclusively by UpdateCandle
// would call a healthy 30m frozen: the 5m children keep arriving and the parent keeps folding, but
// nothing tells the probe. This is the regression that made production report "1 frozen timeframe".
func TestTimeframeFreshnessSeesAFoldedParentAdvance(t *testing.T) {
	ce := NewCandleEngine(map[string]int{"default": 64})
	now := time.Now().UnixMilli()
	bucket := now - 20*60_000
	bucket -= bucket % (15 * 60_000)
	for i := int64(0); i < 3; i++ {
		ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", candle.Candle{
			Timestamp: bucket + i*5*60_000, Open: 100, High: 101, Low: 99, Close: 100.5,
			Volume: 1, TakerBuyVolume: 0.5, Source: "ws_kline",
		})
	}
	ce.FoldUp("binance", "future", "BTC/USDT", "5m", bucket+2*5*60_000)

	row := ce.TimeframeFreshness("binance", "future", []string{"15m"}, now)[0]
	if !row.HaveData {
		t.Fatalf("15m = %+v, want the fold to have advanced its watermark", row)
	}
	// Exactly the age of the folded bucket: the watermark must carry the PARENT timestamp, not the
	// child's and not the moment the fold ran.
	if want := now - bucket; row.NewestAgeMs < want-5_000 || row.NewestAgeMs > want+5_000 {
		t.Fatalf("15m age = %d, want %d (the folded bucket)", row.NewestAgeMs, want)
	}
}
