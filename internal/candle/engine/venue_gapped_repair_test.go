package engine

import (
	"testing"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// THE VENUE'S WORD IS AUTHORITATIVE UNTIL WE KNOW WE DID NOT HEAR ALL OF IT.
//
// Venue kline frames carry the bucket's RUNNING TOTAL, not a delta. That makes the transport
// self-healing while frames keep arriving — any later frame supersedes every lost one — and
// permanently lossy the moment an interruption spans a bucket's CLOSE: the venue never resends a
// finished bucket, so the last total received before the silence becomes final, short by whatever
// traded in the remainder. It then folds up into all seven coarser timeframes.
//
// Every repair route was vetoed. venueHasSpoken refused any non-kline payload at all four merge
// sites, so a correct REST total was fetched, charged against the REST budget, and discarded. And
// the targeted re-read in checkOpenTimeContinuity is gated on HasGap, which had NO producer in a
// venue-owned scope — the tape's confirmMissing is skipped for every owned timeframe.
//
// HasGap is that admission, written by the restart path, which is the only place that knows
// frames were lost across an interval it can name.
func TestGappedVenueBucketAcceptsARepair(t *testing.T) {
	const ex, mt, sym, tf = "binance", "future", "REPAIR/USDT", "5m"
	ce := NewCandleEngine(map[string]int{"default": 100})

	const ts = int64(1_785_974_400_000)
	ce.UpdateCandle(ex, mt, sym, tf, candle.Candle{
		Timestamp: ts, Open: 10, High: 12, Low: 9, Close: 11,
		Volume: 100, TakerBuyVolume: 60, TakerSellVolume: 40,
		Source: string(redisstore.CandleMergeSourceWSKline),
	})

	restTotal := candle.Candle{
		Timestamp: ts, Open: 10, High: 13, Low: 9, Close: 12,
		Volume: 175, TakerBuyVolume: 100, TakerSellVolume: 75,
		Source: string(redisstore.CandleMergeSourceRESTTotal),
	}

	// Untouched, the venue owns it and the REST total must be refused.
	stored := *ce.GetCandleByTimestamp(ex, mt, sym, tf, ts)
	if !candle.VenueHasSpoken(stored, restTotal) {
		t.Fatal("an intact venue bucket accepted a REST total — ownership is not being enforced")
	}

	// Once the restart admits the bucket is incomplete, the repair must be allowed to land.
	if n := ce.MarkOpenBucketGapped(ex, mt, tf, []string{sym}); n != 1 {
		t.Fatalf("MarkOpenBucketGapped marked %d buckets, want 1", n)
	}
	stored = *ce.GetCandleByTimestamp(ex, mt, sym, tf, ts)
	if !stored.HasGap {
		t.Fatal("the bucket was not marked incomplete")
	}
	if candle.VenueHasSpoken(stored, restTotal) {
		t.Fatal("a bucket we KNOW is short still vetoes its only remaining source of truth — the " +
			"repair is fetched, charged against the REST budget, and thrown away")
	}
}

// Marking must hit the NEWEST bucket only: older ones are finished and correct.
func TestMarkOpenBucketGappedTouchesOnlyTheNewest(t *testing.T) {
	const ex, mt, sym, tf = "binance", "future", "NEWEST/USDT", "5m"
	ce := NewCandleEngine(map[string]int{"default": 100})

	const base = int64(1_785_974_400_000)
	const fiveMin = int64(300_000)
	for i := range 3 {
		ce.UpdateCandle(ex, mt, sym, tf, candle.Candle{
			Timestamp: base + int64(i)*fiveMin, Open: 10, High: 12, Low: 9, Close: 11,
			Volume: 100, Source: string(redisstore.CandleMergeSourceWSKline),
		})
	}
	ce.MarkOpenBucketGapped(ex, mt, tf, []string{sym})

	for i := range 3 {
		c := ce.GetCandleByTimestamp(ex, mt, sym, tf, base+int64(i)*fiveMin)
		if c == nil {
			t.Fatalf("bucket %d vanished", i)
		}
		if want := i == 2; c.HasGap != want {
			t.Fatalf("bucket %d HasGap=%v, want %v", i, c.HasGap, want)
		}
	}
}
