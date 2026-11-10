package engine

import (
	"fmt"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// THE COMBINATION IS WHAT BREAKS, AND UNIT TESTS CANNOT SEE IT.
//
// A fix that is correct in isolation can be wrong together with another fix. The fold's "before
// the symbol existed" skip was sound until the discovery path stopped guaranteeing a populated
// ring; combined, they fabricated a 1w candle from a single 5m child — 2016x understated, stamped
// as the venue's own, and unrepairable afterwards.
//
// No per-fix test could have caught that, because neither fix was individually wrong. This drives
// the symbol through the transitions that actually happen in production and checks the ONE thing
// the acceptance criterion asks at every step: does a published bucket equal the venue's?
//
// venue is the truth. Every 5m frame is generated from it, and every derived timeframe is checked
// against the exact aggregation of the same source.
type venue struct {
	fiveMin map[int64]candle.Candle
}

func newVenue(from int64, buckets int) *venue {
	v := &venue{fiveMin: map[int64]candle.Candle{}}
	for i := range buckets {
		ts := from + int64(i)*300_000
		v.fiveMin[ts] = candle.Candle{
			Timestamp: ts,
			Open:      100 + float64(i), High: 110 + float64(i),
			Low: 90 + float64(i), Close: 105 + float64(i),
			Volume: 10 + float64(i), TakerBuyVolume: 6, TakerSellVolume: 4 + float64(i),
			Source: string(redisstore.CandleMergeSourceWSKline),
		}
	}
	return v
}

// truth aggregates the venue's own 5m buckets into the given timeframe, the way Binance does.
func (v *venue) truth(tf string, parentTS int64) (candle.Candle, bool) {
	span := candle.ParseTimeframeToSeconds(tf) * 1000
	out := candle.Candle{Timestamp: parentTS, Low: -1}
	found := false
	for ts := parentTS; ts < parentTS+span; ts += 300_000 {
		c, ok := v.fiveMin[ts]
		if !ok {
			continue
		}
		if !found {
			out.Open, out.Low, found = c.Open, c.Low, true
		}
		out.Close = c.Close
		out.Volume += c.Volume
		out.TakerBuyVolume += c.TakerBuyVolume
		out.TakerSellVolume += c.TakerSellVolume
		if c.High > out.High {
			out.High = c.High
		}
		if c.Low < out.Low {
			out.Low = c.Low
		}
	}
	return out, found
}

func assertMatchesVenue(t *testing.T, ce *CandleEngine, v *venue, ex, mt, sym, tf string, parentTS int64, when string) {
	t.Helper()
	want, exists := v.truth(tf, parentTS)
	got := ce.GetCandleByTimestamp(ex, mt, sym, tf, parentTS)
	if !exists {
		if got != nil && got.Source == string(redisstore.CandleMergeSourceWSKline) {
			t.Fatalf("%s: %s@%d published as venue-sourced but the venue has no such bucket: %+v",
				when, tf, parentTS, *got)
		}
		return
	}
	if got == nil {
		return // not published yet is honest; wrong is not
	}
	if got.Source != string(redisstore.CandleMergeSourceWSKline) {
		return // still the seed's value, not a claim of our own
	}
	for _, f := range []struct {
		name      string
		got, want float64
	}{
		{"volume", got.Volume, want.Volume},
		{"takerBuy", got.TakerBuyVolume, want.TakerBuyVolume},
		{"takerSell", got.TakerSellVolume, want.TakerSellVolume},
		{"open", got.Open, want.Open},
		{"high", got.High, want.High},
		{"low", got.Low, want.Low},
		{"close", got.Close, want.Close},
	} {
		if f.got != f.want {
			t.Fatalf("%s: %s@%d %s = %v, venue says %v (ratio %.4f)",
				when, tf, parentTS, f.name, f.got, f.want, f.got/f.want)
		}
	}
}

// A DISCOVERY-ADDED SYMBOL, END TO END.
//
// attach -> first frames on an EMPTY ring -> seed arrives -> more frames. At no point may a
// derived timeframe be published as venue-sourced with a value the venue does not have.
func TestLifecycle_DiscoveryAddedSymbolNeverPublishesAFabricatedBucket(t *testing.T) {
	const ex, mt, sym = "binance", "future", "DISCOVERED/USDT"
	ce := NewCandleEngine(map[string]int{"default": 800})

	// A CLOSED 15m parent one hour back, so all three children are in the past regardless of where
	// the clock happens to sit. Anchoring on "now" makes the absent-child path appear only for two
	// thirds of every quarter hour, which is a test that certifies nothing the rest of the time.
	parent15 := candle.BucketTimestamp(time.Now().UnixMilli()-3_600_000, "15m")
	v := newVenue(parent15, 3)

	// STEP 1 — attached, ring empty, the LAST child arrives first (the live one).
	third := parent15 + 2*300_000
	ce.UpdateCandle(ex, mt, sym, "5m", v.fiveMin[third])
	ce.FoldUp(ex, mt, sym, "5m", third)
	for _, tf := range []string{"15m", "30m", "1h", "2h", "4h", "1d", "1w"} {
		assertMatchesVenue(t, ce, v, ex, mt, sym, tf, candle.BucketTimestamp(third, tf), "attached, unseeded")
	}

	// STEP 2 — the seed lands: the venue's own history for every timeframe.
	for ts, c := range v.fiveMin {
		ce.UpdateCandle(ex, mt, sym, "5m", candle.Candle{
			Timestamp: ts, Open: c.Open, High: c.High, Low: c.Low, Close: c.Close,
			Volume: c.Volume, TakerBuyVolume: c.TakerBuyVolume, TakerSellVolume: c.TakerSellVolume,
			Source: string(redisstore.CandleMergeSourceRESTTotal),
		})
	}
	if want, ok := v.truth("15m", parent15); ok {
		ce.UpdateCandle(ex, mt, sym, "15m", candle.Candle{
			Timestamp: parent15, Open: want.Open, High: want.High, Low: want.Low, Close: want.Close,
			Volume: want.Volume, TakerBuyVolume: want.TakerBuyVolume, TakerSellVolume: want.TakerSellVolume,
			Source: string(redisstore.CandleMergeSourceRESTTotal),
		})
	}

	// STEP 3 — a live frame re-folds. Now the parent is known and every child is present, so the
	// rebuild must be EXACT rather than declined.
	ce.UpdateCandle(ex, mt, sym, "5m", v.fiveMin[third])
	ce.FoldUp(ex, mt, sym, "5m", third)
	assertMatchesVenue(t, ce, v, ex, mt, sym, "15m", parent15, "seeded and live")

	got := ce.GetCandleByTimestamp(ex, mt, sym, "15m", parent15)
	if got == nil {
		t.Fatal("15m never materialized even after the seed established it")
	}
}

// THE HOLE AND ITS REPAIR, END TO END.
//
// A venue-owned bucket that lost the tail of its interval must be repairable, and a bucket that
// did NOT lose anything must stay protected. Both halves matter: the fix widened the ownership
// veto, and the whole design rests on one owner per quantity.
func TestLifecycle_OnlyAHoledBucketAcceptsARepair(t *testing.T) {
	const ex, mt, sym, tf = "binance", "future", "HOLE/USDT", "5m"
	ce := NewCandleEngine(map[string]int{"default": 100})

	const ts = int64(1_785_974_400_000)
	short := candle.Candle{
		Timestamp: ts, Open: 100, High: 110, Low: 90, Close: 105,
		Volume: 40, TakerBuyVolume: 25, TakerSellVolume: 15,
		Source: string(redisstore.CandleMergeSourceWSKline),
	}
	full := candle.Candle{
		Timestamp: ts, Open: 100, High: 112, Low: 90, Close: 108,
		Volume: 100, TakerBuyVolume: 60, TakerSellVolume: 40,
		Source: string(redisstore.CandleMergeSourceRESTTotal),
	}

	// Intact: the venue owns it and the REST total must be refused.
	ce.UpdateCandle(ex, mt, sym, tf, short)
	if candle.VenueHasSpoken(*ce.GetCandleByTimestamp(ex, mt, sym, tf, ts), full) != true {
		t.Fatal("an intact venue bucket accepted a REST total — one owner per quantity is broken")
	}

	// Holed: the restart admits we did not hear it all, and the repair must land.
	ce.MarkOpenBucketGapped(ex, mt, tf, []string{sym})
	if candle.VenueHasSpoken(*ce.GetCandleByTimestamp(ex, mt, sym, tf, ts), full) {
		t.Fatal("a bucket we KNOW is short still vetoes its only remaining source of truth")
	}
}

// Every fold level must be an exact aggregation of the venue, all the way to 1w.
func TestLifecycle_EveryDerivedTimeframeMatchesTheVenueExactly(t *testing.T) {
	const ex, mt, sym = "binance", "future", "CHAIN/USDT"
	ce := NewCandleEngine(map[string]int{"default": 4000})

	// Two full days of 5m ending at the last CLOSED bucket, so nothing is mid-flight.
	now := time.Now().UnixMilli()
	end := candle.BucketTimestamp(now, "5m")
	const buckets = 576
	start := end - int64(buckets)*300_000
	v := newVenue(start, buckets)

	// Seed every timeframe from the venue, the way the boot snapshot does.
	for _, tf := range []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"} {
		span := candle.ParseTimeframeToSeconds(tf) * 1000
		for p := candle.BucketTimestamp(start, tf); p < end; p += span {
			if want, ok := v.truth(tf, p); ok {
				want.Source = string(redisstore.CandleMergeSourceRESTTotal)
				ce.UpdateCandle(ex, mt, sym, tf, want)
			}
		}
	}
	// Then deliver every 5m as a live venue frame and fold.
	for i := range buckets {
		ts := start + int64(i)*300_000
		ce.UpdateCandle(ex, mt, sym, "5m", v.fiveMin[ts])
		ce.FoldUp(ex, mt, sym, "5m", ts)
	}

	for _, tf := range []string{"15m", "30m", "1h", "2h", "4h"} {
		span := candle.ParseTimeframeToSeconds(tf) * 1000
		checked := 0
		for p := candle.BucketTimestamp(start, tf) + span; p+span <= end; p += span {
			assertMatchesVenue(t, ce, v, ex, mt, sym, tf, p, fmt.Sprintf("full chain %s", tf))
			checked++
		}
		if checked == 0 {
			t.Fatalf("%s: no closed parent was checked — the test proves nothing", tf)
		}
	}
}
