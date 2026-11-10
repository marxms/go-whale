package engine

import (
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// A CHILD THAT NEVER EXISTED IS NOT A CHILD THAT IS MISSING, BUT THE RING
// AND THE EXISTENCE OF A PARENT DO NOT PROVE WHICH CASE OCCURRED.
//
// The fold documents two kinds of partial — "not started yet" (skip) and "started and absent"
// (block). A symbol listed mid-period has a third: children that predate its listing, which the
// venue does not publish either and which can therefore never arrive.
//
// Blocking on those freezes the parent for the rest of the period. A symbol onboarded mid-week has
// no 1d buckets before its listing day, and the venue publishes none either. The 1w parent would
// stay pinned at its seed all week and then CLOSE at that value when the next Monday opens — a
// permanently wrong weekly candle. Discovery picks such symbols up on day one, by volume, and
// ordinary feed observation cannot grade 1d/1w.
func TestFoldPreservesListingParentWhenListingIsUnproven(t *testing.T) {
	const ex, mt, sym = "binance", "future", "NEWLISTING/USDT"
	ce := NewCandleEngine(map[string]int{"default": 800})

	// A fixed past week keeps this case independent of the calendar: 2026-01-05 is
	// a Monday at 00:00 UTC.
	const weekOpen = int64(1767571200000)
	const day = int64(86_400_000)

	// Simulated listing three days into the week. The engine has not received
	// independent proof of that date, so it cannot assume the earlier days absent.
	listed := weekOpen + 3*day
	for ts := listed; ts < weekOpen+7*day; ts += day {
		ce.UpdateCandle(ex, mt, sym, "1d", candle.Candle{
			Timestamp: candle.BucketTimestamp(ts, "1d"),
			Open:      10, High: 12, Low: 9, Close: 11,
			Volume: 100, TakerBuyVolume: 60, TakerSellVolume: 40,
			Source: string(redisstore.CandleMergeSourceWSKline),
		})
	}

	// The parent alone does not prove the missing children predate listing: the
	// ring may simply lack history. Keep this venue record until a listing floor
	// is proved or the missing children arrive.
	ce.UpdateCandle(ex, mt, sym, "1w", candle.Candle{
		Timestamp: weekOpen,
		Open:      10, High: 12, Low: 9, Close: 11,
		Volume: 1, Source: string(redisstore.CandleMergeSourceRESTTotal),
	})

	ce.FoldUp(ex, mt, sym, "1d", candle.BucketTimestamp(listed, "1d"))

	got := ce.GetCandleByTimestamp(ex, mt, sym, "1w", weekOpen)
	if got == nil {
		t.Fatal("1w was never rebuilt: the fold blocked on daily buckets that predate the listing " +
			"and can never arrive, so the weekly candle stays at its seed and closes wrong")
	}
	if got.Volume <= 0 {
		t.Fatalf("1w rebuilt with no volume: %+v", *got)
	}
	if got.Volume != 1 || got.Source != string(redisstore.CandleMergeSourceRESTTotal) {
		t.Fatalf("unproven listing replaced the seeded parent: %+v", *got)
	}
}

func TestFoldWeeklyParentWithProvenMidweekListing(t *testing.T) {
	const ex, mt, sym = "binance", "future", "NEWLISTING/USDT"
	ce := NewCandleEngine(nil)
	const day = int64(24 * time.Hour / time.Millisecond)
	weekOpen := candle.BucketTimestamp(time.Now().Add(-8*24*time.Hour).UnixMilli(), "1w")
	listing := weekOpen + 3*day + day/2
	parent := candle.Candle{Timestamp: weekOpen, Open: 10, High: 12, Low: 9, Close: 11, Volume: 1, Source: string(redisstore.CandleMergeSourceRESTTotal)}
	ce.LoadStoredCandles(ex, mt, sym, "1w", []candle.Candle{parent})
	ce.SetListingFloors(ex, mt, []string{sym}, map[string]int64{sym: listing})
	for i := 3; i < 7; i++ {
		child := parent
		child.Timestamp, child.Volume, child.Source = weekOpen+int64(i)*day, 100, string(redisstore.CandleMergeSourceWSKline)
		ce.UpdateCandle(ex, mt, sym, "1d", child)
	}
	ce.FoldUp(ex, mt, sym, "1d", weekOpen+3*day)
	got := ce.GetCandleByTimestamp(ex, mt, sym, "1w", weekOpen)
	if got == nil || got.Volume != 400 {
		t.Fatalf("proved prelisting days prevented weekly fold: %+v", got)
	}
}

// Absence INSIDE the symbol's own history must still block — that is the guard the third case must
// not weaken.
func TestFoldStillBlocksOnAGenuineHoleInsideHistory(t *testing.T) {
	const ex, mt, sym = "binance", "future", "HOLE/USDT"
	ce := NewCandleEngine(map[string]int{"default": 800})

	// Anchored three hours in the PAST so every child bucket has demonstrably started. Anchoring on
	// "now" would skip whenever the clock sat in the first half of an hour, and a test that skips
	// certifies nothing.
	base := candle.BucketTimestamp(time.Now().UnixMilli()-3*3_600_000, "1h")
	const halfHour = int64(1_800_000)

	// 1h = 2 x 30m. Write the FIRST 30m child and leave the second absent although it has started.
	ce.UpdateCandle(ex, mt, sym, "30m", candle.Candle{
		Timestamp: base, Open: 10, High: 12, Low: 9, Close: 11,
		Volume: 100, Source: string(redisstore.CandleMergeSourceWSKline),
	})
	// A later 30m bucket exists, so `base+halfHour` is inside history, not before it.
	ce.UpdateCandle(ex, mt, sym, "30m", candle.Candle{
		Timestamp: base + 2*halfHour, Open: 10, High: 12, Low: 9, Close: 11,
		Volume: 100, Source: string(redisstore.CandleMergeSourceWSKline),
	})

	ce.FoldUp(ex, mt, sym, "30m", base)

	if got := ce.GetCandleByTimestamp(ex, mt, sym, "1h", base); got != nil {
		t.Fatalf("1h was published from one of two children: %+v — a real hole was folded over "+
			"and the bucket is short by construction", *got)
	}
}

// AN EMPTY RING IS NOT A MID-PERIOD LISTING, AND TELLING THEM APART IS THE WHOLE POINT.
//
// The first version of the third-partial rule tested "older than the oldest bucket we hold". That
// measures how full the RING is, not when the symbol was listed, and from inside the two are
// identical. Combined with attaching the venue feed before seeding, it fabricated data: a newly
// discovered symbol takes its first 5m frame with an empty ring, every earlier child is "older than
// our oldest", all of them are skipped, and 15m through 1w are each published from that ONE child —
// 288x understated for 1d, 2016x for 1w, stamped ws_kline.
//
// And unrepairable: when the seed runs minutes later, venueHasSpoken vetoes the REST total because
// the fabricated bucket carries the venue's own source.
//
// The parent must already be known. Absent, nothing has established this symbol's history, so there
// is no basis for calling a missing child non-existent.
func TestFoldRefusesToFabricateAParentFromAnEmptyRing(t *testing.T) {
	const ex, mt, sym = "binance", "future", "JUSTATTACHED/USDT"
	ce := NewCandleEngine(map[string]int{"default": 800})

	// Exactly the state of a symbol discovery just attached: no history, one frame.
	//
	// Anchored in a CLOSED 15m parent and delivering only its LAST child, so the two earlier
	// children are demonstrably in the past and absent. Anchoring on "now" would make this depend
	// on the clock: when the live 5m happens to be the parent's FIRST child the other two are in
	// the future, the fold correctly publishes the open parent from what has started, and the test
	// would fail for a behavior that is right.
	parent15 := candle.BucketTimestamp(time.Now().UnixMilli()-3_600_000, "15m")
	fiveTS := parent15 + 2*300_000
	ce.UpdateCandle(ex, mt, sym, "5m", candle.Candle{
		Timestamp: fiveTS, Open: 10, High: 12, Low: 9, Close: 11,
		Volume: 100, TakerBuyVolume: 60, TakerSellVolume: 40,
		Source: string(redisstore.CandleMergeSourceWSKline),
	})

	ce.FoldUp(ex, mt, sym, "5m", fiveTS)

	for _, tf := range []string{"15m", "30m", "1h", "2h", "4h", "1d", "1w"} {
		ts := candle.BucketTimestamp(fiveTS, tf)
		if got := ce.GetCandleByTimestamp(ex, mt, sym, tf, ts); got != nil {
			t.Fatalf("%s was published from a single 5m child on an unseeded symbol: volume=%.2f "+
				"source=%s — a fabricated bucket the seed can no longer correct", tf, got.Volume, got.Source)
		}
	}
}

// Once the seed has established the history, the same symbol folds normally.
func TestFoldProceedsOnceTheParentIsSeeded(t *testing.T) {
	const ex, mt, sym = "binance", "future", "NOWSEEDED/USDT"
	ce := NewCandleEngine(map[string]int{"default": 800})

	now := time.Now().UnixMilli()
	fifteenTS := candle.BucketTimestamp(now-3*900_000, "15m")
	const fiveMin = int64(300_000)

	// The seed brought the 15m parent and all three 5m children.
	ce.UpdateCandle(ex, mt, sym, "15m", candle.Candle{
		Timestamp: fifteenTS, Open: 10, High: 12, Low: 9, Close: 11,
		Volume: 1, Source: string(redisstore.CandleMergeSourceRESTTotal),
	})
	for i := range 3 {
		ce.UpdateCandle(ex, mt, sym, "5m", candle.Candle{
			Timestamp: fifteenTS + int64(i)*fiveMin, Open: 10, High: 12, Low: 9, Close: 11,
			Volume: 100, TakerBuyVolume: 60, TakerSellVolume: 40,
			Source: string(redisstore.CandleMergeSourceWSKline),
		})
	}

	ce.FoldUp(ex, mt, sym, "5m", fifteenTS)

	got := ce.GetCandleByTimestamp(ex, mt, sym, "15m", fifteenTS)
	if got == nil {
		t.Fatal("15m was not rebuilt although every child is present")
	}
	if got.Volume != 300 {
		t.Fatalf("15m volume = %.2f, want 300 (three children of 100)", got.Volume)
	}
}
