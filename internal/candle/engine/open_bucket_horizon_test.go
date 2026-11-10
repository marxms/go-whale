package engine

import (
	"math"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// THE CUTOFF. These tests pin the rule that decides the open-bucket merge by the horizon of each
// measurement, never by arrival order or magnitude.
//
// Deciding by arrival order produces BOTH signs from one bug: a 497-symbol cohort measured
// 258 exact / 173 short / 66 OVER in a single bucket instant. Which side a symbol landed on
// depended only on whether its buffer drained before or after the REST response arrived.
//
// THREE clocks decide it:
//
//	RequestedAt  when the request that produced a TOTAL was ISSUED   (lower bound on its horizon)
//	FetchedAt    when its response was PARSED                        (upper bound on its horizon)
//	LastTradeAt  how far OUR live accumulation reaches               (its newest trade)
//
// The true horizon H is bracketed, never published: RequestedAt ≤ H ≤ FetchedAt. Each end answers
// one question and neither substitutes for the other — using the upper end as a lower bound
// discards trades the total never contained.
//
// A fixture that sets none of them still passes: with no horizon to compare against there is
// nothing to decide, and the merge keeps its conservative behavior. An unstamped producer must
// fail SAFE rather than silently gain new powers.

const horizonBucketTs = int64(1_700_000_100_000)

// liveDeltaAt is a live increment whose newest trade landed at lastTradeAt.
func liveDeltaAt(volume, takerBuy, takerSell float64, lastTradeAt int64) candle.Candle {
	return candle.Candle{
		Timestamp: horizonBucketTs,
		Open:      100, High: 100, Low: 100, Close: 100,
		Volume:          volume,
		TakerBuyVolume:  takerBuy,
		TakerSellVolume: takerSell,
		LastTradeAt:     lastTradeAt,
		Source:          string(redisstore.CandleMergeSourceWSDelta),
	}
}

// restTotalAt is a bucket TOTAL from the exchange, covering [bucket open, fetchedAt].
func restTotalAt(volume, takerBuy, takerSell float64, fetchedAt int64) candle.Candle {
	// requestedAt is the LOWER end of the bracket around the true horizon. Real producers capture
	// it before the call; 2s of flight is representative of what the seeder measures.
	return restTotalBracketed(volume, takerBuy, takerSell, fetchedAt-2_000, fetchedAt)
}

func restTotalBracketed(volume, takerBuy, takerSell float64, requestedAt, fetchedAt int64) candle.Candle {
	return candle.Candle{
		Timestamp: horizonBucketTs,
		Open:      100, High: 100, Low: 100, Close: 100,
		Volume:          volume,
		TakerBuyVolume:  takerBuy,
		TakerSellVolume: takerSell,
		RequestedAt:     requestedAt,
		FetchedAt:       fetchedAt,
		Source:          string(redisstore.CandleMergeSourceRESTTotal),
	}
}

func peek(t *testing.T, ce *CandleEngine) candle.Candle {
	t.Helper()
	c, ok := ce.PeekLatestCandle("binance", "future", "ETH/USDT", "5m")
	if !ok {
		t.Fatal("no open bucket in the buffer")
	}
	return c
}

// TestOpenBucket_ATotalIsElectedNeverAdded is the correction of a fix that was worse than the
// defect it replaced, and it is the reason this file exists in its current shape.
//
// The first version reasoned: FetchedAt proves our live tail reaches PAST anything the total can
// contain, so instead of letting the total supersede our measurement, ADD them. That is wrong.
// latest.Volume covers [bucket open, LastTradeAt] and the total covers [bucket open, H]. The two
// windows OVERLAP, and summing counts the overlap twice.
//
// Shipped to production for one deploy. Four live batches of 25 (truth 100) followed by a total of
// 75 produced 175 — a 75% INFLATION, from the very "accumulate a total" mechanism this whole
// change set exists to eliminate.
//
// Splitting the live figure at H would be the correct remedy and is not available: a bucket
// records how far its accumulation REACHES (LastTradeAt), never where it BEGAN, so "the part
// after H" cannot be recovered from anything stored. Absent that, max() is the only safe
// election. It can under-count the tail — and under-counting is a deficit the gate SEES, while
// inflation is permanent, because the seed union MAX-merges upward.
func TestOpenBucket_ATotalIsElectedNeverAdded(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})
	const snapshotAt = horizonBucketTs + 180_000

	// Four live batches of 25: truth for the bucket so far is 100.
	for _, ts := range []int64{snapshotAt - 60_000, snapshotAt - 40_000, snapshotAt - 20_000, snapshotAt + 10_000} {
		ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", liveDeltaAt(25, 15, 10, ts))
	}
	// The exchange total covers [open, snapshotAt] — the first three batches, 75.
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m",
		restTotalBracketed(75, 45, 30, snapshotAt-2_000, snapshotAt))

	got := peek(t, ce)
	if got.Volume > 100+1e-9 {
		t.Fatalf("volume = %v — INFLATED past the truth of 100. The live window and the total "+
			"OVERLAP, so adding them counts the overlap twice; a total is elected, never added", got.Volume)
	}
	if sum := got.TakerBuyVolume + got.TakerSellVolume; sum > got.Volume+1e-9 {
		t.Fatalf("takerBuy+takerSell = %v exceeds volume %v", sum, got.Volume)
	}
}

// TestOpenBucket_DroppedDeltaStillCarriesItsMark: only the FLOW of a delta inside the horizon is a
// duplicate. HasGap is the ONLY trigger of the targeted repair (app/open_time_continuity.go reads
// it off the closing candle), so dropping the whole record made a batch that was both
// already-counted AND gap-marked silence its own repair.
func TestOpenBucket_DroppedDeltaStillCarriesItsMark(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})
	const snapshotAt = horizonBucketTs + 180_000

	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m",
		restTotalBracketed(100, 60, 40, snapshotAt-2_000, snapshotAt))
	d := liveDeltaAt(8, 5, 3, snapshotAt-3_000) // inside the horizon: flow is a duplicate
	d.HasGap = true
	d.SideUnknownCount = 7
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", d)

	got := peek(t, ce)
	if got.Volume != 100 {
		t.Fatalf("volume = %v, want 100 — the flow WAS a duplicate", got.Volume)
	}
	if !got.HasGap {
		t.Error("HasGap was dropped with the duplicate flow — it is the only trigger of the repair")
	}
	if got.SideUnknownCount != 7 {
		t.Errorf("sideUnknownCount = %d, want 7 — the only evidence of unattributable volume", got.SideUnknownCount)
	}
}

// TestOpenBucket_DeltaInsideTheHorizonIsNotCountedTwice is the OVER half — same defect, other sign.
//
// The total lands first, and a batch already sitting in the buffer drains afterwards carrying
// trades from BEFORE the request that produced it. Those trades are inside the total by
// construction; adding them again produces an overage that is an exact multiple of trade
// quantities rather than a rounding residue.
//
// The cutoff is RequestedAt, the LOWER end of the bracket around the horizon: the exchange cannot
// have answered before it was asked. Using FetchedAt here instead — the upper end — discards
// trades the total never contained.
//
// RED if the cutoff is missing: 100 + 8 = 108.
func TestOpenBucket_DeltaInsideTheHorizonIsNotCountedTwice(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})
	const snapshotAt = horizonBucketTs + 180_000

	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m",
		restTotalAt(100, 60, 40, snapshotAt))
	// A batch whose NEWEST trade is still before the snapshot: wholly inside the total.
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m",
		liveDeltaAt(8, 5, 3, snapshotAt-3_000)) // older than requestedAt (= snapshotAt-2000)

	if got := peek(t, ce).Volume; got != 100 {
		t.Fatalf("volume = %v, want 100 — every trade in that batch predates the REQUEST that "+
			"produced the total, so the exchange already counts them; summing is the double count", got)
	}
}

// TestOpenBucket_DeltaPastTheHorizonStillAccumulates is the guard on the two tests above.
//
// A cutoff that dropped too much would satisfy both of them while quietly starving the bucket,
// and starving is the failure this whole activity exists to remove. A batch whose trades are
// NEWER than the horizon is not in the total and MUST still be added.
func TestOpenBucket_DeltaPastTheHorizonStillAccumulates(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})
	const snapshotAt = horizonBucketTs + 180_000

	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m",
		restTotalAt(100, 60, 40, snapshotAt))
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m",
		liveDeltaAt(8, 5, 3, snapshotAt+1_000))

	if got := peek(t, ce).Volume; got != 108 {
		t.Fatalf("volume = %v, want 108 — these trades are NEWER than the snapshot, so the total "+
			"cannot contain them and dropping them would starve the bucket", got)
	}
}

// TestOpenBucket_TakerLegsAreElectedFromOneRecord pins the invariant that makes
// `buy + sell > volume` inexpressible.
//
// Volume, taker-buy and taker-sell describe ONE observation. Electing each independently with
// its own max() lets the volume come from the exchange record and a leg from the live one, and
// that combination is the only way `buy + sell > volume` can arise — each producer honors the
// identity alone.
//
// RED if the trio is elected independently: vol=100.4 buy=70 sell=31.4 → 101.4 > 100.4.
func TestOpenBucket_TakerLegsAreElectedFromOneRecord(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})
	const snapshotAt = horizonBucketTs + 180_000

	// Live is BEHIND the horizon, so the total legitimately supersedes it. The live legs are
	// each individually larger than the total's on one side, which is what tempted the
	// independent maxima.
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m",
		liveDeltaAt(100, 70, 30, snapshotAt-1_000))
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m",
		restTotalAt(100.4, 69, 31.4, snapshotAt))

	got := peek(t, ce)
	if sum := got.TakerBuyVolume + got.TakerSellVolume; sum > got.Volume+1e-9 {
		t.Fatalf("takerBuy+takerSell = %v EXCEEDS volume %v — the legs were elected from a "+
			"different record than the volume, which is the only way this is expressible",
			sum, got.Volume)
	}
}

// TestOpenBucket_ATotalWithNoSplitDoesNotEraseAMeasuredOne guards the atomic election against
// the obvious way to get it wrong.
//
// Not every producer carries the taker pair — a REST kline from a venue whose endpoint has no
// taker field arrives 0/0. Taking the trio blindly from whichever record wins on volume would
// let that 0/0 replace a real measurement with a fabricated "no aggression at all", which is a
// worse lie than a slightly stale volume: consumers read the pair as evidence.
func TestOpenBucket_ATotalWithNoSplitDoesNotEraseAMeasuredOne(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})
	const snapshotAt = horizonBucketTs + 180_000

	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m",
		liveDeltaAt(50, 30, 20, snapshotAt-1_000))
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m",
		restTotalAt(100, 0, 0, snapshotAt)) // wins on volume, carries no split

	got := peek(t, ce)
	if got.TakerBuyVolume == 0 && got.TakerSellVolume == 0 {
		t.Fatal("a total carrying no taker split erased a measured pair — that fabricates " +
			"'no aggression' from an absence of data")
	}
	if sum := got.TakerBuyVolume + got.TakerSellVolume; sum > got.Volume+1e-9 {
		t.Fatalf("takerBuy+takerSell = %v exceeds volume %v", sum, got.Volume)
	}
}

// TestAppliedVolume_MatchesWhatWasAdmitted is the discriminator this investigation kept designing
// and never building, asserted as an invariant rather than left to production to reveal.
//
// Every counter that existed answered "what did we do with the trades that ARRIVED", and every one
// read zero while buckets closed short against the exchange. Each was correct; none was pointed at
// the question. This pair points at it: if the volume the engine APPLIED equals the volume the
// handler ADMITTED, then nothing is lost inside this process and the shortfall is upstream. If it
// does not, the shortfall is ours.
//
// The test exercises all three paths that apply a delta — first push, new bucket, accumulation —
// because missing any one of them makes the pair disagree by construction and manufactures the
// deficit it exists to detect.
func TestAppliedVolume_MatchesWhatWasAdmitted(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})
	rec := newEngineTestMetrics()
	ce.SetMetrics(rec.port())
	const scope = "5m"
	before := rec.applied("AV1", "future", scope)

	var admitted float64
	push := func(ts int64, vol float64) {
		admitted += vol
		c := liveDeltaAt(vol, vol*0.6, vol*0.4, ts+1)
		c.Timestamp = ts
		ce.UpdateCandle("AV1", "future", "ETH/USDT", scope, c)
	}
	base := horizonBucketTs
	push(base, 10)         // first push
	push(base, 15)         // accumulation
	push(base+300_000, 20) // new bucket
	push(base+300_000, 25) // accumulation in the new bucket

	got := rec.applied("AV1", "future", scope) - before
	if math.Abs(got-admitted) > 1e-9 {
		t.Fatalf("applied %v against admitted %v — the pair MUST agree when nothing is lost, or "+
			"it reports a deficit that is an artifact of its own accounting", got, admitted)
	}
}

// klineFrame is the venue's own candle, pushed live.
func klineFrame(volume, takerBuy, takerSell float64, eventAt int64) candle.Candle {
	return candle.Candle{
		Timestamp: horizonBucketTs,
		Open:      100, High: 105, Low: 95, Close: 102,
		Volume:          volume,
		TakerBuyVolume:  takerBuy,
		TakerSellVolume: takerSell,
		KlineEventAt:    eventAt,
		Source:          string(redisstore.CandleMergeSourceWSKline),
	}
}

// TestOwner_AVenueCandleREPLACES_evenDownwards is the discriminating assertion, and the first
// version of it was VACUOUS.
//
// That version asserted a second, LARGER frame wins. Without the owner path a ws_kline candle
// falls through to the total machinery and is elected by max(), which gives the same answer — so
// the test passed with the feature disabled and proved nothing. Same failure the independent
// verification already caught once: a fixture that cannot express the defect is not coverage.
//
// What separates REPLACE from max() is a newer frame carrying a SMALLER number. The venue OWNS
// these fields; if it restates them downwards we follow, because "authoritative" that only holds
// while the number grows is just max() with extra steps.
//
// The fixture is deliberately unphysical — a running total does not shrink, and gate K1 measured
// 80 closed buckets with zero revisions — because its job is to isolate the MECHANISM, not to
// model the market.
//
// RED without the owner path: max(180, 100) = 180.
func TestOwner_AVenueCandleREPLACES_evenDownwards(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", klineFrame(180, 110, 70, 1_000))
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", klineFrame(100, 60, 40, 2_000))

	got := peek(t, ce)
	if got.Volume != 100 || got.TakerBuyVolume != 60 || got.TakerSellVolume != 40 {
		t.Fatalf("got vol=%v buy=%v sell=%v, want the venue's newest reading 100/60/40 — the venue "+
			"OWNS these fields, so a later restatement stands even when it is smaller. Electing the "+
			"larger is max(), which cannot tell supersession from coincidence",
			got.Volume, got.TakerBuyVolume, got.TakerSellVolume)
	}
}

// TestOwner_AnOlderFrameIsRejectedByTheVenueClock: the SDK dispatches every websocket message on
// its own goroutine, so two frames for one symbol race and can swap. Ordering by ARRIVAL would let
// a stale reading overwrite a fresher one — the same class of defect as the watermark that
// discarded a lower trade id as "already seen".
//
// The fixture makes the stale frame LARGER on purpose: that is the only shape max() and
// order-by-venue-clock disagree on, and the earlier version of this test made it smaller, so both
// rules gave the same answer and the assertion was vacuous.
//
// RED without the owner path: max(100, 180) = 180.
func TestOwner_AnOlderFrameIsRejectedByTheVenueClock(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", klineFrame(100, 60, 40, 2_000))
	// Arrives LATER but the venue emitted it EARLIER, and it is LARGER.
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", klineFrame(180, 110, 70, 1_000))

	if got := peek(t, ce).Volume; got != 100 {
		t.Fatalf("volume = %v, want 100 — a frame the venue emitted EARLIER must not win just "+
			"because it arrived second and happens to be bigger", got)
	}
}

// TestOwner_OurOwnMarksSurviveTheReplace: HasGap and SideUnknownCount describe THIS PROCESS's
// treatment of the bucket, not the market. The venue has nothing to say about them, and letting
// its silence erase them would destroy the only trigger the repair path reads.
func TestOwner_OurOwnMarksSurviveTheReplace(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})
	marked := klineFrame(180, 110, 70, 1_000)
	marked.HasGap = true
	marked.SideUnknownCount = 5
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", marked)
	// The newer frame is SMALLER, so only the owner path reaches this state at all.
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", klineFrame(100, 60, 40, 2_000))

	if v := peek(t, ce).Volume; v != 100 {
		t.Fatalf("volume = %v, want 100 — this test only exercises the owner path if the replace "+
			"actually happened", v)
	}

	got := peek(t, ce)
	if !got.HasGap {
		t.Error("HasGap was erased by a venue frame — it is the only trigger of the targeted repair")
	}
	if got.SideUnknownCount != 5 {
		t.Errorf("sideUnknownCount = %d, want 5 — the venue does not measure it and cannot clear it", got.SideUnknownCount)
	}
}

// TestOwner_ATotalDoesNotOverwriteAVenueCandle closes the half of the hand-off the first
// implementation missed, and it was caught in PRODUCTION rather than by a test.
//
// The tape was made to stop writing, but the seeder and gap recovery push rest_total straight to
// the engine and never pass through the handler that knows about ownership. Minutes after the
// first deploy the newest bucket carried source=rest_total while older ones carried ws_kline —
// two writers on one bucket again, which is the exact topology this design exists to remove.
//
// The rule is narrow on purpose: it protects a bucket the venue has ALREADY spoken for and says
// nothing about the rest, so historical seeding still works. The kline stream only covers from
// subscription forward.
func TestOwner_ATotalDoesNotOverwriteAVenueCandle(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", klineFrame(180, 110, 70, 1_000))
	// The seeder's snapshot of the same bucket, arriving afterwards.
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m",
		restTotalBracketed(150, 90, 60, 2_000, 3_000))

	got := peek(t, ce)
	if got.Volume != 180 || got.Source != string(redisstore.CandleMergeSourceWSKline) {
		t.Fatalf("got vol=%v source=%q, want 180 from ws_kline — a snapshot of a number must not "+
			"overwrite the number itself", got.Volume, got.Source)
	}
}

// TestOwner_ASeededBucketTheVenueNeverSpokeForIsUntouched keeps the rule above from becoming a
// blanket veto. The kline stream begins at subscription, so everything before it has no venue
// candle and the seed is its ONLY source.
func TestOwner_ASeededBucketTheVenueNeverSpokeForIsUntouched(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m",
		restTotalBracketed(150, 90, 60, 2_000, 3_000))

	if got := peek(t, ce).Volume; got != 150 {
		t.Fatalf("volume = %v, want 150 — a bucket the venue never spoke for must still be seeded", got)
	}
}

// TestOwner_EveryWritePathHonoursTheVenue is the guard against a rule stated in one place and
// absent from the others.
//
// Ownership and the atomic trio election must hold at every merge site. When only one path
// enforced them, another kept overwriting buckets it did not own. The fix was correct each time;
// the COVERAGE was the defect.
//
// This asserts the rule at every entry point that can write a bucket, table-driven, so a new path
// added without honoring it fails here rather than in production.
func TestOwner_EveryWritePathHonoursTheVenue(t *testing.T) {
	t.Parallel()

	const scope = "5m"
	venue := klineFrame(180, 110, 70, 1_000)
	intruder := restTotalBracketed(150, 90, 60, 2_000, 3_000)

	for _, tc := range []struct {
		name  string
		write func(ce *CandleEngine)
	}{
		{"UpdateCandle", func(ce *CandleEngine) {
			ce.UpdateCandle("binance", "future", "ETH/USDT", scope, intruder)
		}},
		{"SeedCurrentBucketCandle", func(ce *CandleEngine) {
			ce.SeedCurrentBucketCandle("binance", "future", "ETH/USDT", scope, intruder)
		}},
		{"SeedCandles", func(ce *CandleEngine) {
			ce.SeedCandles("binance", "future", "ETH/USDT", scope, []candle.Candle{intruder})
		}},
		{"UpdateCandlesBatch", func(ce *CandleEngine) {
			ce.UpdateCandlesBatch("binance", "future", "ETH/USDT", scope, []candle.Candle{intruder})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ce := NewCandleEngine(map[string]int{"default": 10})
			ce.UpdateCandle("binance", "future", "ETH/USDT", scope, venue)
			tc.write(ce)

			got, ok := ce.PeekLatestCandle("binance", "future", "ETH/USDT", scope)
			if !ok {
				t.Fatal("the bucket vanished")
			}
			if got.Volume != 180 || got.Source != string(redisstore.CandleMergeSourceWSKline) {
				t.Fatalf("%s overwrote the venue's candle: vol=%v source=%q, want 180 from ws_kline",
					tc.name, got.Volume, got.Source)
			}
		})
	}
}

// TestFold_ARebuiltParentIsExactAndIdempotent is the write path the ownership commit claimed and
// did not build, and the two properties that make it safe.
//
// EXACT: a parent is the sum of its children on flow, the max/min on the extremes, and the first
// and last on the prices. Nothing is approximated.
//
// IDEMPOTENT: it REBUILDS rather than accumulates. Accumulating a total double-counts the moment
// anything is applied twice — a venue resend, a restart, a retry — and nothing in a stream
// guarantees exactly-once. Running the fold twice must give the same answer, and that is what
// removes the delivery guarantee from the requirements.
func TestFold_ARebuiltParentIsExactAndIdempotent(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 100})
	base := candle.BucketTimestamp(horizonBucketTs, "15m")

	// Three 5m children with distinct prices so open/close/high/low are all distinguishable.
	for i, spec := range []struct{ o, h, l, c, v, tb, ts float64 }{
		{100, 110, 95, 105, 10, 6, 4},
		{105, 120, 100, 115, 20, 12, 8},
		{115, 118, 90, 92, 30, 18, 12},
	} {
		ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", candle.Candle{
			Timestamp: base + int64(i)*300_000,
			Open:      spec.o, High: spec.h, Low: spec.l, Close: spec.c,
			Volume: spec.v, TakerBuyVolume: spec.tb, TakerSellVolume: spec.ts,
			Source: string(redisstore.CandleMergeSourceWSKline), KlineEventAt: int64(i + 1),
		})
	}

	ce.FoldUp("binance", "future", "ETH/USDT", "5m", base)
	got := ce.GetCandleByTimestamp("binance", "future", "ETH/USDT", "15m", base)
	if got == nil {
		t.Fatal("the parent was not built")
	}
	for _, c := range []struct {
		name      string
		got, want float64
	}{
		{"volume", got.Volume, 60},
		{"takerBuy", got.TakerBuyVolume, 36},
		{"takerSell", got.TakerSellVolume, 24},
		{"open", got.Open, 100},
		{"close", got.Close, 92},
		{"high", got.High, 120},
		{"low", got.Low, 90},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}

	// IDEMPOTENCE: folding again must not move a single number.
	before := *got
	ce.FoldUp("binance", "future", "ETH/USDT", "5m", base)
	after := ce.GetCandleByTimestamp("binance", "future", "ETH/USDT", "15m", base)
	if after.Volume != before.Volume || after.TakerBuyVolume != before.TakerBuyVolume {
		t.Fatalf("a second fold changed the parent: %v -> %v — it ACCUMULATED instead of "+
			"rebuilding, which double-counts on any resend or restart", before.Volume, after.Volume)
	}
}

// TestFold_APartialParentIsLeftAlone: a fold over a subset publishes a bucket that is short by
// construction and labels it as measured — the exact failure this activity exists to remove,
// arriving dressed as an improvement.
func TestFold_APartialParentIsLeftAlone(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 100})
	base := candle.BucketTimestamp(horizonBucketTs, "15m")

	// Only TWO of the three children exist.
	for i := 0; i < 2; i++ {
		ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", candle.Candle{
			Timestamp: base + int64(i)*300_000, Open: 100, High: 100, Low: 100, Close: 100,
			Volume: 10, Source: string(redisstore.CandleMergeSourceWSKline), KlineEventAt: int64(i + 1),
		})
	}
	ce.FoldUp("binance", "future", "ETH/USDT", "5m", base)

	if got := ce.GetCandleByTimestamp("binance", "future", "ETH/USDT", "15m", base); got != nil {
		t.Fatalf("a parent was built from 2 of 3 children (volume %v) — that is a bucket short by "+
			"construction, published as measured", got.Volume)
	}
}

// TestFold_TheChainReaches1w in one pass: a week holds 2016 five-minute buckets and the ring holds
// 800, so 1w cannot be rebuilt from 5m — the data is not there. Folding step by step reads between
// 2 and 7 buckets per level and arrives with the same exactness.
func TestFold_TheChainReaches1w(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 4000})
	weekStart := candle.BucketTimestamp(horizonBucketTs, "1w")
	const n = 2016 // 5m buckets in a week

	for i := 0; i < n; i++ {
		ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", candle.Candle{
			Timestamp: weekStart + int64(i)*300_000, Open: 100, High: 100, Low: 100, Close: 100,
			Volume: 1, TakerBuyVolume: 0.6, TakerSellVolume: 0.4,
			Source: string(redisstore.CandleMergeSourceWSKline), KlineEventAt: int64(i + 1),
		})
		ce.FoldUp("binance", "future", "ETH/USDT", "5m", weekStart+int64(i)*300_000)
	}

	for _, tc := range []struct {
		tf   string
		want float64
	}{{"15m", 3}, {"30m", 6}, {"1h", 12}, {"2h", 24}, {"4h", 48}, {"1d", 288}, {"1w", 2016}} {
		c := ce.GetCandleByTimestamp("binance", "future", "ETH/USDT", tc.tf, candle.BucketTimestamp(weekStart, tc.tf))
		if c == nil {
			t.Errorf("%s was never built — the chain stopped short", tc.tf)
			continue
		}
		if c.Volume != tc.want {
			t.Errorf("%s volume = %v, want %v", tc.tf, c.Volume, tc.want)
		}
	}
}

// TestFold_TheOPENParentFoldsWhatHasStarted separates the two kinds of partial.
//
// A child whose bucket has not STARTED cannot exist, and waiting for it would freeze the open
// parent until it closes — a whole week for 1w, sitting on its boot seed and never moving. A child
// whose bucket HAS started and is absent is missing data, and folding over it publishes a short
// bucket as measured.
//
// The fixture computes how many children have actually started rather than assuming: hard-coding
// two of three fails whenever "now" falls past the third boundary. A time-dependent test is a
// flaky one, and a flaky gate gets muted.
func TestFold_TheOPENParentFoldsWhatHasStarted(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 100})
	now := time.Now().UnixMilli()
	base := candle.BucketTimestamp(now, "15m")

	started := 0
	for i := 0; i < 3; i++ {
		if base+int64(i)*300_000 > now {
			break
		}
		ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", candle.Candle{
			Timestamp: base + int64(i)*300_000, Open: 100, High: 100, Low: 100, Close: 100,
			Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4,
			Source: string(redisstore.CandleMergeSourceWSKline), KlineEventAt: int64(i + 1),
		})
		started++
	}
	ce.FoldUp("binance", "future", "ETH/USDT", "5m", base)

	got := ce.GetCandleByTimestamp("binance", "future", "ETH/USDT", "15m", base)
	if got == nil {
		t.Fatal("the OPEN parent was not built — it would sit on its boot seed until it closes, " +
			"which is a week for 1w")
	}
	if want := float64(started) * 10; got.Volume != want {
		t.Fatalf("volume = %v, want %v — the open parent holds what has happened SO FAR", got.Volume, want)
	}
}

// TestFold_AStartedButMissingChildStillBlocks is the other half, and it is what keeps the rule
// above from becoming "fold whatever is lying around". A child whose bucket has started and is
// absent is missing DATA, and folding over it publishes a short bucket as measured.
func TestFold_AStartedButMissingChildStillBlocks(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 100})
	// Well in the past, so all three children have started long ago.
	base := candle.BucketTimestamp(time.Now().UnixMilli()-6*3600_000, "15m")

	// Only the first and THIRD exist: the middle one has started and is missing.
	for _, i := range []int{0, 2} {
		ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", candle.Candle{
			Timestamp: base + int64(i)*300_000, Open: 100, High: 100, Low: 100, Close: 100,
			Volume: 10, Source: string(redisstore.CandleMergeSourceWSKline), KlineEventAt: int64(i + 1),
		})
	}
	ce.FoldUp("binance", "future", "ETH/USDT", "5m", base)

	if got := ce.GetCandleByTimestamp("binance", "future", "ETH/USDT", "15m", base); got != nil {
		t.Fatalf("a parent was built with a started child MISSING (volume %v) — that is a bucket "+
			"short by construction, published as measured", got.Volume)
	}
}

// TestFold_PricesComeFromChildrenThatTraded pins the price-election rule.
//
// A child in which nothing traded carries the PREVIOUS close on open, high, low AND close — four
// prices that never occurred in its own interval. Guarding only `low` is not enough: illiquid
// symbols can fail on PRICE while volume and both taker legs match exactly, when the first 5m
// child of the bucket had no trades.
func TestFold_PricesComeFromChildrenThatTraded(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 100})
	base := candle.BucketTimestamp(horizonBucketTs, "15m")

	// child 0: NOTHING traded — carries the previous close (50) on every price field
	// child 1: traded,real range 100..120
	// child 2: NOTHING traded — carries 110 forward
	for i, spec := range []struct{ o, h, l, c, v float64 }{
		{50, 50, 50, 50, 0},
		{100, 120, 90, 110, 25},
		{110, 110, 110, 110, 0},
	} {
		ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", candle.Candle{
			Timestamp: base + int64(i)*300_000,
			Open:      spec.o, High: spec.h, Low: spec.l, Close: spec.c, Volume: spec.v,
			Source: string(redisstore.CandleMergeSourceWSKline), KlineEventAt: int64(i + 1),
		})
	}
	ce.FoldUp("binance", "future", "ETH/USDT", "5m", base)

	got := ce.GetCandleByTimestamp("binance", "future", "ETH/USDT", "15m", base)
	if got == nil {
		t.Fatal("the parent was not built")
	}
	for _, c := range []struct {
		name      string
		got, want float64
	}{{"open", got.Open, 100}, {"close", got.Close, 110}, {"high", got.High, 120}, {"low", got.Low, 90}} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v — an untraded child carries prices that never occurred in "+
				"its own interval and must not set the parent's", c.name, c.got, c.want)
		}
	}
	if got.Volume != 25 {
		t.Errorf("volume = %v, want 25", got.Volume)
	}
}

// TestFold_AnEntirelyUntradedParentKeepsTheCarriedPrice: if NO child traded, the interval genuinely
// had no activity and the carried price is exactly what the venue reports for such a bucket.
// Reporting zeros instead would invent a print.
func TestFold_AnEntirelyUntradedParentKeepsTheCarriedPrice(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 100})
	base := candle.BucketTimestamp(horizonBucketTs, "15m")
	for i := 0; i < 3; i++ {
		ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", candle.Candle{
			Timestamp: base + int64(i)*300_000, Open: 50, High: 50, Low: 50, Close: 50, Volume: 0,
			Source: string(redisstore.CandleMergeSourceWSKline), KlineEventAt: int64(i + 1),
		})
	}
	ce.FoldUp("binance", "future", "ETH/USDT", "5m", base)

	got := ce.GetCandleByTimestamp("binance", "future", "ETH/USDT", "15m", base)
	if got == nil {
		t.Fatal("an untraded parent must still be built — the venue publishes one")
	}
	if got.Open != 50 || got.Close != 50 || got.High != 50 || got.Low != 50 || got.Volume != 0 {
		t.Fatalf("got o=%v h=%v l=%v c=%v v=%v, want the carried 50 on every price and zero volume",
			got.Open, got.High, got.Low, got.Close, got.Volume)
	}
}
