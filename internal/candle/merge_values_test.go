package candle

import "testing"

const mergeValuesBucketTs = int64(1_700_000_100_000)

// TestMergeCandleValues_TotalElectsTheTrioAtomically pins the atomic-election rule at the shared
// merge site. It is reachable from MergeLateIntoClosed, from MergeCandleValues directly, and from
// UnionSeedInto.
//
// Three independent maxima are forbidden: stored{100,60,40} + total{100,45,55} would yield
// volume=100 buy=60 sell=55, a sum of 115 that ValidateCandleAggregates itself rejects.
func TestMergeCandleValues_TotalElectsTheTrioAtomically(t *testing.T) {
	t.Parallel()

	stored := Candle{Timestamp: mergeValuesBucketTs, Volume: 100, TakerBuyVolume: 60, TakerSellVolume: 40}
	incoming := Candle{Timestamp: mergeValuesBucketTs, Volume: 100, TakerBuyVolume: 45, TakerSellVolume: 55}

	got := MergeCandleValues(stored, incoming, true)
	if sum := got.TakerBuyVolume + got.TakerSellVolume; sum > got.Volume+1e-9 {
		t.Fatalf("takerBuy+takerSell = %v exceeds volume %v — the legs were elected from a "+
			"different record than the volume", sum, got.Volume)
	}
	if ok, msg := ValidateCandleAggregates(got); !ok {
		t.Fatalf("the merged candle fails the project's own invariant: %s", msg)
	}
}

// TestOwner_TheLabelIsNotTransferable closes the way a wrong number could become permanent.
//
// Copying incoming.Source blindly would let a REST total that won the magnitude election inherit
// the ws_kline credential from the bucket it overwrote. VenueHasSpoken would then defend that
// number against every later correction — including the venue's own.
func TestOwner_TheLabelIsNotTransferable(t *testing.T) {
	t.Parallel()

	stored := Candle{
		Timestamp: mergeValuesBucketTs, Volume: 100, TakerBuyVolume: 60, TakerSellVolume: 40,
		Source: string(CandleMergeSourceWSKline), KlineEventAt: 1_000,
	}
	// An inflated total, larger on magnitude, wearing no venue credential of its own.
	intruder := Candle{
		Timestamp: mergeValuesBucketTs, Volume: 500, TakerBuyVolume: 300, TakerSellVolume: 200,
		Source: string(CandleMergeSourceRESTTotal),
	}

	got := MergeCandleValues(stored, intruder, true)
	if got.Volume != 100 || got.Source != string(CandleMergeSourceWSKline) {
		t.Fatalf("got vol=%v source=%q — the venue's bucket must survive an inflated total",
			got.Volume, got.Source)
	}
}

// TestOwner_ALateVenueFrameREPLACES_orderedByItsOwnClock: a venue frame arriving after the
// bucket rolled must not be decided by MAGNITUDE, or a restatement DOWNWARDS is discarded in
// silence. Magnitude is the one rule this engine states can never be used.
func TestOwner_ALateVenueFrameREPLACES_orderedByItsOwnClock(t *testing.T) {
	t.Parallel()

	stored := Candle{
		Timestamp: mergeValuesBucketTs, Volume: 500, TakerBuyVolume: 300, TakerSellVolume: 200,
		Source: string(CandleMergeSourceWSKline), KlineEventAt: 1_000,
	}
	// The venue restates the bucket DOWNWARDS, later on its own clock.
	revision := Candle{
		Timestamp: mergeValuesBucketTs, Volume: 100, TakerBuyVolume: 60, TakerSellVolume: 40,
		Source: string(CandleMergeSourceWSKline), KlineEventAt: 2_000,
	}

	if got := MergeCandleValues(stored, revision, true); got.Volume != 100 {
		t.Fatalf("volume = %v, want the venue's restatement 100 — deciding by magnitude discards a "+
			"downward revision in silence", got.Volume)
	}
	// And the older frame must not win by arriving later.
	older := revision
	older.KlineEventAt = 500
	older.Volume = 900
	if got := MergeCandleValues(revision, older, true); got.Volume != 100 {
		t.Fatalf("volume = %v, want 100 — an earlier venue frame must not win by arriving second", got.Volume)
	}
}
