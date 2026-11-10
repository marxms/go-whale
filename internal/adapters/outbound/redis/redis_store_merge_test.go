package redisstore

import (
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

func TestNormalizeCandleMergeSource_DefaultsToWSDelta(t *testing.T) {
	if got := normalizeCandleMergeSource(""); got != CandleMergeSourceWSDelta {
		t.Fatalf("expected default source ws_delta, got %s", got)
	}
}

func TestMergeOpenCandleBySource_WSDeltaIsAdditive(t *testing.T) {
	now := int64(1234)
	current := candle.Candle{
		Timestamp: 1000,
		Open:      100,
		High:      110,
		Low:       90,
		Close:     105,
		Volume:    10,
		CreatedAt: 111,
	}
	incoming := candle.Candle{
		Timestamp: 1000,
		Open:      101,
		High:      115,
		Low:       88,
		Close:     112,
		Volume:    4,
	}

	merged := mergeOpenCandleBySource(current, incoming, CandleMergeSourceWSDelta, now)

	if merged.Open != 100 {
		t.Fatalf("expected open=100, got %.2f", merged.Open)
	}
	if merged.High != 115 {
		t.Fatalf("expected high=115, got %.2f", merged.High)
	}
	if merged.Low != 88 {
		t.Fatalf("expected low=88, got %.2f", merged.Low)
	}
	if merged.Close != 112 {
		t.Fatalf("expected close=112, got %.2f", merged.Close)
	}
	if merged.Volume != 14 {
		t.Fatalf("expected volume=14, got %.2f", merged.Volume)
	}
	if merged.CreatedAt != 111 {
		t.Fatalf("expected created_at preserved, got %d", merged.CreatedAt)
	}
	if merged.LastUpdate != now {
		t.Fatalf("expected last_update=%d, got %d", now, merged.LastUpdate)
	}
}

func TestMergeOpenCandleBySource_RESTTotalIsIdempotent(t *testing.T) {
	now := int64(1234)
	current := candle.Candle{
		Timestamp: 1000,
		Open:      100,
		High:      110,
		Low:       90,
		Close:     105,
		Volume:    10,
		CreatedAt: 111,
	}
	incoming := candle.Candle{
		Timestamp: 1000,
		Open:      99,
		High:      115,
		Low:       88,
		Close:     112,
		Volume:    9,
	}

	merged := mergeOpenCandleBySource(current, incoming, CandleMergeSourceRESTTotal, now)

	if merged.Open != 99 {
		t.Fatalf("expected REST-open authoritative (99), got %.2f", merged.Open)
	}
	if merged.Volume != 10 {
		t.Fatalf("expected volume=max(10,9)=10, got %.2f", merged.Volume)
	}

	incomingHigher := incoming
	incomingHigher.Volume = 12
	mergedHigher := mergeOpenCandleBySource(current, incomingHigher, CandleMergeSourceRESTTotal, now)
	if mergedHigher.Volume != 12 {
		t.Fatalf("expected volume=max(10,12)=12, got %.2f", mergedHigher.Volume)
	}

	repeated := mergeOpenCandleBySource(mergedHigher, incomingHigher, CandleMergeSourceRESTTotal, now+1)
	if repeated.Volume != 12 {
		t.Fatalf("expected repeated REST total merge to stay idempotent at 12, got %.2f", repeated.Volume)
	}
}

func TestMergeOpenCandleBySource_SnapshotTotalUsesMaxVolume(t *testing.T) {
	now := int64(1234)
	current := candle.Candle{
		Timestamp: 1000,
		Open:      100,
		High:      110,
		Low:       90,
		Close:     105,
		Volume:    10,
		CreatedAt: 111,
	}
	incoming := candle.Candle{
		Timestamp: 1000,
		Open:      101,
		High:      115,
		Low:       88,
		Close:     112,
		Volume:    9,
	}

	merged := mergeOpenCandleBySource(current, incoming, CandleMergeSourceSnapshotTotal, now)

	if merged.Open != 100 {
		t.Fatalf("expected open to preserve current for snapshot totals, got %.2f", merged.Open)
	}
	if merged.Volume != 10 {
		t.Fatalf("expected volume=max(10,9)=10, got %.2f", merged.Volume)
	}
}

func TestMergeClosedCandleBySource_UsesMaxVolume(t *testing.T) {
	now := int64(5555)
	current := candle.Candle{
		Timestamp: 1000,
		Open:      100,
		High:      110,
		Low:       90,
		Close:     105,
		Volume:    10,
		CreatedAt: 111,
	}
	incoming := candle.Candle{
		Timestamp: 1000,
		Open:      99,
		High:      115,
		Low:       88,
		Close:     112,
		Volume:    9,
	}

	mergedWS := mergeClosedCandleBySource(current, incoming, CandleMergeSourceWSDelta, now)
	if mergedWS.Open != 100 {
		t.Fatalf("expected WS closed merge to preserve current open=100, got %.2f", mergedWS.Open)
	}
	if mergedWS.Volume != 10 {
		t.Fatalf("expected WS closed merge volume=max(10,9)=10, got %.2f", mergedWS.Volume)
	}

	mergedREST := mergeClosedCandleBySource(current, incoming, CandleMergeSourceRESTTotal, now)
	if mergedREST.Open != 99 {
		t.Fatalf("expected REST closed merge to use incoming open=99, got %.2f", mergedREST.Open)
	}
	if mergedREST.Volume != 10 {
		t.Fatalf("expected REST closed merge volume=max(10,9)=10, got %.2f", mergedREST.Volume)
	}
}

func TestMergeOpenCandleBySource_AggregatesAdditiveOnWSDelta(t *testing.T) {
	now := int64(2000)
	current := candle.Candle{
		Timestamp:        1000,
		Open:             100,
		High:             110,
		Low:              90,
		Close:            105,
		Volume:           10,
		CreatedAt:        111,
		TakerBuyVolume:   6,
		TakerSellVolume:  4,
		SideUnknownCount: 1,
		LateTradeCount:   0,
		Source:           "binance",
		AggregateMethod:  "native_ws",
	}
	incoming := candle.Candle{
		Timestamp:        1000,
		Open:             101,
		High:             115,
		Low:              88,
		Close:            112,
		Volume:           4,
		TakerBuyVolume:   3,
		TakerSellVolume:  1, // ignored — recomputed from taker fields
		SideUnknownCount: 0,
		LateTradeCount:   2,
	}

	merged := mergeOpenCandleBySource(current, incoming, CandleMergeSourceWSDelta, now)

	if merged.TakerBuyVolume != 9 {
		t.Fatalf("takerBuy: want 9 got %v", merged.TakerBuyVolume)
	}
	if merged.TakerSellVolume != 5 {
		t.Fatalf("takerSell: want 5 got %v", merged.TakerSellVolume)
	}
	if merged.SideUnknownCount != 1 {
		t.Fatalf("sideUnknownCount: want 1 got %d", merged.SideUnknownCount)
	}
	if merged.LateTradeCount != 2 {
		t.Fatalf("lateTradeCount: want 2 got %d", merged.LateTradeCount)
	}
	if merged.Source != "binance" {
		t.Fatalf("source: want binance got %q", merged.Source)
	}
	if merged.AggregateMethod != "native_ws" {
		t.Fatalf("aggregateMethod: want native_ws got %q", merged.AggregateMethod)
	}
}

func TestMergeOpenCandleBySource_RESTTotalPreservesAggregates(t *testing.T) {
	now := int64(2000)
	current := candle.Candle{
		Timestamp:        1000,
		Open:             100,
		High:             110,
		Low:              90,
		Close:            105,
		Volume:           10,
		CreatedAt:        111,
		TakerBuyVolume:   6,
		TakerSellVolume:  4,
		SideUnknownCount: 1,
	}
	// REST total carries no taker fields → adding zeros must preserve current.
	incoming := candle.Candle{
		Timestamp: 1000,
		Open:      99,
		High:      115,
		Low:       88,
		Close:     112,
		Volume:    9,
	}

	merged := mergeOpenCandleBySource(current, incoming, CandleMergeSourceRESTTotal, now)

	if merged.TakerBuyVolume != 6 || merged.TakerSellVolume != 4 {
		t.Fatalf("REST merge must not erode existing taker fields, got buy=%v sell=%v",
			merged.TakerBuyVolume, merged.TakerSellVolume)
	}
}

func TestMergeClosedCandleBySource_AggregatesReplaced(t *testing.T) {
	now := int64(5555)
	current := candle.Candle{
		Timestamp:        1000,
		Open:             100,
		Close:            105,
		Volume:           10,
		TakerBuyVolume:   6,
		TakerSellVolume:  4,
		SideUnknownCount: 1,
		LateTradeCount:   1,
		Source:           "binance",
		AggregateMethod:  "native_ws",
		HasGap:           false,
	}
	incoming := candle.Candle{
		Timestamp:        1000,
		Open:             100,
		Close:            105,
		Volume:           10,
		TakerBuyVolume:   9,
		TakerSellVolume:  1,
		SideUnknownCount: 0,
		LateTradeCount:   3,
		Source:           "binance-rest",
		AggregateMethod:  "rest_replay",
		HasGap:           true,
	}

	merged := mergeClosedCandleBySource(current, incoming, CandleMergeSourceRESTTotal, now)

	if merged.TakerBuyVolume != 9 || merged.TakerSellVolume != 1 {
		t.Fatalf("closed merge must replace taker fields, got buy=%v sell=%v",
			merged.TakerBuyVolume, merged.TakerSellVolume)
	}
	if merged.Source != "binance-rest" || merged.AggregateMethod != "rest_replay" {
		t.Fatalf("closed merge must replace source/method, got source=%q method=%q",
			merged.Source, merged.AggregateMethod)
	}
	if !merged.HasGap {
		t.Fatalf("hasGap should be true after replacement")
	}
}

// TestMergeOpenCandleBySource_SnapshotTotalReplacesAggregates: a SnapshotTotal carries the
// engine's cumulative bucket candle (authoritative total), so the merge must REPLACE the
// order-flow aggregates with incoming, never add (adding inflated taker_sum > volume live).
func TestMergeOpenCandleBySource_SnapshotTotalReplacesAggregates(t *testing.T) {
	now := int64(2000)
	current := candle.Candle{
		Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50,
		TakerBuyVolume: 30, TakerSellVolume: 20,
	}
	incoming := candle.Candle{
		Timestamp: 1000, Open: 100, High: 112, Low: 90, Close: 106, Volume: 60,
		TakerBuyVolume: 36, TakerSellVolume: 24,
	}
	merged := mergeOpenCandleBySource(current, incoming, CandleMergeSourceSnapshotTotal, now)

	if merged.TakerBuyVolume != 36 || merged.TakerSellVolume != 24 {
		t.Fatalf("SnapshotTotal must REPLACE taker, got buy=%v sell=%v (summed-bug would be 66/44)",
			merged.TakerBuyVolume, merged.TakerSellVolume)
	}
	if merged.Volume != 60 {
		t.Fatalf("volume MAX: want 60 got %v", merged.Volume)
	}
	if merged.TakerBuyVolume+merged.TakerSellVolume > merged.Volume {
		t.Fatalf("INVARIANT taker_sum (%v) > volume (%v)", merged.TakerBuyVolume+merged.TakerSellVolume, merged.Volume)
	}
}

// TestMergeOpenCandleBySource_RepeatedSnapshotNoInflation reproduces the live bug: the same
// cumulative engine candle flushed ~15x within a 5m bucket. An additive merge grows taker
// ~15x (taker_sum >> volume); the replace rule keeps it at the engine total.
func TestMergeOpenCandleBySource_RepeatedSnapshotNoInflation(t *testing.T) {
	now := int64(2000)
	engine := candle.Candle{
		Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50,
		TakerBuyVolume: 30, TakerSellVolume: 20,
	}
	merged := engine
	for range 15 {
		merged = mergeOpenCandleBySource(merged, engine, CandleMergeSourceSnapshotTotal, now)
	}
	if merged.TakerBuyVolume != 30 || merged.TakerSellVolume != 20 {
		t.Fatalf("repeated snapshots inflated taker: buy=%v sell=%v (want 30/20)", merged.TakerBuyVolume, merged.TakerSellVolume)
	}
	if merged.TakerBuyVolume+merged.TakerSellVolume > merged.Volume {
		t.Fatalf("INVARIANT taker_sum (%v) > volume (%v) after repeated snapshots",
			merged.TakerBuyVolume+merged.TakerSellVolume, merged.Volume)
	}
}

// TestMergeOpenCandleBySource_TotalDoesNotLetThePartsExceedTheWhole covers a branch the existing
// tests structurally could not reach.
//
// For a declared total, volume MAX-merges while the taker legs ACCUMULATED, so the parts could
// exceed the whole: current{100,60,40} with incoming{100,45,55} produced volume=100 buy=105
// sell=95 — a sum of 200 against a volume of 100.
//
// The reason it survived is worth naming: the sibling test passes an incoming whose taker pair is
// ZERO, so the accumulating branch adds nothing and the defect is invisible. A fixture that cannot
// express the failure is not coverage.
func TestMergeOpenCandleBySource_TotalDoesNotLetThePartsExceedTheWhole(t *testing.T) {
	t.Parallel()

	now := int64(1_700_000_000_000)
	current := candle.Candle{Timestamp: now, Volume: 100, TakerBuyVolume: 60, TakerSellVolume: 40}
	incoming := candle.Candle{Timestamp: now, Volume: 100, TakerBuyVolume: 45, TakerSellVolume: 55}

	got := mergeOpenCandleBySource(current, incoming, CandleMergeSourceRESTTotal, now)
	if sum := got.TakerBuyVolume + got.TakerSellVolume; sum > got.Volume+1e-9 {
		t.Fatalf("takerBuy+takerSell = %v exceeds volume %v — the whole MAX-merges and the parts "+
			"must obey the same rule", sum, got.Volume)
	}
}

// TestMergeOpenCandleBySource_TheVenueSourceIsRecognisedAsATotal guards a silent unit error.
//
// normalizeCandleMergeSource maps anything it does not recognize to ws_delta, so a venue candle —
// which is a bucket TOTAL — would have been ACCUMULATED here. That is the same shape as the 4,4x
// gap-recovery inflation the function already guards against, and as the +82,5% ETH bucket. A
// source this store does not know is not a harmless label.
func TestMergeOpenCandleBySource_TheVenueSourceIsRecognisedAsATotal(t *testing.T) {
	t.Parallel()

	if got := normalizeCandleMergeSource(CandleMergeSourceWSKline); got != CandleMergeSourceWSKline {
		t.Fatalf("normalised to %q — an unrecognized total is accumulated as a delta", got)
	}
	if !isTotalMergeSource(CandleMergeSourceWSKline) {
		t.Fatal("the venue's own candle is a bucket TOTAL and must dispatch as one")
	}
}

// TestMergeOpenCandleBySource_NothingOverwritesTheVenue mirrors the engine's veto into the hot
// copy, and it is not redundant: /api/timeseries is a MERGED read that resolves a duplicate
// timestamp in favor of hot, so a correct engine value shadowed by a stale hot one is invisible
// to every gate that reads the API. That is exactly how the gap was found — the newest bucket kept
// coming back as rest_total while the engine held ws_kline.
func TestMergeOpenCandleBySource_NothingOverwritesTheVenue(t *testing.T) {
	t.Parallel()

	now := int64(1_700_000_000_000)
	venue := candle.Candle{
		Timestamp: now, Volume: 180, TakerBuyVolume: 110, TakerSellVolume: 70,
		Source: string(CandleMergeSourceWSKline),
	}
	intruder := candle.Candle{
		Timestamp: now, Volume: 150, TakerBuyVolume: 90, TakerSellVolume: 60,
		Source: string(CandleMergeSourceRESTTotal),
	}

	got := mergeOpenCandleBySource(venue, intruder, CandleMergeSourceRESTTotal, now)
	if got.Volume != 180 || got.Source != string(CandleMergeSourceWSKline) {
		t.Fatalf("got vol=%v source=%q, want the venue's 180 — a snapshot of a number must not "+
			"overwrite the number itself", got.Volume, got.Source)
	}
}
