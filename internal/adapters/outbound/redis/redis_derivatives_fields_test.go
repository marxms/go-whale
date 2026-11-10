package redisstore

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"
)

func newTestDerivStore(t *testing.T, timeframes ...string) (*DerivativesRedisStore, *redis.Client) {
	t.Helper()
	m := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: m.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	return NewDerivativesRedisStore(client,
		config.DerivativesConfig{MaxSeriesSize: 200},
		candle.NewTimeframeRegistry(timeframes)), client
}

// TestSaveSnapshot_AdditiveOpenInterestField asserts the migration is additive BY
// FIELD: the contract quantity is written alongside the notional one, the notional
// keeps its name and its value, and nothing existing is reinterpreted. The rollback
// is "stop reading the new field", which only holds if the old one is still there.
func TestSaveSnapshot_AdditiveOpenInterestField(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1h")

	snap := &derivatives.DerivativesSnapshot{
		Exchange:         "binance",
		MarketType:       "future",
		Symbol:           "BTCUSDT",
		Timestamp:        1_700_000_000_000,
		OpenInterestBase: fptr(84_231.5),        // contracts — decision input
		OpenInterest:     fptr(5_432_100_000.0), // notional — display only
		FundingRate:      fptr(0.0001),
	}
	if err := store.SaveSnapshot(ctx, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	latest, err := store.GetLatestSnapshot(ctx, "binance", "future", "BTCUSDT")
	if err != nil {
		t.Fatalf("GetLatestSnapshot: %v", err)
	}
	if latest.OpenInterestBase == nil || *latest.OpenInterestBase != 84_231.5 {
		t.Fatalf("openInterestBase = %v, want 84231.5", latest.OpenInterestBase)
	}
	if latest.OpenInterest == nil || *latest.OpenInterest != 5_432_100_000.0 {
		t.Fatalf("openInterest = %v, want 5432100000 — writing the new field changed the old one", latest.OpenInterest)
	}

	// The wire names matter as much as the values: five consumers read this key
	// directly, and a renamed field is an outage for all of them.
	raw, err := client.Get(ctx, derivLatestScopedKey("binance", "future", "BTCUSDT")).Bytes()
	if err != nil {
		t.Fatalf("raw get: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	for _, field := range []string{"openInterestBase", "openInterest"} {
		if _, ok := decoded[field]; !ok {
			t.Fatalf("field %q missing from the persisted member: %s", field, raw)
		}
	}

	// Same guarantee on the series member.
	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1h", 1)
	if err != nil {
		t.Fatalf("GetSnapshotSeries: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("series length = %d, want 1", len(series))
	}
	if series[0].OpenInterestBase == nil || series[0].OpenInterest == nil {
		t.Fatalf("series member lost an open-interest field: %+v", series[0])
	}
}

// TestSaveSnapshot_PartialFetchDoesNotEraseFields is the merge asymmetry: the series
// merged field by field while the latest key overwrote the whole snapshot, so a cycle
// that fetched funding but not open interest erased an open interest that had been
// read correctly minutes earlier.
func TestSaveSnapshot_PartialFetchDoesNotEraseFields(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "1h")

	full := &derivatives.DerivativesSnapshot{
		Exchange:             "binance",
		MarketType:           "future",
		Symbol:               "BTCUSDT",
		Timestamp:            1_700_000_000_000,
		OpenInterestBase:     fptr(84_231.5),
		OpenInterest:         fptr(5_432_100_000.0),
		LongShortRatio:       fptr(1.8),
		FundingRate:          fptr(0.0001),
		FundingRate8h:        fptr(0.0002),
		FundingIntervalHours: fptr(4),
		NStep:                derivatives.NFromMeasuredSpacing,
		InterestRate:         fptr(0.0001),
	}
	if err := store.SaveSnapshot(ctx, full); err != nil {
		t.Fatalf("SaveSnapshot(full): %v", err)
	}

	// Next cycle: only funding came back. Everything else failed to fetch and is
	// ABSENT, not zero.
	partial := &derivatives.DerivativesSnapshot{
		Exchange:    "binance",
		MarketType:  "future",
		Symbol:      "BTCUSDT",
		Timestamp:   1_700_000_060_000,
		FundingRate: fptr(0.0003),
	}
	if err := store.SaveSnapshot(ctx, partial); err != nil {
		t.Fatalf("SaveSnapshot(partial): %v", err)
	}

	latest, err := store.GetLatestSnapshot(ctx, "binance", "future", "BTCUSDT")
	if err != nil {
		t.Fatalf("GetLatestSnapshot: %v", err)
	}
	if latest.OpenInterestBase == nil || *latest.OpenInterestBase != 84_231.5 {
		t.Fatalf("openInterestBase erased by a partial fetch: %v", latest.OpenInterestBase)
	}
	if latest.OpenInterest == nil || *latest.OpenInterest != 5_432_100_000.0 {
		t.Fatalf("openInterest erased by a partial fetch: %v", latest.OpenInterest)
	}
	if latest.LongShortRatio == nil || *latest.LongShortRatio != 1.8 {
		t.Fatalf("longShortRatio erased by a partial fetch: %v", latest.LongShortRatio)
	}
	// FUNDING IS THE EXCEPTION, AND THE EXCEPTION IS THE POINT.
	//
	// Open interest and the long/short ratio are INDEPENDENT observations: preserving
	// one while another is refreshed leaves both individually true, which is why the
	// assertions above are right.
	//
	// The funding fields are not independent — they are one rate plus the interval it
	// was normalized by, bound by fundingRate8h == fundingRate × 8/N. Requiring the
	// metadata to survive here is the defect the live fleet exposed: with the fresh 0.0003
	// and the preserved N=4, the preserved 0.0002 no longer normalizes anything
	// (0.0003 × 8/4 = 0.0006). That assertion would protect a state that cannot be true.
	//
	// So the group travels with the rate. This partial read carried a rate and nothing
	// derived, so nothing derived survives — absent, which is visibly missing, instead
	// of stale, which is confidently wrong.
	if latest.FundingRate == nil || *latest.FundingRate != 0.0003 {
		t.Fatalf("fundingRate = %v, want the fresh 0.0003", latest.FundingRate)
	}
	if latest.FundingRate8h != nil {
		t.Fatalf("fundingRate8h = %v survived a read that produced no interval — it now describes the rate it displaced",
			*latest.FundingRate8h)
	}
	if latest.FundingIntervalHours != nil {
		t.Fatalf("fundingIntervalHours = %v outlived the rate it described", *latest.FundingIntervalHours)
	}
	if latest.NStep != "" {
		t.Fatalf("nStep = %q outlived the rate it described", latest.NStep)
	}
	if latest.Timestamp != 1_700_000_060_000 {
		t.Fatalf("timestamp = %d, want the fresh one", latest.Timestamp)
	}
}

// TestMergeDerivativesSnapshots_ClockDoesNotGoBackwards asserts a late-arriving older
// read may still contribute a field but cannot backdate the record it lands in.
func TestMergeDerivativesSnapshots_ClockDoesNotGoBackwards(t *testing.T) {
	existing := &derivatives.DerivativesSnapshot{Timestamp: 2000, ObservedAt: 2100, FundingRate: fptr(0.1)}
	incoming := &derivatives.DerivativesSnapshot{Timestamp: 1000, ObservedAt: 1100, LongShortRatio: fptr(1.2)}

	merged := mergeDerivativesSnapshots(existing, incoming)
	if merged.Timestamp != 2000 || merged.ObservedAt != 2100 {
		t.Fatalf("clock moved backwards: ts=%d observedAt=%d", merged.Timestamp, merged.ObservedAt)
	}
	if merged.LongShortRatio == nil || merged.FundingRate == nil {
		t.Fatalf("merge dropped a field: %+v", merged)
	}
}

// TestMergeDerivativesSnapshots_CoversEveryOptionalField is the guard against the
// defect this whole file exists for: a field added to the domain type and forgotten
// in the merge is written once and then silently dropped forever.
func TestMergeDerivativesSnapshots_CoversEveryOptionalField(t *testing.T) {
	existing := &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT", Timestamp: 1,
	}
	incoming := &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT", Timestamp: 2,

		OpenInterestBase:     fptr(1),
		OpenInterest:         fptr(2),
		OIChangePct:          fptr(3),
		FundingRate:          fptr(4),
		NextFundingRate:      fptr(5),
		FundingRate8h:        fptr(6),
		FundingIntervalHours: fptr(7),
		NStep:                derivatives.NFromMetadataSeed,
		InterestRate:         fptr(8),
		FundingAtAnchor:      func() *bool { b := true; return &b }(),
		LongShortRatio:       fptr(9),
		LSLongPct:            fptr(10),
		LSShortPct:           fptr(11),
	}

	merged := mergeDerivativesSnapshots(existing, incoming)

	// Round-tripping through JSON compares the WIRE shape, so a field the merge
	// forgot shows up as a missing key rather than as a subtly different struct.
	wantJSON, err := json.Marshal(incoming)
	if err != nil {
		t.Fatalf("marshal incoming: %v", err)
	}
	gotJSON, err := json.Marshal(merged)
	if err != nil {
		t.Fatalf("marshal merged: %v", err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("merge did not carry every field.\n got: %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestMergeDerivativesSnapshots_UnavailableIntervalDropsDerivedFunding asserts the
// one place where preserving a field is the WRONG answer.
//
// fundingRate8h, fundingIntervalHours and fundingAtAnchor are not observations, they
// are products of an interval. When the ladder reports it has no interval the incoming
// snapshot carries none of them, and the field-by-field rule would republish the
// previous ones under a declaration that says none exists — absence coming back as a
// value through the merge.
func TestMergeDerivativesSnapshots_UnavailableIntervalDropsDerivedFunding(t *testing.T) {
	anchored := true
	existing := &derivatives.DerivativesSnapshot{
		Symbol:               "BTCUSDT",
		Timestamp:            1000,
		FundingRate:          fptr(0.0001),
		FundingRate8h:        fptr(0.0001),
		FundingIntervalHours: fptr(8),
		NStep:                derivatives.NFromMeasuredSpacing,
		FundingAtAnchor:      &anchored,
	}
	// A later poll where neither rung produced an interval: the raw rate is still a
	// real observation, everything derived from an interval is not.
	incoming := &derivatives.DerivativesSnapshot{
		Symbol:      "BTCUSDT",
		Timestamp:   2000,
		FundingRate: fptr(0.0002),
		NStep:       derivatives.NUnavailable,
	}

	merged := mergeDerivativesSnapshots(existing, incoming)

	if merged.NStep != derivatives.NUnavailable {
		t.Fatalf("nStep = %q, want unavailable", merged.NStep)
	}
	if merged.FundingRate8h != nil {
		t.Fatalf("fundingRate8h = %v survived nStep=unavailable — the spec forbids emitting one", *merged.FundingRate8h)
	}
	if merged.FundingIntervalHours != nil {
		t.Fatalf("fundingIntervalHours = %v survived nStep=unavailable", *merged.FundingIntervalHours)
	}
	if merged.FundingAtAnchor != nil {
		t.Fatalf("fundingAtAnchor = %v survived nStep=unavailable — the anchor is interestRate × N/8 and there is no N",
			*merged.FundingAtAnchor)
	}
	// The raw reading is an observation and must NOT be collateral damage.
	if merged.FundingRate == nil || *merged.FundingRate != 0.0002 {
		t.Fatalf("raw fundingRate lost: %v", merged.FundingRate)
	}
}

// TestMergeDerivativesSnapshots_IncoherentExistingIsRepairedByTheMerge is the case
// that actually EXERCISES the coherence guard.
//
// The test above it passes with the guard deleted: there the incoming read carries a
// rate, so the whole funding group is copied from incoming, which already has the
// three derived fields nil. The guard runs over an already-clean result and removes
// nothing. This case is different — the incoming read carries NO rate, so the group
// is preserved wholesale from `existing`, and only the guard can undo what `existing`
// was already carrying wrong.
//
// # WHY AN INCOHERENT `existing` IS REACHABLE, NOT HYPOTHETICAL
//
// The record in Redis outlives the build that wrote it. Any snapshot persisted before
// the guard existed — or by a build whose ladder set nStep=unavailable while still
// stamping a normalized rate — sits in the store today with fundingRate8h present
// under nStep=unavailable. Every subsequent rateless poll (a failed FetchFundingRate,
// which is the common case here) copies that state forward untouched. Without the
// guard the incoherence is not a transient, it is PERMANENT: an 8h-equivalent
// republished forever under a declaration that says no interval exists.
//
// That is Lei 2 broken through the back door — absence turning back into a value —
// and the guard is the only thing standing in front of it.
func TestMergeDerivativesSnapshots_IncoherentExistingIsRepairedByTheMerge(t *testing.T) {
	anchored := true
	// Persisted by an older build: the ladder declared it has NO interval, yet the
	// three interval-derived fields are present.
	existing := &derivatives.DerivativesSnapshot{
		Symbol:               "BTCUSDT",
		Timestamp:            1000,
		FundingRate:          fptr(0.0001),
		FundingRate8h:        fptr(0.0008),
		FundingIntervalHours: fptr(8),
		FundingAtAnchor:      &anchored,
		NStep:                derivatives.NUnavailable,
	}
	// A rateless read: the funding fetch failed, so the funding group is preserved
	// from `existing` rather than replaced. Nothing in the incoming snapshot can
	// clear the derived fields — only the guard can.
	incoming := &derivatives.DerivativesSnapshot{
		Symbol:           "BTCUSDT",
		Timestamp:        2000,
		OpenInterestBase: fptr(107163.348),
	}

	merged := mergeDerivativesSnapshots(existing, incoming)

	if merged.NStep != derivatives.NUnavailable {
		t.Fatalf("nStep = %q, want %q — the preserved group keeps its declaration",
			merged.NStep, derivatives.NUnavailable)
	}
	if merged.FundingRate8h != nil {
		t.Fatalf("fundingRate8h = %v survived nStep=unavailable — a normalized rate republished "+
			"under a declaration that no interval exists", *merged.FundingRate8h)
	}
	if merged.FundingIntervalHours != nil {
		t.Fatalf("fundingIntervalHours = %v survived nStep=unavailable — the step and the interval "+
			"contradict each other", *merged.FundingIntervalHours)
	}
	if merged.FundingAtAnchor != nil {
		t.Fatalf("fundingAtAnchor = %v survived nStep=unavailable — the anchor is decided against "+
			"interestRate × N/8 and there is no N", *merged.FundingAtAnchor)
	}
	// The raw rate is an OBSERVATION, not a product of the interval: the guard must
	// not take it down with the derived three.
	if merged.FundingRate == nil || *merged.FundingRate != 0.0001 {
		t.Fatalf("raw fundingRate = %v, want 0.0001 — the observation is not collateral damage",
			merged.FundingRate)
	}
	// And the unrelated observation in the incoming read still lands.
	if merged.OpenInterestBase == nil || *merged.OpenInterestBase != 107163.348 {
		t.Fatalf("openInterestBase = %v, want 107163.348", merged.OpenInterestBase)
	}
}

// TestUpsertSeries_CapIsPerTimeframe asserts the cap that governs retention is the
// timeframe's own. Under the single configured cap of 200 members, the declared
// 288-point baseline window of 5m was unreachable no matter how much was collected.
func TestUpsertSeries_CapIsPerTimeframe(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "5m")

	const tfMs int64 = 5 * 60 * 1000
	startTs := bucketAlignMs(1_700_000_000_000, "5m")

	for i := 0; i < 320; i++ {
		snap := &derivatives.DerivativesSnapshot{
			Exchange:         "binance",
			MarketType:       "future",
			Symbol:           "BTCUSDT",
			Timestamp:        startTs + int64(i)*tfMs,
			OpenInterestBase: fptr(float64(1000 + i)),
		}
		if err := store.SaveSeriesOnly(ctx, snap, "5m"); err != nil {
			t.Fatalf("SaveSeriesOnly[%d]: %v", i, err)
		}
	}

	card, err := client.ZCard(ctx, derivSeriesZsetKey("binance", "future", "BTCUSDT", "5m")).Result()
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if want := store.seriesCapFor("5m"); card != want {
		t.Fatalf("series length = %d, want the 5m cap of %d", card, want)
	}
	if card <= 200 {
		t.Fatalf("series capped at %d — the configured floor still starves the 288-point window", card)
	}
}

// TestMergeDerivativesSnapshots_FundingGroupMovesAtomically pins the invariant that a
// field-by-field merge silently broke on the live fleet.
//
// PROVENANCE: measured against a running instance. Across 611 series points carrying the
// full funding trio, 59 (9.7%) violated
// fundingRate8h == fundingRate × 8/N. The sharpest case was ENA/USDT at N=4h:
//
//	fundingRate 3.716e-05 → fundingRate8h -7.032e-05   (identity demands +7.432e-05)
//	fundingRate 3.221e-05 → fundingRate8h +7.432e-05   (identity demands +6.442e-05)
//
// The +7.432e-05 on the second row is EXACTLY the correct normalization of the FIRST
// row's rate — the 8h-equivalent was one observation behind the rate it claimed to
// normalize, and in the first row that lag put a POSITIVE rate behind a NEGATIVE
// normalization. A trader reading the panel would have seen shorts paying longs on a
// contract where longs were paying shorts.
//
// The cause was that each funding field had its own `if incoming.X != nil` guard, so
// a read carrying the rate but not the derived value updated one and preserved the
// other. The two facts then described different instants.
func TestMergeDerivativesSnapshots_FundingGroupMovesAtomically(t *testing.T) {
	anchored := false
	// A complete, self-consistent previous observation at N=4h.
	existing := &derivatives.DerivativesSnapshot{
		Symbol:               "ENAUSDT",
		Timestamp:            1000,
		FundingRate:          fptr(-3.516e-05),
		FundingRate8h:        fptr(-7.032e-05), // == -3.516e-05 × 8/4 ✓
		FundingIntervalHours: fptr(4),
		InterestRate:         fptr(0.0001),
		NStep:                derivatives.NFromMeasuredSpacing,
		FundingAtAnchor:      &anchored,
	}
	// A later read that carries the RATE but whose interval could not be resolved, so
	// nothing derived came with it. Field-by-field, this updated FundingRate and left
	// every derived field standing — producing the ENA/USDT row above verbatim.
	incoming := &derivatives.DerivativesSnapshot{
		Symbol:      "ENAUSDT",
		Timestamp:   2000,
		FundingRate: fptr(3.716e-05),
	}

	merged := mergeDerivativesSnapshots(existing, incoming)

	if merged.FundingRate == nil || *merged.FundingRate != 3.716e-05 {
		t.Fatalf("fundingRate = %v, want the incoming observation 3.716e-05", merged.FundingRate)
	}
	// The whole derived group came from the incoming read, which carried none of it.
	// Absence is the correct answer here: a stale 8h-equivalent is worse than none,
	// because none is visibly missing and stale is confidently wrong.
	if merged.FundingRate8h != nil {
		t.Fatalf("fundingRate8h = %v survived a read that did not produce one — this is the ENA/USDT defect",
			*merged.FundingRate8h)
	}
	if merged.FundingIntervalHours != nil {
		t.Fatalf("fundingIntervalHours = %v outlived the rate it described", *merged.FundingIntervalHours)
	}
	if merged.FundingAtAnchor != nil {
		t.Fatalf("fundingAtAnchor = %v outlived the rate it described", *merged.FundingAtAnchor)
	}
	if merged.NStep != "" {
		t.Fatalf("nStep = %q outlived the rate it described", merged.NStep)
	}
}

// TestMergeDerivativesSnapshots_RatelessReadPreservesTheWholeGroup is the other
// direction of the same invariant: when FetchFundingRate fails there is no
// observation at all, and the previous complete group must survive UNTOUCHED.
// Adopting just the incoming interval would leave the interval describing a rate that
// was never observed — the same incoherence, mirrored.
func TestMergeDerivativesSnapshots_RatelessReadPreservesTheWholeGroup(t *testing.T) {
	anchored := true
	existing := &derivatives.DerivativesSnapshot{
		Symbol:               "BTCUSDT",
		Timestamp:            1000,
		FundingRate:          fptr(0.0001),
		FundingRate8h:        fptr(0.0001),
		FundingIntervalHours: fptr(8),
		InterestRate:         fptr(0.0001),
		NStep:                derivatives.NFromMeasuredSpacing,
		FundingAtAnchor:      &anchored,
	}
	// The funding fetch failed; the interval ladder still resolved a rung, but there
	// is no rate for it to describe.
	incoming := &derivatives.DerivativesSnapshot{
		Symbol:               "BTCUSDT",
		Timestamp:            2000,
		FundingIntervalHours: fptr(4),
		NStep:                derivatives.NFromMetadataSeed,
		OpenInterestBase:     fptr(107163.348),
	}

	merged := mergeDerivativesSnapshots(existing, incoming)

	if merged.FundingRate == nil || *merged.FundingRate != 0.0001 {
		t.Fatalf("fundingRate = %v — a failed funding fetch must not erase the last good rate", merged.FundingRate)
	}
	if merged.FundingRate8h == nil || *merged.FundingRate8h != 0.0001 {
		t.Fatalf("fundingRate8h = %v — it belongs to the preserved rate and must stay with it", merged.FundingRate8h)
	}
	if merged.FundingIntervalHours == nil || *merged.FundingIntervalHours != 8 {
		t.Fatalf("fundingIntervalHours = %v — adopting the incoming 4h would leave it describing a rate observed at 8h",
			merged.FundingIntervalHours)
	}
	if merged.NStep != derivatives.NFromMeasuredSpacing {
		t.Fatalf("nStep = %q — it names the rung that produced the PRESERVED interval", merged.NStep)
	}
	// The unrelated observation in the same read still lands.
	if merged.OpenInterestBase == nil || *merged.OpenInterestBase != 107163.348 {
		t.Fatalf("openInterestBase = %v — open interest is not part of the funding group", merged.OpenInterestBase)
	}
}
