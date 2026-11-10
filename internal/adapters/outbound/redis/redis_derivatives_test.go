package redisstore

import (
	"context"
	"math"
	"testing"

	"github.com/marxms/go-whale/internal/derivatives"
)

// ============================================================================
// AN INVALID VALUE DOES NOT CANCEL THE SERIES
// ============================================================================
//
// `json.Marshal` cannot represent `±Inf` or `NaN`, and the marshal `return` precedes
// `upsertSeriesBatch`: ONE unrepresentable field took down the series write for that symbol on
// ALL timeframes, plus the baseline, with a `retryOp` that repeats the same failure. The
// outcome is not a missing row — it is the whole symbol missing.
//
// Total serialization: replace the unrepresentable field with ABSENCE before the marshal,
// count the discard and continue. The case asserts the three required effects — batch called,
// field absent at the destination and discard counted — and two more the design implies: the
// healthy fields reach the destination, and the discard is counted ONCE (the sweep is before
// `retryOp`, so a retry does not count it again).

func nonFiniteSnapshot() (*derivatives.DerivativesSnapshot, float64) {
	const sadio = 84_231.5
	return &derivatives.DerivativesSnapshot{
		Exchange:         "binance",
		MarketType:       "future",
		Symbol:           "BTCUSDT",
		Timestamp:        1_787_586_300_000,
		OpenInterestBase: fptr(sadio),
		FundingRate:      fptr(math.Inf(1)),
		LongShortRatio:   fptr(1.8342),
	}, sadio
}

func TestSaveSnapshot_NonRepresentableFieldDoesNotCancelTheSeries(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "1h", "1d")
	discarded := 0
	store.SetMetrics(Metrics{SeriesNonFiniteDiscarded: func(field string) {
		if field == "fundingRate" {
			discarded++
		}
	}})

	snap, sadio := nonFiniteSnapshot()

	if err := store.SaveSnapshot(ctx, snap); err != nil {
		t.Fatalf("SaveSnapshot returned %v — an unrepresentable field must be DISCARDED, "+
			"not take down the write", err)
	}

	// (1) The discard was counted, and ONCE: `SaveSnapshot` wraps the write in a `retryOp`, and
	// a sweep running inside it would count again on every attempt.
	if delta := discarded; delta != 1 {
		t.Fatalf("discard counter rose by %v, want 1 — the discard must be counted "+
			"exactly once per snapshot", delta)
	}

	// (2) The field arrives ABSENT at the destination — not zero, not +Inf.
	latest, err := store.GetLatestSnapshot(ctx, "binance", "future", "BTCUSDT")
	if err != nil {
		t.Fatalf("GetLatestSnapshot: %v", err)
	}
	if latest == nil {
		t.Fatal("the `latest` record was not written — the field cleanup skipped step 1")
	}
	if latest.FundingRate != nil {
		t.Fatalf("FundingRate arrived at `latest` as %v — the unrepresentable field had "+
			"to become ABSENT", *latest.FundingRate)
	}

	// (3) The series batch WAS called: the bucket exists and the healthy field is in it.
	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1h", 0)
	if err != nil {
		t.Fatalf("GetSnapshotSeries: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("the series has %d points, want 1 — the batch was not called, and that is the defect: "+
			"one bad row erases the symbol from every timeframe", len(series))
	}
	if series[0].FundingRate != nil {
		t.Fatalf("FundingRate arrived at the series as %v, want absent", *series[0].FundingRate)
	}
	if series[0].OpenInterestBase == nil || *series[0].OpenInterestBase != sadio {
		t.Fatalf("OpenInterestBase = %v, want %v — the healthy field must survive the discard "+
			"of its sibling", series[0].OpenInterestBase, sadio)
	}
	if series[0].LongShortRatio == nil || *series[0].LongShortRatio != 1.8342 {
		t.Fatalf("LongShortRatio = %v, want 1.8342", series[0].LongShortRatio)
	}
}

// THE CONTRAST: a clean snapshot does not move the counter and arrives whole at the
// destination. Without this case, an implementation that erased fields at random would pass
// the case above.
func TestSaveSnapshot_CleanSnapshotIsUntouchedAndUncounted(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "1h")
	discarded := 0
	store.SetMetrics(Metrics{SeriesNonFiniteDiscarded: func(field string) {
		if field == "fundingRate" {
			discarded++
		}
	}})

	snap, sadio := nonFiniteSnapshot()
	snap.FundingRate = fptr(0.0001)

	if err := store.SaveSnapshot(ctx, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	if delta := discarded; delta != 0 {
		t.Fatalf("the counter rose by %v on a clean snapshot", delta)
	}

	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1h", 0)
	if err != nil {
		t.Fatalf("GetSnapshotSeries: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("the series has %d points, want 1", len(series))
	}
	if series[0].FundingRate == nil || *series[0].FundingRate != 0.0001 {
		t.Fatalf("FundingRate = %v, want 0.0001 — the clean path must not be touched", series[0].FundingRate)
	}
	if series[0].OpenInterestBase == nil || *series[0].OpenInterestBase != sadio {
		t.Fatalf("OpenInterestBase = %v, want %v", series[0].OpenInterestBase, sadio)
	}
}

// THE BACKFILL PATH ALSO COUNTS.
//
// `SaveSeriesOnly` writes through the codec guard, which replaces non-finite values with
// absence: the series is preserved (right) and the discard stays SILENT (wrong). The counter
// exists on the snapshot path too, and the backfill's two callers (`history_pagination.go`,
// `derivatives_poller.go`) arrive here RAW. Counting only on the live path would pass the
// whole suite with the silence in place.
func TestSaveSeriesOnly_BackfillCountsTheDiscardLikeTheLivePath(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "1h")
	discarded := 0
	store.SetMetrics(Metrics{SeriesNonFiniteDiscarded: func(field string) {
		if field == "fundingRate" {
			discarded++
		}
	}})

	snap, sadio := nonFiniteSnapshot()

	if err := store.SaveSeriesOnly(ctx, snap, "1h"); err != nil {
		t.Fatalf("SaveSeriesOnly returned %v — an unrepresentable field must be DISCARDED, "+
			"not take down the write", err)
	}

	if delta := discarded; delta != 1 {
		t.Fatalf("discard counter rose by %v on the backfill, want 1 — the discard must be "+
			"counted here too, and once per snapshot", delta)
	}

	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1h", 0)
	if err != nil {
		t.Fatalf("GetSnapshotSeries: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("the series has %d points, want 1", len(series))
	}
	if series[0].FundingRate != nil {
		t.Fatalf("FundingRate arrived at the series as %v, want absent — never zero and never +Inf",
			*series[0].FundingRate)
	}
	if series[0].OpenInterestBase == nil || *series[0].OpenInterestBase != sadio {
		t.Fatalf("OpenInterestBase = %v, want %v — the healthy sibling survives the discard",
			series[0].OpenInterestBase, sadio)
	}

	// CONTRACT CONTRAST: the backfill is SERIES ONLY. It does not write the `latest` key, and
	// that difference must not be erased by the shared sanitization.
	latest, err := store.GetLatestSnapshot(ctx, "binance", "future", "BTCUSDT")
	if err != nil {
		t.Fatalf("GetLatestSnapshot: %v", err)
	}
	if latest != nil {
		t.Fatalf("the backfill wrote the `latest` key (%v) — it is SaveSeriesONLY", latest)
	}
}

// The CALLER's snapshot is not mutated: the discard happens on a copy. The structure the poller
// still holds remains what the feed delivered, not a version only the store knows.
func TestSaveSnapshot_DoesNotMutateTheCallersSnapshot(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "1h")

	snap, _ := nonFiniteSnapshot()
	if err := store.SaveSnapshot(ctx, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	if snap.FundingRate == nil || !math.IsInf(*snap.FundingRate, 1) {
		t.Fatalf("the caller's snapshot was mutated: FundingRate = %v", snap.FundingRate)
	}
}
