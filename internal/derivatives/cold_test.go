package derivatives

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// --- two-tier writer doubles -----------------------------------------------

type recordingDerivColdStore struct {
	calls  int
	points []DerivativesColdPoint
	err    error
}

func (c *recordingDerivColdStore) WriteDerivativesBatch(_ context.Context, points []DerivativesColdPoint) error {
	c.calls++
	c.points = append(c.points, points...)
	return c.err
}

func fp(v float64) *float64 { return &v }

func sampleBatch(n int) DerivativesBatch {
	base := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC).UnixMilli()
	snaps := make([]*DerivativesSnapshot, 0, n)
	for i := 0; i < n; i++ {
		snaps = append(snaps, &DerivativesSnapshot{
			Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT",
			Timestamp:        base + int64(i)*86_400_000,
			OpenInterestBase: fp(106117.954 + float64(i)),
			FundingRate:      fp(0.0001),
			LongShortRatio:   fp(0.9554),
		})
	}
	return DerivativesBatch{
		Timeframe:     "1d",
		Source:        "coinalyze",
		DenominatedIn: "BASE_ASSET",
		Label:         LabelIntervalStart,
		Snapshots:     snaps,
	}
}

// TestTieredWriter_DurableFailureAbortsTheCycle: a durable-table failure rejects the batch, and
// the error arrives WRAPPED so that whoever handles it knows the cause.
//
// KILLS the mutant that removes the ZSET write without inverting criticality.
func TestTieredWriter_DurableFailureAbortsTheCycle(t *testing.T) {
	causa := errors.New("questdb down")
	cold := &recordingDerivColdStore{err: causa}
	writer := NewDerivativesTieredWriter(cold)

	err := writer.WriteBatch(context.Background(), sampleBatch(3))
	if err == nil {
		t.Fatal("the durable failure was swallowed — the cycle lost its failure point")
	}
	if !errors.Is(err, causa) {
		t.Fatalf("the returned error lost the cause: %v", err)
	}
}

// TestTieredWriter_NoDurableTierIsRefusedNotSilentlyDropped: a nil `cold` was once legitimate
// degradation because the ZSET received alone. With no ZSET there is no destination left, and a
// collected batch that goes nowhere must fail.
//
// KILLS the mutant that keeps the `return nil` from when there were two tiers — the worst of the
// four, because in a deployment without QuestDB it discards 100% of the collection without one log
// line.
func TestTieredWriter_NoDurableTierIsRefusedNotSilentlyDropped(t *testing.T) {
	writer := NewDerivativesTieredWriter(nil)

	err := writer.WriteBatch(context.Background(), sampleBatch(2))
	if err == nil {
		t.Fatal("the batch was silently discarded because there is no durable tier")
	}
	if !strings.Contains(err.Error(), "durable tier") {
		t.Fatalf("the error does not name the cause: %v", err)
	}
}

// TestTieredWriter_UndeclaredLabelIsRefusedBeforeTheDurableWrite is the admission rule at the new
// place.
//
// The refusal lived in `SaveCoinalyzeSeries`, which left with the ZSET. Without it here, the
// durable row would pass with `label_convention="unspecified"` — which
// `admissibleDerivativesRows` ADMITS, because the field is not empty — and the bucket would have
// been decided by guess.
//
// KILLS the mutant that removes the ZSET writer and takes the refusal with it.
func TestTieredWriter_UndeclaredLabelIsRefusedBeforeTheDurableWrite(t *testing.T) {
	cold := &recordingDerivColdStore{}
	writer := NewDerivativesTieredWriter(cold)

	batch := sampleBatch(3)
	batch.Label = LabelUnspecified

	if err := writer.WriteBatch(context.Background(), batch); err == nil {
		t.Fatal("the batch without a declared label convention was accepted")
	}
	if cold.calls != 0 {
		t.Fatalf("%d call(s) to the durable tier after the refusal, wanted 0", cold.calls)
	}
}

// NEGATIVE CONTROL FOR THE THREE ABOVE: the complete batch, with a live durable tier and a
// declared convention, PASSES. Without it, "refuse everything" would satisfy the three cases
// above.
func TestTieredWriter_ADeclaredBatchWithALiveDurableTierPasses(t *testing.T) {
	cold := &recordingDerivColdStore{}
	writer := NewDerivativesTieredWriter(cold)

	if err := writer.WriteBatch(context.Background(), sampleBatch(3)); err != nil {
		t.Fatalf("the good batch was refused: %v", err)
	}
	if len(cold.points) != 3 {
		t.Fatalf("%d point(s) in the durable tier, wanted 3", len(cold.points))
	}
}

// TestTieredWriter_AnEmptyBatchIsNotAFailure: a cycle in which the provider returned nothing is
// routine, and the empty-batch check comes BEFORE the missing-tier one — there is nothing to
// write, so there is nothing to fail.
func TestTieredWriter_AnEmptyBatchIsNotAFailure(t *testing.T) {
	batch := sampleBatch(0)
	if err := NewDerivativesTieredWriter(nil).WriteBatch(context.Background(), batch); err != nil {
		t.Fatalf("an empty batch returned an error: %v", err)
	}
	cold := &recordingDerivColdStore{}
	if err := NewDerivativesTieredWriter(cold).WriteBatch(context.Background(), batch); err != nil {
		t.Fatalf("an empty batch returned an error: %v", err)
	}
	if cold.calls != 0 {
		t.Fatalf("%d call(s) to the durable tier for an empty batch, wanted 0", cold.calls)
	}
}

// TestTieredWriter_TheDurableTierIsCalledOncePerBatch: the writer does not relay point by point.
// One call per point would reintroduce write amplification at the upper port.
func TestTieredWriter_TheDurableTierIsCalledOncePerBatch(t *testing.T) {
	cold := &recordingDerivColdStore{}
	writer := NewDerivativesTieredWriter(cold)

	if err := writer.WriteBatch(context.Background(), sampleBatch(50)); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if cold.calls != 1 {
		t.Fatalf("%d call(s) to the durable tier for 50 points, wanted 1", cold.calls)
	}
	if len(cold.points) != 50 {
		t.Fatalf("%d point(s) delivered, wanted 50", len(cold.points))
	}
}

// TestTieredWriter_TheDurableBucketIsTheAlignedBucket: the durable bucket is what the alignment
// rule declares, not the snapshot's raw instant. The rule remains the same the venue ZSET uses
// (`redisstore.BucketAlignMs`), because the long read's join — recent ZSET + deep table — depends
// on both naming the same bucket.
func TestTieredWriter_TheDurableBucketIsTheAlignedBucket(t *testing.T) {
	cold := &recordingDerivColdStore{}
	writer := NewDerivativesTieredWriter(cold)

	batch := sampleBatch(1)
	intervalStart := batch.Snapshots[0].Timestamp

	if err := writer.WriteBatch(context.Background(), batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(cold.points) != 1 {
		t.Fatalf("%d durable point(s), wanted 1", len(cold.points))
	}

	// The feed labels the START of the interval, and the internal anchor is the start — MEASURED
	// on both providers, both return `t` = today 00:00Z. The bucket is the labeled instant itself;
	// adding a day would write the durable tier in the future, misaligned from the ZSET this path
	// exists to accompany.
	want := time.UnixMilli(intervalStart).UTC()
	if got := cold.points[0].Timestamp; !got.Equal(want) {
		t.Fatalf("durable bucket=%s, wanted %s", got, want)
	}
}

// TestColdPointOf_TheEightHourEquivalentCrossesTheWriterUntouched ⭐ is the ENGINE half: the
// normalization is applied EXACTLY ONCE, and it already happened in polling.
//
// Nothing in the crossing prevents it from being applied again here, and the effect of applying
// it twice is (8/N)²: 64x at N=1h, 16x at N=2h, 4x at N=4h and **1x at N=8h**. The fleet's majors
// (BTC, ETH, SOL, DOGE, XRP) are all N=8h, so a second layer would come up GREEN on them and lie
// only in the tail — which is exactly where nobody looks.
//
// The case uses N=1h on purpose, because that is the error's maximum exponent.
func TestColdPointOf_TheEightHourEquivalentCrossesTheWriterUntouched(t *testing.T) {
	batch := DerivativesBatch{
		Timeframe:     "1h",
		Source:        "coinalyze",
		DenominatedIn: "BASE_ASSET",
		Label:         LabelIntervalStart,
		Snapshots: []*DerivativesSnapshot{{
			Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT",
			Timestamp:            time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC).UnixMilli(),
			FundingRate:          fp(0.0001),
			FundingIntervalHours: fp(1),
			NStep:                NFromMeasuredSpacing,
			// ALREADY NORMALIZED on the polling path: 0.0001 × 8/1.
			FundingRate8h: fp(0.0008),
		}},
	}

	point := coldPointOf(batch.Snapshots[0], batch)

	if point.FundingRate8h == nil {
		t.Fatal("FundingRate8h missing from the durable row")
	}
	if got := *point.FundingRate8h; got != 0.0008 {
		t.Fatalf("FundingRate8h=%v, wanted 0.0008 — the value polling already normalized. "+
			"0.0064 would be the second application of 8/N, that is (8/1)² on the raw value", got)
	}
	// And the RAW value is untouched: it is the operand, not the result.
	if point.FundingRate == nil {
		t.Fatal("FundingRate missing from the durable row")
	}
	if got := *point.FundingRate; got != 0.0001 {
		t.Fatalf("FundingRate=%v, wanted 0.0001 — the raw value is the operand, not the result", got)
	}
}

// ============================================================================
// THE DURABLE ROW CARRIES THE UNIT, IN PROTOCOL ORDER
// ============================================================================

func TestColdPointOf_TheDeclarationGovernsTheRecordedBucket(t *testing.T) {
	const timeframe = "1h"
	// 2026-08-21T00:00:00Z — exact boundary, so the displacement is the period's and not that of
	// some leftover.
	instant := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC).UnixMilli()

	batchWith := func(label LabelConvention) DerivativesBatch {
		return DerivativesBatch{
			Timeframe:     timeframe,
			Source:        "coinalyze",
			DenominatedIn: "BASE_ASSET",
			Label:         label,
			Snapshots: []*DerivativesSnapshot{{
				Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT",
				Timestamp:        instant,
				OpenInterestBase: fp(1000),
			}},
		}
	}

	inicio := coldPointOf(batchWith(LabelIntervalStart).Snapshots[0], batchWith(LabelIntervalStart))
	fim := coldPointOf(batchWith(LabelIntervalEnd).Snapshots[0], batchWith(LabelIntervalEnd))

	// (1) THE BUCKET MOVES. By one period: the end of `[S, E)` belongs to `S`, so the same
	// instant labeled as end falls one period EARLIER.
	esperadoFim := time.UnixMilli(instant).UTC().Add(-time.Hour)
	if inicio.Timestamp.Equal(fim.Timestamp) {
		t.Fatalf("the two conventions wrote the SAME bucket (%v) — the declaration does not govern "+
			"alignment", inicio.Timestamp)
	}
	if !fim.Timestamp.Equal(esperadoFim) {
		t.Fatalf("the end label wrote %v, wanted %v — the recession is ONE period",
			fim.Timestamp, esperadoFim)
	}

	// (2) THE APPLIED CONVENTION IS RECORDED. Without it, the data would not say where it came
	// from, and a future migration could not distinguish the two origins in the same table.
	if inicio.LabelConvention != LabelIntervalStart.String() {
		t.Fatalf("the start row recorded convention %q, wanted %q",
			inicio.LabelConvention, LabelIntervalStart.String())
	}
	if fim.LabelConvention != LabelIntervalEnd.String() {
		t.Fatalf("the end row recorded convention %q, wanted %q",
			fim.LabelConvention, LabelIntervalEnd.String())
	}
}
