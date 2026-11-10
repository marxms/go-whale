// Package storetest is the shared contract suite for derivatives persistence
// adapters. It is ordinary code so an adapter can import it from its _test.go
// and run the same cases against its real implementation; the fakes here are
// the extensibility proof, not production adapters.
package storetest

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
)

// Fixture identity shared by every store-contract case. These values are sample data, not
// adapter configuration, so they are named once instead of repeated in each case.
const (
	fixtureExchange   = "binance"
	fixtureMarketType = "future"
	fixtureCurrency   = "usd"
)

// HotStore is the hot-tier port set: snapshots, series and the latest key.
type HotStore interface {
	derivatives.Writer
	derivatives.Reader
}

// BaselineStore is the ruler port, plus the recompute call the suite uses to
// produce one. It is separate from HotStore because a cold-only adapter may not
// own a ruler.
type BaselineStore interface {
	derivatives.Writer
	derivatives.BaselineReader
	RecomputeBaseline(ctx context.Context, exchange, marketType, symbol, timeframe string) (*derivatives.DerivativesBaseline, error)
}

// ColdStore is the durable derivatives port.
type ColdStore interface {
	derivatives.ColdWriter
	derivatives.ColdReader
}

const suiteTimeframe = "1h"

func scope(exchange, marketType, symbol string) derivatives.DerivativesScope {
	return derivatives.DerivativesScope{
		Source:     derivatives.SourceVenue,
		Exchange:   exchange,
		MarketType: marketType,
		Symbol:     symbol,
		Timeframe:  suiteTimeframe,
	}
}

func ptrF(v float64) *float64 { return &v }
func ptrB(v bool) *bool       { return &v }

// RunHot runs the hot-tier contract suite.
func RunHot(t *testing.T, newStore func(t *testing.T) HotStore) {
	t.Helper()

	t.Run("optional_fields_roundtrip", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		ts := time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC).UnixMilli()
		in := &derivatives.DerivativesSnapshot{
			Exchange:             fixtureExchange,
			MarketType:           fixtureMarketType,
			Symbol:               "OPT/FIELDS",
			Timestamp:            ts,
			Source:               derivatives.SourceVenue,
			OpenInterestBase:     ptrF(123.5),
			OpenInterest:         ptrF(456.75),
			FundingRate:          ptrF(0.0001),
			FundingRate8h:        ptrF(0.0001),
			FundingIntervalHours: ptrF(8),
			NStep:                derivatives.NFromMeasuredSpacing,
			InterestRate:         ptrF(0.0001),
			FundingAtAnchor:      ptrB(true),
			NextFundingRate:      ptrF(0.0002),
			LongShortRatio:       ptrF(1.5),
			LSLongPct:            ptrF(60),
			LSShortPct:           ptrF(40),
		}
		if err := store.SaveSnapshot(ctx, in); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}

		got, err := store.GetLatestSnapshot(ctx, fixtureExchange, fixtureMarketType, "OPT/FIELDS")
		if err != nil {
			t.Fatalf("GetLatestSnapshot: %v", err)
		}
		if got == nil {
			t.Fatal("the stored snapshot is not readable")
		}
		assertSnapshotFields(t, *got, *in)
	})

	t.Run("absence_is_not_zero_and_merge_keeps_missing_fields", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		ts := time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC).UnixMilli()
		if err := store.SaveSnapshot(ctx, &derivatives.DerivativesSnapshot{
			Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "MERGE/FIELDS",
			Timestamp: ts, Source: derivatives.SourceVenue,
			OpenInterestBase: ptrF(10), LongShortRatio: ptrF(2),
		}); err != nil {
			t.Fatalf("initial SaveSnapshot: %v", err)
		}
		// Funding-only cycle: it must not erase the open-interest reading above.
		if err := store.SaveSnapshot(ctx, &derivatives.DerivativesSnapshot{
			Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "MERGE/FIELDS",
			Timestamp: ts + 1000, Source: derivatives.SourceVenue,
			FundingRate: ptrF(0.0001), FundingRate8h: ptrF(0.0001),
			FundingIntervalHours: ptrF(8), NStep: derivatives.NFromMeasuredSpacing,
		}); err != nil {
			t.Fatalf("funding-only SaveSnapshot: %v", err)
		}

		got, err := store.GetLatestSnapshot(ctx, fixtureExchange, fixtureMarketType, "MERGE/FIELDS")
		if err != nil || got == nil {
			t.Fatalf("GetLatestSnapshot: got=%v err=%v", got, err)
		}
		if got.OpenInterestBase == nil || *got.OpenInterestBase != 10 {
			t.Fatalf("the funding-only cycle erased the open interest: %v", got.OpenInterestBase)
		}
		if got.LongShortRatio == nil || *got.LongShortRatio != 2 {
			t.Fatalf("the funding-only cycle erased long/short: %v", got.LongShortRatio)
		}
		if got.FundingRate == nil || *got.FundingRate != 0.0001 {
			t.Fatalf("funding was not written: %v", got.FundingRate)
		}
		// Absent stays absent: no unmeasured field became zero.
		if got.InterestRate != nil || got.FundingAtAnchor != nil || got.OpenInterest != nil {
			t.Fatalf("an absent field was invented: %+v", got)
		}
	})

	t.Run("source_per_field", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		ts := time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC).UnixMilli()
		in := &derivatives.DerivativesSnapshot{
			Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "SOURCE/FIELD",
			Timestamp: ts, Source: derivatives.SourceVenue,
			OpenInterestBase: ptrF(10),
			FieldSources:     map[string]string{"openInterestBase": derivatives.SourceCoinalyze},
		}
		if err := store.SaveSnapshot(ctx, in); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		got, err := store.GetLatestSnapshot(ctx, fixtureExchange, fixtureMarketType, "SOURCE/FIELD")
		if err != nil || got == nil {
			t.Fatalf("GetLatestSnapshot: got=%v err=%v", got, err)
		}
		if !got.HasMixedProvenance() {
			t.Fatalf("per-field provenance did not survive: %v", got.FieldSources)
		}
		if got.FieldSources["openInterestBase"] != derivatives.SourceCoinalyze {
			t.Fatalf("field source = %q, want %q", got.FieldSources["openInterestBase"], derivatives.SourceCoinalyze)
		}
	})

	t.Run("latest_is_not_series", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		base := time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC)
		first := &derivatives.DerivativesSnapshot{
			Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "LATEST/SERIES",
			Timestamp: base.UnixMilli(), Source: derivatives.SourceVenue,
			OpenInterestBase: ptrF(10),
		}
		second := &derivatives.DerivativesSnapshot{
			Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "LATEST/SERIES",
			Timestamp: base.Add(time.Hour).UnixMilli(), Source: derivatives.SourceVenue,
			OpenInterestBase: ptrF(11),
		}
		if err := store.SaveSnapshot(ctx, first); err != nil {
			t.Fatalf("SaveSnapshot first: %v", err)
		}
		if err := store.SaveSnapshot(ctx, second); err != nil {
			t.Fatalf("SaveSnapshot second: %v", err)
		}

		latest, err := store.GetLatestSnapshot(ctx, fixtureExchange, fixtureMarketType, "LATEST/SERIES")
		if err != nil || latest == nil {
			t.Fatalf("GetLatestSnapshot: got=%v err=%v", latest, err)
		}
		if latest.Timestamp != second.Timestamp {
			t.Fatalf("latest.Timestamp = %d, want %d — the latest key is the newest point", latest.Timestamp, second.Timestamp)
		}

		series, err := store.GetSnapshotSeries(ctx, fixtureExchange, fixtureMarketType, "LATEST/SERIES", suiteTimeframe, 10)
		if err != nil {
			t.Fatalf("GetSnapshotSeries: %v", err)
		}
		if len(series) != 2 {
			t.Fatalf("series has %d point(s), want 2 — latest does not replace history", len(series))
		}
		if series[0].Timestamp <= series[1].Timestamp {
			t.Fatalf("series is not in descending order: %d, %d", series[0].Timestamp, series[1].Timestamp)
		}
	})

	t.Run("label_convention_floors_the_series_bucket", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		// 01:30 belongs to the 01:00 bucket under the start convention.
		raw := time.Date(2026, 1, 2, 1, 30, 0, 0, time.UTC)
		in := &derivatives.DerivativesSnapshot{
			Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "LABEL/CONV",
			Timestamp: raw.UnixMilli(), Source: derivatives.SourceVenue,
			OpenInterestBase: ptrF(10),
		}
		if err := store.SaveSnapshot(ctx, in); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}

		series, err := store.GetSnapshotSeries(ctx, fixtureExchange, fixtureMarketType, "LABEL/CONV", suiteTimeframe, 10)
		if err != nil {
			t.Fatalf("GetSnapshotSeries: %v", err)
		}
		if len(series) != 1 {
			t.Fatalf("series has %d point(s), want 1", len(series))
		}
		want := time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC).UnixMilli()
		if series[0].Timestamp != want {
			t.Fatalf("bucket = %d, want %d — the series is stamped at the START of the interval", series[0].Timestamp, want)
		}
		latest, err := store.GetLatestSnapshot(ctx, fixtureExchange, fixtureMarketType, "LABEL/CONV")
		if err != nil || latest == nil {
			t.Fatalf("GetLatestSnapshot: got=%v err=%v", latest, err)
		}
		if latest.Timestamp != raw.UnixMilli() {
			t.Fatalf("latest.Timestamp = %d, want the raw instant %d — latest is not a bucket", latest.Timestamp, raw.UnixMilli())
		}
	})

	t.Run("cancel_refuses_the_write", func(t *testing.T) {
		store := newStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		snap := &derivatives.DerivativesSnapshot{
			Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "CANCEL/DERIV",
			Timestamp: time.Now().UnixMilli(), Source: derivatives.SourceVenue,
			OpenInterestBase: ptrF(1),
		}
		if err := store.SaveSeriesOnly(ctx, snap, suiteTimeframe); err == nil {
			t.Error("SaveSeriesOnly accepted a canceled context")
		}
	})
}

// RunBaseline runs the ruler contract: absence is (nil, nil), and a computed
// baseline is persisted and readable.
func RunBaseline(t *testing.T, newStore func(t *testing.T) BaselineStore) {
	t.Helper()

	t.Run("absent_baseline_is_not_an_error", func(t *testing.T) {
		store := newStore(t)
		got, err := store.GetBaseline(context.Background(), fixtureExchange, fixtureMarketType, "NO/RULER", suiteTimeframe)
		if err != nil {
			t.Fatalf("absent GetBaseline returned an error: %v", err)
		}
		if got != nil {
			t.Fatalf("absent GetBaseline returned %+v", got)
		}
	})

	t.Run("recompute_persists_a_readable_ruler", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < 40; i++ {
			snap := &derivatives.DerivativesSnapshot{
				Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "RULER/USDT",
				Timestamp:        base.Add(time.Duration(i) * time.Hour).UnixMilli(),
				Source:           derivatives.SourceVenue,
				OpenInterestBase: ptrF(100 + float64(i)),
			}
			if err := store.SaveSeriesOnly(ctx, snap, suiteTimeframe); err != nil {
				t.Fatalf("SaveSeriesOnly[%d]: %v", i, err)
			}
		}

		computed, err := store.RecomputeBaseline(ctx, fixtureExchange, fixtureMarketType, "RULER/USDT", suiteTimeframe)
		if err != nil {
			t.Fatalf("RecomputeBaseline: %v", err)
		}
		if computed == nil {
			t.Fatal("RecomputeBaseline returned nil")
		}
		got, err := store.GetBaseline(ctx, fixtureExchange, fixtureMarketType, "RULER/USDT", suiteTimeframe)
		if err != nil {
			t.Fatalf("GetBaseline: %v", err)
		}
		if got == nil {
			t.Fatal("the computed ruler was not persisted")
		}
		if got.WindowN <= 0 || got.NMin <= 0 {
			t.Fatalf("ruler with no declared window: WindowN=%d NMin=%d", got.WindowN, got.NMin)
		}
		if got.Timeframe != suiteTimeframe {
			t.Fatalf("Timeframe = %q, want %q", got.Timeframe, suiteTimeframe)
		}
	})
}

// RunCold runs the durable derivatives contract: every column that is written
// must be readable, absence is not zero, the source is part of the identity, and
// the read is ascending and inclusive.
func RunCold(t *testing.T, newStore func(t *testing.T) ColdStore) {
	t.Helper()

	t.Run("every_optional_field_roundtrips", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		base := time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC)
		in := []derivatives.DerivativesColdPoint{{
			Timestamp: base, Source: derivatives.SourceVenue,
			Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "COLD/OPT", Timeframe: suiteTimeframe,
			DenominatedIn: fixtureCurrency, LabelConvention: derivatives.LabelIntervalStart.String(),
			OpenInterestBase: ptrF(10), FundingRate: ptrF(0.0001), FundingRate8h: ptrF(0.0001),
			LongShortRatio: ptrF(1.5), FundingIntervalHours: ptrF(8),
			NStep: derivatives.NFromMeasuredSpacing, OpenInterest: ptrF(20),
			NextFundingRate: ptrF(0.0002), InterestRate: ptrF(0.0001),
			FundingAtAnchor: ptrB(false), LSLongPct: ptrF(60), LSShortPct: ptrF(40),
		}}
		if err := store.WriteDerivativesBatch(ctx, in); err != nil {
			t.Fatalf("WriteDerivativesBatch: %v", err)
		}
		got := query(ctx, t, store, scope(fixtureExchange, fixtureMarketType, "COLD/OPT"), base.Add(-time.Hour), base.Add(time.Hour))
		if len(got) != 1 {
			t.Fatalf("%d row(s) read, want 1", len(got))
		}
		assertSnapshotFields(t, *got[0], derivatives.DerivativesSnapshot{
			Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "COLD/OPT",
			Timestamp: base.UnixMilli(), Source: derivatives.SourceVenue,
			OpenInterestBase: in[0].OpenInterestBase, FundingRate: in[0].FundingRate,
			FundingRate8h: in[0].FundingRate8h, LongShortRatio: in[0].LongShortRatio,
			FundingIntervalHours: in[0].FundingIntervalHours, NStep: in[0].NStep,
			OpenInterest: in[0].OpenInterest, NextFundingRate: in[0].NextFundingRate,
			InterestRate: in[0].InterestRate, FundingAtAnchor: in[0].FundingAtAnchor,
			LSLongPct: in[0].LSLongPct, LSShortPct: in[0].LSShortPct,
		})
	})

	t.Run("absence_is_not_zero", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		base := time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC)
		zero := 0.0
		in := []derivatives.DerivativesColdPoint{{
			Timestamp: base, Source: derivatives.SourceVenue,
			Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "COLD/ZERO", Timeframe: suiteTimeframe,
			DenominatedIn: fixtureCurrency, LabelConvention: derivatives.LabelIntervalStart.String(),
			// Funding zero IS a reading; open interest absent is NOT zero.
			FundingRate: &zero,
		}}
		if err := store.WriteDerivativesBatch(ctx, in); err != nil {
			t.Fatalf("WriteDerivativesBatch: %v", err)
		}
		got := query(ctx, t, store, scope(fixtureExchange, fixtureMarketType, "COLD/ZERO"), base.Add(-time.Hour), base.Add(time.Hour))
		if len(got) != 1 {
			t.Fatalf("%d row(s) read, want 1", len(got))
		}
		if got[0].FundingRate == nil || *got[0].FundingRate != 0 {
			t.Fatalf("zero funding came back as %v — zero is a reading", got[0].FundingRate)
		}
		if got[0].OpenInterestBase != nil {
			t.Fatalf("absent open interest came back as %v, not nil", *got[0].OpenInterestBase)
		}
	})

	t.Run("source_is_part_of_the_identity", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		base := time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC)
		in := []derivatives.DerivativesColdPoint{
			{
				Timestamp: base, Source: derivatives.SourceVenue,
				Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "COLD/SOURCE", Timeframe: suiteTimeframe,
				DenominatedIn: fixtureCurrency, LabelConvention: derivatives.LabelIntervalStart.String(),
				OpenInterestBase: ptrF(10),
			},
			{
				Timestamp: base, Source: derivatives.SourceCoinalyze,
				Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "COLD/SOURCE", Timeframe: suiteTimeframe,
				DenominatedIn: fixtureCurrency, LabelConvention: derivatives.LabelIntervalStart.String(),
				OpenInterestBase: ptrF(20),
			},
		}
		if err := store.WriteDerivativesBatch(ctx, in); err != nil {
			t.Fatalf("WriteDerivativesBatch: %v", err)
		}
		venue := query(ctx, t, store, scope(fixtureExchange, fixtureMarketType, "COLD/SOURCE"), base.Add(-time.Hour), base.Add(time.Hour))
		coinalyzeScope := scope(fixtureExchange, fixtureMarketType, "COLD/SOURCE")
		coinalyzeScope.Source = derivatives.SourceCoinalyze
		coinalyze := query(ctx, t, store, coinalyzeScope, base.Add(-time.Hour), base.Add(time.Hour))
		if len(venue) != 1 || len(coinalyze) != 1 {
			t.Fatalf("rows per source = %d/%d, want 1/1", len(venue), len(coinalyze))
		}
		if venue[0].Source != derivatives.SourceVenue || coinalyze[0].Source != derivatives.SourceCoinalyze {
			t.Fatalf("the source did not cross: %q/%q", venue[0].Source, coinalyze[0].Source)
		}
	})

	t.Run("range_is_inclusive_and_ascending", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		base := time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC)
		in := make([]derivatives.DerivativesColdPoint, 0, 3)
		for i := 0; i < 3; i++ {
			in = append(in, derivatives.DerivativesColdPoint{
				Timestamp: base.Add(time.Duration(i) * time.Hour), Source: derivatives.SourceVenue,
				Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "COLD/RANGE", Timeframe: suiteTimeframe,
				DenominatedIn: fixtureCurrency, LabelConvention: derivatives.LabelIntervalStart.String(),
				OpenInterestBase: ptrF(float64(i)),
			})
		}
		if err := store.WriteDerivativesBatch(ctx, in); err != nil {
			t.Fatalf("WriteDerivativesBatch: %v", err)
		}
		got := query(ctx, t, store, scope(fixtureExchange, fixtureMarketType, "COLD/RANGE"), base.Add(time.Hour), base.Add(2*time.Hour))
		if len(got) != 2 {
			t.Fatalf("%d row(s) in the range, want 2", len(got))
		}
		if got[0].Timestamp >= got[1].Timestamp {
			t.Fatalf("the read is not in ascending order: %d, %d", got[0].Timestamp, got[1].Timestamp)
		}
	})

	t.Run("dedup_keeps_one_row_and_the_last_write", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		base := time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC)
		first := derivatives.DerivativesColdPoint{
			Timestamp: base, Source: derivatives.SourceVenue,
			Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "COLD/DEDUP", Timeframe: suiteTimeframe,
			DenominatedIn: fixtureCurrency, LabelConvention: derivatives.LabelIntervalStart.String(),
			OpenInterestBase: ptrF(1),
		}
		second := first
		second.OpenInterestBase = ptrF(2)
		if err := store.WriteDerivativesBatch(ctx, []derivatives.DerivativesColdPoint{first}); err != nil {
			t.Fatalf("initial WriteDerivativesBatch: %v", err)
		}
		if err := store.WriteDerivativesBatch(ctx, []derivatives.DerivativesColdPoint{second}); err != nil {
			t.Fatalf("revision WriteDerivativesBatch: %v", err)
		}
		got := query(ctx, t, store, scope(fixtureExchange, fixtureMarketType, "COLD/DEDUP"), base.Add(-time.Hour), base.Add(time.Hour))
		if len(got) != 1 {
			t.Fatalf("DEDUP left %d row(s), want 1", len(got))
		}
		if got[0].OpenInterestBase == nil || *got[0].OpenInterestBase != 2 {
			t.Fatalf("the last write did not win: %v", got[0].OpenInterestBase)
		}
	})

	t.Run("cancel_refuses_the_write", func(t *testing.T) {
		store := newStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		base := time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC)
		point := derivatives.DerivativesColdPoint{
			Timestamp: base, Source: derivatives.SourceVenue,
			Exchange: fixtureExchange, MarketType: fixtureMarketType, Symbol: "COLD/CANCEL", Timeframe: suiteTimeframe,
			DenominatedIn: fixtureCurrency, LabelConvention: derivatives.LabelIntervalStart.String(),
			OpenInterestBase: ptrF(1),
		}
		if err := store.WriteDerivativesBatch(ctx, []derivatives.DerivativesColdPoint{point}); err == nil {
			t.Error("WriteDerivativesBatch accepted a canceled context")
		}
	})
}

func query(ctx context.Context, t *testing.T, store ColdStore, scope derivatives.DerivativesScope, from, to time.Time) []*derivatives.DerivativesSnapshot {
	t.Helper()
	got, err := store.QueryDerivativesRange(ctx, scope, from, to)
	if err != nil {
		t.Fatalf("QueryDerivativesRange: %v", err)
	}
	return got
}

func assertSnapshotFields(t *testing.T, got, want derivatives.DerivativesSnapshot) {
	t.Helper()
	if got.Exchange != want.Exchange || got.MarketType != want.MarketType || got.Symbol != want.Symbol {
		t.Errorf("identity = %s/%s/%s, want %s/%s/%s",
			got.Exchange, got.MarketType, got.Symbol, want.Exchange, want.MarketType, want.Symbol)
	}
	if got.Timestamp != want.Timestamp {
		t.Errorf("timestamp = %d, want %d", got.Timestamp, want.Timestamp)
	}
	if got.Source != want.Source {
		t.Errorf("source = %q, want %q", got.Source, want.Source)
	}
	floats := []struct {
		name      string
		got, want *float64
	}{
		{"openInterestBase", got.OpenInterestBase, want.OpenInterestBase},
		{"openInterest", got.OpenInterest, want.OpenInterest},
		{"fundingRate", got.FundingRate, want.FundingRate},
		{"fundingRate8h", got.FundingRate8h, want.FundingRate8h},
		{"fundingIntervalHours", got.FundingIntervalHours, want.FundingIntervalHours},
		{"interestRate", got.InterestRate, want.InterestRate},
		{"nextFundingRate", got.NextFundingRate, want.NextFundingRate},
		{"longShortRatio", got.LongShortRatio, want.LongShortRatio},
		{"lsLongPct", got.LSLongPct, want.LSLongPct},
		{"lsShortPct", got.LSShortPct, want.LSShortPct},
	}
	for _, f := range floats {
		if (f.got == nil) != (f.want == nil) {
			t.Errorf("%s: got=%v want=%v — absence became a value or vice-versa", f.name, f.got, f.want)
			continue
		}
		if f.got != nil && *f.got != *f.want {
			t.Errorf("%s = %v, want %v", f.name, *f.got, *f.want)
		}
	}
	if (got.FundingAtAnchor == nil) != (want.FundingAtAnchor == nil) {
		t.Errorf("fundingAtAnchor: got=%v want=%v", got.FundingAtAnchor, want.FundingAtAnchor)
	} else if got.FundingAtAnchor != nil && *got.FundingAtAnchor != *want.FundingAtAnchor {
		t.Errorf("fundingAtAnchor = %v, want %v", *got.FundingAtAnchor, *want.FundingAtAnchor)
	}
	if got.NStep != want.NStep {
		t.Errorf("nStep = %q, want %q", got.NStep, want.NStep)
	}
}

// FakeHot is the minimal in-memory hot-tier implementation: latest key plus one
// series per timeframe, newest-first reads.
type FakeHot struct {
	mu       sync.Mutex
	latest   map[string]*derivatives.DerivativesSnapshot
	series   map[string]map[int64]*derivatives.DerivativesSnapshot
	baseline map[string]*derivatives.DerivativesBaseline
}

// NewFakeHot builds the hot fake.
func NewFakeHot() *FakeHot {
	return &FakeHot{
		latest:   map[string]*derivatives.DerivativesSnapshot{},
		series:   map[string]map[int64]*derivatives.DerivativesSnapshot{},
		baseline: map[string]*derivatives.DerivativesBaseline{},
	}
}

func latestKey(exchange, marketType, symbol string) string {
	return fmt.Sprintf("%s:%s:%s", exchange, marketType, symbol)
}

func seriesKey(exchange, marketType, symbol, timeframe string) string {
	return fmt.Sprintf("%s:%s:%s:%s", exchange, marketType, symbol, timeframe)
}

func alignStart(ts int64, timeframe string) int64 {
	tfMs := int64(3600_000)
	if timeframe != "1h" {
		return ts
	}
	return ts / tfMs * tfMs
}

func copySnapshot(s *derivatives.DerivativesSnapshot) *derivatives.DerivativesSnapshot {
	if s == nil {
		return nil
	}
	c := *s
	return &c
}

// mergeFields is the same field-level rule the production merge states: an
// incoming non-nil field replaces, an incoming nil preserves.
func mergeFields(existing, incoming *derivatives.DerivativesSnapshot) *derivatives.DerivativesSnapshot {
	out := copySnapshot(existing)
	if out == nil {
		out = &derivatives.DerivativesSnapshot{}
	}
	out.Exchange, out.MarketType, out.Symbol = incoming.Exchange, incoming.MarketType, incoming.Symbol
	if incoming.Timestamp > out.Timestamp {
		out.Timestamp = incoming.Timestamp
	}
	if incoming.Source != "" {
		out.Source = incoming.Source
	}
	apply := func(dst **float64, src *float64) {
		if src != nil {
			v := *src
			*dst = &v
		}
	}
	apply(&out.OpenInterestBase, incoming.OpenInterestBase)
	apply(&out.OpenInterest, incoming.OpenInterest)
	apply(&out.FundingRate, incoming.FundingRate)
	apply(&out.FundingRate8h, incoming.FundingRate8h)
	apply(&out.FundingIntervalHours, incoming.FundingIntervalHours)
	apply(&out.InterestRate, incoming.InterestRate)
	apply(&out.NextFundingRate, incoming.NextFundingRate)
	apply(&out.LongShortRatio, incoming.LongShortRatio)
	apply(&out.LSLongPct, incoming.LSLongPct)
	apply(&out.LSShortPct, incoming.LSShortPct)
	if incoming.FundingAtAnchor != nil {
		v := *incoming.FundingAtAnchor
		out.FundingAtAnchor = &v
	}
	if incoming.NStep != "" {
		out.NStep = incoming.NStep
	}
	if incoming.FieldSources != nil {
		out.FieldSources = incoming.FieldSources
	}
	return out
}

func (f *FakeHot) saveSeriesLocked(snapshot *derivatives.DerivativesSnapshot, timeframe string) {
	key := seriesKey(snapshot.Exchange, snapshot.MarketType, snapshot.Symbol, timeframe)
	rows := f.series[key]
	if rows == nil {
		rows = map[int64]*derivatives.DerivativesSnapshot{}
		f.series[key] = rows
	}
	aligned := alignStart(snapshot.Timestamp, timeframe)
	merged := mergeFields(rows[aligned], snapshot)
	merged.Timestamp = aligned
	rows[aligned] = merged
}

// SaveSnapshot stores a snapshot for its scope.
func (f *FakeHot) SaveSnapshot(ctx context.Context, snapshot *derivatives.DerivativesSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snapshot == nil {
		return fmt.Errorf("derivatives: nil snapshot")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := latestKey(snapshot.Exchange, snapshot.MarketType, snapshot.Symbol)
	f.latest[key] = mergeFields(f.latest[key], snapshot)
	f.saveSeriesLocked(snapshot, suiteTimeframe)
	return nil
}

// SaveSeriesOnly stores only the series row of a snapshot.
func (f *FakeHot) SaveSeriesOnly(ctx context.Context, snapshot *derivatives.DerivativesSnapshot, timeframe string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snapshot == nil {
		return fmt.Errorf("derivatives: nil snapshot")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveSeriesLocked(snapshot, timeframe)
	return nil
}

// GetLatestSnapshot returns the newest snapshot of one scope, or nil.
func (f *FakeHot) GetLatestSnapshot(ctx context.Context, exchange, marketType, symbol string) (*derivatives.DerivativesSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return copySnapshot(f.latest[latestKey(exchange, marketType, symbol)]), nil
}

// GetSnapshotSeries returns up to limit snapshots newest-first.
func (f *FakeHot) GetSnapshotSeries(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]*derivatives.DerivativesSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rows := f.series[seriesKey(exchange, marketType, symbol, timeframe)]
	stamps := make([]int64, 0, len(rows))
	for ts := range rows {
		stamps = append(stamps, ts)
	}
	sort.Slice(stamps, func(i, j int) bool { return stamps[i] > stamps[j] })
	out := make([]*derivatives.DerivativesSnapshot, 0, len(stamps))
	for _, ts := range stamps {
		out = append(out, copySnapshot(rows[ts]))
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// GetBaseline returns the stored baseline of one series, or nil.
func (f *FakeHot) GetBaseline(ctx context.Context, exchange, marketType, symbol, timeframe string) (*derivatives.DerivativesBaseline, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	baseline := f.baseline[seriesKey(exchange, marketType, symbol, timeframe)]
	if baseline == nil {
		return nil, nil
	}
	c := *baseline
	return &c, nil
}

// RecomputeBaseline recomputes and stores the baseline.
func (f *FakeHot) RecomputeBaseline(ctx context.Context, exchange, marketType, symbol, timeframe string) (*derivatives.DerivativesBaseline, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	baseline := &derivatives.DerivativesBaseline{
		Exchange: exchange, MarketType: marketType, Symbol: symbol, Timeframe: timeframe,
		WindowN: 288, NMin: 30, UpdatedAt: time.Now().UnixMilli(),
	}
	f.baseline[seriesKey(exchange, marketType, symbol, timeframe)] = baseline
	c := *baseline
	return &c, nil
}

// FakeCold is the minimal in-memory durable implementation.
type FakeCold struct {
	mu     sync.Mutex
	series map[string]map[string]derivatives.DerivativesColdPoint
}

// NewFakeCold builds the durable fake.
func NewFakeCold() *FakeCold {
	return &FakeCold{series: map[string]map[string]derivatives.DerivativesColdPoint{}}
}

func coldKey(scope derivatives.DerivativesScope) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s", scope.Source, scope.Exchange, scope.MarketType, scope.Symbol, scope.Timeframe)
}

// WriteDerivativesBatch appends points to the fake cold tier.
func (f *FakeCold) WriteDerivativesBatch(ctx context.Context, points []derivatives.DerivativesColdPoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, point := range points {
		if point.Declared() != nil || point.Empty() {
			continue
		}
		key := coldKey(derivatives.DerivativesScope{
			Source: point.Source, Exchange: point.Exchange, MarketType: point.MarketType,
			Symbol: point.Symbol, Timeframe: point.Timeframe,
		})
		rows := f.series[key]
		if rows == nil {
			rows = map[string]derivatives.DerivativesColdPoint{}
			f.series[key] = rows
		}
		rows[point.Timestamp.UTC().Format(time.RFC3339Nano)] = point
	}
	return nil
}

// QueryDerivativesRange returns the points of one scope inside the requested window.
func (f *FakeCold) QueryDerivativesRange(ctx context.Context, scope derivatives.DerivativesScope, from, to time.Time) ([]*derivatives.DerivativesSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rows := f.series[coldKey(scope)]
	var out []*derivatives.DerivativesSnapshot
	for _, point := range rows {
		ts := point.Timestamp.UTC()
		if (ts.Equal(from) || ts.After(from)) && (ts.Equal(to) || ts.Before(to)) {
			out = append(out, snapshotOfColdPoint(scope, point))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out, nil
}

func snapshotOfColdPoint(scope derivatives.DerivativesScope, point derivatives.DerivativesColdPoint) *derivatives.DerivativesSnapshot {
	return &derivatives.DerivativesSnapshot{
		Exchange: scope.Exchange, MarketType: scope.MarketType, Symbol: scope.Symbol,
		Timestamp: point.Timestamp.UnixMilli(), Source: scope.Source,
		OpenInterestBase: point.OpenInterestBase, FundingRate: point.FundingRate,
		FundingRate8h: point.FundingRate8h, LongShortRatio: point.LongShortRatio,
		FundingIntervalHours: point.FundingIntervalHours, NStep: point.NStep,
		OpenInterest: point.OpenInterest, NextFundingRate: point.NextFundingRate,
		InterestRate: point.InterestRate, FundingAtAnchor: point.FundingAtAnchor,
		LSLongPct: point.LSLongPct, LSShortPct: point.LSShortPct,
	}
}
