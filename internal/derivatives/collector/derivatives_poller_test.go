package collector

import (
	"context"
	"math"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"
)

// ============================================================================
// FINITENESS IN LSR INGESTION
// ============================================================================
//
// The history path does not go through JSON: the adapter delivers canonical points and the
// collector's filter is the last door before the series. `NaN == 0` is false, as is `+Inf == 0`:
// without this guard a non-finite entry PASSED and was written.
//
// The case asserts the EFFECT in the store: the non-finite entry does not arrive, the finite one
// arrives with the right value, and what was written is finite. Without the finite contrast, a
// predicate rejecting everything would pass.
func TestBootstrapLSRForTimeframe_NonFiniteEntriesNeverReachTheStore(t *testing.T) {
	t.Parallel()

	const timeframe = "5m"
	periodMs, _ := candle.ParseTimeframeMs(timeframe)

	noveSete, maisInf, menosInf, finito := math.NaN(), math.Inf(1), math.Inf(-1), 1.2
	ratios := []float64{noveSete, maisInf, menosInf, finito}

	store := &recordingStore{}
	var calls int
	client := &fakeRestClient{lsrHistory: func(_, _ string, _ int64, _, endMs int64) []derivatives.LongShortRatioPoint {
		calls++
		if calls > 1 {
			// An empty page ends the walk: the first one is what matters.
			return nil
		}
		points := make([]derivatives.LongShortRatioPoint, 0, len(ratios))
		for i, ratio := range ratios {
			points = append(points, derivatives.LongShortRatioPoint{
				Timestamp: endMs - int64(i)*periodMs,
				Ratio:     ratio,
			})
		}
		return points
	}}

	poller := &DerivativesPoller{
		cfg:             config.DerivativesConfig{Enabled: true, MaxSeriesSize: 200},
		store:           store,
		tfReg:           candle.NewTimeframeRegistry([]string{timeframe}),
		fundingProfiles: NewFundingProfileRegistry(),
	}

	saved := poller.bootstrapLSRForTimeframe(
		context.Background(), client, testScope(), timeframe)

	if want := 1; saved != want {
		t.Fatalf("the walk reported %d saved points, want %d — the three non-finite entries "+
			"should have been discarded and the finite one should have passed", saved, want)
	}
	if got := store.count(); got != 1 {
		t.Fatalf("the store received %d points, want 1", got)
	}
	for i, s := range store.saved {
		if s.LongShortRatio == nil {
			t.Fatalf("point %d written without a ratio", i)
		}
		if v := *s.LongShortRatio; math.IsNaN(v) || math.IsInf(v, 0) {
			t.Fatalf("point %d wrote the non-finite ratio %v — the history filter let through "+
				"exactly what it exists to stop", i, v)
		}
		if *s.LongShortRatio != finito {
			t.Fatalf("point %d has ratio %v, want %v", i, *s.LongShortRatio, finito)
		}
	}
}
