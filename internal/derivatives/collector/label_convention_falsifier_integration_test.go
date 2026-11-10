//go:build integration

// THE ADDITIVE FALSIFIER AGAINST THE LIVE VENUE — the half a fixture cannot give.
//
// The arithmetic lives in `label_convention_falsifier_test.go` and runs on a versioned fixture,
// with no network. What only the venue can answer is whether it CHANGED: a fixture stays green
// after an endpoint swaps its label convention, and that is exactly the behavior `LabelConvention`
// declares and this case checks.
//
// THE TAG IS `integration` BECAUSE A NETWORK CALL CANNOT BE UNIT: a test carrying it without the
// tag contaminates the gate with an external dependency, and one hiding it behind an `if` never
// runs. Here it is explicit and runs by invocation:
//
//	go test -tags=integration -run TestLive ./internal/derivatives/collector/
//
// PACING: the `/futures/data/*` endpoint is public and the weight budget is 2400/min. This case
// makes TWO calls, with the same `pacing` as this directory's integrity probe.
//
// WHAT IT does **NOT** PROVE: that OI or LSR are start-labeled. The measured additive field is
// the `buyVol` of `takerlongshortRatio`, which is not one of the endpoints the collector ingests —
// for the siblings, what was measured is that the two conventions are indistinguishable. See the
// unit file's header; the transfer is forbidden there and remains forbidden here.
package collector

import (
	"context"
	"net/url"
	"testing"

	"github.com/marxms/go-whale/internal/derivatives"
)

// takerRow is the subset of the response the falsifier consumes: the label and the SUMMED field.
// The other fields (`buyRatio`, `sellRatio`) do not enter: a ratio is not additive.
type takerRow struct {
	BuyVol    string `json:"buyVol"`
	Timestamp int64  `json:"timestamp"`
}

// liveSymbol is the same symbol as the fixture's measurement, so the live case compares the same
// contract the fixture describes.
const liveSymbol = "BTCUSDT"

// TestLiveAdditiveFalsifierSeparatesStartFromEnd is the live case, and it asserts BOTH polarities
// on purpose: confirmation alone does not distinguish a working falsifier from a function returning
// `true`; refusal alone does not distinguish one that works from one returning `false`.
func TestLiveAdditiveFalsifierSeparatesStartFromEnd(t *testing.T) {
	ctx := context.Background()

	var daily []takerRow
	getJSON(ctx, t, "/futures/data/takerlongshortRatio", url.Values{
		"symbol": {liveSymbol},
		"period": {"1d"},
		"limit":  {"3"},
	}, &daily)
	if len(daily) == 0 {
		t.Fatal("the endpoint returned zero daily bars — without a bar there is nothing to compare")
	}
	// The LAST published bar is the one still open (today's label); the penultimate is the last
	// COMPLETE day, and it is the one matching 288 5m sub-buckets.
	bar := daily[len(daily)-1]
	if len(daily) >= 2 {
		bar = daily[len(daily)-2]
	}
	barValue, err := parseFloat(bar.BuyVol)
	if err != nil {
		t.Fatalf("daily bar buyVol: %v", err)
	}

	var subs []takerRow
	getJSON(ctx, t, "/futures/data/takerlongshortRatio", url.Values{
		"symbol": {liveSymbol},
		"period": {"5m"},
		"limit":  {"500"},
	}, &subs)

	aggregate := subBucketAggregate{windowStartMs: bar.Timestamp, windowEndMs: bar.Timestamp + fixtureDayWidthMs}
	for _, row := range subs {
		if row.Timestamp < aggregate.windowStartMs || row.Timestamp >= aggregate.windowEndMs {
			continue
		}
		v, err := parseFloat(row.BuyVol)
		if err != nil {
			t.Fatalf("sub-bucket buyVol %d: %v", row.Timestamp, err)
		}
		aggregate.sum += v
		aggregate.count++
	}
	if aggregate.count != fixtureSubBucketCount {
		t.Fatalf("the interval labeled %d has %d 5m sub-buckets, and a full day has %d — the sub-bucket window is not the one the declaration claims, or the series is short",
			bar.Timestamp, aggregate.count, fixtureSubBucketCount)
	}

	residual, confirmed := additiveResidual(
		derivatives.LabelIntervalStart,
		additiveBar{labelMs: bar.Timestamp, value: barValue},
		aggregate,
		fixtureDayWidthMs,
	)
	if !confirmed {
		t.Fatalf("the START declaration was not confirmed by the live venue: deviation %.6f (%d sub-buckets, sum %.2f, bar %.2f)",
			residual, aggregate.count, aggregate.sum, barValue)
	}
	if residual > additiveFalsifierTolerance {
		t.Fatalf("deviation %.6f above the declared tolerance %.6f", residual, additiveFalsifierTolerance)
	}
	if _, opposite := additiveResidual(
		derivatives.LabelIntervalEnd,
		additiveBar{labelMs: bar.Timestamp, value: barValue},
		aggregate,
		fixtureDayWidthMs,
	); opposite {
		t.Fatal("the END convention was confirmed by the same window — the falsifier stopped discriminating against the live venue")
	}

	t.Logf("additive falsifier: bar %d = %.2f, sum of %d sub-buckets = %.2f, deviation %.6f",
		bar.Timestamp, barValue, aggregate.count, aggregate.sum, residual)
}
