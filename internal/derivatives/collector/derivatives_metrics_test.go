package collector

import (
	"testing"
)

func TestSupportForIsDeclaredPerFeed(t *testing.T) {
	tests := []struct {
		name      string
		feed      derivativesFeed
		timeframe string
		want      periodSupport
	}{
		{"open interest serves five minutes", feedOpenInterest, "5m", periodAvailable},
		{"open interest serves the daily period", feedOpenInterest, "1d", periodAvailable},
		{"open interest does not serve the weekly period", feedOpenInterest, "1w", periodUnavailable},
		// Verified live: the daily period answers normally. Remapping it to 4h would be based
		// on a comment, not a measurement.
		{"long short ratio serves the daily period", feedLongShortRatio, "1d", periodAvailable},
		{"long short ratio does not serve the weekly period", feedLongShortRatio, "1w", periodUnavailable},
		// Settlements are events with no period parameter, so the fan-out reaches any bucket.
		{"funding is period agnostic", feedFundingRate, "1w", periodAvailable},
		// A period nobody measured is not evidence of unavailability.
		{"an unmeasured period is undeclared", feedOpenInterest, "7m", periodUndeclared},
		{"case and padding do not change the answer", feedOpenInterest, " 5M ", periodAvailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := supportFor(tt.feed, tt.timeframe); got != tt.want {
				t.Errorf("supportFor(%s, %q) = %s, want %s", tt.feed, tt.timeframe, got, tt.want)
			}
		})
	}
}

func TestClassifyEmptyCollection(t *testing.T) {
	tests := []struct {
		name      string
		feed      derivativesFeed
		timeframe string
		want      collectionOutcome
	}{
		// These two must not produce the same log line: a served period with no data is a
		// legitimate zero, an unserved one is unsupported.
		{"a served period with nothing in it is a legitimate zero", feedOpenInterest, "5m", outcomeEmpty},
		{"a period the provider does not serve is unsupported", feedOpenInterest, "1w", outcomeUnsupported},
		{"a period nobody measured is neither", feedOpenInterest, "7m", outcomeEmptyUndeclared},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyEmptyCollection(tt.feed, tt.timeframe); got != tt.want {
				t.Errorf("classifyEmptyCollection(%s, %q) = %s, want %s", tt.feed, tt.timeframe, got, tt.want)
			}
		})
	}
}

func TestCompareFundingInterval(t *testing.T) {
	declared := func(s string) *string { return &s }

	tests := []struct {
		name          string
		measuredHours float64
		hasMeasured   bool
		declared      *string
		want          intervalComparison
	}{
		{"agreement", 8, true, declared("8h"), intervalAgree},
		{"agreement within the jitter tolerance", 8.1, true, declared("8h"), intervalAgree},
		{"the exchange declares eight and settles at four", 4, true, declared("8h"), intervalDiverge},
		{"the exchange declares eight and settles at one", 1, true, declared("8h"), intervalDiverge},
		{"no series yet, so no spacing to measure", 0, false, declared("8h"), intervalNoMeasurement},
		{"measured but the metadata is absent", 4, true, nil, intervalNoMetadata},
		{"measured but the metadata is unreadable", 4, true, declared("every so often"), intervalNoMetadata},
		{"neither rung", 0, false, nil, intervalNoMeasurement},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, _ := compareFundingInterval(tt.measuredHours, tt.hasMeasured, tt.declared)
			if got != tt.want {
				t.Errorf("comparison = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestPublishFeedAvailabilityCoversTheProductRegistry(t *testing.T) {
	// The matrix is published from the timeframes the product actually asks for, so a
	// registry entry no feed serves is visible instead of being inferred from silence.
	timeframes := []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"}
	publishFeedAvailability(Metrics{}, timeframes)

	for _, feed := range []derivativesFeed{feedOpenInterest, feedLongShortRatio} {
		if got := supportFor(feed, "1w"); got != periodUnavailable {
			t.Errorf("%s at 1w = %s, want unavailable — the registry ships a timeframe this feed cannot serve", feed, got)
		}
	}
}

func TestFormatHoursIsStableAsALabel(t *testing.T) {
	tests := []struct {
		hours float64
		want  string
	}{
		{8, "8"},
		{4, "4"},
		{1, "1"},
		{0.5, "0.5"},
	}
	for _, tt := range tests {
		if got := formatHours(tt.hours); got != tt.want {
			t.Errorf("formatHours(%v) = %q, want %q", tt.hours, got, tt.want)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ABSENCE IS DECLARED PER (SOURCE, QUANTITY, TIMEFRAME).
//
// The matrix needs THREE dimensions, not two: the venue row, whose funding quantity is
// period-agnostic, must not answer for the aggregator — the ONLY writer of the durable tier,
// which serves weekly for no quantity at all. "We did not ask" and "it is not served" are
// different answers, and a map that infers the second from a missing key returns the first.
// ─────────────────────────────────────────────────────────────────────────────

// THE MUTANT THIS CASE KILLS: deleting a `periodUnavailable` entry from the Coinalyze row.
// The entry disappears, the lookup does not find the key, and the answer falls to
// `periodUndeclared` — "we never asked" — for a period the API answers with 400.
func TestCoinalyzeAbsenceIsDeclaredAndNotInferredFromAMissingKey(t *testing.T) {
	for _, feed := range declaredFeeds() {
		for _, tf := range []string{"3m", "8h", "3d", "1w"} {
			got := supportForSource(sourceCoinalyze, feed, tf)
			if got != periodUnavailable {
				t.Errorf("coinalyze/%s at %s = %s, want unavailable — absence must be DECLARED, not inferred from a missing key", feed, tf, got)
			}
		}
	}
}

// THE VENUE ROW IS NOT EVIDENCE ABOUT THE DURABLE TIER. The venue serves settlements as an
// EVENT, with no period parameter, so its `1w` exists; Coinalyze serves
// `/funding-rate-history` with `interval`, and the accepted list ends at `daily`.
//
// Without this assertion, `supportFor(feedFundingRate, "1w") == periodAvailable` — true of
// the venue — would pass as an answer about the durable tier, and coverage would inherit an
// availability the durable writer does not have.
func TestFundingIsPeriodAgnosticAtTheVenueAndPeriodBoundAtTheAggregator(t *testing.T) {
	if got := supportForSource(sourceVenue, feedFundingRate, "1w"); got != periodAvailable {
		t.Errorf("venue/funding at 1w = %s, want available — the venue serves settlements as events", got)
	}
	if got := supportForSource(sourceCoinalyze, feedFundingRate, "1w"); got != periodUnavailable {
		t.Errorf("coinalyze/funding at 1w = %s, want unavailable — its endpoint takes `interval` and the accepted list ends at daily", got)
	}
}

// TWO LISTS WOULD BE TWO TRUTHS. The set declared available in the Coinalyze row must EQUAL,
// in both directions, the key set of `coinalyzeIntervals` — which is what the poller actually
// translates for the API.
func TestCoinalyzeAvailableSetEqualsTheProviderIntervalMap(t *testing.T) {
	declared := map[string]bool{}
	for tf, support := range coinalyzePeriodSupport() {
		if support == periodAvailable {
			declared[tf] = true
		}
	}
	for tf := range coinalyzeIntervals() {
		if !declared[tf] {
			t.Errorf("%s is translated by coinalyzeIntervals and the support table does not declare it available", tf)
		}
	}
	for tf := range declared {
		if _, ok := coinalyzeIntervals()[tf]; !ok {
			t.Errorf("%s is declared available and coinalyzeIntervals cannot translate it — the declaration would promise a call that does not exist", tf)
		}
	}
}

// "WE NEVER ASKED" REMAINS A THIRD ANSWER, per source and per quantity.
func TestUndeclaredSurvivesTheSourceAxis(t *testing.T) {
	tests := []struct {
		name      string
		source    derivativesSource
		feed      derivativesFeed
		timeframe string
	}{
		{"period nobody measured at Coinalyze", sourceCoinalyze, feedOpenInterest, "7m"},
		{"source the matrix does not declare", derivativesSource("bybit"), feedOpenInterest, "1h"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := supportForSource(tt.source, tt.feed, tt.timeframe); got != periodUndeclared {
				t.Errorf("supportForSource(%s, %s, %q) = %s, want undeclared", tt.source, tt.feed, tt.timeframe, got)
			}
		})
	}
}

// THE DECLARATION REACHES THE REGISTRY, and it arrives with the source. A mechanism that knew
// the answer and published it without the axis would be a disconnected cable: the operator
// would read `feed_available{feed="funding_rate",timeframe="1w"} = 1` and conclude coverage.
func TestPublishFeedAvailabilityCarriesTheSourceAxis(t *testing.T) {
	rec := newCollectorTestMetrics()
	publishFeedAvailability(rec.port(), []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"})

	if got := rec.feedAvailable[ckey(string(sourceVenue), string(feedFundingRate), "1w")]; got != true {
		t.Errorf("venue/funding_rate/1w published as %v, want 1", got)
	}
	if got := rec.feedAvailable[ckey(string(sourceCoinalyze), string(feedFundingRate), "1w")]; got != false {
		t.Errorf("coinalyze/funding_rate/1w published as %v, want 0 — the durable source does not serve weekly", got)
	}
	if got := rec.feedAvailable[ckey(string(sourceCoinalyze), string(feedOpenInterest), "1d")]; got != true {
		t.Errorf("coinalyze/open_interest/1d published as %v, want 1", got)
	}
}
