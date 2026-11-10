package collector

import (
	"log/slog"
	"sort"
	"strconv"
	"strings"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─────────────────────────────────────────────────────────────────────────────
// DERIVATIVES OBSERVABILITY
//
// Measurement only: nothing here alters what is collected; it makes visible what would
// otherwise be indistinguishable in the logs.
//
// The four facts this file exposes:
//
//  1. A period the provider does NOT serve answers HTTP 200 with `[]`. That produced
//     exactly the same log line as a symbol that legitimately has no data in the
//     window — "OI: 0". Two different states, one observation.
//  2. The depth actually assembled per (symbol, timeframe) — points AND time span.
//     A series of 200 points is 16h40 at 5m and 200 days at 1d; the count alone
//     does not say how far back a window computation can reach.
//  3. Whether the settlement interval the exchange DECLARES agrees with the spacing
//     actually MEASURED between settlements. The declared value goes stale by
//     exchange rule, and it goes stale in the direction that fabricates an extreme.
//  4. The (feed × timeframe) availability matrix, recorded PER FEED. `1w` is absent
//     from open interest and from long/short ratio while the product's timeframe
//     registry includes it — that is a property of each feed, not of the family.
//
// CARDINALITY. The fleet is capped at 500 symbols and the timeframe registry holds
// single digits, so `symbol` and `timeframe` are bounded labels here. They are used
// only on the depth gauges, where the per-pair value IS the measurement; the request
// counters stay symbol-free so their series count does not track the fleet.
// ─────────────────────────────────────────────────────────────────────────────

// derivativesFeed names one upstream series. The availability matrix is per feed
// because the feeds do not share a period vocabulary.
type derivativesFeed string

// derivativesSource names the PROVENANCE of a reading, which is the third axis the
// matrix needs and did not have.
//
// The venue answers from the exchange's own API; `coinalyze` is the aggregator that
// feeds the durable tier. They do NOT share a period vocabulary, and — the part that
// cost a timeframe — they do not even agree on whether funding HAS a period:
//
//   - the venue serves settlements as EVENTS, with no period parameter, so the fan-out
//     reaches whichever bucket contains them, including `1w`;
//   - Coinalyze serves `/funding-rate-history` with an `interval`, and the accepted
//     list ends at `daily` — so `1w` funding answers 400 there.
//
// This is why the venue row is NOT evidence about the durable tier: the venue never
// writes it. `WriteBatch` has exactly one non-test caller and it is the Coinalyze
// poller.
type derivativesSource string

const (
	// sourceVenue is the exchange's own API. The exchange IDENTITY already travels in
	// the `exchange` label, so this value names the ROLE, not the venue.
	sourceVenue derivativesSource = "venue"
	// sourceCoinalyze is the aggregator, and the ONLY writer of the durable tier. The
	// string is the same one the durable `source` column carries.
	sourceCoinalyze derivativesSource = "coinalyze"
)

// declaredSources is the publication order of the matrix, fixed so the startup log
// reads the same way on every boot.
func declaredSources() []derivativesSource {
	return []derivativesSource{sourceVenue, sourceCoinalyze}
}

const (
	feedOpenInterest   derivativesFeed = "open_interest"
	feedLongShortRatio derivativesFeed = "long_short_ratio"
	feedFundingRate    derivativesFeed = "funding_rate"
)

// declaredFeeds is the publication order of the matrix; it is fixed so the startup
// log reads the same way on every boot.
func declaredFeeds() []derivativesFeed {
	return []derivativesFeed{feedOpenInterest, feedLongShortRatio, feedFundingRate}
}

// periodSupport is a THREE-state answer, not a boolean.
//
// "Undeclared" exists because absence of a measurement is not evidence of
// unavailability: a period nobody probed must not be published as unavailable, and
// must not be skipped as if it were.
type periodSupport int

const (
	periodUndeclared periodSupport = iota
	periodUnavailable
	periodAvailable
)

func (s periodSupport) String() string {
	switch s {
	case periodAvailable:
		return "available"
	case periodUnavailable:
		return "unavailable"
	default:
		return "undeclared"
	}
}

// feedPeriodSupport is the MEASURED support table, one row per feed.
//
// Open interest and long/short ratio share the same period vocabulary on Binance
// USDⓈ-M: 5m, 15m, 30m, 1h, 2h, 4h and 1d are served; 1w is not. The unsupported
// entries are listed explicitly rather than inferred from the absence of a key,
// because "we know it is not served" and "we never asked" schedule different work.
//
// Funding rate carries NO period parameter — settlements are events, fanned out into
// whichever bucket contains them — so every timeframe is available for it by
// construction.
// Two distinct provider behaviors are both recorded as `periodUnavailable`, because
// the operational consequence is identical — do not ask: `1m`, `3m`, `3d`, `1w` and
// `1M` answer HTTP 200 with an empty array (the failure this file exists to make
// visible), while `8h` answers with a hard error.
func feedPeriodSupport() map[derivativesFeed]map[string]periodSupport {
	return map[derivativesFeed]map[string]periodSupport{
		feedOpenInterest: {
			"1m":         periodUnavailable, // HTTP 200 with [], not an error
			"3m":         periodUnavailable,
			"5m":         periodAvailable,
			timeframe15m: periodAvailable,
			timeframe30m: periodAvailable,
			"1h":         periodAvailable,
			"2h":         periodAvailable,
			"4h":         periodAvailable,
			"8h":         periodUnavailable,
			"1d":         periodAvailable,
			"3d":         periodUnavailable,
			"1w":         periodUnavailable,
			"1M":         periodUnavailable,
		},
		feedLongShortRatio: {
			"1m":         periodUnavailable,
			"3m":         periodUnavailable,
			"5m":         periodAvailable,
			timeframe15m: periodAvailable,
			timeframe30m: periodAvailable,
			"1h":         periodAvailable,
			"2h":         periodAvailable,
			// Verified live: `period=1d` answers normally. Remapping it to 4h would fill the
			// daily ZSET with 4h readings aligned to daily buckets.
			"4h": periodAvailable,
			"8h": periodUnavailable,
			"1d": periodAvailable,
			"3d": periodUnavailable,
			"1w": periodUnavailable,
			"1M": periodUnavailable,
		},
		feedFundingRate: nil, // period-agnostic; see supportForSource
	}
}

// coinalyzePeriodSupport is the Coinalyze period vocabulary, and it is ONE row shared by
// the three feeds because this provider takes the same `interval` on all three history
// endpoints — open interest, funding rate and long/short ratio alike.
//
// The accepted list is not inferred: the API itself publishes it in the body of a 400 —
// measured `1min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 12hour, daily`, the
// same measurement `coinalyzeIntervals` is built from. THERE IS NO WEEKLY.
//
// The unserved periods are listed EXPLICITLY, exactly like the venue row above, because
// "we know it is not served" and "we never asked" schedule different work: the first is
// a cell to declare to the client, the second is a measurement nobody took. Inferring
// unavailability from a missing key collapses the two, and a collapsed cell is how `1w`
// spent a deploy emitting one WARN per cycle instead of being declared once.
//
// The available half is kept honest by a ratchet in the test: this row's available set
// must EQUAL the key set of `coinalyzeIntervals`. Two lists would be two truths.
func coinalyzePeriodSupport() map[string]periodSupport {
	return map[string]periodSupport{
		"1m":         periodAvailable,
		"3m":         periodUnavailable,
		"5m":         periodAvailable,
		timeframe15m: periodAvailable,
		timeframe30m: periodAvailable,
		"1h":         periodAvailable,
		"2h":         periodAvailable,
		"4h":         periodAvailable,
		"6h":         periodAvailable,
		"8h":         periodUnavailable,
		"12h":        periodAvailable,
		"1d":         periodAvailable,
		"3d":         periodUnavailable,
		"1w":         periodUnavailable,
		// MONTHLY IS DELIBERATELY ABSENT, and not because nobody measured it. The lookup
		// lowercases before it reads, so `1M` and `1m` are the SAME key — a monthly cell
		// here could never be reached, and a declaration the lookup cannot reach is
		// decorative. The product registry has no monthly bucket, so nothing asks. The
		// venue rows above carry the dead key; there it is harmless, because both readings
		// answer `unavailable` anyway.
	}
}

// sourcePeriodSupport is the matrix on all three axes: (source, feed, timeframe).
//
// A feed mapped to a NIL row is PERIOD-AGNOSTIC — it carries no period parameter, so
// every timeframe is reachable by construction. A feed ABSENT from a source's map is
// undeclared, which is a different answer and stays a different answer.
func sourcePeriodSupport() map[derivativesSource]map[derivativesFeed]map[string]periodSupport {
	return map[derivativesSource]map[derivativesFeed]map[string]periodSupport{
		sourceVenue: feedPeriodSupport(),
		sourceCoinalyze: {
			feedOpenInterest:   coinalyzePeriodSupport(),
			feedLongShortRatio: coinalyzePeriodSupport(),
			// NOT nil, and that is the whole point of this row existing. Unlike the venue,
			// this provider's funding endpoint takes an `interval`.
			feedFundingRate: coinalyzePeriodSupport(),
		},
	}
}

// supportForSource answers the matrix for one cell, on all three axes.
func supportForSource(source derivativesSource, feed derivativesFeed, timeframe string) periodSupport {
	feeds, ok := sourcePeriodSupport()[source]
	if !ok {
		return periodUndeclared
	}
	row, ok := feeds[feed]
	if !ok {
		return periodUndeclared
	}
	if row == nil {
		return periodAvailable
	}
	support, ok := row[strings.TrimSpace(strings.ToLower(timeframe))]
	if !ok {
		return periodUndeclared
	}
	return support
}

// supportFor answers the matrix for one cell of the VENUE, which is the source every
// caller in this package collects from.
func supportFor(feed derivativesFeed, timeframe string) periodSupport {
	return supportForSource(sourceVenue, feed, timeframe)
}

// publishFeedAvailability records the (source × feed × timeframe) matrix once, at
// startup.
//
// It is published from the product's OWN timeframe registry rather than from the
// support table, because the question the matrix answers is "of the timeframes this
// product asks for, which ones can each source actually serve for each feed" — a cell
// the product never requests is not an operational fact.
//
// EVERY SOURCE IS PUBLISHED, not just the one this package polls. The absence that
// matters is the aggregator's, because it is the aggregator that writes the durable
// tier; a matrix that only spoke for the venue would report `1w` funding as served and
// leave the durable hole invisible.
func publishFeedAvailability(metrics Metrics, timeframes []string, logs ...*slog.Logger) {
	log := logger.OrDiscard(logs...)
	for _, source := range declaredSources() {
		for _, feed := range declaredFeeds() {
			var available, unavailable, undeclared []string
			for _, tf := range timeframes {
				switch supportForSource(source, feed, tf) {
				case periodAvailable:
					metrics.setFeedAvailable(string(source), string(feed), tf, true)
					available = append(available, tf)
				case periodUnavailable:
					metrics.setFeedAvailable(string(source), string(feed), tf, false)
					unavailable = append(unavailable, tf)
				default:
					// Deliberately not published — see the metric's Help.
					undeclared = append(undeclared, tf)
				}
			}
			sort.Strings(available)
			sort.Strings(unavailable)
			sort.Strings(undeclared)
			log.Info("derivatives availability",
				"source", source,
				"feed", feed,
				"available", joinOrDash(available),
				"unavailable", joinOrDash(unavailable),
				"undeclared", joinOrDash(undeclared))
		}
	}
}

func joinOrDash(values []string) string {
	if len(values) == 0 {
		return "—"
	}
	return strings.Join(values, ",")
}

// collectionOutcome is the classified result of one feed × timeframe collection.
type collectionOutcome string

const (
	outcomePoints          collectionOutcome = "points"
	outcomeEmpty           collectionOutcome = "empty"
	outcomeEmptyUndeclared collectionOutcome = "empty_undeclared"
	outcomeUnsupported     collectionOutcome = "unsupported"
	outcomeError           collectionOutcome = "error"
)

// classifyEmptyCollection tells a legitimate zero from a period the provider does not
// serve. It exists because the provider answers HTTP 200 with `[]` for both.
func classifyEmptyCollection(feed derivativesFeed, timeframe string) collectionOutcome {
	switch supportFor(feed, timeframe) {
	case periodUnavailable:
		return outcomeUnsupported
	case periodUndeclared:
		return outcomeEmptyUndeclared
	default:
		return outcomeEmpty
	}
}

func recordFeedRequest(metrics Metrics, exchange string, feed derivativesFeed, timeframe string, outcome collectionOutcome) {
	metrics.incFeedRequest(exchange, string(feed), timeframe, string(outcome))
}

func recordHistoryPage(metrics Metrics, exchange string, feed derivativesFeed, timeframe string) {
	metrics.incHistoryPage(exchange, string(feed), timeframe)
}

// recordCollectedDepth publishes the depth actually assembled for one pair.
//
// A span is only published when at least two points exist: a single point spans no
// time, and publishing 0 for it would make "one lonely reading" indistinguishable
// from "many readings, all at the same instant".
func recordCollectedDepth(metrics Metrics, exchange string, feed derivativesFeed, symbol, timeframe string, points int, oldestMs, newestMs int64) {
	metrics.setCollectedPoints(exchange, string(feed), symbol, timeframe, float64(points))
	if points < 2 || oldestMs <= 0 || newestMs <= oldestMs {
		return
	}
	metrics.setCollectedSpanSeconds(exchange, string(feed), symbol, timeframe, float64(newestMs-oldestMs)/1000)
}

// intervalComparison is the classified relation between the two rungs of the ladder.
type intervalComparison string

const (
	intervalAgree         intervalComparison = "agree"
	intervalDiverge       intervalComparison = "diverge"
	intervalNoMeasurement intervalComparison = "no_measurement"
	intervalNoMetadata    intervalComparison = "no_metadata"
)

// compareFundingInterval classifies the measured spacing against the declared one.
//
// The tolerance is the same one the measurement itself uses: settlement timestamps
// jitter, so exact equality would report a divergence on every symbol.
func compareFundingInterval(measuredHours float64, hasMeasured bool, declared *string) (intervalComparison, float64, bool) {
	declaredHours, hasDeclared := 0.0, false
	if declared != nil {
		declaredHours, hasDeclared = parseIntervalHours(*declared)
	}
	switch {
	case !hasMeasured || measuredHours <= 0:
		return intervalNoMeasurement, declaredHours, hasDeclared
	case !hasDeclared:
		return intervalNoMetadata, 0, false
	case abs(measuredHours-declaredHours) <= measuredIntervalTolerance:
		return intervalAgree, declaredHours, true
	default:
		return intervalDiverge, declaredHours, true
	}
}

// recordFundingIntervalComparison publishes the divergence between the rung that
// carries evidence and the rung that carries the exchange's word.
func recordFundingIntervalComparison(metrics Metrics, exchange, symbol string, measuredHours float64, hasMeasured bool, declared *string) {
	result, declaredHours, hasDeclared := compareFundingInterval(measuredHours, hasMeasured, declared)
	metrics.incFundingIntervalComparison(exchange, string(result))
	if result != intervalDiverge || !hasDeclared {
		return
	}
	metrics.incFundingIntervalDivergence(
		exchange, symbol, formatHours(measuredHours), formatHours(declaredHours),
	)
}

func formatHours(hours float64) string {
	return strconv.FormatFloat(hours, 'g', -1, 64)
}

// recordFundingIntervalMeasurement records which exit of `revalidateFundingInterval` was taken.
// See fundingIntervalMeasurementTotal.
func recordFundingIntervalMeasurement(metrics Metrics, exchange, outcome string) {
	metrics.incFundingIntervalMeasurement(exchange, outcome)
}

// recordFundingProfileRegistrySize publishes the registry size. See
// fundingProfileRegistryEntries.
func recordFundingProfileRegistrySize(metrics Metrics, n int) {
	metrics.setFundingProfileRegistryEntries(n)
}
