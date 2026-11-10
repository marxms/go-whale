package httpapi

import (
	"fmt"
	"strings"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/derivatives/collector"
)

// derivativesContractVersion identifies this wire declaration. It moves only on a material
// change (unit, estimator, baseline window or threshold table), which applies prospectively
// and never recomputes history, so readings under two versions are never pooled.
const derivativesContractVersion = "derivatives-integrity/2"

// FieldRole says what a field is for, which a JSON key cannot carry. Two fields hold open
// interest and only one may reach a decision: notional is contracts x price, so its variation
// measures price.
type FieldRole string

const (
	// RoleDecision — may feed a threshold, a score or a ranking.
	RoleDecision FieldRole = "decision"
	// RoleDisplay — may be shown, never compared or ordered.
	RoleDisplay FieldRole = "display"
	// RoleProvenance — describes how a sibling field was produced.
	RoleProvenance FieldRole = "provenance"
	// RoleDeprecated — still emitted for consumers mid-migration, already
	// superseded, and not to be built upon.
	RoleDeprecated FieldRole = "deprecated"
)

// Window declares what span a number covers. Levels are point samples; increments always span
// one bar of the selected timeframe, since the timeframe selector is the scale selector.
const (
	windowPoint = "point"
)

// timestampLabelBucketStart — the timestamp beside these fields marks the start of the
// timeframe bucket the record was filed under, and it is served only where a bucket exists
// (see newMeasurementDeclaration).
//
// bucketAlignMs floors the observation into its bucket and upsertSeries stamps that floor on
// the member, and redisstore.BucketClosed reads the label as a start (labelTs+tfMs <= nowMs).
// The label a consumer aligns on and the label the closure test runs on must therefore be the
// same end of the bar. The value "periodEnd" would describe the exchange, not this field.
//
// One field and not two: openInterestObservedAtMs already carries the per-record instant.
const timestampLabelBucketStart = "bucketStart"

// fundingBucketAggregation declares what a bucket holds when the period contains several
// settlements. Within a bucket the funding group moves whole and the last writer wins
// (mergeDerivativesSnapshots), so the value is a sample of the period, never its sum or
// average. It is declared on funding because open interest is a level nobody sums, while
// funding is a flow where summing is the natural instinct.
const fundingBucketAggregation = "last settlement in the bucket — NOT a sum and NOT an average of the period. " +
	"A 1d bucket on an 8h-settled symbol carries the day's LAST print (measured 2026-08-20: 9.422e-05, " +
	"the 16:00 settlement, against a day sum of 2.9077e-04). Read it as a SAMPLE of the period, never as its total"

// FieldDeclaration is the per-field descriptor that turns an ad-hoc map into a
// self-describing envelope: what the number is measured in, what span it covers,
// what the timestamp marks, how several readings inside one bucket were reduced to
// this one, and whether it may be decided upon.
type FieldDeclaration struct {
	Field          string `json:"field"`
	Unit           string `json:"unit"`
	Window         string `json:"window"`
	TimestampLabel string `json:"timestampLabel,omitempty"`
	// Aggregation says how a period holding MORE THAN ONE reading was reduced to the
	// single number served. Empty where the question does not arise — a field the period
	// only ever holds once has nothing to aggregate.
	Aggregation string    `json:"aggregation,omitempty"`
	Role        FieldRole `json:"role"`
	Note        string    `json:"note,omitempty"`
}

// derivativesFields declares every derivatives field this package serializes. It is copied on
// read so no handler can mutate what the next request serves.
func derivativesFields() []FieldDeclaration {
	return []FieldDeclaration{
		{
			Field:  "source",
			Unit:   "provider name",
			Window: windowPoint,
			Role:   RoleProvenance,
			Note: "who MEASURED this point — the provider, not the venue. `exchange` says which market the " +
				"number is about; the two differ for an aggregator serving a Binance perpetual. It carries no " +
				"timestamp label because it labels no instant, and it is per POINT because a series can be " +
				"stitched from two providers across the ZSET cap",
		},
		{
			Field:  "fieldSources",
			Unit:   "provider name per field",
			Window: windowPoint,
			Role:   RoleProvenance,
			Note: "present ONLY on a STITCHED point — one whose fields did not all come from the provider in " +
				"`source`. The deep-window read merges the hot tier over the durable one field by field, and the " +
				"result carries one `source`, the hot one, over numbers the cold tier measured (measured: 156 " +
				"buckets between 2026-02-05 and 2026-07-10). Its absence means the point is of a single " +
				"provenance and `source` describes every field",
		},
		{
			Field:          "openInterestBase",
			Unit:           "contracts",
			Window:         windowPoint,
			TimestampLabel: timestampLabelBucketStart,
			Role:           RoleDecision,
			Note:           "open interest in base units — the only open-interest quantity admissible as a decision input",
		},
		{
			Field:          "openInterest",
			Unit:           "quote currency (notional)",
			Window:         windowPoint,
			TimestampLabel: timestampLabelBucketStart,
			Role:           RoleDisplay,
			Note:           "contracts × price by identity; its variation measures price, not position flow",
		},
		{
			Field:          "openInterestObservedAtMs",
			Unit:           "epoch milliseconds",
			Window:         windowPoint,
			TimestampLabel: timestampLabelBucketStart,
			Role:           RoleProvenance,
			Note: "when the open-interest reading in THIS record was taken, which is not the " +
				"record's own timestamp. A poll whose open-interest fetch failed still stamps a " +
				"fresh timestamp and still merges its other fields, so the stored record can hold " +
				"a reading from t0 under a clock of t1; this field is what lets the next poll " +
				"measure its window against the reading rather than against the clock, and " +
				"without it that window comes out SHORTER than the span it covers. Moves as one " +
				"group with openInterestBase — a reading and the instant it was taken never separate",
		},
		{
			Field:          "oiChangePctWindowMs",
			Unit:           "milliseconds",
			Window:         "self (it IS the window of oiChangePct)",
			TimestampLabel: timestampLabelBucketStart,
			Role:           RoleDisplay,
			Note: "the MEASURED span between the two polls that produced oiChangePct, not the " +
				"configured poll interval — the loop's ticker can be lapped by its own work, and " +
				"when it is, the real spacing lands on a multiple of the configured one (measured " +
				"2026-08-09: interval 300s, observed gaps of exactly 300s and 600s). It exists so " +
				"the percentage is never rendered bare: a reader who cannot see the span cannot " +
				"tell +0.5% over nine minutes from +0.5% over four. Travels with oiChangePct and " +
				"is stripped from the series alongside it",
		},
		{
			Field:          "oiChangePct",
			Unit:           "percent",
			Window:         "measured per reading, published as oiChangePctWindowMs",
			TimestampLabel: timestampLabelBucketStart,
			Role:           RoleDeprecated,
			Note: "legacy delta between consecutive polls, not between bars of this timeframe; " +
				"superseded by the normalized increment. NO LONGER EMITTED ON THE SERIES ROUTES: the " +
				"write path stops it at the timeframe boundary, because a poll-to-poll number sitting " +
				"in a 1d bucket claims a window nobody measured. It survives on the latest snapshot, " +
				"where 'since the previous poll' is a true statement about the record it sits in",
		},
		{
			Field:          "fundingRate",
			Unit:           "fraction per settlement interval",
			Window:         "one settlement interval",
			TimestampLabel: timestampLabelBucketStart,
			Aggregation:    fundingBucketAggregation,
			Role:           RoleDisplay,
			Note:           "raw as published; not comparable across symbols because the interval differs — compare fundingRate8h",
		},
		{
			Field:          "nextFundingRate",
			Unit:           "fraction per settlement interval",
			Window:         "one settlement interval",
			TimestampLabel: timestampLabelBucketStart,
			Role:           RoleDisplay,
			Note:           "forward-looking estimate published by the exchange",
		},
		{
			Field:          "fundingRate8h",
			Unit:           "fraction per 8h",
			Window:         "8h equivalent",
			TimestampLabel: timestampLabelBucketStart,
			// Normalization does not undo the sampling: fundingRate8h derives from the same
			// print that survived the bucket.
			Aggregation: fundingBucketAggregation,
			Role:        RoleDecision,
			Note:        "fundingRate × 8/N; emitted only when nStep is measured or seed",
		},
		{
			Field:  "fundingIntervalHours",
			Unit:   "hours",
			Window: windowPoint,
			Role:   RoleProvenance,
			Note:   "N — the settlement interval actually used to produce fundingRate8h",
		},
		{
			Field:  "nStep",
			Unit:   nStepEnumDeclaration(),
			Window: windowPoint,
			Role:   RoleProvenance,
			Note:   "which rung of the derivation ladder produced N",
		},
		{
			Field:  "interestRate",
			Unit:   "fraction per 8h",
			Window: windowPoint,
			Role:   RoleProvenance,
			Note:   "the contract's own interest rate; zero for 158 of 730 TRADING symbols, so a global reading of fundingRate = 0 is invalid",
		},
		{
			Field:  "fundingAtAnchor",
			Unit:   "boolean",
			Window: windowPoint,
			Role:   RoleProvenance,
			Note:   "funding pinned at interestRate × N/8 — the reading is censored by the clamp, not measured",
		},
		{
			Field:          "longShortRatio",
			Unit:           "ratio of accounts (long / short)",
			Window:         windowPoint,
			TimestampLabel: timestampLabelBucketStart,
			// Display, not decision: this is the raw level. The level was removed from every
			// gate because an absolute cut fires at wildly different rates per symbol, and the
			// scored quantity is the one-bar increment published as zLongShortRatio.
			Role: RoleDisplay,
			Note: "much of its dispersion sits BETWEEN symbols rather than within them (ICC 0.74-0.80 " +
				"once the within-symbol autocorrelation is corrected for), so it is usable only against the " +
				"symbol's own baseline and never against a global absolute threshold. Whether that " +
				"between-symbol component is stable is NOT established: the measured ICC falls monotonically " +
				"as the window widens and had not settled at ~40 days",
		},
		{
			Field:          "lsLongPct",
			Unit:           "fraction of accounts",
			Window:         windowPoint,
			TimestampLabel: timestampLabelBucketStart,
			Role:           RoleDisplay,
		},
		{
			Field:          "lsShortPct",
			Unit:           "fraction of accounts",
			Window:         windowPoint,
			TimestampLabel: timestampLabelBucketStart,
			Role:           RoleDisplay,
		},
	}
}

// derivativesLimitations are the methodology's own limits, carried on the wire so they reach
// the surface that renders the numbers.
func derivativesLimitations() []string {
	return []string{
		"no predictive edge has been established for these readings; they are a corrected measurement, not a forecast",
		"funding is a step function known only at settlement — a bucket without fundingRate has no funding reading, and absence is not zero",
		"an absent field is absent; no neutral value is substituted for it",
	}
}

// Declared measurement units and windows for the derived columns. Every normalized field
// is scored the same way, so the declaration names the invariant once.
const (
	limitationMADUnits = "MAD units (1.4826 × MAD), signed"
	limitationOneBar   = "one bar of the selected timeframe"
)

// normalizedFields declares the derived columns, which live in their own object
// because they are not fields of the persisted snapshot — they are computed from it
// against the symbol's own baseline.
func normalizedFields() []FieldDeclaration {
	return []FieldDeclaration{
		{
			Field:          "zOpenInterest",
			Unit:           limitationMADUnits,
			Window:         limitationOneBar,
			TimestampLabel: timestampLabelBucketStart,
			Role:           RoleDecision,
			Note:           "robust z of g(t) = ln C(t) − ln C(t−1) over CONTRACTS, against the strictly trailing baseline of this same (symbol, timeframe); |z| > 3 fires on 12.42% of bars AT 5m, so no Gaussian threshold applies (0.270% would be the Gaussian expectation — an error of 45x). The timeframe is stated beside the rate because the rate varies by timeframe; the retired 12.14% figure predates the frozen-dataset measurement of 688,985 points",
		},
		{
			Field:          "zLongShortRatio",
			Unit:           limitationMADUnits,
			Window:         limitationOneBar,
			TimestampLabel: timestampLabelBucketStart,
			Role:           RoleDecision,
			Note: "robust z of ln LSR(t) − ln LSR(t−1) over ONE BAR, against the strictly trailing baseline " +
				"of this same (symbol, timeframe). Contract /1 scored the LEVEL instead, and the level carries " +
				"ρ₁ ≈ 0.99, leaving n_eff ≈ 4 behind a nominal window of 288 — |z| ≥ 3 fired on " +
				"0.00% of bars at the median symbol with 80-86 of 150 sampled symbols never triggering at all. " +
				"A THRESHOLD TABLE HAS BEEN MEASURED FOR THIS AXIS IN ITS INCREMENT FORM (task 2.5, six " +
				"timeframes) and it is NOT the open-interest ladder — that one does not transfer. The two " +
				"tails differ in SHAPE, not in rarity: p99/p95 is 1.747-1.806 here against 1.90-2.31 on open " +
				"interest, so 'extreme' sits a shorter distance past its own p95 here — but every ladder is " +
				"built as extreme = p95 x (p99/p95), so an 'extreme' on either axis is that axis's own p99 " +
				"and fires on ~1% of its own bars by construction. A combined severity may rely on the two " +
				"measured anchors (p95, p99) and on nothing between or beyond them. This note previously " +
				"declared that no table existed (it predated the measurement, and a consumer honoring it " +
				"would have refused to score an axis the server scores) and then that an 'extreme' here was " +
				"a LESS rare event than on open interest, which the ladder construction refutes",
		},
		{
			Field:          "zPrice",
			Unit:           limitationMADUnits,
			Window:         limitationOneBar,
			TimestampLabel: timestampLabelBucketStart,
			Role:           RoleDecision,
			Note: "robust z of r(t) = ln close(t) − ln close(t−1), against the strictly trailing baseline of this " +
				"same (symbol, timeframe). The close comes from the CANDLE series, never from the notional open " +
				"interest — that quantity is contracts × price, so a return derived from it would carry position " +
				"flow. Scored on the same bar as the other two axes, and absent when that bar has no candle",
		},
	}
}

// MeasurementDeclaration is the envelope half of the contract: what the numbers
// beside it are, on which timeframe, under which version.
type MeasurementDeclaration struct {
	Version   string             `json:"version"`
	Timeframe string             `json:"timeframe,omitempty"`
	Fields    []FieldDeclaration `json:"fields"`
	// Normalized declares the derived columns of the `normalized` object. They are
	// kept apart from Fields because Fields describes the persisted snapshot exactly,
	// one declaration per stored field, and that one-to-one is what makes the drift
	// guard able to fail.
	Normalized  []FieldDeclaration `json:"normalizedFields"`
	Limitations []string           `json:"limitations"`
	// DurableAbsences declares the cells the durable tier does not cover on this timeframe,
	// one per quantity, with the reason. It is additive: absence of durability is not absence
	// of data, so the route keeps serving what it has. Omitted when there is no absence.
	DurableAbsences []collector.DurableAbsenceError `json:"durableAbsences,omitempty"`
}

// newMeasurementDeclaration builds the declaration for one response, copying the package
// tables so a handler cannot alias what the next request serves. A response with no timeframe
// carries no bucket, so the timestamp label is dropped rather than restated: the latest-snapshot
// route's timestamp is the unfloored poll clock.
func newMeasurementDeclaration(timeframe string) MeasurementDeclaration {
	fields := append([]FieldDeclaration(nil), derivativesFields()...)
	normalized := append([]FieldDeclaration(nil), normalizedFields()...)
	if timeframe == "" {
		dropTimestampLabel(fields)
		dropTimestampLabel(normalized)
	}
	return MeasurementDeclaration{
		Version:    derivativesContractVersion,
		Timeframe:  timeframe,
		Fields:     fields,
		Normalized: normalized,
		// The absence declaration comes from the measured matrix in the collector, not from a
		// second list here.
		DurableAbsences: collector.DurableAbsences(timeframe),
		Limitations:     append([]string(nil), derivativesLimitations()...),
	}
}

// dropTimestampLabel clears the label in place. It is only ever handed the per-response
// COPY, never the package table.
func dropTimestampLabel(fields []FieldDeclaration) {
	for i := range fields {
		fields[i].TimestampLabel = ""
	}
}

// declareProvenance stamps the provenance of a read onto every point of it. Provenance is
// already a fact of the key, since each key family has a single writer; writing it into the
// member too would freeze a second copy at the day it was written. A point without a
// declaration gets fallback, which is what its key family means. It returns copies so no caller
// mutates a shared object.
func declareProvenance(snapshots []*derivatives.DerivativesSnapshot, fallback string) []*derivatives.DerivativesSnapshot {
	out := make([]*derivatives.DerivativesSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot == nil {
			continue
		}
		declared := *snapshot
		if declared.Source == "" {
			declared.Source = fallback
		}
		out = append(out, &declared)
	}
	return out
}

// singleProvenanceSeries keeps only the points that share the provenance of the newest intact
// one, which is the point the reading is about.
//
// The baseline is a median and robust scale over increments; two providers sample at different
// instants and round differently, so a mixed ruler measures the distance between providers and
// presents it as market variance. Filtering rather than refusing is deliberate: a stitched
// series is the normal outcome of the deep-window read. Stitched points are excluded too,
// because their Source describes only part of their fields.
func singleProvenanceSeries(snapshots []*derivatives.DerivativesSnapshot) []*derivatives.DerivativesSnapshot {
	var want string
	found := false
	for _, snapshot := range snapshots {
		if snapshot != nil && !snapshot.HasMixedProvenance() {
			want = snapshot.Source
			found = true
			break
		}
	}
	if !found {
		// No intact point: returning the raw series would put the stitched points back into
		// the ruler. The reading goes out sealed with the reason.
		return semCostura(snapshots)
	}

	out := make([]*derivatives.DerivativesSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot == nil || snapshot.Source != want {
			continue
		}
		if snapshot.HasMixedProvenance() {
			continue
		}
		out = append(out, snapshot)
	}
	return out
}

// semCostura returns the points that are NOT a stitch of two provenances.
func semCostura(snapshots []*derivatives.DerivativesSnapshot) []*derivatives.DerivativesSnapshot {
	out := make([]*derivatives.DerivativesSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot != nil && snapshot.HasMixedProvenance() {
			continue
		}
		out = append(out, snapshot)
	}
	return out
}

// FundingDerivation declares, per symbol, which rung produced N. It is read from the persisted
// member, never inferred here: inferring it at the edge would manufacture the confidence the
// ladder exists to withhold.
type FundingDerivation struct {
	NStep         derivatives.NDerivationStep `json:"nStep"`
	IntervalHours *float64                    `json:"intervalHours,omitempty"`
	// AsOf is the timestamp of the record the declaration was read from, so a
	// stale declaration is visible as stale rather than as current.
	AsOf   int64  `json:"asOf,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// deriveFunding reports the funding derivation for a newest-first series. It walks toward the
// oldest looking for the first declared rung, because a backfilled bucket carries no declaration
// and the newest bucket alone could hide a measured interval one bucket down.
func deriveFunding(snapshots []*derivatives.DerivativesSnapshot) FundingDerivation {
	if len(snapshots) == 0 {
		return FundingDerivation{
			NStep:  derivatives.NUnavailable,
			Reason: "no derivatives record for this symbol",
		}
	}

	for _, snap := range snapshots {
		if snap == nil || snap.NStep == "" {
			continue
		}
		if snap.NStep == derivatives.NUnavailable {
			return FundingDerivation{
				NStep:  derivatives.NUnavailable,
				AsOf:   snap.Timestamp,
				Reason: "neither measured spacing nor metadata yielded a settlement interval",
			}
		}
		return FundingDerivation{
			NStep:         snap.NStep,
			IntervalHours: snap.FundingIntervalHours,
			AsOf:          snap.Timestamp,
		}
	}

	return FundingDerivation{
		NStep:  derivatives.NUnavailable,
		Reason: "no record in this window declares a settlement interval",
	}
}

// hasDerivedInterval reports whether a rung produced an interval for this bucket, the
// precondition for serving an 8h-equivalent. It delegates instead of restating the ladder.
func hasDerivedInterval(step derivatives.NDerivationStep) bool {
	return step.Normalizes()
}

// ── normalized readings ───────────────────────────────────────────────────

// NormalizedAxis is one scored column: the raw quantity, its robust z, and the state of the
// ruler that produced it. The state is always present, including when Z is absent: dropping the
// axis would make "not scorable yet" indistinguishable from "unremarkable".
type NormalizedAxis struct {
	// Value is the quantity that was scored: the one-bar log increment of the axis —
	// contracts, long/short ratio or close. Absent when the inputs to compute it are
	// not there.
	Value *float64 `json:"value,omitempty"`
	// Z is the signed robust z. Absent whenever State is not mature.
	Z *float64 `json:"z,omitempty"`
	// State is mature, immature or degenerate; degenerate is named apart from
	// immature because "not enough history yet" and "this symbol does not move" are
	// different facts and only one of them improves with time.
	State derivatives.BaselineState `json:"state"`
	// N and NMin let a consumer see how far the sample is from usable.
	N    int `json:"n"`
	NMin int `json:"nMin"`

	// SpanMs is how far back the sample reaches, which N alone does not say: 288 points cover a
	// day if consecutive and four days if a quarter are missing. A drifting span means a
	// drifting correlation structure while N stays flat. Absent when no baseline was built or
	// for a single-point baseline.
	SpanMs int64 `json:"spanMs,omitempty"`
	// Reason names what was missing when nothing could be computed.
	Reason string `json:"reason,omitempty"`
}

// NormalizedReading carries every scored axis for the newest bar of the series.
type NormalizedReading struct {
	// Transport failure is distinct from absent history or an immature baseline.
	UnavailableReason string `json:"unavailableReason,omitempty"`
	// Timestamp is the bar the reading belongs to, so a stale reading is visible as
	// stale rather than as current. All three axes below are scored on THIS bar.
	Timestamp      int64          `json:"timestamp,omitempty"`
	WindowN        int            `json:"windowN,omitempty"`
	OpenInterest   NormalizedAxis `json:"zOpenInterest"`
	LongShortRatio NormalizedAxis `json:"zLongShortRatio"`
	Price          NormalizedAxis `json:"zPrice"`
}

// priceIncrement is the price axis's input, already resolved by the caller: the close lives in
// the candle series, not the derivatives snapshot. It arrives as a value plus the reason it is
// absent, so absence never turns into a number on the way in.
type priceIncrement struct {
	value *float64
	// missing names what was not there. It is REQUIRED whenever value is nil.
	missing string
}

// axisFromBaseline scores one value against one axis of the baseline. A nil baseline is not a
// degenerate one: it means no ruler was built for this pair, reported as immature with a reason.
func axisFromBaseline(b *derivatives.Baseline, nMin int, value *float64, missing string) NormalizedAxis {
	axis := NormalizedAxis{NMin: nMin, State: derivatives.BaselineImmature}
	if b == nil {
		axis.Reason = "no baseline has been computed for this (symbol, timeframe) yet"
		return axis
	}

	axis.N = b.N
	axis.SpanMs = b.SpanMs
	axis.State = b.State(nMin)
	if value == nil {
		axis.Reason = missing
		return axis
	}
	axis.Value = value

	z, state, ok := b.Z(*value, nMin)
	axis.State = state
	if !ok {
		// A score that was not produced must say why; otherwise state and n are present, z is
		// absent and reason is empty, which is indistinguishable from a healthy valueless axis.
		axis.Reason = notScorable(state, nMin, b.N)
		return axis
	}
	axis.Z = &z
	return axis
}

// notScorable names why the ruler refused the score. Immature and degenerate are different facts
// and only one improves with time.
func notScorable(state derivatives.BaselineState, nMin, n int) string {
	switch state {
	case derivatives.BaselineImmature:
		return fmt.Sprintf("the baseline holds %d of the %d observations a score needs", n, nMin)
	case derivatives.BaselineDegenerate:
		return "the baseline has no spread to divide by: half or more of its increments are identical"
	default:
		return "the baseline refused a score for this value"
	}
}

// scoredPair picks the two buckets a reading is taken over, newest-first: the last CLOSED
// bucket and the one stored before it. The newest bucket keeps being rewritten until its bar
// turns over, so scoring it produces a sawtooth in phase with the bar; both members of the pair
// have taken their final write. Closure is decided against the clock, not by position: when the
// poller stalls, the newest stored bucket is already closed and skipping it would discard a
// valid bar. It delegates to redisstore.BucketClosed so the ruler excludes the same bar.
func scoredPair(snapshots []*derivatives.DerivativesSnapshot, tfMs, nowMs int64) (newest, previous *derivatives.DerivativesSnapshot) {
	for _, snap := range snapshots {
		if snap == nil {
			continue
		}
		if newest == nil {
			if !redisstore.BucketClosed(snap.Timestamp, tfMs, nowMs) {
				continue // still open: its value can still change
			}
			newest = snap
			continue
		}
		previous = snap
		break
	}
	return newest, previous
}

// scoredBarTimestamp reports the bar the reading belongs to, so the caller can resolve
// the price increment on the SAME bar the other two axes are scored on. Zero when the
// series holds no closed bucket.
func scoredBarTimestamp(snapshots []*derivatives.DerivativesSnapshot, tfMs, nowMs int64) int64 {
	newest, _ := scoredPair(snapshots, tfMs, nowMs)
	if newest == nil {
		return 0
	}
	return newest.Timestamp
}

// scoredSeriesBar reports the bar a reading of this series is scored on: the newest closed
// bucket surviving the provenance filter. The price increment is resolved outside
// normalizedFromSeries and must land on the same bar as the other two axes. The filter runs on
// the same immutable input, so a second copy of the rule is what is avoided, not the second pass.
func scoredSeriesBar(snapshots []*derivatives.DerivativesSnapshot, tfMs, nowMs int64) int64 {
	return scoredBarTimestamp(singleProvenanceSeries(snapshots), tfMs, nowMs)
}

// The filter's two sentences name the cause that produced the absence — the provenance
// discard and the bar it took — instead of describing the state before it.
const (
	filterDiscardedThePredecessor = "the preceding bar was discarded by the provenance filter: a series stitched from more than one source has no predecessor an increment could be taken against"
	filterDiscardedEveryClosedBar = "every closed bar in the stored series was discarded by the provenance filter: a series stitched from more than one source has no bar a reading could be scored on"
)

// refuseScoresBehindTheReference refuses the score, never the data, of every axis whose scored
// bar sits behind the newest closed bar the warm series holds.
//
// The warm tier is a clock here, never a ruler: no value from it enters the score, only the
// answer to "up to which bar the market has closed". The decision is about the bar, not about
// n, so a young symbol with an immature ruler keeps its immaturity reason. Only axes that
// produced a score are refused; an axis that arrived without one keeps its own cause. `state`
// remains the truth of the ruler — z absent whenever the state is not mature is a one-way
// implication — and the refusal lives in the reason.
func refuseScoresBehindTheReference(reading NormalizedReading, referenceBar int64) NormalizedReading {
	if referenceBar <= 0 || reading.Timestamp <= 0 || reading.Timestamp >= referenceBar {
		return reading
	}
	reason := fmt.Sprintf(
		"the scored bar %s is behind the newest closed bar %s the warm series holds; the score is refused rather than served stale",
		barInstant(reading.Timestamp), barInstant(referenceBar))
	for _, axis := range []*NormalizedAxis{&reading.OpenInterest, &reading.LongShortRatio, &reading.Price} {
		if axis.Z == nil {
			continue
		}
		axis.Z = nil
		axis.Reason = reason
	}
	return reading
}

// barInstant renders a bar label for a human reading a reason, in UTC and RFC3339 because the
// label is a UTC bucket start.
func barInstant(tsMs int64) string {
	return time.UnixMilli(tsMs).UTC().Format(time.RFC3339)
}

// normalizedFromSeries computes the reading for the last closed bar of a newest-first series;
// which bar that is, is scoredPair's decision. Every increment spans exactly one bar of labels
// and is taken only between adjacent buckets, because bridging a hole would widen the window
// and produce a phantom reading. The price increment arrives already resolved from the candle
// series. referenceBar is the warm tier's newest closed bar; zero means no reference, and the
// seal is inert rather than refusing scores for lack of a clock.
func normalizedFromSeries(baseline *derivatives.DerivativesBaseline, snapshots []*derivatives.DerivativesSnapshot,
	tfMs, nowMs int64, price priceIncrement, referenceBar int64,
) NormalizedReading {
	// The ruler sees one provenance. See singleProvenanceSeries. The raw series is kept because
	// a reason must name what the filter took, not describe the post-filter state as the store.
	raw := snapshots
	snapshots = singleProvenanceSeries(raw)

	reading := NormalizedReading{}

	nMin := 0
	var oiBaseline, lsrBaseline, priceBaseline *derivatives.Baseline
	if baseline != nil {
		nMin = baseline.NMin
		reading.WindowN = baseline.WindowN
		oiBaseline = baseline.OpenInterest
		lsrBaseline = baseline.LongShortRatio
		priceBaseline = baseline.Price
	}

	newest, previous := scoredPair(snapshots, tfMs, nowMs)

	// What the filter took: scoredPair runs over the raw series a second time and the comparison
	// is between the two decisions, not point counts. Both calls are pure over the same input.
	rawNewest, rawPrevious := scoredPair(raw, tfMs, nowMs)
	filterTookTheScoredBar := newest == nil && rawNewest != nil
	filterTookThePredecessor := newest != nil && previous == nil && rawPrevious != nil

	// An empty series means nothing was written for this symbol; a series whose only bucket is
	// still open means the first bar has not closed yet, which resolves itself.
	const noRecord = "no derivatives record for this symbol"
	const noClosedBar = "the only stored bar is still open; a reading is taken on the last CLOSED bar"

	noBar := noRecord
	if newest == nil {
		for _, snap := range snapshots {
			if snap != nil {
				noBar = noClosedBar
				break
			}
		}
		if filterTookTheScoredBar {
			noBar = filterDiscardedEveryClosedBar
		}
	}

	var oiIncrement, lsrIncrement *float64
	oiMissing, lsrMissing := noBar, noBar
	priceMissing := price.missing
	if newest == nil && priceMissing == "" {
		priceMissing = noBar
	}

	if newest != nil {
		reading.Timestamp = newest.Timestamp

		// The axes share the adjacency and pairing gates but not their inputs: a bar can carry
		// one and not the other, so each names its own missing half.
		adjacent := previous != nil && tfMs > 0 && newest.Timestamp-previous.Timestamp == tfMs

		// The single-bar sentence is true only when the store holds one closed bar; the filter
		// case gets its own sentence.
		noPredecessor := "only one closed bar is available; an increment needs two"
		if filterTookThePredecessor {
			noPredecessor = filterDiscardedThePredecessor
		}

		switch {
		case newest.OpenInterestBase == nil:
			oiMissing = "the scored bar carries no open interest in contracts"
		case previous == nil:
			oiMissing = noPredecessor
		case previous.OpenInterestBase == nil:
			oiMissing = "the preceding bar carries no open interest in contracts"
		case !adjacent:
			oiMissing = "the scored bar and the one before it are not adjacent; an increment is never taken across a hole"
		default:
			if g, ok := derivatives.LogIncrement(*newest.OpenInterestBase, *previous.OpenInterestBase); ok {
				oiIncrement = &g
			} else {
				oiMissing = "open interest is not positive, so it has no logarithm"
			}
		}

		switch {
		case newest.LongShortRatio == nil:
			lsrMissing = "the scored bar carries no long/short ratio"
		case previous == nil:
			lsrMissing = noPredecessor
		case previous.LongShortRatio == nil:
			lsrMissing = "the preceding bar carries no long/short ratio"
		case !adjacent:
			lsrMissing = "the scored bar and the one before it are not adjacent; an increment is never taken across a hole"
		default:
			if l, ok := derivatives.LogIncrement(*newest.LongShortRatio, *previous.LongShortRatio); ok {
				lsrIncrement = &l
			} else {
				lsrMissing = "the long/short ratio is not positive, so it has no logarithm"
			}
		}
	}

	reading.OpenInterest = axisFromBaseline(oiBaseline, nMin, oiIncrement, oiMissing)
	reading.LongShortRatio = axisFromBaseline(lsrBaseline, nMin, lsrIncrement, lsrMissing)
	reading.Price = axisFromBaseline(priceBaseline, nMin, price.value, priceMissing)
	// The seal lives here because this is the only place that emits a score, and the three
	// axes share one bar.
	return refuseScoresBehindTheReference(reading, referenceBar)
}

// ── response envelopes ────────────────────────────────────────────────────

// DerivativesSeriesPoint keeps the nested shape the frontend already reads.
type DerivativesSeriesPoint struct {
	Timestamp       int64                            `json:"timestamp"`
	SourceTimestamp int64                            `json:"sourceTimestamp"`
	Snapshot        *derivatives.DerivativesSnapshot `json:"snapshot"`
}

// DerivativesSeriesResponse is GET /api/derivatives/{symbol}/series.
type DerivativesSeriesResponse struct {
	Status     string                   `json:"status"`
	Symbol     string                   `json:"symbol"`
	Exchange   string                   `json:"exchange"`
	MarketType string                   `json:"marketType"`
	Timeframe  string                   `json:"timeframe"`
	Count      int                      `json:"count"`
	Data       []DerivativesSeriesPoint `json:"data"`
	Funding    FundingDerivation        `json:"funding"`
	// Normalized is the scored reading of the newest bar. It is always present,
	// carrying its own state — an unscorable pair is sealed, not omitted.
	Normalized  NormalizedReading      `json:"normalized"`
	Measurement MeasurementDeclaration `json:"measurement"`
	// Coverage says how much of what was requested is here: "all there is" and "all you asked
	// for" are different facts.
	Coverage SeriesCoverage `json:"coverage"`
}

// SeriesCoverage compares the request with what was served. Partial is derived, so a boolean
// cannot diverge from the numbers beside it.
type SeriesCoverage struct {
	// Requested is how many buckets the request asked for.
	Requested int `json:"requested"`
	// Served is how many came back.
	Served int `json:"served"`
	// Partial is `Served < Requested`.
	Partial bool `json:"partial"`
	// Reason names what limited the result, when something did.
	Reason string `json:"reason,omitempty"`

	// Fields says, per measurement field, on how many served points the value exists. Partial
	// speaks of buckets, so it can be false while an entire axis is missing. A completely
	// absent field appears as zero rather than vanishing from the map: vanishing reads as "I
	// did not ask".
	Fields map[string]int `json:"fields,omitempty"`
}

// newSeriesCoverage builds the coverage declaration.
func newSeriesCoverage(requested, served int, points []DerivativesSeriesPoint) SeriesCoverage {
	cov := SeriesCoverage{
		Requested: requested,
		Served:    served,
		Partial:   served < requested,
		Fields:    fieldFillOf(points),
	}
	if cov.Partial {
		// The reason states what is known: the series ended before the request.
		cov.Reason = "the available series ends before the requested range"
	}
	return cov
}

// fieldFillOf counts, per measurement field, on how many points the value exists.
//
// The four keys are ALWAYS emitted, including with zero — see the `Fields` comment.
func fieldFillOf(points []DerivativesSeriesPoint) map[string]int {
	fill := map[string]int{
		"openInterestBase": 0,
		"fundingRate":      0,
		"fundingRate8h":    0,
		"longShortRatio":   0,
	}
	for _, p := range points {
		if p.Snapshot == nil {
			continue
		}
		if p.Snapshot.OpenInterestBase != nil {
			fill["openInterestBase"]++
		}
		if p.Snapshot.FundingRate != nil {
			fill["fundingRate"]++
		}
		if p.Snapshot.FundingRate8h != nil {
			fill["fundingRate8h"]++
		}
		if p.Snapshot.LongShortRatio != nil {
			fill["longShortRatio"]++
		}
	}
	return fill
}

// BatchDerivativesSeriesEntry extends the existing flat batch point with the decision
// quantities, additively by field. The embedded point keeps `openInterest` unchanged on the
// wire; migrating means reading `openInterestBase`.
type BatchDerivativesSeriesEntry struct {
	BatchDerivativesSeriesPoint

	// OpenInterestBase is the decision quantity, in contracts.
	OpenInterestBase *float64 `json:"openInterestBase,omitempty"`
	// FundingRate8h is present only when the interval has a declared rung.
	FundingRate8h *float64 `json:"fundingRate8h,omitempty"`
	// NStep travels per point because a backfilled bucket and a live bucket in
	// the same series can carry different provenance.
	NStep           derivatives.NDerivationStep `json:"nStep,omitempty"`
	FundingAtAnchor *bool                       `json:"fundingAtAnchor,omitempty"`
	// Source travels per point for the same reason NStep does, and one more: a series crossing
	// the ZSET cap is stitched from two providers. This point is flat, so what is not written
	// here does not reach the fleet-wide consumer.
	Source string `json:"source,omitempty"`
}

// BatchDerivativesSeriesEnvelope is GET /api/batch/derivatives-series.
type BatchDerivativesSeriesEnvelope struct {
	Timeframe string                                   `json:"timeframe"`
	Data      map[string][]BatchDerivativesSeriesEntry `json:"data"`
	// Coverage carries one declaration per symbol: the limit is per symbol, so a batch can have
	// twenty-nine complete and one truncated.
	Coverage map[string]SeriesCoverage `json:"coverage"`
	// Funding carries one derivation per symbol — the ladder rung is a property
	// of the symbol's contract, not of the batch.
	Funding map[string]FundingDerivation `json:"funding"`
	// Normalized carries one scored reading per symbol: the ruler is per (symbol, timeframe),
	// so reading belongs to the symbol and cannot be hoisted to the batch.
	Normalized  map[string]NormalizedReading `json:"normalized"`
	Measurement MeasurementDeclaration       `json:"measurement"`
}

// nStepEnumDeclaration publishes the rung vocabulary from the domain ladder. A literal would
// go stale as soon as a rung is added, making consumers reject values the system emits.
func nStepEnumDeclaration() string {
	rungs := derivatives.NDerivationSteps()
	names := make([]string, 0, len(rungs))
	for _, rung := range rungs {
		names = append(names, string(rung))
	}
	return "enum(" + strings.Join(names, "|") + ")"
}
