package derivatives

// NDerivationStep names which rung of the derivation ladder produced the funding
// settlement interval for a symbol.
//
// The ladder is measured spacing > metadata seed > inherited > nothing, and the step is
// part of the wire contract rather than an internal detail: `/fapi/v1/fundingInfo` is an
// EXCEPTION TABLE by construction, not a complete register, so a consumer that cannot
// tell a measured interval from a seeded one cannot tell a trustworthy 8h-equivalent
// from a guessed one.
//
// # THE RUNG IS ABOUT ONE BUCKET, NOT ABOUT THE SYMBOL
//
// The first three rungs answer "what is this contract's cadence today". The fourth
// answers a different question, and it is the one a SEEDED series asks: does today's
// cadence describe THIS bucket. A seeding run writes thousands of retroactive buckets —
// in 1d, back to 2021 — while the measurement covers the last sixteen settlements, and
// the venue changes a contract's cadence by rule. Stamping a 2021 bucket "measured"
// because today is 8h claims evidence for an instant nobody looked at, and the error is
// the ratio between the two cadences: 2× or 8×, on the field a cost comparison reads.
type NDerivationStep string

const (
	// NFromMeasuredSpacing — the interval came from the observed spacing of recent
	// settlements. This is the only rung that carries evidence.
	NFromMeasuredSpacing NDerivationStep = "measured"
	// NFromMetadataSeed — the interval came from exchange metadata and has not been
	// confirmed against observed settlements yet.
	NFromMetadataSeed NDerivationStep = "seed"
	// NInheritedOutsideWindow — an interval is known for the contract, but it was
	// observed AFTER this bucket, so it is inherited rather than evidenced here. The
	// interval travels (it is the best thing known about the contract) and the
	// 8h-equivalent does NOT: normalizing with a cadence that may not have been in
	// force is the fabrication the rung exists to name.
	NInheritedOutsideWindow NDerivationStep = "inherited"
	// NUnavailable — neither rung produced an interval. Funding is NOT normalized and
	// no 8h-equivalent is emitted; see the measurement-integrity spec.
	NUnavailable NDerivationStep = "unavailable"
)

// NDerivationSteps returns the WHOLE ladder, in ladder order.
//
// It exists so that whoever PUBLISHES the rung vocabulary — the envelope's field
// declaration, an integrity query looking for an incoherent row — reads it from here
// instead of retyping it. A rung added to the ladder reaches those places on its own; a
// hand-copied enum would reject a value the system emits.
func NDerivationSteps() []NDerivationStep {
	return []NDerivationStep{
		NFromMeasuredSpacing,
		NFromMetadataSeed,
		NInheritedOutsideWindow,
		NUnavailable,
	}
}

// Normalizes reports whether this rung may carry an 8h-equivalent.
//
// It is the precondition for emitting one and for serving one, and it lives here so the
// two ends cannot drift: a rung added on one side and forgotten on the other is how a
// value with no evidence behind it reaches a cost comparison. The zero value — an empty
// step, which is what a record written before the ladder existed carries — normalizes
// nothing, by the same rule.
func (s NDerivationStep) Normalizes() bool {
	return s == NFromMeasuredSpacing || s == NFromMetadataSeed
}

// The derivatives provenances of this system.
//
// They are PROVIDER names, not venue names, because the provider decides the unit, the label
// convention and retention — the three things that cannot be mixed on the same ruler.
const (
	// SourceVenue — the number came from the exchange's own API. It is the provenance of
	// everything this system ingested before a second one existed, and therefore what a point
	// WITHOUT a declaration means: the legacy key family has a single writer.
	SourceVenue = "venue"
	// SourceCoinalyze — the number came from the aggregator, whose history depth is why it
	// exists here.
	SourceCoinalyze = "coinalyze"
)

// DerivativesSnapshot is a point-in-time derivatives reading for one symbol.
// The JSON shape is 1:1 with the analyzer's TypeScript DerivativesSnapshot.
//
// # THE UNIT RULE
//
// OpenInterestBase (contracts) is the DECISION quantity. OpenInterest (notional) is
// kept under its existing name for display and is NEVER an input to a threshold, a
// score or a ranking.
//
// The reason is an identity, not a preference: notional is contracts × price, so
//
//	Δln(notional) ≡ Δln(contracts) + Δln(price)
//
// and the revaluation term — where nobody traded and only the price moved —
// correlates +0.982 with the candle return and carries 51-76% of the variance. A
// panel ordered by notional change is a panel ordered by price change wearing another
// name.
//
// # WHY BOTH FIELDS COEXIST IN THE SAME MEMBER
//
// The migration is additive by FIELD, not by key. Nothing existing is reinterpreted,
// so the rollback is "stop reading the new field" rather than a restore.
type DerivativesSnapshot struct {
	Exchange   string `json:"exchange"`
	MarketType string `json:"marketType"`
	Symbol     string `json:"symbol"`
	Timestamp  int64  `json:"timestamp"`
	ObservedAt int64  `json:"observedAt,omitempty"`

	// Source is WHO MEASURED this point.
	//
	// It is not the exchange: `Exchange` says which market the number speaks of, and Source
	// says through which door it entered. The two diverge in the case motivating the field — a
	// Coinalyze point about a Binance perpetual has `exchange: binance` and `source:
	// coinalyze`, and without the second term it is indistinguishable from a point that came
	// from the venue itself.
	//
	// The distinction is not bibliographic: the MAD baseline is a ruler, and a ruler built over
	// points from two providers measures the distance BETWEEN them and calls it market
	// variance. The `zOpenInterest` derived from it is the decision axis of the scoring jury.
	Source string `json:"source,omitempty"`

	// --- open interest -------------------------------------------------------

	// OpenInterestBase is open interest in BASE UNITS (contracts). Decision input.
	OpenInterestBase *float64 `json:"openInterestBase,omitempty"`
	// OpenInterestObservedAtMs is when THIS open-interest reading was taken, which is
	// not the same instant as `Timestamp`.
	//
	// WHY THE TWO DIVERGE, and why the difference is a live defect and not pedantry.
	// `Timestamp` is stamped on every poll whether or not the open-interest fetch
	// succeeded — the fetch error is logged and the cycle continues, because a funding
	// failure must not take open interest down with it, and vice versa. The merge then
	// advances `Timestamp` to the newer of the two records while PRESERVING the
	// open-interest fields the incoming poll did not carry. After one failed fetch the
	// stored record therefore holds an open-interest reading from t0 under a timestamp
	// of t1, and the next successful poll would measure its window from t1 — reporting
	// a span SHORTER than the one it actually covered, which makes the move look faster
	// than it was. That is the same "percentage over a window nobody measured" this
	// field group exists to abolish, arriving one door further in.
	//
	// It moves as part of the OPEN-INTEREST GROUP — same discipline the funding group
	// already follows — so a reading and the instant it was taken can never separate.
	OpenInterestObservedAtMs *int64 `json:"openInterestObservedAtMs,omitempty"`
	// OpenInterest is the notional value. DISPLAY ONLY — never a decision input.
	OpenInterest *float64 `json:"openInterest,omitempty"`
	// OIChangePct is the legacy poll-to-poll delta. Superseded by the normalized
	// increment; kept while consumers migrate.
	//
	// IT LIVES ON THE `latest` KEY ONLY. The series write path strips it, because a
	// poll-to-poll number stamped into a timeframe bucket claims a window it was
	// never measured over — see redisstore.dropPollToPollOIChange.
	OIChangePct *float64 `json:"oiChangePct,omitempty"`
	// OIChangePctWindowMs is the SPAN the delta above was actually taken over: the
	// elapsed time between the two polls that produced it, measured, not assumed.
	//
	// WHY IT IS MEASURED AND NOT DERIVED FROM THE CONFIGURED INTERVAL. The two are not
	// the same number. The poll loop is a ticker whose work can outlast its period, and
	// when it does the Go runtime drops ticks rather than queueing them, so the real
	// spacing lands on a MULTIPLE of the configured interval. Measured in production:
	// `poll_interval_sec: 300` with a sweep spanning 562s across a third of the fleet
	// produced gaps of exactly 300s and 600s and nothing in between — the signature of a
	// ticker being lapped. Printing "5 min" beside a delta that covers ten would be a
	// fabricated horizon on a measured value.
	//
	// The cycle is PACED to fit inside its period (polling.admissionInterval), so that
	// multiple should be 1 in steady state. That does not retire this field: the pacing is
	// sized against a measured per-symbol cost, an overrun is still possible and is reported
	// when it happens, and a window that is asserted rather than measured is exactly the claim
	// this field exists to avoid making.
	//
	// Absent when there was no previous reading to difference against, which is the
	// same condition under which OIChangePct itself is absent. The two travel together
	// or not at all: a percentage whose window is unknown is what the requirement
	// "a percentage of open-interest change SHALL NOT be rendered without the window it
	// covers" exists to forbid.
	OIChangePctWindowMs *int64 `json:"oiChangePctWindowMs,omitempty"`

	// --- funding -------------------------------------------------------------

	// FundingRate is the raw per-interval rate as published.
	FundingRate     *float64 `json:"fundingRate,omitempty"`
	NextFundingRate *float64 `json:"nextFundingRate,omitempty"`
	// FundingRate8h is the rate normalized to an 8-hour equivalent. Emitted only
	// when the interval is known; see NStep.
	FundingRate8h *float64 `json:"fundingRate8h,omitempty"`
	// FundingIntervalHours is N — the settlement interval actually used.
	FundingIntervalHours *float64 `json:"fundingIntervalHours,omitempty"`
	// NStep declares WHICH rung produced FundingIntervalHours.
	NStep NDerivationStep `json:"nStep,omitempty"`
	// InterestRate is the contract's own interest rate, per 8 hours. It is NOT a
	// constant: 158 of 730 TRADING symbols carry zero, BNBUSDT among them.
	InterestRate *float64 `json:"interestRate,omitempty"`
	// FundingAtAnchor reports that funding is pinned at the clamp dead band rather
	// than being a free reading. Derived from the published identity
	// F = P + (I - P) = I, so it needs no additional feed.
	FundingAtAnchor *bool `json:"fundingAtAnchor,omitempty"`

	// --- long / short --------------------------------------------------------

	LongShortRatio *float64 `json:"longShortRatio,omitempty"`
	LSLongPct      *float64 `json:"lsLongPct,omitempty"`
	LSShortPct     *float64 `json:"lsShortPct,omitempty"`

	// --- per-field provenance -------------------------------------------------

	// FieldSources declares, PER FIELD, which provenance the value of that field came from.
	//
	// # WHY `Source` ALONE IS NOT ENOUGH
	//
	// A point can be STITCHED from two tiers: the deep-window read overlays the hot tier on the
	// durable one field by field, and the result comes out with ONE `Source` — the hot one's,
	// because it was the base of the copy. MEASURED ON THE LAN: 156 buckets in which the ZSET
	// has the bucket and does NOT have `openInterestBase`, while the durable tier has both. The
	// delivered point claims the hot source over a number the cold source measured.
	//
	// This is not cosmetic: the baseline ruler is a median and a robust scale over the series'
	// INCREMENTS, and two providers measuring the same market do not return the same number. A
	// ruler built over the seam measures the distance between providers and presents it as
	// market variance — and the `zOpenInterest` derived from it is the scoring jury's decision
	// axis.
	//
	// # WHEN IT IS FILLED
	//
	// ONLY when the point is in fact MIXED. A single-provenance point already declares itself
	// through `Source`, and repeating the same string per field on each of a series' 2,000
	// points would be wire weight without information. The key is the field's JSON name, the
	// same one the route returns.
	FieldSources map[string]string `json:"fieldSources,omitempty"`
}

// HasMixedProvenance reports whether any field declares a provenance different from the point's.
//
// It is the predicate the baseline ruler consumes to REFUSE the point: a mixed point is not a
// bad reading, it is two stitched readings, and the distance between them is not market variance.
func (s *DerivativesSnapshot) HasMixedProvenance() bool {
	if s == nil {
		return false
	}
	for _, source := range s.FieldSources {
		if source != s.Source {
			return true
		}
	}
	return false
}

// FundingAnchor returns the value at which the clamp pins funding for a contract
// whose interest rate is interestRate (expressed per 8 hours) settling every
// nHours hours.
//
// The dead band anchors funding AT THE INTEREST RATE, scaled by the interval — not
// at zero. Measured across 14,762 settlement rows with no exception: 8h contracts
// pin at 1.0e-4, 4h at 5.0e-5, 1h at 1.25e-5.
//
// ok is false when nHours is not usable, so callers never divide by zero.
func FundingAnchor(interestRate, nHours float64) (anchor float64, ok bool) {
	if nHours <= 0 {
		return 0, false
	}
	return interestRate * nHours / 8.0, true
}

// NormalizeFunding8h converts a raw per-interval funding rate into its 8-hour
// equivalent.
//
// This is the exact inverse of a division the exchange itself applies: Binance
// publishes F = [P + clamp(I - P, ±0.05%)] / (8/N). Normalizing is undoing that
// division so two symbols on different settlement cadences become comparable — the
// same displayed 0.01% costs 0.03%/day at 8h and 0.24%/day at 1h.
//
// ok is false when nHours is not usable; callers MUST NOT emit an 8h-equivalent then.
func NormalizeFunding8h(rate, nHours float64) (normalized float64, ok bool) {
	if nHours <= 0 {
		return 0, false
	}
	return rate * 8.0 / nHours, true
}

// fundingAnchorEpsilon bounds the float comparison against the anchor.
//
// The published anchors are exact decimal constants (1e-4, 5e-5, 1.25e-5) that do not
// round-trip exactly through float64, so equality has to be approximate. The bound is
// four orders of magnitude below the smallest anchor: tight enough that a genuinely
// free reading never matches, loose enough to absorb representation error.
const fundingAnchorEpsilon = 1e-9

// IsFundingAtAnchor reports whether a raw funding rate sits at its contract's anchor,
// meaning the clamp is binding and the number is censored rather than measured.
//
// A pinned value must not be presented as a free reading with four decimals of
// implied precision — for a large share of the universe that column carries no
// information at all while the underlying premium keeps moving.
func IsFundingAtAnchor(rate, interestRate, nHours float64) (atAnchor, ok bool) {
	anchor, ok := FundingAnchor(interestRate, nHours)
	if !ok {
		return false, false
	}
	return absFloat(rate-anchor) <= fundingAnchorEpsilon, true
}

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// LabelConvention says whether a feed's timestamp names the START or the END of the
// interval it describes.
//
// # WHY THIS EXISTS
//
// Bucketing a derivatives point means answering "which interval is this?". A feed that
// labels the END of an interval and a feed that labels its START describe the SAME
// window with timestamps one bucket apart, so aligning both by the same arithmetic puts
// them in different buckets — funding from one source and open interest from the other
// stop describing the same minute, and nothing errors. The reading is simply wrong, and
// wrong quietly, which is the failure mode a chart cannot show.
//
// The archived alignment spec of this project already ruled on it: adopting a
// start-labeled feed "SHALL be blocked until alignment derives the bucket from that
// declaration". This type is that declaration.
//
// # UNSPECIFIED IS NOT A DEFAULT
//
// The zero value is invalid ON PURPOSE. A convention that defaults would be a guess
// wearing a type, and the guess is right for exactly the feeds that already exist —
// which is precisely the set that would never exercise it. A new feed that forgets to
// declare must be REFUSED at ingestion, not silently aligned as if it were the old one.
type LabelConvention uint8

const (
	// LabelUnspecified is the zero value, and it is invalid. See the type's doc.
	LabelUnspecified LabelConvention = iota
	// LabelIntervalStart — the timestamp names the first instant of the interval.
	LabelIntervalStart
	// LabelIntervalEnd — the timestamp names the instant the interval closed.
	//
	// THE 00:00Z OBSERVATION DOES NOT DISCRIMINATE start from end: a POINT-SAMPLE series produces
	// the SAME observation under both conventions, because the value belongs to the label
	// instant. The basis of each feed's declaration is `polling.derivativesLabelDeclarations`:
	// field nature + evidence, endpoint by endpoint. For a point sample and for an event the
	// falsifier separating the two conventions does NOT EXIST — it exists only for an ADDITIVE
	// field, and the only measured additive feed is not ingested by this collector. Declaring is
	// a modeling choice, not a measurement conclusion.
	//
	// THE CONSEQUENCE is per LAYER:
	//
	//   · DURABLE TIER: the label is CONSUMED. `coldPointOf`
	//     (`engine/derivatives_cold_store.go`) aligns `Timestamp` by `batch.Label` and writes
	//     `LabelConvention` into the row, so declaring `LabelIntervalEnd` MOVES the stored bucket
	//     one period — and moving an already-written series is not reversible.
	//   · BINANCE/REDIS LAYER: the declaration is INERT for alignment. `upsertSeries` and
	//     `upsertSeriesBatch` pin `LabelIntervalStart` on write, and only admission reads the
	//     declaration table. There it shifts nothing.
	//
	// The residual of this asymmetry is declared in `storage/redis_derivatives.go` and in the
	// declaration table (`polling/derivatives_poller.go`).
	LabelIntervalEnd
)

// Valid reports whether the convention was actually declared.
func (c LabelConvention) Valid() bool {
	return c == LabelIntervalStart || c == LabelIntervalEnd
}

// String names the convention for a log line. An undeclared feed is named as such
// rather than printed as a number, because the log is what a maintainer reads when
// ingestion refuses.
func (c LabelConvention) String() string {
	switch c {
	case LabelIntervalStart:
		return "interval-start"
	case LabelIntervalEnd:
		return "interval-end"
	default:
		return "unspecified"
	}
}
