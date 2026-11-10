package collector

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
)

// ─────────────────────────────────────────────────────────────────────────────
// FUNDING NORMALIZATION — the three-rung ladder
//
// A raw funding rate is a rate per settlement interval, and 8h, 4h and 1h coexist, so raw
// numbers are not comparable across symbols. Binance publishes F = [P + clamp(I-P, ±0.05%)] /
// (8/N), so multiplying by 8/N is the exact inverse of the exchange's own division.
//
// /fapi/v1/fundingInfo is an exception table, not a complete register, and the interval changes
// over time, so the value is resolved through four rungs in order of evidence: measured
// spacing, metadata seed, inherited (known but observed after this bucket), and unavailable
// (emit nothing). The rung travels on the wire with the value.
// ─────────────────────────────────────────────────────────────────────────────

// fundingProfile is what the ladder resolves to for one symbol.
type fundingProfile struct {
	IntervalHours float64
	Step          derivatives.NDerivationStep
	// ObservedFromMs is the oldest instant the measurement covers. Zero means no window; a
	// bucket older than an existing window is inherited. See profileForBucket.
	ObservedFromMs int64
}

// measuredCadence is a settlement interval together with the span it was observed over; using
// either without the other is the defect.
type measuredCadence struct {
	Hours          float64
	ObservedFromMs int64
}

// fundingIntervalRevalidationInterval is the cadence at which a symbol's settlement interval is
// measured again. The exchange moves a capped contract to 1h and restores it after sixteen
// quiet periods (16 hours), so measuring every 6h puts at least two observations inside that
// span; measuring once per process puts zero, and a stale N scales the result by up to 8x.
const fundingIntervalRevalidationInterval = 6 * time.Hour

// fundingIntervalRevalidationLimit is how many recent settlements a revalidation measures.
// It is deliberately shallow: measureIntervalHours returns the mode, and a deep window keeps
// returning the superseded cadence while it dominates the sample.
const fundingIntervalRevalidationLimit = 16

// FundingProfileRegistry remembers the measured rung per contract between polls. A live poll
// sees one reading and no spacing, so bootstrap measures the series and deposits the result;
// the live path prefers it and re-measures on the cadence above.
//
// Entries are keyed by SymbolScope (exchange, marketType, symbol), never by ticker: a ticker
// key would hand one venue's cadence to another venue's contract. The registry is built once
// by the composition root and injected, because a poller-lifetime registry would lose every
// measurement on restart. The poll fans out, so reads are concurrent with bootstrap and
// revalidation writes.
type FundingProfileRegistry struct {
	mu      sync.RWMutex
	entries map[string]fundingMeasurement

	// nowMs is the clock. It is a field so a test can advance time without sleeping;
	// production leaves it nil and gets the wall clock.
	nowMs func() int64
}

// fundingMeasurement is what the registry holds per contract. attemptedAt stamps the attempt,
// not the success, so a symbol whose history yields no gap is not re-fetched every poll.
type fundingMeasurement struct {
	// hours is the measured interval. Zero means the measurement was attempted and
	// produced nothing, which is NOT the same as a symbol never looked at.
	hours float64
	// observedFromMs is the oldest settlement instant the measurement was taken over.
	observedFromMs int64
	attemptedAt    int64
}

// NewFundingProfileRegistry builds the registry the whole process shares.
func NewFundingProfileRegistry() *FundingProfileRegistry {
	return &FundingProfileRegistry{entries: make(map[string]fundingMeasurement)}
}

func (c *FundingProfileRegistry) now() int64 {
	if c.nowMs != nil {
		return c.nowMs()
	}
	return time.Now().UnixMilli()
}

// setMeasured records an interval observed from actual settlement spacing, and stamps
// the attempt that produced it.
func (c *FundingProfileRegistry) setMeasured(metrics Metrics, scope SymbolScope, cadence measuredCadence) {
	// The two early exits are counted separately: "nil receiver" and "non-positive cadence" are
	// different defects and fusing them hides which one fired.
	if c == nil {
		recordFundingIntervalMeasurement(metrics, scope.ExchangeID, "deposit_nil_registry")
		return
	}
	if cadence.Hours <= 0 {
		recordFundingIntervalMeasurement(metrics, scope.ExchangeID, "deposit_zero_hours")
		return
	}
	c.store(metrics, scope.Key(), cadence)
}

// markAttempted records that the spacing was looked for and not found, so the next
// look happens one cadence from now rather than on the next poll.
func (c *FundingProfileRegistry) markAttempted(scope SymbolScope) {
	if c == nil {
		return
	}
	key := scope.Key()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]fundingMeasurement)
	}
	// The measured value survives a failed re-measurement; dropping it on a network blip would
	// demote the symbol to the declared rung.
	entry := c.entries[key]
	entry.attemptedAt = c.now()
	c.entries[key] = entry
}

func (c *FundingProfileRegistry) store(metrics Metrics, key string, cadence measuredCadence) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]fundingMeasurement)
	}
	c.entries[key] = fundingMeasurement{
		hours:          cadence.Hours,
		observedFromMs: cadence.ObservedFromMs,
		attemptedAt:    c.now(),
	}
	recordFundingProfileRegistrySize(metrics, len(c.entries))
}

// measuredFor returns the measured interval for a contract, together with the span it
// was observed over, if one was ever observed.
func (c *FundingProfileRegistry) measuredFor(scope SymbolScope) (measuredCadence, bool) {
	if c == nil {
		return measuredCadence{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[scope.Key()]
	if !ok || entry.hours <= 0 {
		return measuredCadence{}, false
	}
	return measuredCadence{Hours: entry.hours, ObservedFromMs: entry.observedFromMs}, true
}

// revalidationDue reports whether this symbol's interval is past the declared cadence. A symbol
// nobody has looked at is due, which covers a listing that appeared after bootstrap.
func (c *FundingProfileRegistry) revalidationDue(scope SymbolScope) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	entry, ok := c.entries[scope.Key()]
	c.mu.RUnlock()
	if !ok {
		return true
	}
	return c.now()-entry.attemptedAt >= fundingIntervalRevalidationInterval.Milliseconds()
}

// resolveFundingInterval walks the ladder and reports both the interval and the rung
// that produced it.
func resolveFundingInterval(measured measuredCadence, hasMeasured bool, declared *string) fundingProfile {
	if hasMeasured && measured.Hours > 0 {
		return fundingProfile{
			IntervalHours:  measured.Hours,
			Step:           derivatives.NFromMeasuredSpacing,
			ObservedFromMs: measured.ObservedFromMs,
		}
	}
	if declared != nil {
		if hours, ok := parseIntervalHours(*declared); ok {
			return fundingProfile{IntervalHours: hours, Step: derivatives.NFromMetadataSeed}
		}
	}
	return fundingProfile{Step: derivatives.NUnavailable}
}

// profileForBucket narrows a symbol-level profile to one bucket. A bucket older than the oldest
// settlement the measurement covers is outside its span: the interval still travels, but the
// rung stops claiming "measured" and no 8h-equivalent is derived. This matters because seeding
// writes up to 2,000 retroactive buckets while the measurement covers recent settlements. A
// profile with no window, or a bucket with no timestamp, is returned untouched.
func profileForBucket(profile fundingProfile, bucketMs int64) fundingProfile {
	if profile.Step != derivatives.NFromMeasuredSpacing {
		return profile
	}
	if profile.ObservedFromMs <= 0 || bucketMs <= 0 || bucketMs >= profile.ObservedFromMs {
		return profile
	}
	profile.Step = derivatives.NInheritedOutsideWindow
	return profile
}

// parseIntervalHours reads the exchange's interval notation ("8h", "4h", "1h").
func parseIntervalHours(s string) (float64, bool) {
	trimmed := strings.TrimSpace(strings.ToLower(s))
	if trimmed == "" {
		return 0, false
	}
	if !strings.HasSuffix(trimmed, "h") {
		return 0, false
	}
	hours, err := strconv.ParseFloat(strings.TrimSuffix(trimmed, "h"), 64)
	if err != nil || hours <= 0 {
		return 0, false
	}
	return hours, true
}

// fundingSettlementTimestamps extracts the settlement instants from a funding history,
// which is what the measured rung of the ladder is derived from.
//
// Rows without a timestamp are skipped rather than defaulted: a fabricated instant
// would produce a fabricated gap, and the gap is the whole measurement.
func fundingSettlementTimestamps(history []derivatives.FundingRateHistoryPoint) []int64 {
	if len(history) == 0 {
		return nil
	}
	out := make([]int64, 0, len(history))
	for _, row := range history {
		if row.Timestamp == nil {
			continue
		}
		out = append(out, *row.Timestamp)
	}
	return out
}

// measuredIntervalTolerance bounds how far an observed gap may sit from a candidate
// interval and still count as that interval. Settlements land on a schedule but the
// recorded timestamps jitter, so exact equality would reject nearly every real gap.
const measuredIntervalTolerance = 0.25 // hours

// measureIntervalHours derives the settlement interval from observed spacing, the only rung
// that carries evidence. It returns the mode of the gaps: an interval change leaves a mixture,
// and the mean would be a value no contract ever used. A single settlement yields no gap.
func measureIntervalHours(timestampsMs []int64) (float64, bool) {
	if len(timestampsMs) < 2 {
		return 0, false
	}

	// Sorted ascending is the shape the exchange returns; tolerate either direction
	// by taking the absolute gap.
	buckets := make(map[float64]int)
	for i := 1; i < len(timestampsMs); i++ {
		deltaMs := timestampsMs[i] - timestampsMs[i-1]
		if deltaMs < 0 {
			deltaMs = -deltaMs
		}
		if deltaMs == 0 {
			continue
		}
		hours := float64(deltaMs) / (1000 * 60 * 60)
		snapped, known := snapToKnownInterval(hours)
		if !known {
			// A gap matching no venue cadence is not a cadence: counting it lets missing
			// settlements win the mode and fabricate an N under the "measured" rung.
			continue
		}
		buckets[snapped]++
	}

	var bestHours float64
	var bestCount int
	for hours, count := range buckets {
		// Ties resolve toward the shorter interval: under-normalizing a fast-settling
		// symbol understates its cost, which is the safer direction than fabricating
		// an extreme on a slow one.
		if count > bestCount || (count == bestCount && hours < bestHours) {
			bestHours, bestCount = hours, count
		}
	}
	if bestCount == 0 {
		return 0, false
	}
	return bestHours, true
}

// knownIntervals are the settlement cadences the venue actually uses.
func knownIntervals() []float64 {
	return []float64{1, 2, 4, 8}
}

// snapToKnownInterval snaps an observed gap to a venue cadence and reports when it matches none.
// Returning the gap untouched would let a gap no contract settled on become the mode.
func snapToKnownInterval(hours float64) (float64, bool) {
	for _, candidate := range knownIntervals() {
		if abs(hours-candidate) <= measuredIntervalTolerance {
			return candidate, true
		}
	}
	return hours, false
}

// observationStartMs is the oldest instant a set of settlements covers — the start of
// the window in which an interval measured from them was in force.
func observationStartMs(timestampsMs []int64) int64 {
	var oldest int64
	for _, ts := range timestampsMs {
		if ts <= 0 {
			continue
		}
		if oldest == 0 || ts < oldest {
			oldest = ts
		}
	}
	return oldest
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// applyFundingNormalization fills the funding fields of a snapshot from a raw reading. It emits
// an 8h-equivalent only when the ladder produced an interval; otherwise the raw rate travels
// and the step says so. The censorship flag comes from the published identity F = P + (I-P) = I
// inside the clamp dead band, because a pinned value is not a measurement.
func applyFundingNormalization(
	snap *derivatives.DerivativesSnapshot,
	rate *float64,
	interestRate *float64,
	profile fundingProfile,
) {
	if snap == nil {
		return
	}

	snap.NStep = profile.Step
	if profile.IntervalHours > 0 {
		hours := profile.IntervalHours
		snap.FundingIntervalHours = &hours
	}

	if interestRate != nil {
		ir := *interestRate
		snap.InterestRate = &ir
	}

	// The rung decides, and it decides for THIS bucket: inherited and unavailable both
	// carry no evidence here, so neither produces an 8h-equivalent nor an anchor verdict.
	if rate == nil || !profile.Step.Normalizes() {
		return
	}

	if normalized, ok := derivatives.NormalizeFunding8h(*rate, profile.IntervalHours); ok {
		snap.FundingRate8h = &normalized
	}

	if interestRate != nil {
		if atAnchor, ok := derivatives.IsFundingAtAnchor(*rate, *interestRate, profile.IntervalHours); ok {
			flag := atAnchor
			snap.FundingAtAnchor = &flag
		}
	}
}
