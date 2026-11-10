package collector

import (
	"errors"
	"fmt"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

// DURABLE COVERAGE PER TIMEFRAME — the intersection, not the list.
//
// # THE DEFECT THIS FILE REMOVES
//
// Durable-tier coverage must not be a literal of three strings written into the wiring. A
// literal cannot answer two questions that change over time: which timeframes the product
// requests, and which ones the durable source can serve. It answers a THIRD — which ones
// someone typed once — and the three coincide only while nobody touches anything.
//
// The two times they stopped coinciding are measured in this repository:
//
//  1. `1w` was in the literal until the first LAN deployment, where collection started
//     logging one WARN per cycle, forever — the provider's accepted list ends at `daily`.
//     Each half had a test: the list was asserted, the interval map was asserted, and nobody
//     asserted that one fits the other.
//  2. `30m` was in the product registry, outside the literal, and lost 254 hours of series
//     when the cap reached the only copy.
//
// # THE RULE, AND WHY IT REQUIRES ALL THREE QUANTITIES
//
// A timeframe enters durable coverage when the durable source serves it for ALL THREE
// quantities. Serving two would produce a row with a permanent hole in one column — erosion
// protection carries it forward forever and the fill checker reports it every cycle, with no
// fix available: the source simply does not have the data.
//
// `periodUndeclared` does NOT enter, and the asymmetry is deliberate: "we never asked" is not a
// license to ask in production. A timeframe no source serves for any quantity is refused LOUDLY
// at startup, not silently here.

// DurableTimeframes returns, in the order the product registers them, the timeframes with a
// durable tier.
//
// `registered` is `candle_engine.timeframes` — the product registry — and is the only parameter
// because the other half of the intersection is a property of the binary, not of the
// configuration: who serves what is provider measurement, declared in `sourcePeriodSupport`.
func DurableTimeframes(registered []string) []string {
	out := make([]string, 0, len(registered))
	for _, timeframe := range registered {
		if durableSourceServes(timeframe) {
			out = append(out, timeframe)
		}
	}
	return out
}

// durableSourceServes says whether the durable-tier source serves this timeframe for all three
// quantities. It asks the MATRIX, and the row of the source that actually writes the durable tier
// — the venue row would answer `available` for funding at any bucket and return coverage nobody
// writes.
func durableSourceServes(timeframe string) bool {
	for _, feed := range declaredFeeds() {
		if supportForSource(sourceCoinalyze, feed, timeframe) != periodAvailable {
			return false
		}
	}
	return true
}

// ─────────────────────────────────────────────────────────────────────────────
// THE DECLARED ABSENCE.
//
// # ABSENCE OF DURABILITY IS NOT ABSENCE OF DATA
//
// This is the distinction the previous requirement wording erased, and the price is MEASURED:
//
//	`1w` in QuestDB ............. 0 rows
//	`1w` through the route ...... 30 weekly points, 2026-02-02 to 2026-08-24
//	                              27 of them with `fundingRate8h` (90%)
//
// Both measurements are true at the same time. The existing source writes ALL timeframes into
// the ZSET, and the venue settlement takes no period parameter — so the weekly timeframe has
// data. What it lacks is a durable tier, because Coinalyze is its only writer and does not
// publish a weekly interval.
//
// Satisfying "declare the absence" by making the route stop serving `1w` would erase 200 days
// of weekly funding with the gate green. Therefore the declaration is ADDITIVE: it names what is
// missing and does not touch what exists.
//
// # WHY A TYPED VALUE, NOT A STRING
//
// The absence is per `(source, quantity, timeframe)` — three coordinates — and whoever receives
// it must be able to ask about any of them. A ready-made sentence forces the consumer to parse
// text to know which quantity is meant, which is how a reason becomes decoration.
// ─────────────────────────────────────────────────────────────────────────────

// ErrNoDurableTier is the class sentinel: the durable-tier source does not serve this cell.
//
// It is the CLASS, never the reason. The reason varies per cell and travels in DurableAbsenceError.
var ErrNoDurableTier = errors.New("polling: cell without a durable tier")

// DurableAbsenceError declares ONE cell missing from the durable tier, with the three coordinates
// and the reason.
type DurableAbsenceError struct {
	// Source is the source that would make the cell durable, not the one serving the hot tier.
	Source string `json:"source"`
	// Feed is the quantity: open interest, funding rate or long/short ratio.
	Feed string `json:"feed"`
	// Timeframe is the requested bucket.
	Timeframe string `json:"timeframe"`
	// Reason says WHY, and distinguishes "we know it is not served" from "we never asked".
	Reason string `json:"reason"`
}

func (a DurableAbsenceError) Error() string {
	return fmt.Sprintf("%s/%s em %s: %s", a.Source, a.Feed, a.Timeframe, a.Reason)
}

// Unwrap puts the cell in the sentinel's chain, so `errors.Is(err, ErrNoDurableTier)` answers
// about any absence without the caller knowing the coordinates.
func (a DurableAbsenceError) Unwrap() error { return ErrNoDurableTier }

const (
	reasonUnavailable = "the durable-tier source does not publish this interval — the API's accepted list ends at `daily`, and a weekly request answers 400"
	reasonUndeclared  = "nobody measured this interval against the durable-tier source — this is absence of measurement, not a declared absence of support"
)

// DurableAbsences declares, in the quantities' publication order, the cells of this timeframe
// the durable tier does not cover. An EMPTY slice means complete coverage.
//
// NO SYNTHETIC POINT IS PRODUCED HERE: aggregating `1d` into `1w` would blind the
// single-provenance ruler, and neither open interest — sampled at the bucket's OPEN — nor
// long/short ratio — which is a ratio, and a mean of ratios is not a ratio of aggregates —
// admits the algebra aggregation would require. The weekly anchor is Monday 00:00 UTC by a
// three-day offset, which does not come from a division.
func DurableAbsences(timeframe string) []DurableAbsenceError {
	var out []DurableAbsenceError
	for _, feed := range declaredFeeds() {
		support := supportForSource(sourceCoinalyze, feed, timeframe)
		if support == periodAvailable {
			continue
		}
		reason := reasonUnavailable
		if support == periodUndeclared {
			reason = reasonUndeclared
		}
		out = append(out, DurableAbsenceError{
			Source:    string(sourceCoinalyze),
			Feed:      string(feed),
			Timeframe: timeframe,
			Reason:    reason,
		})
	}
	return out
}

// TimeframeSupportRows declares the support matrix for the startup gate: one row per
// `(source, quantity)`, answered against the product's timeframe registry.
//
// # WHY IT IS BUILT HERE AND TRAVELS AS DATA
//
// The gate lives in `internal/platform/config`, and `internal/derivatives/collector` imports
// `internal/platform/config` — the reverse dependency does not exist and must not. So the matrix
// crosses the boundary as a VALUE: whoever knows which source serves which interval is whoever
// collects it, and the gate compares two sets it received ready-made.
//
// The answer is given AGAINST THE REGISTRY, not over the provider's whole vocabulary, for the
// same reason as `publishFeedAvailability`: a cell the product never requests is not an
// operational fact. And all three answers survive — served, unserved, and never measured, the
// last by ABSENCE from both sets.
func TimeframeSupportRows(registered []string) []config.TimeframeSupportRow {
	rows := make([]config.TimeframeSupportRow, 0, len(declaredSources())*len(declaredFeeds()))
	for _, source := range declaredSources() {
		for _, feed := range declaredFeeds() {
			row := config.TimeframeSupportRow{Source: string(source), Feed: string(feed)}
			for _, timeframe := range registered {
				switch supportForSource(source, feed, timeframe) {
				case periodAvailable:
					row.Served = append(row.Served, timeframe)
				case periodUnavailable:
					row.Unserved = append(row.Unserved, timeframe)
				default:
					// In neither set — this is how "never measured" reaches the gate without
					// becoming a refusal.
				}
			}
			rows = append(rows, row)
		}
	}
	return rows
}

// DurableSeedHorizon returns the depth durable-tier seeding reaches ON PURPOSE — the boundary no
// retention may cross.
//
// It derives from two MEASURED quantities, and from no typed number: the provider's point
// ceiling per response (2,000) and the deepest timeframe in durable coverage (derived from
// configuration in `DurableTimeframes`). In `1d` that gives 2,000 days, and the live check
// matches: `min(timestamp)` of `derivatives` is exactly the reach the provider measurement
// predicted.
//
// A retention window shorter than this would erase, on every boot, the history seeding just
// wrote — the retention incident repeated on the table this path grows.
//
// It returns zero when there is no durable coverage: without a covered timeframe there is no
// deep seeding, and an invented boundary would be retention disabled under another name.
func DurableSeedHorizon(timeframes []string) time.Duration {
	var deepest time.Duration
	for _, timeframe := range DurableTimeframes(timeframes) {
		ms, ok := candle.ParseTimeframeMs(timeframe)
		if !ok || ms <= 0 {
			continue
		}
		horizon := time.Duration(ms) * time.Millisecond * coinalyzeMaxPointsPerResponse
		if horizon > deepest {
			deepest = horizon
		}
	}
	return deepest
}

// DurableSeedPoints is how many points seeding requests per `(symbol, quantity, timeframe)`.
//
// It is the provider's response ceiling, MEASURED — requesting more days does not bring more
// points, it brings the same ~2,000 most recent ones. Exposed because the sizing sheet needs it
// to project disk cost, and a second copy of the number there would be the usual divergence.
func DurableSeedPoints() int { return coinalyzeMaxPointsPerResponse }
