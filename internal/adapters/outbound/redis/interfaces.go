package redisstore

import (
	"sort"

	"github.com/marxms/go-whale/internal/candle"
)

// ============================================================================
// MULTI-EXCHANGE INTERFACES (new - with exchange context)
// ============================================================================

// Aliases kept while consumers migrate to the candle-owned ports. New code
// must use candle.CandleMergeSource, candle.CandleUpdate and candle.SymbolInfo.
type (
	// CandleMergeSource names the producer whose values were merged into a candle.
	CandleMergeSource = candle.CandleMergeSource
	// CandleUpdate is one live candle update bound for the hot tier.
	CandleUpdate = candle.CandleUpdate
	// SymbolInfo identifies one active symbol and its scope.
	SymbolInfo = candle.SymbolInfo
	// CandleReq asks for one (symbol, timeframe) window in a batch read.
	CandleReq = candle.CandleReq
)

// Merge-source labels as exported by the candle package.
const (
	CandleMergeSourceWSDelta       = candle.CandleMergeSourceWSDelta
	CandleMergeSourceRESTTotal     = candle.CandleMergeSourceRESTTotal
	CandleMergeSourceSnapshotTotal = candle.CandleMergeSourceSnapshotTotal
	CandleMergeSourceWSKline       = candle.CandleMergeSourceWSKline
)

// ============================================================================
// DERIVATIVES STORAGE
// ============================================================================

// The derivatives persistence surface is defined by the consumer ports in
// internal/derivatives (Writer, Reader, BaselineReader, PriceReader).
// Architecture: Per-timeframe ZSET with data in members, capped at maxSeriesSize.
//
// Key patterns:
//   - derivatives:snapshot:latest:{exchange}:{marketType}:{symbol}       → SET TTL 900s (real-time)
//   - derivatives:series:{exchange}:{marketType}:{symbol}:{timeframe}   → ZSET, NO TTL
//
// THE CAP IS NOT CITED HERE ON PURPOSE. A comment that cites a number can go stale while the
// running system moves on. The effective cap is `derivatives.series_cap_by_timeframe[tf]`,
// falling back to `derivatives.max_series_size`, raised to the baseline-window floor; it is
// resolved by `seriesCapFor` and PRINTED per timeframe at startup.
// ============================================================================
// DURABLE TIER COVERAGE — WHAT IT DOES NOT CARRY, PER FIELD
// ============================================================================
//
// THIS DECLARATION LIVES HERE, AND NOT ONLY IN THE DESIGN, BECAUSE THIS IS WHERE WHOEVER
// CAPS THIS FAMILY WILL READ. The rule it supports: no cell may be capped below what the
// source re-supplies until a durable tier carries ALL of its fields — and coverage absence
// is PER FIELD, not per timeframe. A cap decided by looking only at the covered timeframe
// list erases magnitudes no database stores.
//
// The durable table does not cover every field. `UncoveredMemberFields` iterates all
// declared fields, so a single uncovered field keeps the whole derivatives cell uncappable.
// The only durable-tier writer is the Coinalyze collector (`coinalyze_wiring.go`), and of
// the fields that have a column several are written only when the value exists and stay
// NULL while no VENUE writer exists. A column that can be written is not stored data.
//
// Either the durable schema gains the columns, or those fields live only in the ZSET window,
// and capping it shortens their horizon. There is no third option, and choosing one
// silently is what this declaration prevents.

// DerivativesDurableCoverage maps EACH snapshot magnitude to the durable-tier column that
// carries it. An empty string means: NO column, on any timeframe.
//
// The key is the field's JSON name, the same one the route returns — that is how a reader of
// the API response finds the corresponding row here.
//
// THE VALUE IS A COLUMN NAME, OF ANY TYPE. Not every magnitude is `DOUBLE`: the rung that
// produced the funding interval is a STATE NAME, and in the durable tier it is `SYMBOL`.
// Declaring a column here says nothing about its type, and nothing here can say — the write
// is what checks it.
//
// IT IS CHECKED MECHANICALLY, not by reading: `durable_coverage_test.go` asserts that every
// struct field appears here (none disappears by omission), and
// `engine/durable_coverage_test.go` asserts that the set of columns the durable write
// actually emits — `floats ∪ symbols`, minus the seven symbols that identify the row — is
// EQUAL to the set declared here as covered. Declaring coverage for a field the table does
// not have fails; writing an undeclared column does too.
func DerivativesDurableCoverage() map[string]string {
	return map[string]string{
		// --- the SIX that HAVE a column ---
		"openInterestBase": "open_interest_base",
		"fundingRate":      "funding_rate",
		"fundingRate8h":    "funding_rate_8h",
		"longShortRatio":   "long_short_ratio",
		// The two funding-interval fields. They are coverage HERE because the durable write
		// emits them: declaring before the writer exists would be written permission to cap
		// a cell assuming the cold tier covers it.
		"fundingIntervalHours": "funding_interval_hours",
		"nStep":                "n_step", // which rung produced the interval above; SYMBOL column

		// The SIX remaining readable fields. They are coverage HERE because both halves hold:
		// the durable write emits them and `derivativesReadColumns` asks for them. Declaring
		// on only one half would be written permission to cap the cell and destroy the only
		// readable copy.
		"openInterest":    "open_interest", // notional
		"nextFundingRate": "next_funding_rate",
		"interestRate":    "interest_rate",
		"fundingAtAnchor": "funding_at_anchor", // predicate; BOOLEAN column
		"lsLongPct":       "ls_long_pct",
		"lsShortPct":      "ls_short_pct",

		// --- the ONLY one that does NOT have one, and will not ---
		//
		// `dropPollToPollOIChange` nils it on the series write AND on the read, so it never
		// reaches the ZSET member: it lives in the `latest` record, with the TTL. A column
		// for it would stamp a poll delta into a window it never measured.
		//
		// CONSEQUENCE RECORDED: `UncoveredMemberFields` iterates ALL fields, so this line
		// alone keeps every derivatives cell INCAPABLE of being capped. That is the current
		// state, not a regression — but it is what separates "honest coverage" from
		// "released cap", and the decision belongs to the owner, not to this map.
		"oiChangePct": "",
	}
}

// DeclaredDurableColumns is the declaration above reduced to the SET of columns, without the
// fields that have none.
//
// It exists because two callers perform the same reduction — startup wiring
// (`app.durableOrderSets`) and the CI probe (`cmd/boot-verdict`) — and a second hand-written
// reduction in either one would be the divergence the order gate exists to catch, written
// inside the gate's own caller.
func DeclaredDurableColumns() []string {
	out := make([]string, 0, len(DerivativesDurableCoverage()))
	for _, coluna := range DerivativesDurableCoverage() {
		if coluna == "" {
			continue
		}
		out = append(out, coluna)
	}
	sort.Strings(out)
	return out
}
