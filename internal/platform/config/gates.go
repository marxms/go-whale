package config

// gates.go — the STARTUP GATES, and the line between what REFUSES and what WARNS.
//
// THE RULE THAT GOVERNS THE WHOLE TABLE: only a gate whose condition compares TWO READ NUMBERS
// is FATAL.
//
// The justification is measured, not aesthetic. This service has no hot reload — `fsnotify`,
// `WatchConfig` and `SIGHUP` have ZERO occurrences in `internal/` and `cmd/` — so changing
// config means restarting the container. A fatal gate over a PROJECTION turns "wrong config"
// into "service that will not start", with no way back: the operator cannot even start the
// process to fix the number that blocked the start. And the projection has a ±10% band by
// construction, so it would refuse because of noise.
//
// The projection therefore WARNS and ERRORS in log and metric — it never refuses a start. Fatal
// is reserved for deterministic comparisons: `MAX_INDICATOR_VALUES > CONFIG GET
// zset-max-listpack-entries` (whose traversal converts the key to `skiplist` IRREVERSIBLY —
// measured 63.3 → 142.2 B/member), the eviction policy against an immortal family, and the
// sole copy against a policy that evicts everything. None of them needs `A`, which is why the
// refusal can happen BEFORE the fleet starts.
//
// AND AN UNREAD NUMBER DOES NOT BECOME A VERDICT. If either side of a fatal comparison could
// not be read, the gate declares that it did not evaluate. Refusing based on a reserve literal
// would be passing sentence without evidence — exactly the defect these gates exist to remove.

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrStartupGate is the sentinel every fatal gate wraps, so a caller matches with errors.Is
// instead of comparing strings.
var ErrStartupGate = errors.New("config: startup gate refused the configuration")

// GateSeverity ranks a gate verdict.
type GateSeverity int

const (
	// GateInfo records something worth declaring that is not a problem.
	GateInfo GateSeverity = iota
	// GateWarn is a condition the operator should see. It never stops the boot.
	GateWarn
	// GateError is over budget: logged and counted, and STILL never stops the boot, because it
	// rests on an estimate.
	GateError
	// GateFatal refuses the configuration. Reserved for comparisons of two READ numbers.
	GateFatal
)

func (s GateSeverity) String() string {
	switch s {
	case GateInfo:
		return "INFO"
	case GateWarn:
		return "WARNS"
	case GateError:
		return "ERROR"
	case GateFatal:
		return "REFUSES"
	default:
		return "?"
	}
}

// GateResult is one row of the verdict.
type GateResult struct {
	Name     string
	Severity GateSeverity
	Message  string

	// Invariant names WHAT THIS VERDICT REQUIRES, and SatisfiedBy says WHAT SATISFIES IT.
	//
	// They exist because a refusal that only describes the state found has no way back, and
	// this service has no hot reload: wrong config means restarting the container, and an
	// operator who does not know what satisfies the gate restarts into the same verdict. Worse
	// here than anywhere else, because the TWO eviction gates refuse under COMPLEMENTARY
	// policies — `allkeys-*` accuses a sole copy, `volatile-ttl` accuses an immortal family —
	// and without the declared satisfier the operator swaps one refusal for the other thinking
	// progress was made.
	//
	// Only a REFUSAL fills them in. A verdict that does not stop the process does not need a
	// way back, because the process started.
	Invariant   string
	SatisfiedBy string
}

func (r GateResult) String() string {
	if r.Severity != GateFatal {
		return fmt.Sprintf("[GATE] %-8s %s: %s", r.Severity, r.Name, r.Message)
	}
	return fmt.Sprintf("[GATE] %-8s %s: %s | %s", r.Severity, r.Name, r.Message, r.refusalPath())
}

// refusalPath renders the missing invariant and what satisfies it — or says, loudly, that the
// refusal did not declare them.
//
// A REFUSAL without a declared invariant is a gate that stops the process without saying what
// is missing. It is not silenced here: the text accuses its own omission, so it appears in the
// startup log instead of becoming a generic error the operator cannot act on.
func (r GateResult) refusalPath() string {
	switch {
	case r.Invariant == "" && r.SatisfiedBy == "":
		return "INVARIANT NOT DECLARED: this gate refuses without naming what is missing or what satisfies it"
	case r.Invariant == "":
		return fmt.Sprintf("INVARIANT NOT DECLARED · SATISFIED BY: %s", r.SatisfiedBy)
	case r.SatisfiedBy == "":
		return fmt.Sprintf("INVARIANT MISSING: %s · SATISFIER NOT DECLARED", r.Invariant)
	default:
		return fmt.Sprintf("INVARIANT MISSING: %s · SATISFIED BY: %s", r.Invariant, r.SatisfiedBy)
	}
}

// GateInputs carries every number the table compares. Zero means NOT READ for every field that
// comes off a server or an environment — and a gate whose input was not read declares that
// instead of assuming a value.
type GateInputs struct {
	// IndicatorCap is MAX_INDICATOR_VALUES, effective.
	IndicatorCap int64
	// ZSetMaxListpackEntries is CONFIG GET zset-max-listpack-entries — a SERVER DIRECTIVE, not
	// physics. It is why this number can never be a literal in code.
	ZSetMaxListpackEntries int64

	// SeedCandlesLimit is ANALYZER_MARKET_DATA_CANDLES_LIMIT and MinWarmupCandles is the single
	// source in libs/common/src/config/warmup.ts. Their difference is what the seed can
	// actually deliver.
	SeedCandlesLimit int64
	MinWarmupCandles int64

	// DeclaredCapByTimeframe is what the operator asked for; BaselineFloorByTimeframe is the
	// floor the ruler needs in order to mature. The floor is not configurable downwards.
	DeclaredCapByTimeframe   map[string]int64
	BaselineFloorByTimeframe map[string]int64

	// RegisteredTimeframes is tfReg.All() — every timeframe the process actually serves, which
	// is NOT the same set as the one with an explicit cap entry. CapEntriesByTimeframe is the
	// cap map as WRITTEN in config (a different question from DeclaredCapByTimeframe above,
	// which is the resolved per-timeframe number the floor gate compares), and CapConfigKey
	// names the key the operator would have to edit — so the warning points at a place and not
	// at a concept.
	RegisteredTimeframes  []string
	CapEntriesByTimeframe map[string]int64
	CapConfigKey          string

	// THE DURABLE COVERAGE PER FIELD, and the cap a cell keeps without an entry of its own.
	//
	// DefaultSeriesCap is `derivatives.max_series_size` — the number the cell keeps when
	// NOBODY writes an entry for it. It is against that number that "applying a cap" is
	// decided: an entry only shortens the horizon when it falls BELOW it. Zero means NOT READ.
	//
	// DurableCellCoverage is the coverage declaration crossed with the column list the read
	// asks for — written ∧ readable, per FIELD. See cap_coverage.go.
	DefaultSeriesCap    int64
	DurableCellCoverage DurableCellCoverage

	// MaxMemoryPolicy comes from INFO memory; ImmortalFamilies are the families measured with
	// no TTL. PolicyIsFatal is the switch that turns the policy gate fatal — see the note on
	// the gate itself.
	MaxMemoryPolicy  string
	ImmortalFamilies []string
	PolicyIsFatal    bool

	// THE SOLE-COPY GUARD, which is what prevents the `allkeys-lru` flip from arriving before
	// the migration. SoleCopyEvaluated reports whether BOTH boundaries could be read;
	// SoleCopyKeysAtRisk is how many `1d`/`1w` keys still have a hot candle older than the
	// durable coverage; SoleCopyDetail is the line the alarm prints.
	SoleCopyEvaluated  bool
	SoleCopyKeysAtRisk int
	SoleCopyDetail     string

	// THE ORDER BETWEEN A NEW COLUMN AND EROSION PROTECTION.
	//
	// DurableColumnsDeclared are the columns the coverage declaration says the durable tier
	// writes; ErosionGuardedColumns are the ones the partial-rewrite protection carries
	// forward. Both are READ from the running binary, and nil means NOT READ — an unread set
	// does not become a verdict.
	DurableColumnsDeclared []string
	ErosionGuardedColumns  []string

	// COVERAGE PER TIMEFRAME.
	//
	// TimeframeSupport is the declared support matrix, one row per `(source, feed)`, READ from
	// the binary being started — it is provider measurement, not configuration. `nil` means
	// NOT READ, and an unread matrix does not become a verdict, by the same rule as the other
	// gates. Crossed with RegisteredTimeframes above, it answers the question that otherwise
	// becomes a WARN every cycle, forever: is there a configured timeframe that no source
	// serves in any feed?
	TimeframeSupport []TimeframeSupportRow

	// ProjectedMiB is the sizing sheet's estimate; MaxMemoryBytes and UsedMemoryBytes are read.
	ProjectedMiB    float64
	MaxMemoryBytes  int64
	UsedMemoryBytes int64

	// The policy ratios, from the retention block. Zero means UNDECLARED, and an undeclared
	// threshold is reported as undeclared rather than replaced by a code default.
	WarnRatio            float64
	ErrorRatio           float64
	MeasuredStartupRatio float64
}

// TimeframeSupportRow declares what ONE `(source, feed)` cell serves.
//
// # WHY TWO SETS, AND NOT ONE WITH THE IMPLICIT COMPLEMENT
//
// Because the third answer exists. A timeframe outside both `Served` and `Unserved` is
// UNDECLARED — nobody measured this cell — and "we never asked" cannot refuse a start. A
// single set would collapse the two absences and turn a missing measurement into a boot
// refusal, which is a sentence without evidence.
type TimeframeSupportRow struct {
	// Source is the provenance: the venue, the durable-tier aggregator, and so on.
	Source string
	// Feed is the measured quantity.
	Feed string
	// Served are the timeframes this cell serves, MEASURED.
	Served []string
	// Unserved are the ones it declares it does NOT serve, also measured.
	Unserved []string
}

// EvaluatePreflightGates runs the SUBSET the boot can evaluate before the fleet exists, and it
// is the only evaluation whose verdict is allowed to refuse the start.
//
// # WHY A SUBSET, AND NOT THE WHOLE TABLE
//
// The sizing sheet runs AFTER discovery, because `A` comes from `SCARD active_symbols:*` and
// at boot time that set is empty. Refusing the start from it would kill the process in
// mid-flight — with the poller, Coinalyze and HTTP already up — which is worse than the wrong
// config.
//
// But the gates that REFUSE do not need `A`: the comparisons below read two numbers from the
// server and the keyspace, all available before anything starts. Separating them is what lets
// the refusal be SYNCHRONOUS and BEFORE the fleet, while keeping the projection — which needs
// `A` and has a ±10% band — deferred and NEVER fatal.
//
// The list is checked by equality against the whole table: `gates_test.go` fails if any fatal
// verdict of `EvaluateGates` is not reproduced here. Without that ratchet, a new fatal gate
// would be born outside the only point that acts on the verdict.
func EvaluatePreflightGates(in GateInputs) []GateResult {
	return []GateResult{
		gateIndicatorCapAgainstListpackCliff(in),
		gateEvictionPolicyAgainstImmortalFamilies(in),
		gateSoleCopyAgainstEvictEverythingPolicy(in),
		gateDurableColumnsAgainstErosionGuard(in),
		gateTimeframeServableByAnySource(in),
		gateCapEntryAgainstDurableCellCoverage(in),
	}
}

// EvaluateGates runs the whole gate table and returns one row per rule.
func EvaluateGates(in GateInputs) []GateResult {
	var out []GateResult
	out = append(out, gateIndicatorCapAgainstListpackCliff(in))
	out = append(out, gateIndicatorCapAgainstSeedCeiling(in))
	out = append(out, gateDeclaredCapsAgainstBaselineFloor(in)...)
	out = append(out, gateTimeframesWithoutAnExplicitCapEntry(in))
	out = append(out, gateEvictionPolicyAgainstImmortalFamilies(in))
	out = append(out, gateSoleCopyAgainstEvictEverythingPolicy(in))
	out = append(out, gateDurableColumnsAgainstErosionGuard(in))
	out = append(out, gateTimeframeServableByAnySource(in))
	out = append(out, gateCapEntryAgainstDurableCellCoverage(in))
	out = append(out, gateProjectionAgainstMaxMemory(in))
	out = append(out, gateMeasuredOccupancyAtStartup(in))
	return out
}

// FatalGateError collapses the verdict into an error when — and only when — a fatal row fired.
func FatalGateError(results []GateResult) error {
	var reasons []string
	for _, r := range results {
		if r.Severity == GateFatal {
			// THE REFUSAL CARRIES THE WAY BACK. An error that only states the found
			// condition sends the operator restarting into the same verdict.
			reasons = append(reasons, fmt.Sprintf("%s: %s | %s", r.Name, r.Message, r.refusalPath()))
		}
	}
	if len(reasons) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrStartupGate, reasons)
}

// gateDurableColumnsAgainstErosionGuard enforces the order: no new column is written before
// the erosion protection covers that column.
//
// # WHY THIS IS A COMPARISON OF TWO READ SETS, AND NOT A PROJECTION
//
// Both sides come from the binary being started: one is the durable coverage declaration, the
// other is what the partial-rewrite protection actually carries forward. There is no estimate,
// no band, and the verdict is the same on every run of the same build. It has the same shape
// as the other refusals — two read facts — except that they are sets instead of numbers.
//
// # WHY IT REFUSES
//
// The consequence of starting this way is not degradation: it is DESTRUCTION. A column that is
// written and not protected is erased by dedup on the first degraded cycle that rewrites the
// bucket, and the erased value does not come back. Between a process that does not start and a
// process that erases the data the feature exists to record, the one that does not start is the
// cheap one.
//
// An UNREAD set — nil — does not become a verdict, by the same rule as the other gates.
func gateDurableColumnsAgainstErosionGuard(in GateInputs) GateResult {
	const name = "durable-columns-vs-erosion-guard"

	if in.DurableColumnsDeclared == nil || in.ErosionGuardedColumns == nil {
		return GateResult{
			Name:     name,
			Severity: GateWarn,
			Message: fmt.Sprintf(
				"NOT EVALUATED: one of the two sets was not read (declared=%v, guarded=%v). "+
					"Refusing against an absent set would be a verdict without proof",
				in.DurableColumnsDeclared, in.ErosionGuardedColumns),
		}
	}

	guarded := make(map[string]struct{}, len(in.ErosionGuardedColumns))
	for _, col := range in.ErosionGuardedColumns {
		guarded[col] = struct{}{}
	}
	var unprotected []string
	for _, col := range in.DurableColumnsDeclared {
		if _, ok := guarded[col]; !ok {
			unprotected = append(unprotected, col)
		}
	}
	if len(unprotected) == 0 {
		return GateResult{
			Name:     name,
			Severity: GateInfo,
			Message: fmt.Sprintf("the %d declared durable column(s) are under the erosion guard",
				len(in.DurableColumnsDeclared)),
		}
	}
	sort.Strings(unprotected)

	return GateResult{
		Name:     name,
		Severity: GateFatal,
		Message: fmt.Sprintf(
			"durable column(s) declared OUTSIDE the partial-rewrite guard: %s. "+
				"Dedup erases the omitted column on the first degraded cycle, and the erased value does not come back",
			strings.Join(unprotected, ", ")),
		Invariant: "every column declared in DerivativesDurableCoverage is among those the " +
			"erosion guard carries forward",
		SatisfiedBy: fmt.Sprintf(
			"teach the guard to preserve %s — the prior read must bring the column and "+
				"`fillFromPrior` must copy it — OR remove the column from the coverage declaration "+
				"until that exists. There is no third option: a write without the guard erases old data",
			strings.Join(unprotected, ", ")),
	}
}

// gateTimeframeServableByAnySource: a timeframe that no source serves in any feed REFUSES the
// start, by name.
//
// # WHAT IT REPLACES
//
// A WARN every cycle, forever. `1w` once produced a warning line every hour — the shape a
// defect takes when nobody decides what to do with it. A repeated warning is not a diagnosis:
// it disappears into volume and teaches the operator to ignore the whole category.
//
// # WHY IT CAN BE FATAL
//
// Because both sides are READ and the comparison is of sets, with no estimate and no band —
// the same shape as the other refusals. The timeframe registry comes from the configuration
// being started; the support matrix is provider measurement declared in the binary. The
// verdict is the same on every run of the same build.
//
// # AND WHY IT DOES NOT FIRE
//
// `1w` is served by the venue in the funding feed — settlement is an EVENT, with no period
// parameter, and the fan-out reaches any bucket. Measured: 30 weekly points by the route, 27
// with `fundingRate8h`. It has no DURABLE tier, which is a different thing and is declared to
// the client instead of refused here. This gate is a ratchet against a timeframe nobody serves
// entering the configuration — not against the weekly one.
//
// UNDECLARED DOES NOT REFUSE, and leaves as a named warning: absence of measurement is
// absence of measurement.
func gateTimeframeServableByAnySource(in GateInputs) GateResult {
	const name = "timeframe-vs-any-source-support"

	if len(in.RegisteredTimeframes) == 0 || in.TimeframeSupport == nil {
		return GateResult{
			Name:     name,
			Severity: GateWarn,
			Message: fmt.Sprintf(
				"NOT EVALUATED: timeframe registry (%d) or support matrix (%v) was not read. "+
					"Refusing against an absent matrix would be a verdict without proof",
				len(in.RegisteredTimeframes), in.TimeframeSupport),
		}
	}

	served := map[string]struct{}{}
	unserved := map[string]struct{}{}
	for _, row := range in.TimeframeSupport {
		for _, tf := range row.Served {
			served[normalizeTimeframeKey(tf)] = struct{}{}
		}
		for _, tf := range row.Unserved {
			unserved[normalizeTimeframeKey(tf)] = struct{}{}
		}
	}

	var refused, unmeasured []string
	for _, tf := range in.RegisteredTimeframes {
		key := normalizeTimeframeKey(tf)
		if _, ok := served[key]; ok {
			continue
		}
		if _, ok := unserved[key]; ok {
			refused = append(refused, tf)
			continue
		}
		unmeasured = append(unmeasured, tf)
	}
	sort.Strings(refused)
	sort.Strings(unmeasured)

	if len(refused) > 0 {
		return GateResult{
			Name:     name,
			Severity: GateFatal,
			Message: fmt.Sprintf(
				"configured timeframe(s) that NO source serves in any quantity: %s. "+
					"Every cycle would request a series that does not exist and log a warning, forever",
				strings.Join(refused, ", ")),
			Invariant: "every timeframe in candle_engine.timeframes is served by at least one " +
				"(source, quantity) of the declared support matrix",
			SatisfiedBy: fmt.Sprintf(
				"remove %s from candle_engine.timeframes, OR declare in the support matrix the source that "+
					"serves it — with measurement, not a guess. There is no third option: keeping the "+
					"timeframe without a source trades the refusal for a warning every cycle",
				strings.Join(refused, ", ")),
		}
	}

	if len(unmeasured) > 0 {
		return GateResult{
			Name:     name,
			Severity: GateWarn,
			Message: fmt.Sprintf(
				"registered timeframe(s) that NO cell of the matrix measured: %s — this is absence of "+
					"measurement, not a declared absence of support, and therefore it does not refuse",
				strings.Join(unmeasured, ", ")),
		}
	}

	return GateResult{
		Name:     name,
		Severity: GateInfo,
		Message:  fmt.Sprintf("the %d registered timeframes are served by at least one (source, quantity)", len(in.RegisteredTimeframes)),
	}
}

// gateCapEntryAgainstDurableCellCoverage: no cell is capped before the durable column of EVERY
// field the member carries is written AND readable by the route.
//
// # THE RULE IS EXECUTED, NOT CITED
//
// The rule was already written down and was applied backwards: `30m` gained a cap entry, the
// cap reached the only copy, and 254 hours of series vanished. The difference here is that the
// rule is a comparison that runs on every start.
//
// # WHAT COUNTS AS "APPLYING A CAP"
//
// An entry in `series_cap_by_timeframe` BELOW the cap the cell would keep without it. An entry
// that equals or raises it shortens no horizon and destroys no data — refusing the start for
// it would confuse "the operator wrote a number" with "the operator erased data". Shortening is
// what destroys, and that is what this gate asks about.
//
// # WHY IT REFUSES
//
// Same shape and same consequence as `durable-columns-vs-erosion-guard`: two sets READ from the
// build and the configuration being started, with no estimate and no band, and the consequence
// of starting this way is not degradation — it is DESTRUCTION of a copy that does not come
// back. Between a process that does not start and a process that erases the data the feature
// exists to preserve, the one that does not start is the cheap one.
//
// An UNREAD side does not become a verdict, by the same rule as the other gates.
func gateCapEntryAgainstDurableCellCoverage(in GateInputs) GateResult {
	const name = "cap-entry-vs-durable-cell-coverage"

	if len(in.CapEntriesByTimeframe) == 0 {
		return GateResult{
			Name:     name,
			Severity: GateInfo,
			Message:  "no cap entry declared: no cell is being shortened",
		}
	}
	if !in.DurableCellCoverage.Declared() || in.DefaultSeriesCap <= 0 {
		return GateResult{
			Name:     name,
			Severity: GateWarn,
			Message: fmt.Sprintf(
				"NOT EVALUATED: per-field durable coverage read=%t, default cap read=%d. "+
					"Refusing against absent coverage would be a verdict without proof",
				in.DurableCellCoverage.Declared(), in.DefaultSeriesCap),
		}
	}

	timeframes := make([]string, 0, len(in.CapEntriesByTimeframe))
	for tf := range in.CapEntriesByTimeframe {
		timeframes = append(timeframes, tf)
	}
	sort.Strings(timeframes)

	var refused, cells []string
	for _, tf := range timeframes {
		entry := in.CapEntriesByTimeframe[tf]
		if entry <= 0 || entry >= in.DefaultSeriesCap {
			continue
		}
		uncovered := in.DurableCellCoverage.UncoveredMemberFields(tf)
		if len(uncovered) == 0 {
			continue
		}
		refused = append(refused, tf)
		cells = append(cells, fmt.Sprintf("%s: %d below %d, and %d member field(s) without durable write AND read — %s",
			tf, entry, in.DefaultSeriesCap, len(uncovered), strings.Join(uncovered, "; ")))
	}

	if len(refused) == 0 {
		return GateResult{
			Name:     name,
			Severity: GateInfo,
			Message: fmt.Sprintf("the %d declared cap entry/entries shorten no cell without per-field durable coverage",
				len(in.CapEntriesByTimeframe)),
		}
	}

	return GateResult{
		Name:     name,
		Severity: GateFatal,
		Message: fmt.Sprintf(
			"cap shortening cell(s) WITHOUT a written and readable durable tier for every member field: %s. "+
				"The cap removes the WHOLE MEMBER, so the uncovered field goes with it and does not return from anywhere",
			strings.Join(cells, " | ")),
		Invariant: "every cell with a cap entry below derivatives.max_series_size has, for " +
			"EVERY member field, a durable column written and requested by the read path",
		SatisfiedBy: fmt.Sprintf(
			"remove the entry for %s from derivatives.series_cap_by_timeframe — the cell returns to the default cap %d "+
				"and nothing is destroyed —, OR give a durable column to the fields named above AND request it in the read path "+
				"before reintroducing the entry. There is no third option: widening the durable tier to more "+
				"TIMEFRAMES does not cover a field, because the gap is on the FIELD axis",
			strings.Join(refused, ", "), in.DefaultSeriesCap),
	}
}

// gateIndicatorCapAgainstListpackCliff refuses the start, and the reason is irreversibility.
//
// The 512→513 traversal converts the key to `skiplist` and does not revert: measured on the
// fleet, a member grows from 63.3 to 142.2 B — 2.25× the memory for the same data. Both sides
// are READ, so the comparison is deterministic and deserves to refuse the start.
//
// How many fatal gates exist is not a comment assertion: the ratchet
// TestGates_EveryFatalVerdictDeclaresItsInvariantAndSatisfier ENUMERATES them by name and
// fails when one is born outside its reach.
func gateIndicatorCapAgainstListpackCliff(in GateInputs) GateResult {
	const name = "indicator-cap-vs-listpack-cliff"
	if in.IndicatorCap <= 0 || in.ZSetMaxListpackEntries <= 0 {
		return GateResult{
			Name:     name,
			Severity: GateWarn,
			Message: fmt.Sprintf(
				"NOT EVALUATED: one of the two numbers was not read (cap=%d, zset-max-listpack-entries=%d). "+
					"The fatal gate requires TWO numbers read; refusing against a literal would be a verdict without proof",
				in.IndicatorCap, in.ZSetMaxListpackEntries),
		}
	}
	if in.IndicatorCap > in.ZSetMaxListpackEntries {
		return GateResult{
			Name:     name,
			Severity: GateFatal,
			Message: fmt.Sprintf(
				"indicator cap %d ABOVE zset-max-listpack-entries %d (CONFIG GET): the key converts "+
					"to skiplist and does NOT revert — measured 63.3 → 142.2 B/member",
				in.IndicatorCap, in.ZSetMaxListpackEntries),
			Invariant: "MAX_INDICATOR_VALUES ≤ the server's zset-max-listpack-entries",
			SatisfiedBy: fmt.Sprintf(
				"lower MAX_INDICATOR_VALUES to %d or less, OR raise zset-max-listpack-entries on the "+
					"server above %d BEFORE startup — the conversion to skiplist does not revert by itself, "+
					"so a key already converted must be rewritten",
				in.ZSetMaxListpackEntries, in.IndicatorCap),
		}
	}
	return GateResult{
		Name:     name,
		Severity: GateInfo,
		Message:  fmt.Sprintf("indicator cap %d within zset-max-listpack-entries %d (CONFIG GET)", in.IndicatorCap, in.ZSetMaxListpackEntries),
	}
}

// gateIndicatorCapAgainstSeedCeiling warns with BOTH numbers.
//
// 400 > 500 − 120 = 380: the seed delivers 380, not 400. Nothing breaks — but the sheet was
// declaring a depth the source cannot deliver.
func gateIndicatorCapAgainstSeedCeiling(in GateInputs) GateResult {
	const name = "indicator-cap-vs-seed-ceiling"
	if in.IndicatorCap <= 0 || in.SeedCandlesLimit <= 0 || in.MinWarmupCandles <= 0 {
		return GateResult{Name: name, Severity: GateWarn, Message: "NOT EVALUATED: one of the numbers was not read"}
	}
	ceiling := in.SeedCandlesLimit - in.MinWarmupCandles
	if in.IndicatorCap > ceiling {
		return GateResult{
			Name:     name,
			Severity: GateWarn,
			Message: fmt.Sprintf(
				"indicator cap %d ABOVE what the seed delivers: ANALYZER_MARKET_DATA_CANDLES_LIMIT(%d) − "+
					"MIN_WARMUP_CANDLES(%d) = %d",
				in.IndicatorCap, in.SeedCandlesLimit, in.MinWarmupCandles, ceiling),
		}
	}
	return GateResult{
		Name:     name,
		Severity: GateInfo,
		Message:  fmt.Sprintf("indicator cap %d within what the seed delivers (%d)", in.IndicatorCap, ceiling),
	}
}

// gateDeclaredCapsAgainstBaselineFloor: a cap below the floor is RAISED and RECORDED. That is
// already the law in seriesCapFor; the gate exists so the raise is visible, because a silent
// raise is how the sheet started lying about what is resident.
func gateDeclaredCapsAgainstBaselineFloor(in GateInputs) []GateResult {
	const name = "declared-cap-vs-baseline-floor"
	timeframes := make([]string, 0, len(in.DeclaredCapByTimeframe))
	for tf := range in.DeclaredCapByTimeframe {
		timeframes = append(timeframes, tf)
	}
	sort.Strings(timeframes)

	var out []GateResult
	for _, tf := range timeframes {
		declared := in.DeclaredCapByTimeframe[tf]
		floor, ok := in.BaselineFloorByTimeframe[tf]
		if !ok || floor <= 0 || declared <= 0 || declared >= floor {
			continue
		}
		out = append(out, GateResult{
			Name:     name,
			Severity: GateWarn,
			Message: fmt.Sprintf("%s: declared %d BELOW the baseline floor %d — raised to the floor, which is not configurable downward",
				tf, declared, floor),
		})
	}
	if len(out) == 0 {
		return []GateResult{{Name: name, Severity: GateInfo, Message: "no declared cap below the baseline floor"}}
	}
	return out
}

// gateTimeframesWithoutAnExplicitCapEntry warns LOUDLY, NAMING the timeframe.
//
// THIS IS THE REAL TRAP. A NEW timeframe enters `tfReg.All()`, gets no entry of its own, and
// inherits a default nobody chose for it. One day of `1w` costs 1/7 of a member and one day of
// `5m` costs 288 — a 2,016× difference — so inheriting silently is not a configuration detail.
//
// Warn, never refuse: inheriting the default is legitimate behavior, and the operator decides
// whether that timeframe deserves its own entry. What cannot happen is the decision being
// invisible.
func gateTimeframesWithoutAnExplicitCapEntry(in GateInputs) GateResult {
	const name = "timeframe-without-explicit-cap-entry"
	if len(in.RegisteredTimeframes) == 0 {
		return GateResult{Name: name, Severity: GateWarn, Message: "NOT EVALUATED: no registered timeframe was provided"}
	}

	key := in.CapConfigKey
	if key == "" {
		key = "(cap key not provided)"
	}

	// The keys come from a hand-written YAML, so `1D` and ` 1d` must reach the same timeframe as
	// `1d`. It is the MAP that is normalized, not the lookup: normalizing only the lookup side
	// would leave `1D` in the YAML out of reach of a registered `1d`, and the gate would scream
	// about a timeframe that HAS an entry. A false warning is a gate turned off in the second
	// week.
	declared := make(map[string]struct{}, len(in.CapEntriesByTimeframe))
	for key, value := range in.CapEntriesByTimeframe {
		if value > 0 {
			declared[normalizeTimeframeKey(key)] = struct{}{}
		}
	}

	var missing []string
	for _, tf := range in.RegisteredTimeframes {
		if _, ok := declared[normalizeTimeframeKey(tf)]; ok {
			continue
		}
		missing = append(missing, tf)
	}
	if len(missing) == 0 {
		return GateResult{
			Name:     name,
			Severity: GateInfo,
			Message:  fmt.Sprintf("every registered timeframe has its own entry in %s", key),
		}
	}
	sort.Strings(missing)
	return GateResult{
		Name:     name,
		Severity: GateWarn,
		Message: fmt.Sprintf("timeframe(s) WITHOUT their own entry in %s: %s — they inherit the default, which nobody chose for them",
			key, strings.Join(missing, ", ")),
	}
}

// gateEvictionPolicyAgainstImmortalFamilies crosses the POLICY with the PRESENCE of an immortal
// family.
//
// `candles` and `derivatives:series` remain without TTL — the cap is their retention, not the
// TTL, and that is a declared decision, not pending work. Under `allkeys-lru` a key without TTL
// IS recoverable by eviction, so this gate's condition (`volatile-ttl` AND an immortal family)
// simply does not fire.
//
// That is exactly why it can be fatal: fatal here does not refuse the current state — it
// becomes a RATCHET against returning `volatile-ttl` to the compose file and leaving the dataset
// unrecoverable under pressure again. `PolicyIsFatal` is the switch.
func gateEvictionPolicyAgainstImmortalFamilies(in GateInputs) GateResult {
	const name = "eviction-policy-vs-immortal-families"
	if in.MaxMemoryPolicy == "" {
		return GateResult{Name: name, Severity: GateWarn, Message: "NOT EVALUATED: maxmemory_policy was not read"}
	}
	// THE PROPERTY, NOT THE STRING. The defect was never the NAME `volatile-ttl` — it is
	// evicting ONLY those that have a TTL, which leaves the immortal family out of the queue
	// and takes Redis to OOM instead of eviction. `volatile-lru`, `volatile-lfu`,
	// `volatile-random` and `noeviction` have exactly that property, and string equality let
	// all four pass as INFO — quietly bypassing the choice this gate exists to force. Measured:
	// a `CONFIG SET maxmemory-policy volatile-lru` cleared THIS gate and the sole-copy gate at
	// the same time, without migrating a single row.
	//
	// The complement of `evictsEverything` is exactly the set that does not recover the immortal
	// family.
	if evictsEverything(in.MaxMemoryPolicy) || len(in.ImmortalFamilies) == 0 {
		return GateResult{
			Name:     name,
			Severity: GateInfo,
			Message:  fmt.Sprintf("policy %q with %d family(ies) without TTL", in.MaxMemoryPolicy, len(in.ImmortalFamilies)),
		}
	}

	families := append([]string(nil), in.ImmortalFamilies...)
	sort.Strings(families)
	message := fmt.Sprintf(
		"policy %q does NOT evict keys without TTL and there is a resident family WITHOUT TTL (%v): that part of the "+
			"dataset is unrecoverable under pressure",
		in.MaxMemoryPolicy, families)
	if in.PolicyIsFatal {
		return GateResult{
			Name:      name,
			Severity:  GateFatal,
			Message:   message,
			Invariant: "every resident family is recoverable under memory pressure",
			SatisfiedBy: fmt.Sprintf(
				"give a TTL to the family(ies) %v, OR switch maxmemory-policy to allkeys-* (which evicts "+
					"keys without TTL) — and in that case the RET-7 sole-copy migration must run BEFORE, "+
					"otherwise the sole-copy-vs-evict-everything-policy gate starts refusing in place of this one",
				families),
		}
	}
	return GateResult{
		Name:     name,
		Severity: GateWarn,
		Message:  message + " — WARNS while PolicyIsFatal is off",
	}
}

// evictsEverything reports whether the policy evicts ANY key, with or without a TTL.
//
// The whole `allkeys-*` family, not just `allkeys-lru`: `allkeys-lfu` and `allkeys-random` put
// the same keys in the queue, and `allkeys-lfu` is a legitimate alternative. A string-equality
// test against `allkeys-lru` would be a guard that the recommended alternative turns off.
func evictsEverything(policy string) bool {
	return strings.HasPrefix(policy, "allkeys-")
}

// gateSoleCopyAgainstEvictEverythingPolicy is the SOLE-COPY RATCHET.
//
// It crosses two read numbers: the current policy (`INFO memory`) and how many `1d`/`1w` keys
// still have a hot candle older than the durable coverage (the `engine.SoleCopyGuard` guard).
//
// Under `allkeys-*` the LEAST TOUCHED keys leave first, and a weekly chart is the least touched
// in the keyspace: those are exactly the sole-copy cells that would go first. For that reason,
// and only here, the gate REFUSES — the condition is a comparison of two numbers it read, the
// only place where fatal is admitted.
//
// Not measuring is not the same as being clean, and it is not the same as being dirty either:
// QuestDB being down at startup cannot become "the service does not start", because that would
// have no way back. In that case the verdict is the loudest alarm that does not stop the
// process.
func gateSoleCopyAgainstEvictEverythingPolicy(in GateInputs) GateResult {
	const name = "sole-copy-vs-evict-everything-policy"

	if in.MaxMemoryPolicy == "" {
		return GateResult{Name: name, Severity: GateWarn, Message: "NOT EVALUATED: maxmemory_policy was not read"}
	}
	if !evictsEverything(in.MaxMemoryPolicy) {
		return GateResult{
			Name:     name,
			Severity: GateInfo,
			Message: fmt.Sprintf("policy %q does not evict keys without TTL: the sole-copy guard does not apply",
				in.MaxMemoryPolicy),
		}
	}

	if !in.SoleCopyEvaluated {
		return GateResult{
			Name:     name,
			Severity: GateError,
			Message: fmt.Sprintf("policy %q ACTIVE and the sole copy COULD NOT BE MEASURED (%s) — "+
				"under this policy the least-touched keys leave first, and 1d/1w are the least touched in the keyspace",
				in.MaxMemoryPolicy, in.SoleCopyDetail),
		}
	}

	if in.SoleCopyKeysAtRisk > 0 {
		return GateResult{
			Name:     name,
			Severity: GateFatal,
			Message: fmt.Sprintf("policy %q with %d sole-copy key(s) still in Redis (%s) — "+
				"RET-7: the migration PRECEDES the policy change; under this policy those keys are the FIRST to leave",
				in.MaxMemoryPolicy, in.SoleCopyKeysAtRisk, in.SoleCopyDetail),
			Invariant: "zero sole-copy keys in Redis under a policy that evicts everything (RET-7)",
			SatisfiedBy: fmt.Sprintf(
				"move the %d key(s) to the durable tier — the durable coverage of 1d/1w must reach the "+
					"oldest hot candle — and only then keep %q; returning to volatile-ttl does NOT satisfy it, "+
					"because then the eviction-policy-vs-immortal-families gate refuses in place of this one",
				in.SoleCopyKeysAtRisk, in.MaxMemoryPolicy),
		}
	}

	return GateResult{
		Name:     name,
		Severity: GateInfo,
		Message: fmt.Sprintf("policy %q with ZERO sole-copy keys: eviction does not reach unrecoverable data (%s)",
			in.MaxMemoryPolicy, in.SoleCopyDetail),
	}
}

// gateProjectionAgainstMaxMemory: NEVER fatal. See the note at the top of the file.
func gateProjectionAgainstMaxMemory(in GateInputs) GateResult {
	const name = "projection-vs-maxmemory"
	if in.MaxMemoryBytes <= 0 || in.ProjectedMiB <= 0 {
		return GateResult{Name: name, Severity: GateWarn, Message: "NOT EVALUATED: projection or maxmemory missing"}
	}
	maxMiB := float64(in.MaxMemoryBytes) / float64(1<<20)
	ratio := in.ProjectedMiB / maxMiB

	if in.WarnRatio <= 0 && in.ErrorRatio <= 0 {
		return GateResult{
			Name:     name,
			Severity: GateWarn,
			Message: fmt.Sprintf("projection %.1f MiB = %.1f%% of maxmemory, but the target was NOT declared "+
				"(retention.budget_warn_ratio / budget_error_ratio missing)", in.ProjectedMiB, ratio*100),
		}
	}

	switch {
	case in.ErrorRatio > 0 && ratio > in.ErrorRatio:
		return GateResult{
			Name:     name,
			Severity: GateError,
			Message: fmt.Sprintf("projection %.1f MiB = %.1f%% of maxmemory, ABOVE %.0f%% — recorded in log and metric, "+
				"NEVER fatal: without hot reload, refusing startup on an estimate has no way back",
				in.ProjectedMiB, ratio*100, in.ErrorRatio*100),
		}
	case in.WarnRatio > 0 && ratio > in.WarnRatio:
		return GateResult{
			Name:     name,
			Severity: GateWarn,
			Message: fmt.Sprintf("projection %.1f MiB = %.1f%% of maxmemory, above the %.0f%% target",
				in.ProjectedMiB, ratio*100, in.WarnRatio*100),
		}
	default:
		return GateResult{
			Name:     name,
			Severity: GateInfo,
			Message:  fmt.Sprintf("projection %.1f MiB = %.1f%% of maxmemory", in.ProjectedMiB, ratio*100),
		}
	}
}

// gateMeasuredOccupancyAtStartup warns LOUDLY about a READ number — but is still not fatal,
// because the service that does not start is the service that cannot relieve the pressure that
// stopped it from starting.
func gateMeasuredOccupancyAtStartup(in GateInputs) GateResult {
	const name = "measured-occupancy-at-startup"
	if in.MaxMemoryBytes <= 0 || in.UsedMemoryBytes <= 0 {
		return GateResult{Name: name, Severity: GateWarn, Message: "NOT EVALUATED: used_memory or maxmemory was not read"}
	}
	ratio := float64(in.UsedMemoryBytes) / float64(in.MaxMemoryBytes)
	if in.MeasuredStartupRatio <= 0 {
		return GateResult{
			Name:     name,
			Severity: GateWarn,
			Message: fmt.Sprintf("MEASURED occupancy %.1f%%, but the threshold was NOT declared "+
				"(retention.measured_startup_warn_ratio missing)", ratio*100),
		}
	}
	if ratio > in.MeasuredStartupRatio {
		return GateResult{
			Name:     name,
			Severity: GateWarn,
			Message: fmt.Sprintf("MEASURED occupancy %.1f%% ABOVE %.0f%% — a read number, not an estimate",
				ratio*100, in.MeasuredStartupRatio*100),
		}
	}
	return GateResult{
		Name:     name,
		Severity: GateInfo,
		Message:  fmt.Sprintf("MEASURED occupancy %.1f%%", ratio*100),
	}
}
