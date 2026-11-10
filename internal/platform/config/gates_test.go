package config

import (
	"errors"
	"sort"
	"strings"
	"testing"
)

// resultFor finds the row of one gate by name.
func resultFor(t *testing.T, results []GateResult, name string) GateResult {
	t.Helper()
	for _, r := range results {
		if r.Name == name {
			return r
		}
	}
	t.Fatalf("gate %q did not appear in the verdict: %v", name, results)
	return GateResult{}
}

func resultsFor(results []GateResult, name string) []GateResult {
	var out []GateResult
	for _, r := range results {
		if r.Name == name {
			out = append(out, r)
		}
	}
	return out
}

// TestGates_IndicatorCapAboveTheServerCliffRefusesStartup.
//
// THE MUTANT IT KILLS: the one that compares against a literal `512`. The directive is server
// config — raising it on the server must APPROVE the same cap that refused before, and only
// reading does that. It also kills `Math.min(parsed, 1000)`, which accepts 800 without a word.
func TestGates_IndicatorCapAboveTheServerCliffRefusesStartup(t *testing.T) {
	base := GateInputs{IndicatorCap: 513, ZSetMaxListpackEntries: 512}

	refused := EvaluateGates(base)
	row := resultFor(t, refused, "indicator-cap-vs-listpack-cliff")
	if row.Severity != GateFatal {
		t.Fatalf("severity = %v, want GateFatal for cap 513 against ceiling 512: %s", row.Severity, row.Message)
	}
	err := FatalGateError(refused)
	if err == nil || !errors.Is(err, ErrStartupGate) {
		t.Fatalf("FatalGateError = %v, want an ErrStartupGate chain", err)
	}
	if !strings.Contains(err.Error(), "513") || !strings.Contains(err.Error(), "512") {
		t.Fatalf("the fatal error does not carry both read numbers: %v", err)
	}

	// Raising the directive ON THE SERVER approves the SAME cap. That is what a literal 512
	// cannot do.
	raised := base
	raised.ZSetMaxListpackEntries = 1024
	approved := EvaluateGates(raised)
	row = resultFor(t, approved, "indicator-cap-vs-listpack-cliff")
	if row.Severity == GateFatal {
		t.Fatalf("cap 513 kept refusing with the ceiling raised to 1024 — the number is not being read: %s", row.Message)
	}
	if !strings.Contains(row.Message, "1024") {
		t.Fatalf("the approved row does not cite the NEW ceiling: %s", row.Message)
	}
	if err := FatalGateError(approved); err != nil {
		t.Fatalf("FatalGateError = %v, want nil with the ceiling raised", err)
	}
}

// TestGates_CapEqualToTheCliffIsNotFatal — the cliff is the CROSSING (512→513), not 512.
func TestGates_CapEqualToTheCliffIsNotFatal(t *testing.T) {
	results := EvaluateGates(GateInputs{IndicatorCap: 512, ZSetMaxListpackEntries: 512})
	if row := resultFor(t, results, "indicator-cap-vs-listpack-cliff"); row.Severity == GateFatal {
		t.Fatalf("cap 512 == ceiling 512 refused; conversion only happens when CROSSING: %s", row.Message)
	}
}

// TestGates_UnreadNumberNeverProducesAVerdict — refusing against a reserve literal would be a
// sentence without evidence, the defect these gates remove.
func TestGates_UnreadNumberNeverProducesAVerdict(t *testing.T) {
	tests := []struct {
		name string
		in   GateInputs
	}{
		{name: "ceiling not read", in: GateInputs{IndicatorCap: 800, ZSetMaxListpackEntries: 0}},
		{name: "cap not read", in: GateInputs{IndicatorCap: 0, ZSetMaxListpackEntries: 512}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := EvaluateGates(tt.in)
			row := resultFor(t, results, "indicator-cap-vs-listpack-cliff")
			if row.Severity != GateWarn {
				t.Fatalf("severity = %v, want GateWarn when one number is missing: %s", row.Severity, row.Message)
			}
			if !strings.Contains(row.Message, "NOT EVALUATED") {
				t.Fatalf("the row does not declare that it did not evaluate: %s", row.Message)
			}
			if err := FatalGateError(results); err != nil {
				t.Fatalf("FatalGateError = %v, want nil — there is no verdict without both numbers", err)
			}
		})
	}
}

// TestGates_SeedCeilingWarnsWithBothNumbers — 400 > 500 − 120 = 380, and the gate reveals that
// the coupling IS already violated. It warns, never refuses: nothing breaks, but the sheet was
// lying.
func TestGates_SeedCeilingWarnsWithBothNumbers(t *testing.T) {
	results := EvaluateGates(GateInputs{
		IndicatorCap:     400,
		SeedCandlesLimit: 500,
		MinWarmupCandles: 120,
	})
	row := resultFor(t, results, "indicator-cap-vs-seed-ceiling")
	if row.Severity != GateWarn {
		t.Fatalf("severity = %v, want GateWarn: %s", row.Severity, row.Message)
	}
	for _, want := range []string{"400", "500", "120", "= 380"} {
		if !strings.Contains(row.Message, want) {
			t.Fatalf("the row does not carry %q: %s", want, row.Message)
		}
	}
	if err := FatalGateError(results); err != nil {
		t.Fatalf("the seed coupling refused the start: %v", err)
	}
}

// TestGates_DeclaredCapBelowTheBaselineFloorIsRaisedAndRecorded — a silent raise is how the
// declaration started diverging from what is resident.
func TestGates_DeclaredCapBelowTheBaselineFloorIsRaisedAndRecorded(t *testing.T) {
	results := EvaluateGates(GateInputs{
		DeclaredCapByTimeframe:   map[string]int64{"5m": 3, "15m": 291, "1d": 800},
		BaselineFloorByTimeframe: map[string]int64{"5m": 291, "15m": 291, "1d": 33},
	})
	rows := resultsFor(results, "declared-cap-vs-baseline-floor")
	var raised []string
	for _, r := range rows {
		if r.Severity == GateWarn {
			raised = append(raised, r.Message)
		}
	}
	if len(raised) != 1 {
		t.Fatalf("recorded raises = %d, want 1 (only 5m is below the floor): %v", len(raised), rows)
	}
	joined := strings.Join(raised, " | ")
	if !strings.Contains(joined, "5m: declared 3 BELOW the baseline floor 291") {
		t.Fatalf("the raise was not recorded with both numbers: %s", joined)
	}
	if err := FatalGateError(results); err != nil {
		t.Fatalf("the floor refused the start, and it raises — it does not refuse: %v", err)
	}
}

// TestGates_EvictionPolicyCrossesWithTheImmortalFamilies — the mutant it kills is the one that
// checks the policy WITHOUT crossing it with the presence of an immortal family: `volatile-ttl`
// alone is not a defect.
func TestGates_EvictionPolicyCrossesWithTheImmortalFamilies(t *testing.T) {
	tests := []struct {
		name       string
		in         GateInputs
		wantSever  GateSeverity
		wantNaming bool
	}{
		{
			name:       "volatile-ttl with an immortal family warns and NAMES the families",
			in:         GateInputs{MaxMemoryPolicy: "volatile-ttl", ImmortalFamilies: []string{"candles", "derivatives:series"}},
			wantSever:  GateWarn,
			wantNaming: true,
		},
		{
			name:      "volatile-ttl without an immortal family is not a defect",
			in:        GateInputs{MaxMemoryPolicy: "volatile-ttl"},
			wantSever: GateInfo,
		},
		{
			name:      "allkeys-lru with an immortal family approves",
			in:        GateInputs{MaxMemoryPolicy: "allkeys-lru", ImmortalFamilies: []string{"candles"}},
			wantSever: GateInfo,
		},
		// THE SHORTCUT THAT BYPASSES THE GATE. `volatile-lru` is NOT `volatile-ttl`, but it
		// evicts exactly the same keys — only those with a TTL. The immortal family stays
		// unrecoverable and Redis reaches OOM instead of evicting, which is the outcome this
		// gate exists to prevent. A gate that tests the STRING lets the shortcut through; one
		// that tests the PROPERTY does not.
		{
			name:       "volatile-lru with an immortal family also warns — the property is the same",
			in:         GateInputs{MaxMemoryPolicy: "volatile-lru", ImmortalFamilies: []string{"candles", "derivatives:series"}},
			wantSever:  GateWarn,
			wantNaming: true,
		},
		{
			name:      "volatile-lfu with an immortal family also warns",
			in:        GateInputs{MaxMemoryPolicy: "volatile-lfu", ImmortalFamilies: []string{"candles"}},
			wantSever: GateWarn,
		},
		{
			name:      "volatile-random with an immortal family also warns",
			in:        GateInputs{MaxMemoryPolicy: "volatile-random", ImmortalFamilies: []string{"candles"}},
			wantSever: GateWarn,
		},
		// THE EXTREME CASE: `noeviction` evicts NOTHING, not even a key with a TTL. If
		// `volatile-ttl` leaves the immortal family unrecoverable, this one leaves the whole
		// dataset.
		{
			name:      "noeviction with an immortal family warns — it evicts nothing",
			in:        GateInputs{MaxMemoryPolicy: "noeviction", ImmortalFamilies: []string{"candles"}},
			wantSever: GateWarn,
		},
		{
			name:      "allkeys-lfu approves like allkeys-lru — the whole family evicts without TTL",
			in:        GateInputs{MaxMemoryPolicy: "allkeys-lfu", ImmortalFamilies: []string{"candles"}},
			wantSever: GateInfo,
		},
		{
			name:      "an unread policy does not become a verdict",
			in:        GateInputs{ImmortalFamilies: []string{"candles"}},
			wantSever: GateWarn,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := resultFor(t, EvaluateGates(tt.in), "eviction-policy-vs-immortal-families")
			if row.Severity != tt.wantSever {
				t.Fatalf("severity = %v, want %v: %s", row.Severity, tt.wantSever, row.Message)
			}
			if tt.wantNaming && !strings.Contains(row.Message, "derivatives:series") {
				t.Fatalf("the immortal families were not named: %s", row.Message)
			}
		})
	}
}

// TestGates_PolicyBecomesFatalOnlyWhenTheSwitchIsFlipped.
//
// The families did NOT stop being immortal: `candles` and `derivatives:series` remain without
// TTL, and the cap is their retention.
//
// The gate refuses BOTH branches, by different invariants — `volatile-ttl` (and the whole
// complement of `evictsEverything`) by the resident family without TTL, `allkeys-*` by the sole
// copy still in Redis. No policy passes while the sole-copy migration has not run, and that is
// the design, not an accidental impasse.
func TestGates_PolicyBecomesFatalOnlyWhenTheSwitchIsFlipped(t *testing.T) {
	in := GateInputs{MaxMemoryPolicy: "volatile-ttl", ImmortalFamilies: []string{"candles"}}

	beforeF6 := EvaluateGates(in)
	if row := resultFor(t, beforeF6, "eviction-policy-vs-immortal-families"); row.Severity != GateWarn {
		t.Fatalf("with the switch off the policy must WARN, not %v: %s", row.Severity, row.Message)
	}
	if err := FatalGateError(beforeF6); err != nil {
		t.Fatalf("the policy refused the boot that would apply the fix: %v", err)
	}

	in.PolicyIsFatal = true
	afterF6 := EvaluateGates(in)
	if row := resultFor(t, afterF6, "eviction-policy-vs-immortal-families"); row.Severity != GateFatal {
		t.Fatalf("with the switch on the policy must REFUSE, not %v: %s", row.Severity, row.Message)
	}
	if err := FatalGateError(afterF6); !errors.Is(err, ErrStartupGate) {
		t.Fatalf("FatalGateError = %v, want an ErrStartupGate chain", err)
	}
}

// TestGates_ProjectionIsNeverFatal is the never-fatal rule stated as a property.
//
// THE MUTANT IT KILLS: the one that promotes the projection to fatal. Without hot reload, that
// turns "wrong config" into "service that will not start", and the operator cannot even start
// the process to fix the number that blocked the start.
func TestGates_ProjectionIsNeverFatal(t *testing.T) {
	tests := []struct {
		name      string
		projected float64
		wantSever GateSeverity
	}{
		{name: "within target", projected: 1000, wantSever: GateInfo},
		{name: "above the 60% target", projected: 2500, wantSever: GateWarn},
		{name: "above 100% of maxmemory", projected: 4000, wantSever: GateError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := EvaluateGates(GateInputs{
				ProjectedMiB:   tt.projected,
				MaxMemoryBytes: 3500 * (1 << 20),
				WarnRatio:      0.60,
				ErrorRatio:     1.00,
			})
			row := resultFor(t, results, "projection-vs-maxmemory")
			if row.Severity != tt.wantSever {
				t.Fatalf("severity = %v, want %v: %s", row.Severity, tt.wantSever, row.Message)
			}
			if err := FatalGateError(results); err != nil {
				t.Fatalf("the projection refused the start, and it NEVER can: %v", err)
			}
		})
	}
}

// TestGates_UndeclaredRatioIsReportedNotInvented — same rule as ServerFacts.Unreadable.
func TestGates_UndeclaredRatioIsReportedNotInvented(t *testing.T) {
	results := EvaluateGates(GateInputs{
		ProjectedMiB:    4000,
		MaxMemoryBytes:  3500 * (1 << 20),
		UsedMemoryBytes: 3400 * (1 << 20),
	})

	row := resultFor(t, results, "projection-vs-maxmemory")
	if row.Severity != GateWarn || !strings.Contains(row.Message, "target was NOT declared") {
		t.Fatalf("missing target became a verdict instead of a declaration: %v — %s", row.Severity, row.Message)
	}
	row = resultFor(t, results, "measured-occupancy-at-startup")
	if row.Severity != GateWarn || !strings.Contains(row.Message, "threshold was NOT declared") {
		t.Fatalf("missing measured threshold became a verdict: %v — %s", row.Severity, row.Message)
	}
}

// TestGates_MeasuredOccupancyWarnsOnAReadNumber — 90% measured is a read number, not an estimate,
// and it still warns: the service that does not start is the one that cannot relieve the
// pressure.
func TestGates_MeasuredOccupancyWarnsOnAReadNumber(t *testing.T) {
	results := EvaluateGates(GateInputs{
		MaxMemoryBytes:       3500 * (1 << 20),
		UsedMemoryBytes:      3400 * (1 << 20),
		MeasuredStartupRatio: 0.90,
	})
	row := resultFor(t, results, "measured-occupancy-at-startup")
	if row.Severity != GateWarn {
		t.Fatalf("severity = %v, want GateWarn: %s", row.Severity, row.Message)
	}
	if !strings.Contains(row.Message, "MEASURED") {
		t.Fatalf("the row does not say the number is measured: %s", row.Message)
	}
	if err := FatalGateError(results); err != nil {
		t.Fatalf("the measured occupancy refused the start: %v", err)
	}
}

// TestGates_OnlyTheDeterministicComparisonCanBeFatal is the never-fatal invariant asserted over
// the WHOLE table: with every other rule screaming, the boot still proceeds.
func TestGates_OnlyTheDeterministicComparisonCanBeFatal(t *testing.T) {
	everythingWrongButDeterministic := GateInputs{
		IndicatorCap:             400,
		ZSetMaxListpackEntries:   512,
		SeedCandlesLimit:         500,
		MinWarmupCandles:         120,
		DeclaredCapByTimeframe:   map[string]int64{"5m": 3},
		BaselineFloorByTimeframe: map[string]int64{"5m": 291},
		MaxMemoryPolicy:          "volatile-ttl",
		ImmortalFamilies:         []string{"candles", "derivatives:series", "indicators"},
		ProjectedMiB:             9000,
		MaxMemoryBytes:           3500 * (1 << 20),
		UsedMemoryBytes:          3490 * (1 << 20),
		WarnRatio:                0.60,
		ErrorRatio:               1.00,
		MeasuredStartupRatio:     0.90,
	}

	results := EvaluateGates(everythingWrongButDeterministic)
	if err := FatalGateError(results); err != nil {
		t.Fatalf("the start was refused without any comparison of two read numbers failing: %v", err)
	}

	var warns, errs int
	for _, r := range results {
		switch r.Severity {
		case GateWarn:
			warns++
		case GateError:
			errs++
		}
	}
	if warns < 3 || errs < 1 {
		t.Fatalf("the table went silent: %d warnings, %d errors — %v", warns, errs, results)
	}

	// And now the only one that can: a cap above the read ceiling.
	everythingWrongButDeterministic.IndicatorCap = 513
	if err := FatalGateError(EvaluateGates(everythingWrongButDeterministic)); !errors.Is(err, ErrStartupGate) {
		t.Fatalf("the deterministic comparison did not refuse: %v", err)
	}
}

// TestGates_TimeframeWithoutAnExplicitCapEntryIsNamed.
//
// THE MUTANT IT KILLS: the `default < MIN_WARMUP_CANDLES` gate. With `default=121` and `MIN=120`
// it NEVER fires — a gate that is not a gate. What actually happens is different: a new timeframe
// enters `tfReg.All()`, gets no entry of its own and inherits a default nobody chose for it.
func TestGates_TimeframeWithoutAnExplicitCapEntryIsNamed(t *testing.T) {
	in := GateInputs{
		RegisteredTimeframes:  []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"},
		CapEntriesByTimeframe: map[string]int64{"5m": 521, "15m": 521, "30m": 521, "1h": 521, "2h": 521, "4h": 521},
		CapConfigKey:          "candle_engine.redis_cap_by_timeframe",
	}

	row := resultFor(t, EvaluateGates(in), "timeframe-without-explicit-cap-entry")
	if row.Severity != GateWarn {
		t.Fatalf("severity = %v, want GateWarn: %s", row.Severity, row.Message)
	}
	// NAMING is the point: "some timeframe inherited the default" does not tell the operator
	// where to look.
	if !strings.Contains(row.Message, "1d, 1w") {
		t.Fatalf("the timeframes without an entry were not NAMED: %s", row.Message)
	}
	if !strings.Contains(row.Message, "candle_engine.redis_cap_by_timeframe") {
		t.Fatalf("the warning does not say WHICH key to edit: %s", row.Message)
	}
	if err := FatalGateError(EvaluateGates(in)); err != nil {
		t.Fatalf("inheriting the default refused the start, and it only warns: %v", err)
	}
}

// TestGates_ANewTimeframeIsCaughtTheMomentItIsRegistered — the exact scenario: register a new TF
// without an entry ⇒ warn naming the TF.
func TestGates_ANewTimeframeIsCaughtTheMomentItIsRegistered(t *testing.T) {
	covered := GateInputs{
		RegisteredTimeframes:  []string{"5m", "1h"},
		CapEntriesByTimeframe: map[string]int64{"5m": 521, "1h": 521},
		CapConfigKey:          "candle_engine.redis_cap_by_timeframe",
	}
	if row := resultFor(t, EvaluateGates(covered), "timeframe-without-explicit-cap-entry"); row.Severity != GateInfo {
		t.Fatalf("with every TF covered the gate must stay at INFO, not %v: %s", row.Severity, row.Message)
	}

	// The fleet gains "3m". Nothing else changes.
	covered.RegisteredTimeframes = append(covered.RegisteredTimeframes, "3m")
	row := resultFor(t, EvaluateGates(covered), "timeframe-without-explicit-cap-entry")
	if row.Severity != GateWarn || !strings.Contains(row.Message, "3m") {
		t.Fatalf("the new timeframe passed silently: %v — %s", row.Severity, row.Message)
	}
}

// TestGates_CapEntryKeyIsMatchedCaseInsensitively — the keys come from hand-written YAML, so `1D`
// and ` 1d` must reach the same timeframe as `1d`. A strict match would make the gate scream about
// a timeframe that HAS an entry, and a false warning is turned off in the second week.
func TestGates_CapEntryKeyIsMatchedCaseInsensitively(t *testing.T) {
	row := resultFor(t, EvaluateGates(GateInputs{
		RegisteredTimeframes:  []string{"1d"},
		CapEntriesByTimeframe: map[string]int64{"1D": 400},
		CapConfigKey:          "candle_engine.redis_cap_by_timeframe",
	}), "timeframe-without-explicit-cap-entry")
	if row.Severity != GateInfo {
		t.Fatalf("`1D` in the YAML did not reach the registered `1d`: %v — %s", row.Severity, row.Message)
	}
}

// ============================================================================
// THE SOLE-COPY RATCHET
// ============================================================================

// TestGates_SoleCopyBlocksTheEvictEverythingPolicy is the guard that protects the flip.
//
// `allkeys-lru` written in a compose file can be deployed by someone who never read the design.
// Under `allkeys-lru` the LEAST TOUCHED keys leave first, and `candles:*:1d`/`:1w` — 129,190
// candles that exist only in Redis — are the least touched in the keyspace. Documenting the order
// does not stop a `docker compose up`; this gate does.
func TestGates_SoleCopyBlocksTheEvictEverythingPolicy(t *testing.T) {
	tests := []struct {
		name      string
		in        GateInputs
		wantSever GateSeverity
	}{
		{
			// The policy that does not evict keys without a TTL does not reach the two cells,
			// so the remaining sole copy is not ITS risk. That discrimination separates this
			// gate from a generic alarm about pending migration.
			name:      "volatile-ttl with a remaining sole copy: the guard does not apply",
			in:        GateInputs{MaxMemoryPolicy: "volatile-ttl", SoleCopyEvaluated: true, SoleCopyKeysAtRisk: 297},
			wantSever: GateInfo,
		},
		{
			name:      "allkeys-lru with a remaining sole copy: REFUSES",
			in:        GateInputs{MaxMemoryPolicy: "allkeys-lru", SoleCopyEvaluated: true, SoleCopyKeysAtRisk: 297},
			wantSever: GateFatal,
		},
		{
			// The mutant: comparing against the string `allkeys-lru`. `allkeys-lfu` is a
			// legitimate alternative, and it puts the SAME keys in the queue.
			name:      "allkeys-lfu with a remaining sole copy: REFUSES the same",
			in:        GateInputs{MaxMemoryPolicy: "allkeys-lfu", SoleCopyEvaluated: true, SoleCopyKeysAtRisk: 1},
			wantSever: GateFatal,
		},
		{
			name:      "allkeys-random with a remaining sole copy: REFUSES the same",
			in:        GateInputs{MaxMemoryPolicy: "allkeys-random", SoleCopyEvaluated: true, SoleCopyKeysAtRisk: 1},
			wantSever: GateFatal,
		},
		{
			name:      "allkeys-lru after the migration: approves",
			in:        GateInputs{MaxMemoryPolicy: "allkeys-lru", SoleCopyEvaluated: true, SoleCopyKeysAtRisk: 0},
			wantSever: GateInfo,
		},
		{
			// NOT MEASURING IS NOT BEING CLEAN — but it does not stop the process either. A
			// fatal verdict over the ABSENCE of measurement would turn a momentarily down
			// QuestDB into a service that does not start, with no way back. The loudest alarm
			// that is not fatal.
			name:      "allkeys-lru with the sole copy NOT MEASURED: loud alarm, not fatal",
			in:        GateInputs{MaxMemoryPolicy: "allkeys-lru", SoleCopyEvaluated: false, SoleCopyDetail: "questdb fora do ar"},
			wantSever: GateError,
		},
		{
			name:      "policy not read",
			in:        GateInputs{SoleCopyEvaluated: true},
			wantSever: GateWarn,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := EvaluateGates(tt.in)
			row := resultFor(t, results, "sole-copy-vs-evict-everything-policy")
			if row.Severity != tt.wantSever {
				t.Fatalf("severity = %v, want %v: %s", row.Severity, tt.wantSever, row.Message)
			}

			// The ratchet only counts if it STOPS the start, not if it prints a line.
			err := FatalGateError(results)
			if tt.wantSever == GateFatal {
				if !errors.Is(err, ErrStartupGate) {
					t.Fatalf("FatalGateError = %v — the gate printed and let it pass", err)
				}
				if !strings.Contains(err.Error(), "sole-copy-vs-evict-everything-policy") {
					t.Fatalf("the fatal error does not name the rule: %v", err)
				}
			} else if errors.Is(err, ErrStartupGate) {
				t.Fatalf("the start was refused by a state that is not fatal: %v", err)
			}
		})
	}
}

// TestGates_SoleCopyFatalNamesWhereToLook — an alarm that does not say where to look becomes
// noise. The message carries the current policy, the count and the symbol sample.
func TestGates_SoleCopyFatalNamesWhereToLook(t *testing.T) {
	row := resultFor(t, EvaluateGates(GateInputs{
		MaxMemoryPolicy:    "allkeys-lru",
		SoleCopyEvaluated:  true,
		SoleCopyKeysAtRisk: 297,
		SoleCopyDetail:     "1d: 297 of 500 key(s) with a sole copy (12 with no durable row at all) [BTCUSDT ETHUSDT]",
	}), "sole-copy-vs-evict-everything-policy")

	for _, esperado := range []string{"allkeys-lru", "297", "RET-7", "BTCUSDT"} {
		if !strings.Contains(row.Message, esperado) {
			t.Errorf("the message does not carry %q: %s", esperado, row.Message)
		}
	}
}

// ============================================================================
// THE SYNCHRONY BETWEEN THE WHOLE TABLE AND THE PREFLIGHT
// ============================================================================

// fatalNames lists, in order, the rules that refused.
func fatalNames(results []GateResult) []string {
	var out []string
	for _, r := range results {
		if r.Severity == GateFatal {
			out = append(out, r.Name)
		}
	}
	sort.Strings(out)
	return out
}

// TestGates_EveryFatalVerdictIsAlsoProducedByThePreflight is the ratchet that prevents a fatal
// gate from being born outside the only point that ACTS on the verdict.
//
// The preflight (`EvaluatePreflightGates`) is what runs synchronously, before the fleet starts,
// and what returns an error to `App.Run`. The whole table runs later, on the sheet, and only
// prints. If a new fatal gate enters `EvaluateGates` and not the preflight, it becomes exactly the
// defect this fix removes: verdict produced, start not refused.
//
// The assertion is EQUALITY in both directions: the preflight also cannot refuse for a rule the
// whole table does not have.
func TestGates_EveryFatalVerdictIsAlsoProducedByThePreflight(t *testing.T) {
	tests := []struct {
		name      string
		in        GateInputs
		wantFatal []string
	}{
		{
			name:      "indicator cap above the read ceiling",
			in:        GateInputs{IndicatorCap: 513, ZSetMaxListpackEntries: 512},
			wantFatal: []string{"indicator-cap-vs-listpack-cliff"},
		},
		{
			name:      "volatile-ttl with an immortal family and the switch on",
			in:        GateInputs{MaxMemoryPolicy: "volatile-ttl", ImmortalFamilies: []string{"candles"}, PolicyIsFatal: true},
			wantFatal: []string{"eviction-policy-vs-immortal-families"},
		},
		// THE FATAL BRANCH, NOT JUST THE CLASSIFICATION. The generalization from `volatile-ttl`
		// to the complement of `evictsEverything` was proven at WARN severity — but production
		// runs with `PolicyIsFatal: true`, and it is the FATAL branch that actually refuses the
		// start.
		//
		// Without these two cases, a mutant that rewrites `if in.PolicyIsFatal {` as
		// `if in.PolicyIsFatal && in.MaxMemoryPolicy == "volatile-ttl" {` SURVIVES the whole
		// suite — that is, the shortcut the generalization exists to close could be reopened
		// without a single red test.
		{
			name:      "volatile-lru with an immortal family and the switch on IS FATAL",
			in:        GateInputs{MaxMemoryPolicy: "volatile-lru", ImmortalFamilies: []string{"candles"}, PolicyIsFatal: true},
			wantFatal: []string{"eviction-policy-vs-immortal-families"},
		},
		{
			name:      "noeviction with an immortal family and the switch on IS FATAL",
			in:        GateInputs{MaxMemoryPolicy: "noeviction", ImmortalFamilies: []string{"candles"}, PolicyIsFatal: true},
			wantFatal: []string{"eviction-policy-vs-immortal-families"},
		},
		{
			name:      "allkeys-lru with a remaining sole copy",
			in:        GateInputs{MaxMemoryPolicy: "allkeys-lru", SoleCopyEvaluated: true, SoleCopyKeysAtRisk: 297},
			wantFatal: []string{"sole-copy-vs-evict-everything-policy"},
		},
		{
			name: "both deterministic ones at the same time",
			in: GateInputs{
				IndicatorCap: 513, ZSetMaxListpackEntries: 512,
				MaxMemoryPolicy: "allkeys-lfu", SoleCopyEvaluated: true, SoleCopyKeysAtRisk: 1,
			},
			wantFatal: []string{"indicator-cap-vs-listpack-cliff", "sole-copy-vs-evict-everything-policy"},
		},
		{
			name: "durable column declared outside the erosion protection",
			in: GateInputs{
				DurableColumnsDeclared: []string{"funding_rate", "n_step"},
				ErosionGuardedColumns:  []string{"funding_rate"},
			},
			wantFatal: []string{"durable-columns-vs-erosion-guard"},
		},
		{
			name: "configured timeframe that no source serves",
			in: GateInputs{
				RegisteredTimeframes: []string{"1h", "3d"},
				TimeframeSupport: []TimeframeSupportRow{
					{Source: "venue", Feed: "funding_rate", Served: []string{"1h"}, Unserved: []string{"3d"}},
				},
			},
			wantFatal: []string{"timeframe-vs-any-source-support"},
		},
		{
			name: "cap shortening a cell with a field without a durable column",
			in: GateInputs{
				CapEntriesByTimeframe: map[string]int64{"30m": 291},
				DefaultSeriesCap:      800,
				DurableCellCoverage: DurableCellCoverage{
					FieldColumn:       map[string]string{"fundingRate": "funding_rate", "openInterest": ""},
					ReadableColumns:   []string{"timestamp", "funding_rate"},
					DurableTimeframes: []string{"30m"},
				},
			},
			wantFatal: []string{"cap-entry-vs-durable-cell-coverage"},
		},
		{
			// The whole table screaming WITHOUT any deterministic comparison failing: the
			// preflight must agree that nothing refuses. It is the half that stops this test
			// from approving a preflight that refuses for anything.
			name: "table screaming, no deterministic comparison fails",
			in: GateInputs{
				IndicatorCap: 400, ZSetMaxListpackEntries: 512,
				SeedCandlesLimit: 500, MinWarmupCandles: 120,
				DeclaredCapByTimeframe:   map[string]int64{"5m": 3},
				BaselineFloorByTimeframe: map[string]int64{"5m": 291},
				RegisteredTimeframes:     []string{"5m", "1d"},
				CapEntriesByTimeframe:    map[string]int64{"5m": 521},
				MaxMemoryPolicy:          "allkeys-lru",
				ImmortalFamilies:         []string{"candles", "derivatives:series"},
				PolicyIsFatal:            true,
				SoleCopyEvaluated:        true,
				ProjectedMiB:             9000,
				MaxMemoryBytes:           3500 * (1 << 20),
				UsedMemoryBytes:          3490 * (1 << 20),
				WarnRatio:                0.60,
				ErrorRatio:               1.00,
				MeasuredStartupRatio:     0.90,
			},
			wantFatal: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			full := fatalNames(EvaluateGates(tt.in))
			pre := fatalNames(EvaluatePreflightGates(tt.in))

			if strings.Join(full, ",") != strings.Join(tt.wantFatal, ",") {
				t.Fatalf("the whole table refused for [%s], want [%s]",
					strings.Join(full, ","), strings.Join(tt.wantFatal, ","))
			}
			if strings.Join(pre, ",") != strings.Join(full, ",") {
				t.Fatalf("the preflight refused for [%s] and the whole table for [%s] — "+
					"a fatal verdict outside the preflight is a verdict that does NOT refuse the start",
					strings.Join(pre, ","), strings.Join(full, ","))
			}

			preErr := FatalGateError(EvaluatePreflightGates(tt.in))
			if len(tt.wantFatal) == 0 {
				if preErr != nil {
					t.Fatalf("the preflight refused the start without any deterministic comparison failing: %v", preErr)
				}
				return
			}
			if !errors.Is(preErr, ErrStartupGate) {
				t.Fatalf("FatalGateError(preflight) = %v, want an ErrStartupGate chain", preErr)
			}
			for _, name := range tt.wantFatal {
				if !strings.Contains(preErr.Error(), name) {
					t.Fatalf("the preflight error does not name %q: %v", name, preErr)
				}
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE REFUSAL NAMES THE MISSING INVARIANT AND WHAT SATISFIES IT
//
// The new streamer enters a cage: under `allkeys-*` it refuses for a sole copy, under
// `volatile-ttl` it refuses for an immortal family. The two gates are complementary, so a refusal
// that only describes the found state sends the operator swapping one refusal for the other
// thinking progress was made — and this service has no hot reload, so wrong config means
// restarting the container against the same verdict.
//
// The criterion is not "the gate approves". It is: both approve, OR the refusal says which
// invariant is missing and what satisfies it.
// ─────────────────────────────────────────────────────────────────────────────

// TestGates_RefusalNamesTheMissingInvariantAndWhatSatisfiesIt is the case that kills the mutant
// returning a generic error without naming the invariant.
func TestGates_RefusalNamesTheMissingInvariantAndWhatSatisfiesIt(t *testing.T) {
	tests := []struct {
		name string
		in   GateInputs
		gate string
		// wantInvariant and wantSatisfier are fragments that MUST appear in each half.
		wantInvariant []string
		wantSatisfier []string
	}{
		{
			// THE CAGE, SIDE A: the policy that evicts everything accuses the sole copy, and
			// the satisfier must say that returning to volatile-ttl does NOT solve it.
			name: "allkeys-lru with a remaining sole copy",
			in: GateInputs{
				MaxMemoryPolicy:    "allkeys-lru",
				SoleCopyEvaluated:  true,
				SoleCopyKeysAtRisk: 297,
				SoleCopyDetail:     "1d: 297 of 500 key(s) with a sole copy",
			},
			gate:          "sole-copy-vs-evict-everything-policy",
			wantInvariant: []string{"sole-copy", "RET-7"},
			wantSatisfier: []string{"durable tier", "volatile-ttl"},
		},
		{
			// THE CAGE, SIDE B: the policy that preserves keys without a TTL accuses the
			// immortal family, and the satisfier must say that migration precedes the switch.
			name: "volatile-ttl with a resident family without TTL",
			in: GateInputs{
				MaxMemoryPolicy:  "volatile-ttl",
				ImmortalFamilies: []string{"candles", "derivatives:series"},
				PolicyIsFatal:    true,
			},
			gate:          "eviction-policy-vs-immortal-families",
			wantInvariant: []string{"recoverable", "memory pressure"},
			wantSatisfier: []string{"TTL", "allkeys-*", "RET-7"},
		},
		{
			name:          "indicator cap above the server cliff",
			in:            GateInputs{IndicatorCap: 900, ZSetMaxListpackEntries: 512},
			gate:          "indicator-cap-vs-listpack-cliff",
			wantInvariant: []string{"MAX_INDICATOR_VALUES", "zset-max-listpack-entries"},
			wantSatisfier: []string{"512", "900"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := EvaluateGates(tt.in)
			row := resultFor(t, results, tt.gate)
			if row.Severity != GateFatal {
				t.Fatalf("severity = %v, wanted GateFatal: %s", row.Severity, row.Message)
			}

			if row.Invariant == "" {
				t.Errorf("the refusal does not name the missing invariant: %s", row.Message)
			}
			if row.SatisfiedBy == "" {
				t.Errorf("the refusal does not say what satisfies it: %s", row.Message)
			}
			for _, frag := range tt.wantInvariant {
				if !strings.Contains(row.Invariant, frag) {
					t.Errorf("the declared invariant does not mention %q: %q", frag, row.Invariant)
				}
			}
			for _, frag := range tt.wantSatisfier {
				if !strings.Contains(row.SatisfiedBy, frag) {
					t.Errorf("the declared satisfier does not mention %q: %q", frag, row.SatisfiedBy)
				}
			}

			// AND THE ERROR THAT STOPS THE START CARRIES BOTH HALVES. Declaring them in the
			// struct and losing them in the collapse would be the same generic error with one
			// more field: whoever reads the boot log reads the error, not the struct.
			err := FatalGateError(results)
			if !errors.Is(err, ErrStartupGate) {
				t.Fatalf("FatalGateError = %v — the refusal does not stop the start", err)
			}
			if !strings.Contains(err.Error(), row.Invariant) {
				t.Errorf("the startup error does not carry the invariant %q: %v", row.Invariant, err)
			}
			if !strings.Contains(err.Error(), row.SatisfiedBy) {
				t.Errorf("the startup error does not carry the satisfier %q: %v", row.SatisfiedBy, err)
			}
		})
	}
}

// BOTH WAYS OUT OF THE CAGE EXIST, and that is what makes the satisfier a fulfillable promise
// instead of prose. Two are admitted: both gates approve, or the refusal is actionable.
func TestGates_BothEvictionGatesApproveOnceTheirInvariantsHold(t *testing.T) {
	// Exit A — the one the sole-copy satisfier points to: migration done, policy that evicts
	// everything. The immortal-family gate does not apply under allkeys-*.
	depoisDaMigracao := EvaluateGates(GateInputs{
		MaxMemoryPolicy:    "allkeys-lru",
		SoleCopyEvaluated:  true,
		SoleCopyKeysAtRisk: 0,
		ImmortalFamilies:   []string{"candles", "derivatives:series"},
		PolicyIsFatal:      true,
	})
	if err := FatalGateError(depoisDaMigracao); err != nil {
		t.Fatalf("with the migration done under allkeys-lru the start still refuses: %v", err)
	}

	// Exit B — the one the immortal-family satisfier points to: volatile-ttl with the families
	// already under a TTL. The sole copy is not a risk of this policy.
	semFamiliaImortal := EvaluateGates(GateInputs{
		MaxMemoryPolicy:    "volatile-ttl",
		PolicyIsFatal:      true,
		SoleCopyEvaluated:  true,
		SoleCopyKeysAtRisk: 297,
	})
	if err := FatalGateError(semFamiliaImortal); err != nil {
		t.Fatalf("with the families under a TTL the start still refuses: %v", err)
	}
}

// RATCHET: no REFUSAL is born without a way back.
//
// A new fatal gate that forgets the two halves stops the process saying only what it found. The
// ratchet is by equality over the whole table, in the same mold as the one that checks every fatal
// verdict is reproduced by the preflight.
func TestGates_EveryFatalVerdictDeclaresItsInvariantAndSatisfier(t *testing.T) {
	// TWO INPUTS, not one: the two eviction gates are MUTUALLY EXCLUSIVE by policy, so a single
	// input leaves half the fatal table out of reach — and a gate out of the ratchet's reach is
	// a gate it does not protect.
	entradas := []GateInputs{
		{
			IndicatorCap: 900, ZSetMaxListpackEntries: 512,
			MaxMemoryPolicy:  "volatile-ttl",
			ImmortalFamilies: []string{"candles"}, PolicyIsFatal: true,
			SoleCopyEvaluated: true, SoleCopyKeysAtRisk: 297,
		},
		{
			IndicatorCap: 900, ZSetMaxListpackEntries: 512,
			MaxMemoryPolicy:  "allkeys-lru",
			ImmortalFamilies: []string{"candles"}, PolicyIsFatal: true,
			SoleCopyEvaluated: true, SoleCopyKeysAtRisk: 297,
			// THE FOURTH FATAL, which the two eviction inputs do not reach: it depends on no
			// memory policy, and without this line the ratchet would measure three.
			DurableColumnsDeclared: []string{"funding_rate", "n_step"},
			ErosionGuardedColumns:  []string{"funding_rate"},
			// AND THE FIFTH, for the same reason: it compares the registry against the support
			// matrix and looks at no memory at all. Without this line the ratchet would measure
			// four.
			RegisteredTimeframes: []string{"1h", "3d"},
			TimeframeSupport: []TimeframeSupportRow{
				{Source: "venue", Feed: "funding_rate", Served: []string{"1h"}, Unserved: []string{"3d"}},
			},
			// AND THE SIXTH, which also looks at no memory: it crosses the cap entry with the
			// durable coverage PER FIELD. Without these lines the ratchet would measure five,
			// and a fatal outside its reach is a fatal it does not protect.
			CapEntriesByTimeframe: map[string]int64{"5m": 291},
			DefaultSeriesCap:      800,
			DurableCellCoverage: DurableCellCoverage{
				FieldColumn:       map[string]string{"fundingRate": "funding_rate", "openInterest": ""},
				ReadableColumns:   []string{"timestamp", "funding_rate"},
				DurableTimeframes: []string{"5m"},
			},
		},
	}

	seen := map[string]struct{}{}
	for _, in := range entradas {
		for _, r := range EvaluateGates(in) {
			if r.Severity != GateFatal {
				continue
			}
			seen[r.Name] = struct{}{}
			if r.Invariant == "" || r.SatisfiedBy == "" {
				t.Errorf("the fatal gate %q refuses without a way back (invariant=%q, satisfied by=%q)",
					r.Name, r.Invariant, r.SatisfiedBy)
			}
			if !strings.Contains(r.String(), "INVARIANT MISSING") {
				t.Errorf("the log line of fatal gate %q does not show the invariant: %s", r.Name, r.String())
			}
		}
	}

	// AND THE RATCHET CHECKS THAT IT REACHED ALL SIX. Without this it would pass while measuring
	// two.
	for _, name := range []string{
		"indicator-cap-vs-listpack-cliff",
		"eviction-policy-vs-immortal-families",
		"sole-copy-vs-evict-everything-policy",
		"durable-columns-vs-erosion-guard",
		"timeframe-vs-any-source-support",
		"cap-entry-vs-durable-cell-coverage",
	} {
		if _, ok := seen[name]; !ok {
			t.Errorf("the ratchet did not reach the fatal gate %q — it would be born without a way back without anyone noticing", name)
		}
	}
}

// AND THE OMISSION SHOWS UP, instead of becoming a generic error. A fatal built without the two
// halves must accuse its own lack on the line the operator reads.
func TestGates_AFatalWithoutADeclaredInvariantSaysSoOutLoud(t *testing.T) {
	silent := GateResult{Name: "gate-without-a-way-back", Severity: GateFatal, Message: "state found"}
	if !strings.Contains(silent.String(), "INVARIANT NOT DECLARED") {
		t.Errorf("a fatal without an invariant passed as an ordinary refusal: %s", silent.String())
	}
	if err := FatalGateError([]GateResult{silent}); !strings.Contains(err.Error(), "INVARIANT NOT DECLARED") {
		t.Errorf("the startup error hid the omission: %v", err)
	}

	// And a verdict that does NOT stop the process does not need a way back: it is not accused,
	// because the process started.
	aviso := GateResult{Name: "gate-that-warns", Severity: GateWarn, Message: "state found"}
	if strings.Contains(aviso.String(), "INVARIANTE") {
		t.Errorf("a WARN was charged for a way back it does not need: %s", aviso.String())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE ORDER: PROTECTION BEFORE THE FIRST NEW COLUMN
//
// Dedup erases the omitted column on a rewrite. A column that is written and outside the
// protection is destroyed on the first degraded cycle, and the erased value does not come back.
// The order between the two things is therefore a data-safety rule — and a safety rule that
// depends on someone remembering the order is the one found broken elsewhere.
// ─────────────────────────────────────────────────────────────────────────────

func TestGates_ADeclaredColumnOutsideTheErosionGuardRefusesTheBoot(t *testing.T) {
	results := EvaluateGates(GateInputs{
		DurableColumnsDeclared: []string{"funding_rate", "funding_interval_hours", "n_step"},
		ErosionGuardedColumns:  []string{"funding_rate", "open_interest_base"},
	})

	var gate *GateResult
	for i := range results {
		if results[i].Name == "durable-columns-vs-erosion-guard" {
			gate = &results[i]
		}
	}
	if gate == nil {
		t.Fatal("the order gate is not in the table")
	}
	if gate.Severity != GateFatal {
		t.Fatalf("severity = %s, wanted GateFatal — starting this way erases the column on the "+
			"first degraded cycle, and erased does not come back", gate.Severity)
	}
	// BOTH unprotected columns are named: a refusal citing only the first sends the operator to
	// fix half and restart into the same verdict.
	for _, col := range []string{"funding_interval_hours", "n_step"} {
		if !strings.Contains(gate.Message, col) {
			t.Errorf("the refusal does not name %q: %s", col, gate.Message)
		}
	}
	if strings.Contains(gate.Message, "open_interest_base") {
		t.Errorf("the refusal accuses a protected column: %s", gate.Message)
	}
	if gate.Invariant == "" || gate.SatisfiedBy == "" {
		t.Fatalf("the refusal has no way back (invariant=%q, satisfied by=%q)",
			gate.Invariant, gate.SatisfiedBy)
	}
	if !strings.Contains(gate.SatisfiedBy, "n_step") {
		t.Errorf("the satisfier does not say which column to teach the protection: %s", gate.SatisfiedBy)
	}
}

func TestGates_ColumnsFullyGuardedDoNotRefuseTheBoot(t *testing.T) {
	results := EvaluateGates(GateInputs{
		DurableColumnsDeclared: []string{"funding_rate", "open_interest_base"},
		ErosionGuardedColumns: []string{
			"open_interest_base", "funding_rate", "funding_rate_8h", "long_short_ratio",
		},
	})
	for _, r := range results {
		if r.Name == "durable-columns-vs-erosion-guard" && r.Severity == GateFatal {
			t.Fatalf("refused with every declared column under protection: %s", r.Message)
		}
	}
}

// TestGates_AnUnreadColumnSetIsNotAVerdict — the same rule as the other gates: an unread side
// does not become a sentence. It is what keeps the preflight evaluable in a process that has not
// resolved the declaration yet.
func TestGates_AnUnreadColumnSetIsNotAVerdict(t *testing.T) {
	cases := map[string]GateInputs{
		"neither side read": {},
		"only the declaration read": {
			DurableColumnsDeclared: []string{"n_step"},
		},
		"only the protection read": {
			ErosionGuardedColumns: []string{"funding_rate"},
		},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			for _, r := range EvaluateGates(in) {
				if r.Name != "durable-columns-vs-erosion-guard" {
					continue
				}
				if r.Severity != GateWarn {
					t.Fatalf("severity = %s with one unread side, wanted GateWarn", r.Severity)
				}
				if !strings.Contains(r.Message, "NOT EVALUATED") {
					t.Fatalf("the verdict does not declare that it did not evaluate: %s", r.Message)
				}
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// A TIMEFRAME THAT NO SOURCE SERVES REFUSES, BY NAME.
//
// What it replaces: a WARN every cycle, forever. `1w` once produced a warning line every hour —
// the shape a defect takes when nobody decides what to do with it.
// ─────────────────────────────────────────────────────────────────────────────

// THE MUTANT THIS CASE KILLS: degrading the refusal to a log line and letting the service start.
// The three assertions surround it — the severity, the timeframe name in the message, and the
// error `FatalGateError` returns, which is what makes `main` terminate instead of serve.
func TestGates_ATimeframeNoSourceServesRefusesTheBootByName(t *testing.T) {
	in := GateInputs{
		RegisteredTimeframes: []string{"5m", "1h", "3d"},
		TimeframeSupport: []TimeframeSupportRow{
			{Source: "venue", Feed: "open_interest", Served: []string{"5m", "1h"}, Unserved: []string{"3d"}},
			{Source: "venue", Feed: "funding_rate", Served: []string{"5m", "1h"}, Unserved: []string{"3d"}},
			{Source: "coinalyze", Feed: "open_interest", Served: []string{"5m", "1h"}, Unserved: []string{"3d"}},
		},
	}

	results := EvaluateGates(in)
	found := false
	for _, r := range results {
		if r.Name != "timeframe-vs-any-source-support" {
			continue
		}
		found = true
		if r.Severity != GateFatal {
			t.Fatalf("severity %s — a timeframe without a source must REFUSE, not warn per cycle", r.Severity)
		}
		if !strings.Contains(r.Message, "3d") {
			t.Errorf("the refusal does not name the timeframe: %s", r.Message)
		}
		if r.Invariant == "" || r.SatisfiedBy == "" {
			t.Errorf("the refusal does not bring the way back (invariant=%q, satisfied by=%q)", r.Invariant, r.SatisfiedBy)
		}
	}
	if !found {
		t.Fatal("the gate did not appear in the table")
	}

	// AND THE REFUSAL BECOMES AN ERROR. Without this the verdict would be a function value
	// nobody reads — literally the defect `startup_gates.go` was created to remove.
	err := FatalGateError(results)
	if !errors.Is(err, ErrStartupGate) {
		t.Fatalf("the refusal did not become a startup error: %v", err)
	}
	if !strings.Contains(err.Error(), "3d") {
		t.Errorf("the startup error does not name the timeframe: %v", err)
	}
}

// ONE SOURCE IS ENOUGH, AND THE VENUE'S SETTLEMENT FEED IS IT — this is the case that stops the
// gate from refusing the weekly.
//
// `1w` has no DURABLE tier, which is a different thing and is declared to the client. It HAS
// data: measured on the LAN, 30 weekly points by the route, 27 with `fundingRate8h`, because
// settlement is an EVENT and the fan-out reaches any bucket. A gate that refused it would kill
// the boot because of data that exists.
func TestGates_TheWeeklyIsNotRefusedBecauseOneFeedStillServesIt(t *testing.T) {
	registro := []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"}
	in := GateInputs{
		RegisteredTimeframes: registro,
		TimeframeSupport: []TimeframeSupportRow{
			// The venue: OI and LSR do not serve the weekly; settlement serves everything.
			{Source: "venue", Feed: "open_interest", Served: []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d"}, Unserved: []string{"1w"}},
			{Source: "venue", Feed: "long_short_ratio", Served: []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d"}, Unserved: []string{"1w"}},
			{Source: "venue", Feed: "funding_rate", Served: registro},
			// The durable aggregator serves the weekly in no feed.
			{Source: "coinalyze", Feed: "open_interest", Served: []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d"}, Unserved: []string{"1w"}},
			{Source: "coinalyze", Feed: "long_short_ratio", Served: []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d"}, Unserved: []string{"1w"}},
			{Source: "coinalyze", Feed: "funding_rate", Served: []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d"}, Unserved: []string{"1w"}},
		},
	}

	if err := FatalGateError(EvaluateGates(in)); err != nil {
		t.Fatalf("the start was refused for a timeframe that ONE source serves: %v", err)
	}
	for _, r := range EvaluateGates(in) {
		if r.Name == "timeframe-vs-any-source-support" && r.Severity != GateInfo {
			t.Errorf("verdict %s over a fully served registry: %s", r.Severity, r.Message)
		}
	}
}

// "WE NEVER MEASURED" DOES NOT REFUSE, and does not disappear: it leaves as a warning naming the
// timeframe. Without this separation, a missing measurement would become a boot refusal — a
// sentence without evidence.
func TestGates_AnUnmeasuredTimeframeWarnsInsteadOfRefusing(t *testing.T) {
	in := GateInputs{
		RegisteredTimeframes: []string{"1h", "7m"},
		TimeframeSupport: []TimeframeSupportRow{
			{Source: "venue", Feed: "funding_rate", Served: []string{"1h"}, Unserved: []string{"1w"}},
		},
	}

	if err := FatalGateError(EvaluateGates(in)); err != nil {
		t.Fatalf("a missing measurement refused the start: %v", err)
	}
	for _, r := range EvaluateGates(in) {
		if r.Name != "timeframe-vs-any-source-support" {
			continue
		}
		if r.Severity != GateWarn {
			t.Errorf("severity %s, want warn", r.Severity)
		}
		if !strings.Contains(r.Message, "7m") {
			t.Errorf("the warning does not name the unmeasured timeframe: %s", r.Message)
		}
	}
}

// AN UNREAD MATRIX DOES NOT BECOME A VERDICT, the same rule as the other gates.
func TestGates_AnUnreadSupportMatrixIsNotAVerdict(t *testing.T) {
	tests := []struct {
		name string
		in   GateInputs
	}{
		{"matrix missing", GateInputs{RegisteredTimeframes: []string{"1h", "3d"}}},
		{"registry missing", GateInputs{TimeframeSupport: []TimeframeSupportRow{{Source: "venue", Feed: "funding_rate", Unserved: []string{"3d"}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := FatalGateError(EvaluateGates(tt.in)); err != nil {
				t.Fatalf("verdict without both sides read: %v", err)
			}
			for _, r := range EvaluateGates(tt.in) {
				if r.Name == "timeframe-vs-any-source-support" && !strings.Contains(r.Message, "NOT EVALUATED") {
					t.Errorf("the gate decided without both sides: %s", r.Message)
				}
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NOTHING IS CAPPED BEFORE DURABLE WRITTEN AND READABLE DATA EXISTS.
//
// The rule already existed in writing and was applied BACKWARDS: `30m` gained a cap entry and
// 254 hours of series vanished. These cases are the same rule as a comparison that runs, and each
// one kills one of the two mutants named in review.
// ─────────────────────────────────────────────────────────────────────────────

const capCoverageGate = "cap-entry-vs-durable-cell-coverage"

// fullCoverage is the POSITIVE CONTROL: every member quantity has a column, every column is
// asked for by the read, and the timeframe is in the durable tier. Without it the two refusal
// cases below would pass in a gate that always refuses, and a gate that always refuses does not
// discriminate.
func fullCoverage() DurableCellCoverage {
	return DurableCellCoverage{
		FieldColumn: map[string]string{
			"openInterestBase": "open_interest_base",
			"fundingRate":      "funding_rate",
			"longShortRatio":   "long_short_ratio",
		},
		ReadableColumns:   []string{"timestamp", "open_interest_base", "funding_rate", "long_short_ratio"},
		DurableTimeframes: []string{"5m", "1h"},
	}
}

// TestGates_ACapEntryOnACellWithAFieldWithoutAColumnRefusesTheBoot ⭐
//
// THE MUTANT IT KILLS (a): the one that checks coverage ONLY PER TIMEFRAME. Here `5m` is in the
// durable tier and every declared column is readable — a gate that asked "does this timeframe
// have durable data?" would approve the cap. What is missing is on the FIELD axis: notional
// `openInterest` has no column at all, and the cap removes the MEMBER, which is where it lives.
func TestGates_ACapEntryOnACellWithAFieldWithoutAColumnRefusesTheBoot(t *testing.T) {
	coverage := fullCoverage()
	coverage.FieldColumn["openInterest"] = ""

	gate := resultFor(t, EvaluateGates(GateInputs{
		CapEntriesByTimeframe: map[string]int64{"5m": 291},
		DefaultSeriesCap:      800,
		DurableCellCoverage:   coverage,
	}), capCoverageGate)

	if gate.Severity != GateFatal {
		t.Fatalf("severity = %s, wanted GateFatal — capping here erases the whole member, and the field without a column goes with it", gate.Severity)
	}
	if !strings.Contains(gate.Message, "5m") {
		t.Errorf("the refusal does not name the cell: %s", gate.Message)
	}
	if !strings.Contains(gate.Message, "openInterest ") {
		t.Errorf("the refusal does not name the uncovered field: %s", gate.Message)
	}
	if !strings.Contains(gate.Message, "291") || !strings.Contains(gate.Message, "800") {
		t.Errorf("the refusal does not show the shortening (291 below 800): %s", gate.Message)
	}
	if gate.Invariant == "" || gate.SatisfiedBy == "" {
		t.Fatalf("the refusal has no way back (invariant=%q, satisfied by=%q)", gate.Invariant, gate.SatisfiedBy)
	}
	if !errors.Is(FatalGateError(EvaluatePreflightGates(GateInputs{
		CapEntriesByTimeframe: map[string]int64{"5m": 291},
		DefaultSeriesCap:      800,
		DurableCellCoverage:   coverage,
	})), ErrStartupGate) {
		t.Fatal("the preflight did not refuse the start — a verdict outside it is a verdict that refuses nothing")
	}
}

// TestGates_AWrittenColumnTheRouteDoesNotAskForIsNotCoverage ⭐
//
// THE MUTANT IT KILLS (b): the one that accepts a WRITTEN column the route does not return. Here
// every field has a column and the timeframe is in the durable tier; what is missing is the read
// asking for `long_short_ratio`. There is no `SELECT *` in the module, so that column never
// reaches the client — and capping its hot window erases the only deliverable copy.
func TestGates_AWrittenColumnTheRouteDoesNotAskForIsNotCoverage(t *testing.T) {
	coverage := fullCoverage()
	coverage.ReadableColumns = []string{"timestamp", "open_interest_base", "funding_rate"}

	gate := resultFor(t, EvaluateGates(GateInputs{
		CapEntriesByTimeframe: map[string]int64{"5m": 291},
		DefaultSeriesCap:      800,
		DurableCellCoverage:   coverage,
	}), capCoverageGate)

	if gate.Severity != GateFatal {
		t.Fatalf("severity = %s, wanted GateFatal — a written column nobody asked for reaches no consumer", gate.Severity)
	}
	if !strings.Contains(gate.Message, "longShortRatio") || !strings.Contains(gate.Message, "long_short_ratio") {
		t.Errorf("the refusal does not name the unreachable field and column: %s", gate.Message)
	}
	if !strings.Contains(gate.Message, "read does NOT ask for it") {
		t.Errorf("the refusal does not say WHICH of the two halves is missing, and the fixes are different: %s", gate.Message)
	}
}

// TestGates_ACellCoveredInEveryMemberFieldAcceptsTheCap is the other end: with writing AND
// readability in every member field, the declared cap passes. Without this case the two above
// would be satisfied by a gate that refuses any entry.
func TestGates_ACellCoveredInEveryMemberFieldAcceptsTheCap(t *testing.T) {
	results := EvaluateGates(GateInputs{
		CapEntriesByTimeframe: map[string]int64{"5m": 291},
		DefaultSeriesCap:      800,
		DurableCellCoverage:   fullCoverage(),
	})
	gate := resultFor(t, results, capCoverageGate)
	if gate.Severity != GateInfo {
		t.Fatalf("severity = %s with full per-field coverage, wanted INFO: %s", gate.Severity, gate.Message)
	}
	if err := FatalGateError(results); err != nil {
		t.Fatalf("the start was refused with the cell fully covered: %v", err)
	}
}

// TestGates_ACapEntryThatDoesNotShortenIsNotRefused — the gate asks about SHORTENING, not about
// the existence of an entry. An entry equal to or above the default removes no member, and
// refusing for it would confuse "the operator wrote a number" with "the operator erased data".
func TestGates_ACapEntryThatDoesNotShortenIsNotRefused(t *testing.T) {
	coverage := fullCoverage()
	coverage.FieldColumn["openInterest"] = ""

	for name, entry := range map[string]int64{"equal to the default": 800, "above the default": 2000} {
		t.Run(name, func(t *testing.T) {
			gate := resultFor(t, EvaluateGates(GateInputs{
				CapEntriesByTimeframe: map[string]int64{"1h": entry},
				DefaultSeriesCap:      800,
				DurableCellCoverage:   coverage,
			}), capCoverageGate)
			if gate.Severity != GateInfo {
				t.Fatalf("severity = %s for entry %d that shortens nothing: %s", gate.Severity, entry, gate.Message)
			}
		})
	}
}

// TestGates_AnUnreadCapCoverageIsNotAVerdict — the same rule as the other gates: an unread side
// does not become a sentence.
func TestGates_AnUnreadCapCoverageIsNotAVerdict(t *testing.T) {
	cases := map[string]GateInputs{
		"coverage not read": {
			CapEntriesByTimeframe: map[string]int64{"5m": 291},
			DefaultSeriesCap:      800,
		},
		"default cap not read": {
			CapEntriesByTimeframe: map[string]int64{"5m": 291},
			DurableCellCoverage:   fullCoverage(),
		},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			gate := resultFor(t, EvaluateGates(in), capCoverageGate)
			if gate.Severity != GateWarn {
				t.Fatalf("severity = %s with one unread side, wanted GateWarn", gate.Severity)
			}
			if !strings.Contains(gate.Message, "NOT EVALUATED") {
				t.Fatalf("the verdict does not declare that it did not evaluate: %s", gate.Message)
			}
		})
	}
}

// TestUncoveredMemberFields_EachAbsenceCarriesItsOwnRepair — the three reasons are different
// fixes, and a refusal that collapsed them would make the operator guess which to apply.
func TestUncoveredMemberFields_EachAbsenceCarriesItsOwnRepair(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*DurableCellCoverage)
		timeframe string
		want      string
	}{
		{
			name:      "field with no column at all",
			mutate:    func(c *DurableCellCoverage) { c.FieldColumn["openInterest"] = "" },
			timeframe: "5m",
			want:      "openInterest (no durable column",
		},
		{
			name: "written column the read does not ask for",
			mutate: func(c *DurableCellCoverage) {
				c.ReadableColumns = []string{"timestamp", "funding_rate", "long_short_ratio"}
			},
			timeframe: "5m",
			want:      "openInterestBase (open_interest_base is written and the read does NOT ask for it)",
		},
		{
			name:      "timeframe outside the durable tier",
			mutate:    func(_ *DurableCellCoverage) {},
			timeframe: "1w",
			want:      "fundingRate (funding_rate outside the durable tier for 1w)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coverage := fullCoverage()
			tt.mutate(&coverage)
			achados := coverage.UncoveredMemberFields(tt.timeframe)
			if len(achados) == 0 {
				t.Fatalf("no finding for %q — the absence became invisible", tt.timeframe)
			}
			if !strings.Contains(strings.Join(achados, " | "), tt.want) {
				t.Fatalf("findings = %v, wanted one containing %q", achados, tt.want)
			}
		})
	}

	// And UNREAD coverage produces no finding: `nil` is "nobody asked", not "nothing covered",
	// and the two would lead to opposite verdicts.
	var naoLida DurableCellCoverage
	if got := naoLida.UncoveredMemberFields("5m"); got != nil {
		t.Fatalf("unread coverage produced finding %v — a missing measurement became absence", got)
	}
}

// TestGates_NoPolicySwitchClearsBothEvictionGatesWithoutMigrating states it in full: NO POLICY
// SWITCH CLEARS BOTH GATES WITHOUT MIGRATING.
//
// WHAT IT PROVES AND WHAT THE SUITE ALREADY PROVED ARE DIFFERENT THINGS. Each gate already had an
// isolated test, and the fatal branch already covered `volatile-lru` and `noeviction`. What was
// missing is the CONJUNCTION — which is exactly the shape of the measured shortcut. An operator
// under pressure does not reclassify a gate: they type `CONFIG SET maxmemory-policy
// volatile-lru`, a command with no build, and that left BOTH gates green at the same time, with
// the 129,190 sole-copy candles exactly where they were. One test per gate does not see that
// outcome; only the conjunction does.
//
// THE FAMILY BY PROPERTY, NOT BY STRING. The table is the CLOSED set of the eight eviction
// policies Redis admits, and the assertion is the same for all: with a resident family without a
// TTL and a sole copy still in Redis, SOME of the two gates refuses. The two halves are
// complementary by construction (`evictsEverything`), so the pair leaves no gap — and that is
// what the test asserts, instead of asserting a name.
func TestGates_NoPolicySwitchClearsBothEvictionGatesWithoutMigrating(t *testing.T) {
	const (
		gateFamiliaImortal = "eviction-policy-vs-immortal-families"
		gateCopiaUnica     = "sole-copy-vs-evict-everything-policy"
	)

	// The state measured in production BEFORE the migration: resident families without a TTL
	// and a sole copy still in Redis. The veto applies in this state.
	antesDaMigracao := func(policy string) GateInputs {
		return GateInputs{
			MaxMemoryPolicy:    policy,
			ImmortalFamilies:   []string{"candles", "derivatives:series"},
			PolicyIsFatal:      true,
			SoleCopyEvaluated:  true,
			SoleCopyKeysAtRisk: 297,
		}
	}

	tests := []struct {
		policy      string
		quemReprova string
	}{
		// The WHOLE `volatile-*` family. None of them evicts a key WITHOUT a TTL, so all leave
		// the immortal family out of the queue and take Redis to OOM instead of eviction.
		// `volatile-ttl` is the one the compose file declares; the other three are a
		// one-command shortcut.
		{policy: "volatile-ttl", quemReprova: gateFamiliaImortal},
		{policy: "volatile-lru", quemReprova: gateFamiliaImortal},
		{policy: "volatile-lfu", quemReprova: gateFamiliaImortal},
		{policy: "volatile-random", quemReprova: gateFamiliaImortal},
		// The extreme of the same property: it does not evict even a key WITH a TTL.
		{policy: "noeviction", quemReprova: gateFamiliaImortal},
		// The complement: it evicts everything, so the immortal family IS recoverable — and
		// the risk becomes the sole copy, which is the other gate. Neither exit is left
		// unguarded.
		{policy: "allkeys-lru", quemReprova: gateCopiaUnica},
		{policy: "allkeys-lfu", quemReprova: gateCopiaUnica},
		{policy: "allkeys-random", quemReprova: gateCopiaUnica},
	}

	for _, tt := range tests {
		t.Run(tt.policy+" does not clear both gates", func(t *testing.T) {
			in := antesDaMigracao(tt.policy)
			results := EvaluatePreflightGates(in)

			if got := resultFor(t, results, tt.quemReprova).Severity; got != GateFatal {
				t.Fatalf("under %q the gate %s = %v, want %v — the policy cleared the gate that exists to refuse it",
					tt.policy, tt.quemReprova, got, GateFatal)
			}

			err := FatalGateError(results)
			if !errors.Is(err, ErrStartupGate) {
				t.Fatalf("under %q the start was NOT refused (%v) — both gates stayed clean with "+
					"the sole copy exactly where it was", tt.policy, err)
			}
			if !strings.Contains(err.Error(), tt.quemReprova) {
				t.Fatalf("the refusal under %q does not name %s: %v", tt.policy, tt.quemReprova, err)
			}
		})
	}
}

// TestGates_TheVolatileFamilyRefusesWhileAnImmortalFamilyIsResident is the other half: the veto
// holds WHILE A RESIDENT FAMILY WITHOUT A TTL EXISTS — not while a sole copy exists.
//
// THE MUTANT IT KILLS, and the conjunction above does NOT: conditioning the immortal-family
// refusal on sole-copy risk (`if in.PolicyIsFatal && in.SoleCopyKeysAtRisk > 0`). With the
// migration done, the sibling gate approves by construction; if this one yielded too,
// `volatile-lru` would become a PERMANENT destination, with `candles` and `derivatives:series`
// again out of the eviction queue in a Redis that has already measured 89.6% occupancy. The veto
// is not a migration toll: it is the property of the policy against the family that resides.
func TestGates_TheVolatileFamilyRefusesWhileAnImmortalFamilyIsResident(t *testing.T) {
	const gateFamiliaImortal = "eviction-policy-vs-immortal-families"

	for _, policy := range []string{"volatile-ttl", "volatile-lru", "volatile-lfu", "volatile-random"} {
		t.Run(policy+" keeps refusing with the migration done", func(t *testing.T) {
			results := EvaluatePreflightGates(GateInputs{
				MaxMemoryPolicy:    policy,
				ImmortalFamilies:   []string{"candles", "derivatives:series"},
				PolicyIsFatal:      true,
				SoleCopyEvaluated:  true,
				SoleCopyKeysAtRisk: 0,
			})

			if got := resultFor(t, results, gateFamiliaImortal).Severity; got != GateFatal {
				t.Fatalf("with the migration done, %q took gate %s to %v, want %v — the resident family "+
					"without a TTL remains unrecoverable under this policy", policy, gateFamiliaImortal, got, GateFatal)
			}
			if err := FatalGateError(results); !errors.Is(err, ErrStartupGate) {
				t.Fatalf("with the migration done, %q started approving the start: %v", policy, err)
			}
		})
	}
}
