package doctor

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeCompose writes a test compose and returns the path.
func writeCompose(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "compose.yml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

// writeConfig writes a test configuration YAML and returns the path.
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

// composeDeclarando returns a compose with the requested policy AND the listpack step, so that the
// probe has both declarations the step gate compares.
func composeDeclarando(t *testing.T, policy string, listpack int64) string {
	t.Helper()
	return writeCompose(t, fmt.Sprintf(`
services:
  redis:
    command:
      - redis-server
      - --maxmemory-policy
      - %s
      - --zset-max-listpack-entries
      - "%d"
`, policy, listpack))
}

// configMinima declares the two things the gate machine reads from the configuration: the timeframe
// registry and the derivatives block. The timeframes are three that the support matrix declares
// served, so that the per-timeframe support gate is EVALUATED and passes — and does not merely fall
// silent.
func configMinima(t *testing.T) string {
	t.Helper()
	return writeConfig(t, `
candle_engine:
  timeframes: ["1h", "4h", "1d"]
derivatives:
  max_series_size: 800
`)
}

// Paths of the public example artifacts. The test runs from
// internal/platform/doctor, so the repository root is three levels up.
const (
	exampleComposePath  = "../../../compose.example.yaml"
	exampleConfigPath   = "../../../config.example.yaml"
	exampleIndicatorCap = 400
)

// TestFatalVerdictExitsNonZeroNamingGateDeclaredAndRequiredPolicy.
//
// The probe instantiates the gate machine with the policy DECLARED in the compose and fails on a
// fatal verdict. The refusal must name three things: the gate, the declared value and the required
// value. A refusal that only says "failed" tells the operator to restart against the same verdict —
// which is what happened seven times on the LAN.
//
// THE MUTANT IT KILLS: the one that prints only the gate's `err.Error()` without the way back, and
// the one that returns 0 on a fatal verdict.
func TestFatalVerdictExitsNonZeroNamingGateDeclaredAndRequiredPolicy(t *testing.T) {
	path := writeCompose(t, `
services:
  redis:
    command:
      - redis-server
      - --maxmemory-policy
      - volatile-ttl
`)
	var stdout, stderr bytes.Buffer
	code := Run([]string{"-compose", path, "-immortal-families", "candles,derivatives:series"}, &stdout, &stderr)

	if code == 0 {
		t.Fatalf("exit code = 0 under a fatal verdict; the probe exists to refuse the pipeline")
	}
	if code != 1 {
		t.Fatalf("exit code = %d, want 1 — 1 is the fatal verdict and 2 is the unreadable declaration", code)
	}

	refusal := stderr.String()
	for _, want := range []string{
		// the gate
		"eviction-policy-vs-immortal-families",
		// the declared value
		`"volatile-ttl"`,
		// the required value
		"allkeys-*",
		// the resident families that make it fatal
		"candles",
		"derivatives:series",
	} {
		if !strings.Contains(refusal, want) {
			t.Errorf("the refusal does not name %q; full refusal:\n%s", want, refusal)
		}
	}
}

// TestAllkeysIsUncertifiableNotApproved.
//
// The refusal is about a FATAL verdict, and only about it. "No fatal verdict" is NOT "approved".
//
// Under `allkeys-*` the decisive gate is `sole-copy-vs-evict-everything-policy`, and it compares the
// policy against the REMAINING sole copy — a number from the live keyspace. This probe does not open
// Redis, so the gate returns ERROR ("could not be measured"), and ERROR is not FATAL. Approving here
// would mean APPROVED for not having looked.
//
// The practical effect: it would suffice to change the policy in the compose for CI to bless exactly
// the configuration that puts `candles`/`derivatives:series` in the eviction queue — the forbidden
// disarmament, stamped by the tool created to prevent it.
func TestAllkeysIsUncertifiableNotApproved(t *testing.T) {
	// ALL the other inputs are declared, on purpose: thus the only one left blank is the sole
	// copy, and the INDETERMINATE below cannot be credited to any other absence.
	path := composeDeclarando(t, "allkeys-lru", 512)
	var stdout, stderr bytes.Buffer
	code := Run([]string{
		"-compose", path,
		"-immortal-families", "candles,derivatives:series",
		"-config", configMinima(t),
		"-indicator-cap", "400",
	}, &stdout, &stderr)

	if code == 0 {
		t.Fatalf("exit code = 0 — the probe APPROVED a policy whose safety it cannot measure. "+
			"Absence of measurement is not evidence.\nstdout:\n%s\nstderr:\n%s",
			stdout.String(), stderr.String())
	}
	if code != 3 {
		t.Fatalf("exit code = %d, want 3 (INDETERMINATE).\nstderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "INDETERMINATE") {
		t.Errorf("the refusal does not name itself INDETERMINATE; stderr:\n%s", stderr.String())
	}
	// The refusal must say what satisfies it, otherwise the operator swaps one refusal for another
	// thinking they made progress.
	if !strings.Contains(stderr.String(), "SATISFIED BY") {
		t.Errorf("the refusal does not declare the satisfier; stderr:\n%s", stderr.String())
	}
	if !strings.Contains(stdout.String(), "allkeys-lru") {
		t.Errorf("the report does not declare the read policy; stdout:\n%s", stdout.String())
	}
	// THE INPUT THAT WAS LEFT BLANK IS NAMED. Without this, "INDETERMINATE" would be a generic
	// label and whoever reads the job would not know what to declare to get out of it.
	for _, want := range []string{"sole-copy-vs-evict-everything-policy", "SoleCopyKeysAtRisk"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("the refusal does not name %q; stderr:\n%s", want, stderr.String())
		}
	}
}

// TestPolicyWithNoDecisiveUnmeasuredInputExitsZero keeps alive the half that APPROVES.
//
// Without it, the correction above could be satisfied by a probe that ALWAYS fails — which is as
// useless as one that always approves.
//
// GREEN IS NOT A DIFFERENT POLICY: the immortal-family gate tests the PROPERTY ("does not evict
// keys without TTL"), so `noeviction` fails just like `volatile-ttl`. With an immortal resident
// family present there is no green path — either the policy does not evict them (fatal), or it is
// `allkeys-*` and safety depends on a live measurement.
//
// AND GREEN IS NOT ONLY THE POLICY EITHER. Even with `-immortal-families ""` and `noeviction`, FOUR
// of the six gates remained without input and the probe printed APPROVED on top of them. Green
// requires BOTH halves — no gate failing AND no input left blank —, which is why this case also
// declares the listpack step, the indicator cap and the configuration.
//
// THE ASSERTION LEFT PROSE. A stale comment is a regression trap; `TestApprovalMeansEveryGateWasEvaluated`,
// just below, MEASURES that no gate was left unevaluated when the probe approves.
func TestPolicyWithNoDecisiveUnmeasuredInputExitsZero(t *testing.T) {
	path := composeDeclarando(t, "noeviction", 512)
	var stdout, stderr bytes.Buffer
	code := Run([]string{
		"-compose", path,
		"-immortal-families", "",
		"-config", configMinima(t),
		"-indicator-cap", "400",
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0 — no decisive gate was left unmeasured.\nstdout:\n%s\nstderr:\n%s",
			code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "APPROVED") {
		t.Errorf("the report does not declare approval; stdout:\n%s", stdout.String())
	}
}

// TestApprovalMeansEveryGateWasEvaluated ⭐ is the mechanical probe for what approval means.
//
// THE QUESTION IT ASKS IS THE REPORT'S. "APPROVED: no boot gate returned a fatal verdict" is a true
// sentence about the gates that WERE EVALUATED and silent about the others — and that distinction is
// invisible in the job summary, which shows a green check. While it lived only in a comment, four
// gates remained unmeasured without anything failing.
//
// Here the assertion becomes a comparison: if the probe exited ZERO, then NO line of the report may
// declare itself not evaluated, and the SIX preflight gates must be in the report. Both halves are
// necessary — a report that lost an entire gate would also come out without the forbidden sentence.
//
// THE MUTANT IT KILLS: the probe stops filling an input (`RegisteredTimeframes: nil`, for example)
// and does NOT declare the absence. The gate falls to WARN, `FatalGateError` returns nil, the exit
// stays 0 — and the rest of the suite stays green.
func TestApprovalMeansEveryGateWasEvaluated(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{
		"-compose", composeDeclarando(t, "noeviction", 512),
		"-immortal-families", "",
		"-config", configMinima(t),
		"-indicator-cap", "400",
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0 — this case measures what APPROVAL means, and "+
			"without approval it measures nothing.\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}

	// The sentences with which the gates declare that they did not evaluate. They are READ from the
	// report, and not from the gate code, because it is the report that whoever reads the job
	// receives.
	for _, proibida := range []string{"NÃO AVALIADO", "NÃO PÔDE SER MEDIDA"} {
		if strings.Contains(stdout.String(), proibida) {
			t.Errorf("the probe exited ZERO with a gate declaring %q — approval by absence of "+
				"measurement is exactly what is forbidden.\nstdout:\n%s", proibida, stdout.String())
		}
	}

	for _, gate := range []string{
		"indicator-cap-vs-listpack-cliff",
		"eviction-policy-vs-immortal-families",
		"sole-copy-vs-evict-everything-policy",
		"durable-columns-vs-erosion-guard",
		"timeframe-vs-any-source-support",
		"cap-entry-vs-durable-cell-coverage",
	} {
		if !strings.Contains(stdout.String(), gate) {
			t.Errorf("the approval report does not bring gate %q — a gate absent from the report "+
				"cannot be read as evaluated; stdout:\n%s", gate, stdout.String())
		}
	}
}

// TestTheRealDeclarationsOfThisCommitAreEvaluatedAndDoNotRefuse ⭐ is the case that closes the escape
// where the probe instantiated SIX gates and filled the inputs of TWO.
//
// It assembles the inputs from the SAME pure sources that `internal/app/startup_gates.go` uses — the
// durable coverage declaration and the erosion guard (properties of this build), the timeframe
// registry and the derivatives block (the configuration this commit deploys) — and therefore a boot
// gate that becomes FATAL for any of them now takes the pipeline down instead of coming out approved.
//
// THE MUTANTS IT KILLS:
//
//	a durable column declared OUTSIDE the erosion guard
//	    (`internal/adapters/outbound/redis/interfaces.go`) — gate `durable-columns-vs-erosion-guard` FATAL.
//	a timeframe that no source serves in `config.yaml`
//	    (for example `3d`) — gate `timeframe-vs-any-source-support` FATAL.
//
// Both make the real boot gate fatal; without this case the probe keeps printing `APPROVED`, exit 0.
//
// THE POLICY IS SYNTHETIC ON PURPOSE. The deployed compose declares `volatile-ttl`, which is already
// a fatal verdict by itself — and under exit 1 neither of the two mutants above would be
// distinguishable. What this case judges are the OTHER inputs, with the policy set to a value that
// decides nothing.
func TestExampleDeclarationsAreEvaluatedAndDoNotRefuse(t *testing.T) {
	// The step is DERIVED from the example compose, not typed: a second number here would be the
	// divergence the gate exists to catch, written inside the gate's own test.
	listpack := int64FromCompose(exampleComposePath, "redis", listpackFlag)
	if listpack <= 0 {
		t.Fatalf("%s does not declare %s in service redis — the listpack step is what the "+
			"destination server receives, and without it the gate does not have both sides", exampleComposePath, listpackFlag)
	}

	var stdout, stderr bytes.Buffer
	code := Run([]string{
		"-compose", composeDeclarando(t, "noeviction", listpack),
		"-immortal-families", "",
		"-config", exampleConfigPath,
		"-indicator-cap", fmt.Sprint(exampleIndicatorCap),
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0 — the EXAMPLE declarations cannot "+
			"refuse the start.\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}

	// The SIX preflight gates appear in the report: a gate the probe does not instantiate is a gate
	// it cannot fail, and the job summary does not distinguish the two.
	for _, gate := range []string{
		"indicator-cap-vs-listpack-cliff",
		"eviction-policy-vs-immortal-families",
		"sole-copy-vs-evict-everything-policy",
		"durable-columns-vs-erosion-guard",
		"timeframe-vs-any-source-support",
		"cap-entry-vs-durable-cell-coverage",
	} {
		if !strings.Contains(stdout.String(), gate) {
			t.Errorf("the report does not bring gate %q; stdout:\n%s", gate, stdout.String())
		}
	}
}

// TestAnUnfilledEntryIsNamedInsteadOfCertified: the probe that could not fill an input DECLARES the
// absence and refuses to certify.
//
// Filling with zero and moving on would be swapping one escape for another: zero in `IndicatorCap`
// DISABLES the step gate, and a gate disabled for lack of input reads, in the job summary, exactly
// like an approved gate.
//
// THE MUTANT IT KILLS: the one that ignores the list of blank inputs and falls into `APPROVED`.
func TestAnUnfilledEntryIsNamedInsteadOfCertified(t *testing.T) {
	cases := []struct {
		name string
		args func(t *testing.T) []string
		want []string
	}{
		{
			name: "the listpack step is not declared in the compose",
			args: func(t *testing.T) []string {
				return []string{
					"-compose", writeCompose(t, `
services:
  redis:
    command:
      - redis-server
      - --maxmemory-policy
      - noeviction
`),
					"-immortal-families", "",
					"-config", configMinima(t),
					"-indicator-cap", "400",
				}
			},
			want: []string{"indicator-cap-vs-listpack-cliff", "--zset-max-listpack-entries"},
		},
		{
			name: "MAX_INDICATOR_VALUES was not declared via flag",
			args: func(t *testing.T) []string {
				return []string{
					"-compose", composeDeclarando(t, "noeviction", 512),
					"-immortal-families", "",
					"-config", configMinima(t),
				}
			},
			want: []string{"indicator-cap-vs-listpack-cliff", "MAX_INDICATOR_VALUES"},
		},
		{
			name: "the declared configuration could not be read",
			args: func(t *testing.T) []string {
				return []string{
					"-compose", composeDeclarando(t, "noeviction", 512),
					"-immortal-families", "",
					"-config", filepath.Join(t.TempDir(), "does-not-exist.yaml"),
					"-indicator-cap", "400",
				}
			},
			want: []string{
				"timeframe-vs-any-source-support", "candle_engine.timeframes",
				"cap-entry-vs-durable-cell-coverage", "derivatives.series_cap_by_timeframe",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := Run(tc.args(t), &stdout, &stderr)

			if code == 0 {
				t.Fatalf("exit code = 0 with a blank input — the probe certified what "+
					"it did not look at.\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
			}
			if code != 3 {
				t.Fatalf("exit code = %d, want 3 (INDETERMINATE).\nstderr:\n%s", code, stderr.String())
			}
			if strings.Contains(stdout.String(), "APPROVED") {
				t.Errorf("the report prints APPROVED under code 3 — report and verdict disagree:\n%s",
					stdout.String())
			}
			for _, want := range append(tc.want, "SATISFIED BY") {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("the refusal does not name %q; stderr:\n%s", want, stderr.String())
				}
			}
		})
	}
}

// TestACapEntryThatShortensAnUncoveredCellRefusesThePipeline is the direct half: fail if ANY gate
// returns a fatal verdict — and not only the two the probe fills.
//
// The vehicle is `cap-entry-vs-durable-cell-coverage` because it is the gate whose FATAL side only
// the CONFIGURATION can light, and because it has already charged the price once: `30m` gained a cap
// entry, the cap reached the only copy and 254 hours of series disappeared. Without reading
// `derivatives.series_cap_by_timeframe`, a commit that reintroduced that entry came out APPROVED from
// CI and refused boot in production.
//
// The entry is 291 against `max_series_size: 800` — shortening, which is what destroys. An entry
// that equals or raises it is not refused, and that distinction belongs to the gate, not to this
// case.
func TestACapEntryThatShortensAnUncoveredCellRefusesThePipeline(t *testing.T) {
	configComCapQueEncurta := writeConfig(t, `
candle_engine:
  timeframes: ["1h", "4h", "1d"]
derivatives:
  max_series_size: 800
  series_cap_by_timeframe:
    "1h": 291
`)
	var stdout, stderr bytes.Buffer
	code := Run([]string{
		"-compose", composeDeclarando(t, "noeviction", 512),
		"-immortal-families", "",
		"-config", configComCapQueEncurta,
		"-indicator-cap", "400",
	}, &stdout, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1 — shortening a cell without durable written AND readable "+
			"data in every field is a FATAL startup verdict.\nstdout:\n%s\nstderr:\n%s",
			code, stdout.String(), stderr.String())
	}
	for _, want := range []string{"cap-entry-vs-durable-cell-coverage", "1h"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("the refusal does not name %q; stderr:\n%s", want, stderr.String())
		}
	}
	if strings.Contains(stdout.String(), "APPROVED") {
		t.Errorf("the report prints APPROVED under a fatal verdict:\n%s", stdout.String())
	}
}

// TestUnreadableDeclarationIsNotSuccess: verification that could not conclude does NOT become
// success.
//
// The defect has an exact shape in `verify_deployment`, where 11 verifications returned 0 over seven
// refusals. A probe that could not read the policy does not know whether boot is refused — and "I
// do not know" must exit non-zero.
//
// THE MUTANT IT KILLS: the one that treats a missing declaration as an empty policy and moves on to
// the gates, where `MaxMemoryPolicy == ""` becomes WARN and the process returns 0.
func TestUnreadableDeclarationIsNotSuccess(t *testing.T) {
	cases := []struct {
		name    string
		compose string
	}{
		{
			name: "compose without the policy key",
			compose: `
services:
  redis:
    command:
      - redis-server
      - --maxmemory
      - "3500mb"
`,
		},
		{
			name: "key declared without a value",
			compose: `
services:
  redis:
    command:
      - redis-server
      - --maxmemory-policy
`,
		},
		{
			name: "service missing",
			compose: `
services:
  other-service:
    command:
      - redis-server
      - --maxmemory-policy
      - volatile-ttl
`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeCompose(t, tc.compose)
			var stdout, stderr bytes.Buffer
			code := Run([]string{"-compose", path}, &stdout, &stderr)

			if code == 0 {
				t.Fatalf("exit code = 0 with the unreadable declaration; not producing a verdict is not approving.\nstdout:\n%s", stdout.String())
			}
			if code != 2 {
				t.Fatalf("exit code = %d, want 2 — 2 separates 'could not ask' from 'the answer was a refusal'", code)
			}
			if !strings.Contains(stderr.String(), "--maxmemory-policy") {
				t.Errorf("the error output does not name the missing declaration; stderr:\n%s", stderr.String())
			}
		})
	}
}

// TestDeclaredValueWinsOverTheCommentThatDiscussesIt covers the word "declared".
//
// The real compose carries an eighteen-line comment block that DISCUSSES `allkeys-lru` as the
// destination, right above the line that declares `volatile-ttl`. A text extraction would read the
// comment and invert the verdict — it would approve exactly the commit the gate refuses. Whoever
// runs `docker compose up -d` receives the VALUE, never the comment.
//
// THE MUTANT IT KILLS: `grep -A1 maxmemory-policy` instead of reading the interpreted list.
func TestDeclaredValueWinsOverTheCommentThatDiscussesIt(t *testing.T) {
	path := writeCompose(t, `
services:
  redis:
    command:
      - redis-server
      # The destination remains --maxmemory-policy allkeys-lru, AFTER the migration.
      # Until then this line carries the value that does not destroy data.
      - --maxmemory-policy
      - volatile-ttl
`)
	policy, err := policyFromCompose(path, "redis")
	if err != nil {
		t.Fatalf("policyFromCompose: %v", err)
	}
	if policy != "volatile-ttl" {
		t.Fatalf("read policy = %q, want %q — the comment that discusses the policy is not the policy", policy, "volatile-ttl")
	}
}

// TestRefusalNeverReadsAsSuccess: a failed verification cannot report success.
//
// The defect has an exact shape in `verify_deployment`: eleven verifications that detected the
// failure and still returned 0, because `warning()` is `echo`. The deploy that produced seven
// refusals exited `exit 0` in 41 s. The refusal existed; the verdict did not.
//
// THE MUTANT IT KILLS: the one that prints the approval report and the refusal report in the same
// outcome — the probe would keep "reporting" the refusal while whoever reads the job summary sees
// the approval line. Exit code and report must agree, in BOTH directions.
func TestRefusalNeverReadsAsSuccess(t *testing.T) {
	cases := []struct {
		name          string
		policy        string
		families      string
		wantCode      int
		forbiddenOut  string
		forbiddenErrs string
	}{
		{
			name:         "fatal verdict does not print approval",
			policy:       "volatile-ttl",
			wantCode:     1,
			forbiddenOut: "APPROVED",
		},
		{
			// A NON-FATAL VERDICT PRINTS NO REFUSAL. `allkeys-lru` cannot be used here: it stops
			// exiting zero because the probe refuses to approve what it cannot measure.
			//
			// AND CHANGING THE POLICY WAS NOT ENOUGH: the immortal-family gate tests the PROPERTY
			// ("does not evict keys without TTL"), not the string `volatile-ttl` — so `noeviction`
			// fails just the same. With an immortal family present there IS NO green path: either
			// the policy does not evict them (fatal), or it is `allkeys-*` and depends on a live
			// measurement. The legitimate green is the state in which every resident family is
			// recoverable.
			name:          "no fatal verdict does not print a refusal",
			policy:        "noeviction",
			families:      "",
			wantCode:      0,
			forbiddenErrs: "REFUSED",
		},
		{
			// AND THE THIRD STATE: the probe cannot certify `allkeys-*` without reading the
			// keyspace, and an INDETERMINATE that printed APPROVED would be the same
			// report-disagrees-with-verdict defect.
			name:         "indeterminate does not print approval",
			policy:       "allkeys-lru",
			wantCode:     3,
			forbiddenOut: "APPROVED",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The inputs that are NOT under judgment here are all declared, so that each case's
			// exit code comes from the policy and not from a collateral absence.
			path := composeDeclarando(t, tc.policy, 512)

			var stdout, stderr bytes.Buffer
			families := tc.families
			if families == "" && tc.name != "no fatal verdict does not print a refusal" {
				families = "candles"
			}
			code := Run([]string{
				"-compose", path,
				"-immortal-families", families,
				"-config", configMinima(t),
				"-indicator-cap", "400",
			}, &stdout, &stderr)

			if code != tc.wantCode {
				t.Fatalf("exit code = %d, want %d", code, tc.wantCode)
			}
			if tc.forbiddenOut != "" && strings.Contains(stdout.String(), tc.forbiddenOut) {
				t.Errorf("stdout carries %q under exit code %d — report and verdict disagree:\n%s",
					tc.forbiddenOut, code, stdout.String())
			}
			if tc.forbiddenErrs != "" && strings.Contains(stderr.String(), tc.forbiddenErrs) {
				t.Errorf("stderr carries %q under exit code %d — report and verdict disagree:\n%s",
					tc.forbiddenErrs, code, stderr.String())
			}
		})
	}
}
