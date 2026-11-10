// Package doctor answers, without a server, a question otherwise only answered in production:
// **does the compose this commit carries cause boot to be refused?**
//
// # WHY THIS FITS IN A DEPENDENCY-FREE BINARY
//
// `config.EvaluatePreflightGates` is a PURE FUNCTION over `config.GateInputs`
// (`internal/platform/config/gates.go`). It does not talk to Redis, does not talk to QuestDB and
// does not open a port. The entire boot-refusal machine, therefore, runs in a CI `go run` — and it is
// that discovery that makes the lock cheap enough to exist before everything else.
//
// # WHAT IT READS, AND WHAT IT DOES NOT GUESS
//
// The machine runs SIX preflight gates, and any of them can return a fatal verdict. A probe that
// filled in only the memory policy would instantiate all six and be able to evaluate two — the other
// four would come out "NOT EVALUATED" and the job summary would say APPROVED on top of them.
// Measured: one boot gate made FATAL by a durable column outside the erosion guard, and another by a
// timeframe no source serves, both passed this probe with exit 0.
//
// The inputs come from the compose, the configuration, the build and external declarations:
//
//	compose      --maxmemory-policy and --zset-max-listpack-entries from the service's `command`.
//	             Whoever runs `docker compose up -d` receives those VALUES, not the comments next to
//	             them — so it is they, and not the written intention, that decide the verdict.
//	configuration candle_engine.timeframes and the derivatives block, read from the declared YAML.
//	build        this binary's pure functions: the durable coverage declaration, the erosion guard,
//	             the per-timeframe support matrix and the columns the route asks for.
//	             `internal/app/startup_gates.go` assembles the same ones, and for the same reason:
//	             they are properties of this build, not configuration.
//
// Resident families WITHOUT TTL would require a live keyspace and enter via `-immortal-families`,
// whose default mirrors the streamer's retention declaration in `internal/app/sizing_sheet.go` — the
// two families whose `retention` says, in so many words, "NO TTL". DECLARED CAVEAT: it is a SECOND
// copy of that declaration, because `residentFamilies` is not exported; the copy can drift, and the
// flag is the escape hatch when it does. `MAX_INDICATOR_VALUES` is an env var of ANOTHER service (the
// analyzer) and enters via `-indicator-cap`. Without it, the listpack step remains UNMEASURED — and
// is declared as such. The sole-copy count comes from the guard measured at the destination and
// enters via `-sole-copy-keys-at-risk`; without it allkeys remains INDETERMINATE, never presumed
// zero.
//
// # THE EXIT CODE CARRIES THE DISTINCTION
//
//	0  all gates were evaluated and none failed
//	1  FATAL verdict — the declared configuration refuses boot
//	2  the declaration could not be read; the verdict was NOT produced
//	3  INDETERMINATE — some gate was left without one of its inputs and could not be evaluated
//
// None of them is 0. A probe that could not ask cannot report success — a verification that only
// warned and then turned into success let a deploy exit `exit 0` over seven refusals.
package doctor

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/derivatives/collector"
	"github.com/marxms/go-whale/internal/platform/config"
)

const (
	exitOK           = 0
	exitFatalVerdict = 1
	exitUnreadable   = 2
	// exitUncertifiable is the refusal by ABSENCE OF MEASUREMENT, and it exists because the
	// alternative measured the probe, not the configuration.
	//
	// A gate whose input was left blank returns WARNS or ERROR, and neither is FATAL — so
	// `FatalGateError` alone would say "no gate failed" about a gate that did not even run. The
	// verdict came out APPROVED **for not having looked**.
	//
	// That would turn the lock into its opposite: it would suffice to stop passing an input for CI
	// to bless exactly the configuration boot refuses — the forbidden disarmament, stamped by the
	// tool created to prevent it.
	//
	// Absence and zero are different answers, and only zero is evidence.
	exitUncertifiable = 3
)

const (
	// policyFlag is the key the compose uses to declare the eviction policy.
	policyFlag = "--maxmemory-policy"
	// listpackFlag is the server directive that decides the irreversible ZSET encoding step. It is
	// DECLARED in the same `command`, and that is why the probe can read it: boot reads it from
	// `CONFIG GET`, but the value the server has to return is this one.
	listpackFlag = "--zset-max-listpack-entries"
)

// errPolicyNotDeclared separates "the compose does not declare the policy" from "the declared policy
// fails". Both exit non-zero, but for opposite reasons: the first is a question that could not be
// asked, the second is the answer.
var errPolicyNotDeclared = errors.New("doctor: the compose does not declare " + policyFlag)

// defaultImmortalFamilies mirrors the families that `residentFamilies`
// (`internal/app/sizing_sheet.go`) declares with "NO TTL" retention and `streamer` owner.
//
// It is not a measurement: it is the SAME declaration, written where CI can reach it. `indicators`
// stays out because the family has a 7 d TTL and a different owner.
func defaultImmortalFamilies() []string {
	return []string{"candles", "derivatives:series"}
}

// Run executes the doctor command and returns the process exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(stderr)
	composePath := fs.String("compose", "compose.example.yaml",
		"path to the compose file that declares the memory policy")
	service := fs.String("service", "redis", "compose service whose `command` declares the policy")
	families := fs.String("immortal-families", strings.Join(defaultImmortalFamilies(), ","),
		"resident families without TTL, comma-separated")
	configPath := fs.String("config", "config.yaml",
		"path to the YAML configuration that declares candle_engine.timeframes and the derivatives block")
	indicatorCap := fs.Int64("indicator-cap", 0,
		"deployed MAX_INDICATOR_VALUES; 0 = NOT READ, so the listpack step remains unmeasured")
	soleCopy := fs.Int("sole-copy-keys-at-risk", -1,
		"keys at risk measured by the guard at the destination; -1 = NOT MEASURED; 0 = measured with no risk")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUnreadable
	}
	if *soleCopy < -1 {
		_, _ = fmt.Fprintln(stderr, "REFUSED: sole-copy-keys-at-risk must be -1 (not measured) or a non-negative count")
		return exitUnreadable
	}

	policy, err := policyFromCompose(*composePath, *service)
	if err != nil {
		// WRITING THE ERROR TO stdout/stderr IS DISCARDED ON PURPOSE, here and below.
		// This is the probe's own output: if the descriptor died, there is nowhere to report
		// that it died, and changing the exit code because of that would confuse "the gate
		// failed" with "the terminal closed". The verdict travels in the exit code, not in the
		// text.
		_, _ = fmt.Fprintf(stderr, "REFUSED: %v\n", err)
		_, _ = fmt.Fprintf(stderr,
			"The memory policy decides the boot verdict. Without it read, this probe produced no "+
				"verdict — and not producing a verdict is NOT approval.\n")
		return exitUnreadable
	}

	declared := declarations{
		composePath:       *composePath,
		service:           *service,
		policy:            policy,
		immortal:          splitFamilies(*families),
		listpack:          int64FromCompose(*composePath, *service, listpackFlag),
		indicatorCap:      *indicatorCap,
		configPath:        *configPath,
		soleCopy:          *soleCopy,
		soleCopyEvaluated: *soleCopy >= 0,
	}
	declared.cfg, declared.configErr = config.LoadFile(*configPath)

	inputs, blanks := gateInputs(declared)
	results := config.EvaluatePreflightGates(inputs)

	_, _ = fmt.Fprintf(stdout, "compose:  %s (service %s)\n", *composePath, *service)
	_, _ = fmt.Fprintf(stdout, "declared policy:  %s\n", policy)
	_, _ = fmt.Fprintf(stdout, "declared zset-max-listpack-entries:  %s\n", readOrNot(declared.listpack))
	_, _ = fmt.Fprintf(stdout, "MAX_INDICATOR_VALUES (flag):  %s\n", readOrNot(declared.indicatorCap))
	if declared.soleCopyEvaluated {
		_, _ = fmt.Fprintf(stdout, "SoleCopyKeysAtRisk (flag):  %d\n", declared.soleCopy)
	} else {
		_, _ = fmt.Fprintln(stdout, "SoleCopyKeysAtRisk (flag):  NOT READ")
	}
	_, _ = fmt.Fprintf(stdout, "resident families without TTL:  %v\n", declared.immortal)
	_, _ = fmt.Fprintf(stdout, "configuration:  %s\n", declared.configState())
	for _, r := range results {
		_, _ = fmt.Fprintln(stdout, r.String())
	}

	if err := config.FatalGateError(results); err != nil {
		_, _ = fmt.Fprintf(stderr, "\nREFUSED: %v\n", err)
		return exitFatalVerdict
	}

	// THE PROBE DECLARES WHAT IT COULD NOT MEASURE, INSTEAD OF APPROVING FOR NOT HAVING LOOKED.
	//
	// It is not string matching on the gate message: it is the probe knowing which of its INPUTS was
	// left unfilled, because it was the one that assembled them.
	if len(blanks) > 0 {
		_, _ = fmt.Fprintf(stderr,
			"\nINDETERMINATE: %d gate-machine input(s) were left blank, and a gate that could not "+
				"be evaluated returns no fatal verdict — approving here would certify what was not looked at.\n",
			len(blanks))
		for _, b := range blanks {
			_, _ = fmt.Fprintf(stderr, "  · gate %s · input NOT READ: %s (%s)\n    SATISFIED BY: %s\n",
				b.gate, b.input, b.reason, b.satisfiedBy)
		}
		return exitUncertifiable
	}

	_, _ = fmt.Fprintln(stdout, "\nAPPROVED: no boot gate returned a fatal verdict")
	return exitOK
}

// declarations is everything the probe managed to read, with the provenance of each thing.
type declarations struct {
	composePath string
	service     string
	// policy and listpack come from the service's `command` in the compose. listpack ZERO means the
	// directive is not declared there.
	policy   string
	listpack int64
	// These external quantities enter via flag. Zero / empty list are legitimate states and
	// different from "not measured" — see splitFamilies and the note on `-indicator-cap`.
	immortal          []string
	indicatorCap      int64
	soleCopy          int
	soleCopyEvaluated bool
	// cfg is the declared configuration; configErr says it could NOT be read, and in that case
	// everything that would depend on it is left blank instead of becoming zero.
	configPath string
	cfg        config.Config
	configErr  error
}

func (d declarations) configState() string {
	if d.configErr != nil {
		return fmt.Sprintf("%s NOT READ (%v)", d.configPath, d.configErr)
	}
	return fmt.Sprintf("%s (timeframes %v · derivatives.max_series_size %d · %d cap entry/entries)",
		d.configPath, d.cfg.CandleEngine.Timeframes, d.cfg.Derivatives.MaxSeriesSize,
		len(d.cfg.Derivatives.SeriesCapByTimeframe))
}

// blankInput names ONE input this probe could not fill, the gate that depends on it, and what
// satisfies it.
//
// The satisfier is not decoration: a refusal that only describes the state found tells the reader to
// try again against the same verdict. It is the same reason for `GateResult.SatisfiedBy`.
type blankInput struct {
	gate        string
	input       string
	reason      string
	satisfiedBy string
}

// gateInputs assembles the gate machine's inputs from what this probe can read, and returns
// ALONGSIDE it the list of those left blank.
//
// The two halves come out together on purpose. Filling with zero and moving on is the defect: zero
// in `IndicatorCap` DISABLES the listpack step gate, and a gate disabled for lack of input reads
// exactly like an approved gate.
func gateInputs(d declarations) (config.GateInputs, []blankInput) {
	var blanks []blankInput

	// --- the two column sets: properties of this build, always readable from here ---
	declaredColumns := redisstore.DeclaredDurableColumns()
	guardedColumns := derivatives.ErosionGuardedColumns()
	if len(declaredColumns) == 0 || len(guardedColumns) == 0 {
		blanks = append(blanks, blankInput{
			gate:   "durable-columns-vs-erosion-guard",
			input:  "DurableColumnsDeclared / ErosionGuardedColumns",
			reason: "one of the two sets in this build came out empty",
			satisfiedBy: "declare durable coverage in redisstore.DerivativesDurableCoverage and the " +
				"guard in derivatives.ErosionGuardedColumns — an empty set does not become a verdict",
		})
	}

	// --- the listpack step: one side in the compose, the other in an env var of ANOTHER service ---
	if d.listpack <= 0 {
		blanks = append(blanks, blankInput{
			gate:   "indicator-cap-vs-listpack-cliff",
			input:  listpackFlag,
			reason: fmt.Sprintf("the `command` of %q in %s does not declare it", d.service, d.composePath),
			satisfiedBy: "declare " + listpackFlag + " in the service command, with the value the " +
				"destination server actually receives — it decides the irreversible step to skiplist",
		})
	}
	if d.indicatorCap <= 0 {
		blanks = append(blanks, blankInput{
			gate:        "indicator-cap-vs-listpack-cliff",
			input:       "MAX_INDICATOR_VALUES",
			reason:      "it is an environment variable of another service and is not declared in this compose",
			satisfiedBy: "pass -indicator-cap with the deployed value, so the step is COMPARED instead of presumed",
		})
	}

	// --- the sole copy: a number from the LIVE keyspace, and decisive only under a policy that
	// evicts everything ---
	if strings.HasPrefix(d.policy, "allkeys-") && !d.soleCopyEvaluated {
		blanks = append(blanks, blankInput{
			gate:  "sole-copy-vs-evict-everything-policy",
			input: "SoleCopyKeysAtRisk",
			reason: fmt.Sprintf("the declared policy is %q and its safety depends on the remaining sole "+
				"copy, which is a number from the LIVE keyspace", d.policy),
			satisfiedBy: "measure with the sole-copy guard against the destination Redis and durable tier and " +
				"pass -sole-copy-keys-at-risk with that count; zero cannot be presumed",
		})
	}

	// --- what depends on the declared configuration ---
	registry := d.cfg.CandleEngine.Timeframes
	if d.configErr != nil {
		blanks = append(blanks,
			blankInput{
				gate:        "timeframe-vs-any-source-support",
				input:       "candle_engine.timeframes",
				reason:      fmt.Sprintf("%s could not be read (%v)", d.configPath, d.configErr),
				satisfiedBy: "point -config at the YAML this build deploys",
			},
			blankInput{
				gate:        "cap-entry-vs-durable-cell-coverage",
				input:       "derivatives.series_cap_by_timeframe / derivatives.max_series_size",
				reason:      fmt.Sprintf("%s could not be read (%v)", d.configPath, d.configErr),
				satisfiedBy: "point -config at the YAML this build deploys",
			},
		)
		registry = nil
	}

	inputs := config.GateInputs{
		MaxMemoryPolicy:  d.policy,
		ImmortalFamilies: d.immortal,
		// THE SAME POSTURE AS BOOT. `internal/app/startup_gates.go` runs with
		// `PolicyIsFatal: true`; a probe that ran with `false` would report WARN where the real
		// binary refuses, and a CI that warns about a crashloop is the defect again.
		PolicyIsFatal:      true,
		SoleCopyEvaluated:  d.soleCopyEvaluated,
		SoleCopyKeysAtRisk: d.soleCopy,

		IndicatorCap:           d.indicatorCap,
		ZSetMaxListpackEntries: d.listpack,

		// THE COLUMN ORDER, with both sides READ from the binary that is starting. Neither comes
		// from config: they are properties of this build, and that is why the divergence between
		// them can — and should — be caught before the merge.
		DurableColumnsDeclared: declaredColumns,
		ErosionGuardedColumns:  guardedColumns,

		// PER-TIMEFRAME COVERAGE: registry from the configuration that is starting, support
		// matrix measured and declared in this build.
		RegisteredTimeframes: registry,
		TimeframeSupport:     timeframeSupport(registry),

		// THE CAP RULE, with all three sides read from the build and the configuration.
		CapEntriesByTimeframe: capEntries(d.cfg.Derivatives),
		DefaultSeriesCap:      int64(d.cfg.Derivatives.MaxSeriesSize),
		DurableCellCoverage: config.DurableCellCoverage{
			FieldColumn:       redisstore.DerivativesDurableCoverage(),
			ReadableColumns:   derivatives.DurableReadColumns(),
			DurableTimeframes: collector.DurableTimeframes(registry),
		},
	}
	return inputs, blanks
}

// timeframeSupport returns the matrix only when there is a registry to cross it with. A matrix over
// an empty registry would make the gate look evaluated over no set.
func timeframeSupport(registry []string) []config.TimeframeSupportRow {
	if len(registry) == 0 {
		return nil
	}
	return collector.TimeframeSupportRows(registry)
}

// capEntries returns the cap entries AS WRITTEN in the configuration — the same reduction as
// `internal/app/startup_gates.go`. Only the entries, and not the resolved cap: the gate asks who
// REQUESTED shortening, and a timeframe with no entry requested nothing.
func capEntries(cfg config.DerivativesConfig) map[string]int64 {
	if len(cfg.SeriesCapByTimeframe) == 0 {
		return nil
	}
	out := make(map[string]int64, len(cfg.SeriesCapByTimeframe))
	for tf, value := range cfg.SeriesCapByTimeframe {
		out[tf] = int64(value)
	}
	return out
}

// readOrNot prints the number read, or says it was not read. Printing `0` would make the absence of
// a reading look like a measurement of zero, which is this probe's entire confusion.
func readOrNot(v int64) string {
	if v <= 0 {
		return "NOT READ"
	}
	return strconv.FormatInt(v, 10)
}

// composeFile is the minimal slice of the compose this probe needs. Deserializing the whole file
// would couple the probe to every key someone added to the YAML.
type composeFile struct {
	Services map[string]struct {
		Command []string `yaml:"command"`
	} `yaml:"services"`
}

// policyFromCompose returns the declared value right after `--maxmemory-policy` in the service's
// `command`.
//
// The scan is over the list, not over the file text: a `grep` would find the same string inside the
// comment block that discusses the policy — and it is precisely that comment that says
// `volatile-ttl` is not the destination. Reading the comment as if it were the value would invert
// the verdict.
func policyFromCompose(path, service string) (string, error) {
	command, err := composeCommand(path, service)
	if err != nil {
		return "", err
	}
	value, declared, err := declaredValue(command, policyFlag)
	if err != nil {
		return "", fmt.Errorf("doctor: %s declares %s without a value in %q: %w",
			path, policyFlag, service, errPolicyNotDeclared)
	}
	if !declared {
		return "", fmt.Errorf("doctor: %s does not declare %s in %q: %w",
			path, policyFlag, service, errPolicyNotDeclared)
	}
	return value, nil
}

// int64FromCompose returns the numeric directive declared in the same `command`, or ZERO when it is
// not there — and zero, here, means NOT READ, as in every `GateInputs` entry.
func int64FromCompose(path, service, flagName string) int64 {
	command, err := composeCommand(path, service)
	if err != nil {
		return 0
	}
	value, declared, err := declaredValue(command, flagName)
	if err != nil || !declared {
		return 0
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func composeCommand(path, service string) ([]string, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: path comes from the operator (CLI, env or mounted config), not untrusted input.
	if err != nil {
		return nil, fmt.Errorf("doctor: reading %s: %w", path, err)
	}
	var parsed composeFile
	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("doctor: parsing %s: %w", path, err)
	}
	svc, ok := parsed.Services[service]
	if !ok {
		return nil, fmt.Errorf("doctor: %s does not declare service %q: %w", path, service, errPolicyNotDeclared)
	}
	return svc.Command, nil
}

// declaredValue returns the argument following `flagName`. The third return separates "the key is
// there without a value" from "the key is not there" — two distinct declaration defects.
func declaredValue(command []string, flagName string) (value string, declared bool, err error) {
	for i, arg := range command {
		if arg != flagName {
			continue
		}
		if i+1 >= len(command) {
			return "", true, errPolicyNotDeclared
		}
		return command[i+1], true, nil
	}
	return "", false, nil
}

// splitFamilies accepts the empty list as "no resident family without TTL", which is a legitimate
// state — and different from "not measured".
func splitFamilies(csv string) []string {
	var out []string
	for _, part := range strings.Split(csv, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	sort.Strings(out)
	return out
}
