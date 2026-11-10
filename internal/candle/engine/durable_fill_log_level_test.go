package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE "WARNS" HALF OF THE CONJUNCTION, ASSERTED AT THE EMISSION BOUNDARY
//
// The contract is "the verdict SHALL warn without blocking boot", and the two halves need separate
// sensors:
//
//	"without blocking" .. `TestDurableFillProbe_NoStateEverBlocksTheBoot`, which compares against the
//	                      severity CEILING and checks by reflection that `Report` returns nothing;
//	"warn" ............ this file.
//
// Counting call sites says NOTHING about what the call produces. A `Warn` -> `Info` downgrade
// keeps printing: what is lost is the CLASSIFICATION, not the signal — and a finding
// indistinguishable from routine is not read as a finding, neither by an alert nor by a human eye.
// Deleting the whole reporting block, by contrast, truly kills the signal.
//
// The finding "field declared covered and EMPTY" is the operational prerequisite for the entire
// capping axis. Capping the hot window of a field declared covered and empty destroys its only
// readable copy.
//
// The capture stamps the level on each line, so LEVEL and CONTENT become assertions.
// ─────────────────────────────────────────────────────────────────────────────

// lineContaining returns the record of the first captured line containing `substr`.
//
// Returning the RECORD, and not just the level, is what makes it possible to assert the two things the
// contract asks of the same line: the severity it came out with and what it names.
func lineContaining(t *testing.T, sink *logger.Sink, substr string) logger.Record {
	t.Helper()
	for _, r := range sink.Records() {
		if strings.Contains(r.Text(), substr) {
			return r
		}
	}
	t.Fatalf("no captured line contains %q; captured: %v", substr, sink.Records())
	return logger.Record{}
}

// TestDurableFillReport_TheEmptyFieldIsWarnedByNameAndByLevel ⭐ pins the "warns" half.
//
// It catches (a) the finding's `logger.Warn` downgraded to `logger.Info` and (b) the reporting
// block deleted, which a call-site count alone would miss.
func TestDurableFillReport_TheEmptyFieldIsWarnedByNameAndByLevel(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	// The exact shape of the defect that opened the feature: `funding_rate_8h` declared covered,
	// with a write branch, and with ZERO rows. Grace period expired — two daily buckets are
	// 48h and the process already has 100.
	rows := todasAsColunasCheias("1d", 2000)
	rows["1d"]["funding_rate_8h"] = 0

	probeCom(&fakeFillCounter{rows: rows}, 100*time.Hour, log).Report(context.Background())

	finding := lineContaining(t, sink, "funding_rate_8h")

	if finding.Level != logger.LevelWarn {
		t.Errorf("the finding came out at %s, want %s — the effective level in production is %s "+
			"(`LOG_LEVEL=log` falls into the default), so below %s the finding is "+
			"indistinguishable from routine", finding.Level, logger.LevelWarn, logger.LevelInfo,
			logger.LevelWarn)
	}
	// AND THE LINE CARRIES THE MEASUREMENT, not just the alarm: timeframe, column, state and count. It is
	// per field because the consequence is per field — the operator caps the cell of one field.
	for _, required := range []string{
		"1d.funding_rate_8h=" + string(FillEmpty),
		"(0 linha[s])",
	} {
		if !strings.Contains(finding.Text(), required) {
			t.Errorf("the warning does not carry %q: %s", required, finding.Text())
		}
	}
}

// TestDurableFillReport_AHealthyPassSummarisesAtInfoAndWarnsAboutNothing is the negative
// control, and without it the case above would be satisfied by a report that warns ALWAYS.
//
// A checker that screams on every healthy cycle gets turned off in the second week, and then the real
// finding arrives on a channel nobody reads anymore.
func TestDurableFillReport_AHealthyPassSummarisesAtInfoAndWarnsAboutNothing(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	probeCom(&fakeFillCounter{rows: todasAsColunasCheias("1d", 2000)}, 100*time.Hour, log).
		Report(context.Background())

	summary := lineContaining(t, sink, "durable coverage: fields filled")

	if summary.Level != logger.LevelInfo {
		t.Errorf("a pass with no finding came out at %s — only the finding is loud", summary.Level)
	}
	// AND THE SUMMARY CARRIES BOTH NUMBERS, derived from the declaration and not typed in: every
	// declared field filled, none in the grace period.
	declared := len(declaredDurableFields())
	want := fmt.Sprintf("filled=%d", declared)
	if !strings.Contains(summary.Text(), want) {
		t.Errorf("the summary does not carry the measured count (%q): %s", want, summary.Text())
	}
	for _, r := range sink.Records() {
		if r.Level >= logger.LevelWarn {
			t.Errorf("a pass with every column full warned: %s", r)
		}
	}
}
