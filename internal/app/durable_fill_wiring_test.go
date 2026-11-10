package app

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE FILL CHECKER MUST MOVE WITH THE CYCLE
//
// The checker's DECISION — what counts as empty, what counts as grace, what never stops the start —
// is locked in `internal/candle/engine` by `durable_fill_probe_test.go`, which measures it as
// behavior.
//
// What remains here is a WIRING FACT. A checker built and never called is the "sensor with the cable
// unplugged" pattern this repository has paid for twice, and it is especially inviting HERE: building
// the probe at startup and not repeating it every cycle would give an eternal "in grace" report,
// because at the boot instant the new columns have zero rows by definition. The PASS would look
// healthy and the checker would never find anything.
//
// That is why the check is MECHANICAL — a scan — and is declared as such.
// ─────────────────────────────────────────────────────────────────────────────

// fillReportCalls lists, as `file:line`, the PRODUCTION points under `root` that ask for the fill
// report.
func fillReportCalls(t *testing.T, root string) []string {
	t.Helper()
	return productionLines(t, root, func(line string) bool {
		return strings.Contains(line, ".Report(ctx)") && strings.Contains(line, "fill")
	})
}

// productionLines is the scan, written once. It walks the PRODUCTION code under `root` and returns
// `file:line` for each line the predicate accepts.
//
// Extracted because the wiring has more than one mechanical fact to check, and two copied scans would
// diverge the first time one of them gained an exclusion.
func productionLines(t *testing.T, root string, match func(string) bool) []string {
	t.Helper()

	var found []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, readErr := os.ReadFile(path) //nolint:gosec // G304: test-fixture path created or selected by the test.
		if readErr != nil {
			return readErr
		}
		for i, line := range strings.Split(string(body), "\n") {
			if match(line) {
				found = append(found, filepath.Base(path)+":"+strconv.Itoa(i+1))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	return found
}

// TestDurableFillProbe_ReportIsInThePerCycleHook requires ONE call point, INSIDE the hook the
// scheduler runs every cycle — including the immediate startup cycle.
//
// THE DEFECT THIS KILLS remains the same: a checker that only runs at boot reports grace forever and
// never finds anything. The call appears ONCE in a hook the scheduler invokes both times.
// `TestLoop_AfterCycleRunsForEveryCycle` proves the scheduler side; this scan proves the report is
// inside the hook.
func TestDurableFillProbe_ReportIsInThePerCycleHook(t *testing.T) {
	got := fillReportCalls(t, ".")

	if len(got) != 1 {
		t.Fatalf("the fill report is requested at %d point(s) (%s), and must be ONE: "+
			"the post-cycle hook, which runs on the immediate cycle and on every tick. More than one "+
			"would be a second reporting path; none would be the unplugged cable",
			len(got), strings.Join(got, ", "))
	}
	if !strings.HasPrefix(got[0], "coinalyze_wiring.go:") {
		t.Fatalf("the report is requested at %s, and it belongs to the collection cycle — collection "+
			"is what fills the columns, and its progress is what makes the grace period win", got[0])
	}
	if !insideHook(t, "coinalyze_wiring.go", "AfterCycle:", "OnCycle:", "fill.Report(ctx)") {
		t.Fatal("the fill report is not inside the AfterCycle hook: a report outside it is a boot path, " +
			"not a per-cycle report")
	}
}

// insideHook reports whether `needle` appears between `start` and `end` in the production file.
func insideHook(t *testing.T, file, start, end, needle string) bool {
	t.Helper()
	body, err := os.ReadFile(file) //nolint:gosec // G304: test-fixture path created or selected by the test.
	if err != nil {
		t.Fatalf("reading %s: %v", file, err)
	}
	text := string(body)
	from := strings.Index(text, start)
	if from < 0 {
		return false
	}
	to := strings.Index(text[from:], end)
	if to < 0 {
		return false
	}
	return strings.Contains(text[from:from+to], needle)
}

// TestFillReportCalls_DiscriminatesTheUnpluggedCable ⭐ proves the scan above DISCRIMINATES.
// Without it the `PASS` is empty, because the tree has no offender.
//
// The planted path is what the defect would produce: the probe built and never called.
func TestFillReportCalls_DiscriminatesTheUnpluggedCable(t *testing.T) {
	dir := t.TempDir()
	desligado := "\tfill := a.durableFillProbe()\n\t_ = fill\n"
	ligado := "\tfill := a.durableFillProbe()\n\tfill.Report(ctx)\n"

	if err := os.WriteFile(filepath.Join(dir, "desligado.go"), []byte("package x\n\n"+desligado), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := fillReportCalls(t, dir); len(got) != 0 {
		t.Fatalf("the scan accused %v on a path that builds the probe and never calls it", got)
	}

	if err := os.WriteFile(filepath.Join(dir, "coinalyze_wiring.go"), []byte("package x\n\n"+ligado), 0o600); err != nil {
		t.Fatal(err)
	}
	got := fillReportCalls(t, dir)
	if len(got) != 1 || !strings.HasPrefix(got[0], "coinalyze_wiring.go:") {
		t.Fatalf("the scan returned %v, and should accuse the call — without that the ratchet does not "+
			"separate the plugged cable from the unplugged one", got)
	}
}

// TestDurableFillProbe_WithoutQuestDBThereIsNoProbe — without a durable tier there is no table to
// count, and a checker warning about a disabled table would be an alarm about the configuration
// itself.
func TestDurableFillProbe_WithoutQuestDBThereIsNoProbe(t *testing.T) {
	app := &App{}
	if p := app.durableFillProbe(); p != nil {
		t.Fatal("checker built without QuestDB")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE THREE NEW DIMENSIONS MUST REACH THE CHECKER, AND THE REPORT MUST REACH THE CYCLE
//
// Same wiring fact, same form of check. The mutant it kills is the unplugged cable in its cheapest
// form: `Spans:` disappears from the assembly, the checker starts measuring ONE dimension, the derived
// label honestly shrinks — and the `internal/candle/engine` suite stays green, because there the
// window reader is injected by the test.
// ─────────────────────────────────────────────────────────────────────────────

// spanReaderWiring lists the production points that deliver the window reader to the checker.
func spanReaderWiring(t *testing.T, root string) []string {
	t.Helper()
	return productionLines(t, root, func(line string) bool {
		return strings.Contains(line, "Spans:") && strings.Contains(line, "qdbStore")
	})
}

// integrityReportCalls lists the production points that ask for the integrity report.
func integrityReportCalls(t *testing.T, root string) []string {
	t.Helper()
	return productionLines(t, root, func(line string) bool {
		return strings.Contains(line, ".ReportIntegrity(ctx)")
	})
}

func TestDurableIntegrity_TheSpanReaderReachesTheProbe(t *testing.T) {
	got := spanReaderWiring(t, ".")
	if len(got) != 1 {
		t.Fatalf("the window reader is delivered at %d point(s) (%s), and must be ONE: the checker "+
			"assembly. Without it three of the four dimensions have no mechanism, and the "+
			"internal/candle/engine suite does not see — there the reader is injected by the test",
			len(got), strings.Join(got, ", "))
	}
	if !strings.HasPrefix(got[0], "coinalyze_wiring.go:") {
		t.Fatalf("the reader is delivered at %s, and it belongs to the checker assembly", got[0])
	}
}

func TestDurableIntegrity_ReportIsInThePerCycleHook(t *testing.T) {
	got := integrityReportCalls(t, ".")
	if len(got) != 1 {
		t.Fatalf("the integrity report is requested at %d point(s) (%s), and must be ONE: "+
			"the post-cycle hook, which runs on the immediate cycle and on every tick — the same "+
			"reason as the fill report", len(got), strings.Join(got, ", "))
	}
	if !strings.HasPrefix(got[0], "coinalyze_wiring.go:") {
		t.Fatalf("the report is requested at %s, and it belongs to the collection cycle", got[0])
	}
	if !insideHook(t, "coinalyze_wiring.go", "AfterCycle:", "OnCycle:", "fill.ReportIntegrity(ctx)") {
		t.Fatal("the integrity report is not inside the AfterCycle hook: a report outside it is a boot " +
			"path, not a per-cycle report")
	}
}

// ⭐ AND BOTH SCANS DISCRIMINATE. Without this proof their PASS is empty, because the tree has no
// offender.
func TestIntegrityWiringScans_DiscriminateTheUnpluggedCable(t *testing.T) {
	dir := t.TempDir()
	desligado := "\tengine.NewDurableFillProbe(engine.DurableFillProbeOptions{\n\t\tCounter: a.qdbStore,\n\t})\n\tfill.Report(ctx)\n"
	ligado := "\tengine.NewDurableFillProbe(engine.DurableFillProbeOptions{\n\t\tCounter: a.qdbStore,\n\t\tSpans:   a.qdbStore,\n\t})\n\tfill.ReportIntegrity(ctx)\n"

	if err := os.WriteFile(filepath.Join(dir, "desligado.go"), []byte("package x\n\n"+desligado), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := spanReaderWiring(t, dir); len(got) != 0 {
		t.Fatalf("the reader scan accused %v on a path that does not deliver it", got)
	}
	if got := integrityReportCalls(t, dir); len(got) != 0 {
		t.Fatalf("the report scan accused %v on a path that does not request it", got)
	}

	if err := os.WriteFile(filepath.Join(dir, "coinalyze_wiring.go"), []byte("package x\n\n"+ligado), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := spanReaderWiring(t, dir); len(got) != 1 {
		t.Fatalf("the reader scan returned %v — without that it does not separate the plugged cable from the unplugged one", got)
	}
	if got := integrityReportCalls(t, dir); len(got) != 1 {
		t.Fatalf("the report scan returned %v — without that it does not separate the plugged cable from the unplugged one", got)
	}
}
