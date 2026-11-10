package app

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE PROFILE REGISTRY IS BORN ONCE, AT THE COMPOSITION ROOT
//
// The poller HONORS the registry it receives — that is behavior, and it is locked in
// `internal/derivatives/collector` by `TestNewDerivativesPoller_RestartKeepsTheMeasuredRung`, which
// measures the consequence: under a new registry, the restart falls to the declared rung.
//
// What remains here is a WIRING FACT, not behavior: who builds the registry. The lifecycle
// rebuilds the poller on every restart ("Recreate poller with fresh state"), and a second
// construction inside that path would zero the measured rung of the whole fleet without anything
// turning red. That is why the check is MECHANICAL — a scan — and is declared as such.
// ─────────────────────────────────────────────────────────────────────────────

// fundingRegistryConstructions lists, as `file:line`, the PRODUCTION points under `root` that build
// a funding-profile registry.
func fundingRegistryConstructions(t *testing.T, root string) []string {
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
			if strings.Contains(line, "collector.NewFundingProfileRegistry(") {
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

// TestFundingProfileRegistry_IsBuiltOnceByTheCompositionRoot refuses a second construction
// anywhere in the package — the poller restart is the path that invites it.
func TestFundingProfileRegistry_IsBuiltOnceByTheCompositionRoot(t *testing.T) {
	got := fundingRegistryConstructions(t, ".")

	if len(got) != 1 {
		t.Fatalf("the profile registry is built at %d points (%s), and must be ONE: "+
			"the process-lifetime one. A second point is a registry that dies with the poller",
			len(got), strings.Join(got, ", "))
	}
	if !strings.HasPrefix(got[0], "app.go:") {
		t.Fatalf("the registry is built at %s, and must be born at the composition root "+
			"(app.go). Outside it, it lives only as long as whoever built it", got[0])
	}
}

// TestFundingRegistryConstructions_DiscriminatesTheRestart ⭐ proves that the scan above
// DISCRIMINATES. Without it the `PASS` is empty, because the tree has no offender.
//
// The planted line is literally the restart-path line.
func TestFundingRegistryConstructions_DiscriminatesTheRestart(t *testing.T) {
	dir := t.TempDir()
	limpo := "\tderivPoller = a.newDerivPoller()\n"
	ofensor := "\tderivPoller = collector.NewDerivativesPoller(a.cfg.Derivatives, a.derivStore, " +
		"a.cfg.Exchanges, a.tfReg, collector.NewFundingProfileRegistry())\n"

	if err := os.WriteFile(filepath.Join(dir, "limpo.go"), []byte("package x\n\n"+limpo), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := fundingRegistryConstructions(t, dir); len(got) != 0 {
		t.Fatalf("the scan accused %v on a path that builds no registry", got)
	}

	if err := os.WriteFile(filepath.Join(dir, "lifecycle.go"), []byte("package x\n\n"+ofensor), 0o600); err != nil {
		t.Fatal(err)
	}
	got := fundingRegistryConstructions(t, dir)
	if len(got) != 1 || !strings.HasPrefix(got[0], "lifecycle.go:") {
		t.Fatalf("the scan returned %v, and should accuse the construction on the restart path — "+
			"without that the ratchet does not separate right from wrong", got)
	}
}
