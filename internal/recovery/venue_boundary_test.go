package recovery

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRecoveryHasNoVenueSwitch scans the package's non-test sources and fails when any
// venue literal appears. The scan reads the files this package compiles, so a switch
// added later is caught here instead of in production.
func TestRecoveryHasNoVenueSwitch(t *testing.T) {
	// The four literals a per-exchange branch in the recovery core would have to spell.
	// The ports are the only door to a venue; naming one here is how the switch comes back.
	literals := []string{
		"market.Binance",
		"market.BingX",
		`"binance"`,
		`"bingx"`,
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	scanned := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		//nolint:gosec // G304: the path is a package source file under the test directory.
		source, err := os.ReadFile(filepath.Join(".", name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		scanned++
		for _, literal := range literals {
			if strings.Contains(string(source), literal) {
				t.Errorf("%s names a venue with %q; recovery must reach a venue only through its ports",
					name, literal)
			}
		}
	}

	if scanned == 0 {
		t.Fatal("scanned no non-test package file; the boundary proof would pass vacuously")
	}
}
