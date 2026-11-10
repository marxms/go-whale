package app

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

// TestNewApp_FundingProfileSurvivesTheConstructor ⭐ is the missing link of the
// `unclassified — missing: fundingRatePct` incident.
//
// `NewApp` created the funding-profile registry in its FIRST literal and, near the end,
// executed `*app = App{...}` — an overwrite of the WHOLE struct that silently zeroed every field
// the second literal did not name. The field remained declared and the code kept compiling; only
// the value disappeared.
//
// The consequence crossed three layers: the poller received a nil registry, `setMeasured` took
// its early return and DISCARDED every interval measurement, `fundingRate8h` was never emitted,
// and the dashboard classified symbols without `fundingRatePct`. Measured in production: 321
// deposits, ZERO entries in the registry.
func TestNewApp_FundingProfileSurvivesTheConstructor(t *testing.T) {
	app, err := NewApp(
		configParaNewApp(t),
		candle.NewTimeframeRegistry([]string{"1d", "1w"}),
		nil,
		withColdTierConstructor(func(config.QuestDBConfig) (*questdb.Store, error) {
			return nil, errors.New("a missing durable tier is not the subject of this case")
		}),
	)
	if err != nil {
		t.Fatalf("the constructor must not refuse: %v", err)
	}
	if app == nil {
		t.Fatal("`NewApp` returned `(nil, nil)`")
	}
	t.Cleanup(func() { _ = app.Shutdown(context.Background()) })

	if app.fundingProfiles == nil {
		t.Fatal("the funding-profile registry was destroyed inside the constructor " +
			"that created it — it carries the MEASURED step of the interval ladder, and without it " +
			"`fundingRate8h` is not emitted for any symbol")
	}
}

// TestNewApp_DoesNotOverwriteTheWholeStruct kills the CLASS, not the instance.
//
// `fundingProfiles` was the second victim: `realtimeProducer` had already been zeroed by the same
// literal, and the fix at the time was to carry it by hand with a comment asking the next one to
// remember. A request to remember is not a mechanism — the next victim appeared anyway. While the
// overwrite exists, every new field is a potential victim, and no field-specific test covers the
// field that has not been written yet.
func TestNewApp_DoesNotOverwriteTheWholeStruct(t *testing.T) {
	fonte, err := os.ReadFile("app.go")
	if err != nil {
		t.Fatalf("could not read the constructor: %v", err)
	}

	sobrescrita := regexp.MustCompile(`(?m)^\s*\*\s*app\s*=\s*App\{`)
	if loc := sobrescrita.FindIndex(fonte); loc != nil {
		linha := strings.Count(string(fonte[:loc[0]]), "\n") + 1
		t.Fatalf("app.go:%d reintroduced `*app = App{...}`: the whole-struct overwrite "+
			"silently zeroes every field the literal does not name. Assign field by field — "+
			"what is not named remains, instead of becoming zero", linha)
	}
}
