package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestWarmSeedBudgetDefaultsTo120s(t *testing.T) {
	if got := (CandleEngineConfig{}).WarmSeedBudget(); got != defaultWarmSeedBudget {
		t.Fatalf("default warm seed budget = %v, want %v", got, defaultWarmSeedBudget)
	}
	if defaultWarmSeedBudget != 120*time.Second {
		t.Fatalf("default = %v, want 120s", defaultWarmSeedBudget)
	}
}

func TestWarmSeedBudgetHonorsConfig(t *testing.T) {
	if got := (CandleEngineConfig{WarmSeedBudgetSeconds: 180}).WarmSeedBudget(); got != 180*time.Second {
		t.Fatalf("configured warm seed budget = %v, want 180s", got)
	}
}

func TestValidateRejectsWarmSeedBudgetOutsideRange(t *testing.T) {
	for _, secs := range []int{1, 29, 241, 600, 3600} {
		if err := validateWarmSeedBudget(secs); err == nil {
			t.Fatalf("warm_seed_budget_seconds=%d was accepted", secs)
		}
	}
	for _, secs := range []int{0, 30, 120, 240} {
		if err := validateWarmSeedBudget(secs); err != nil {
			t.Fatalf("warm_seed_budget_seconds=%d was rejected: %v", secs, err)
		}
	}
}

// TestWarmSeedUsesTheConfiguredBudget keeps the knob wired to the boot path: a config value
// no caller reads is a documentation lie.
// The env var must work on BOTH load paths: it used to be read only inside loadYAML, so an
// environment-only deploy silently kept 120 s while the operator believed it had set another value.
func TestLoadEnvOnlyAppliesWarmSeedBudget(t *testing.T) {
	t.Setenv("GOWHALE_CONFIG_FILE", "")
	t.Setenv("GOWHALE_QUESTDB_PG_PASSWORD", "envpass")
	t.Setenv("GOWHALE_CANDLE_ENGINE_WARM_SEED_BUDGET_SECONDS", "45")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.CandleEngine.WarmSeedBudget(); got != 45*time.Second {
		t.Fatalf("env-only budget = %v, want 45s", got)
	}
}

func TestLoadEnvOnlyDefaultsTheWarmSeedBudget(t *testing.T) {
	t.Setenv("GOWHALE_CONFIG_FILE", "")
	t.Setenv("GOWHALE_QUESTDB_PG_PASSWORD", "envpass")
	t.Setenv("GOWHALE_CANDLE_ENGINE_WARM_SEED_BUDGET_SECONDS", "")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.CandleEngine.WarmSeedBudget(); got != defaultWarmSeedBudget {
		t.Fatalf("default budget = %v, want %v", got, defaultWarmSeedBudget)
	}
}

// A present-but-invalid value fails the load instead of silently falling back: a typo must not
// leave the operator believing the ceiling was applied.
func TestApplyCandleEngineEnvRejectsInvalidValues(t *testing.T) {
	for _, raw := range []string{"12O", "abc", "1e3", "120s"} {
		t.Setenv("GOWHALE_CANDLE_ENGINE_WARM_SEED_BUDGET_SECONDS", raw)
		cfg := Config{}
		if err := applyCandleEngineEnv(&cfg); err == nil {
			t.Fatalf("%q was accepted", raw)
		}
	}
}

func TestApplyCandleEngineEnvOverridesTheDeclaredValue(t *testing.T) {
	t.Setenv("GOWHALE_CANDLE_ENGINE_WARM_SEED_BUDGET_SECONDS", "60")
	cfg := Config{CandleEngine: CandleEngineConfig{WarmSeedBudgetSeconds: 90}}
	if err := applyCandleEngineEnv(&cfg); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if cfg.CandleEngine.WarmSeedBudgetSeconds != 60 {
		t.Fatalf("budget = %d, want the env's 60", cfg.CandleEngine.WarmSeedBudgetSeconds)
	}
}

func TestWarmSeedUsesTheConfiguredBudget(t *testing.T) {
	source, err := os.ReadFile("../../candle/engine/bootstrap.go")
	if err != nil {
		t.Fatalf("reading bootstrap.go: %v", err)
	}
	text := string(source)
	if !strings.Contains(text, "cfg.CandleEngine.WarmSeedBudget()") {
		t.Fatal("WarmCandleEngine no longer reads the configured budget")
	}
	if strings.Contains(text, "60*time.Second") {
		t.Fatal("WarmCandleEngine still carries the fixed 60 s budget")
	}
}
