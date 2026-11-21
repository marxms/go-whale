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
	for _, secs := range []int{1, 29, 601, 3600} {
		if err := validateWarmSeedBudget(secs); err == nil {
			t.Fatalf("warm_seed_budget_seconds=%d was accepted", secs)
		}
	}
	for _, secs := range []int{0, 30, 120, 600} {
		if err := validateWarmSeedBudget(secs); err != nil {
			t.Fatalf("warm_seed_budget_seconds=%d was rejected: %v", secs, err)
		}
	}
}

// TestWarmSeedUsesTheConfiguredBudget keeps the knob wired to the boot path: a config value
// no caller reads is a documentation lie.
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
