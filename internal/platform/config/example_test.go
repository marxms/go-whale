package config

import "testing"

func TestExampleConfigLoadsAndValidates(t *testing.T) {
	cfg, err := Load("../../../config.example.yaml")
	if err != nil {
		t.Fatalf("config.example.yaml must be a valid starting point: %v", err)
	}
	if cfg.Port != "3000" {
		t.Fatalf("example port = %q, want 3000", cfg.Port)
	}
	if len(cfg.CandleEngine.Timeframes) == 0 {
		t.Fatal("example must declare candle timeframes")
	}
	if cfg.QuestDB.PGPassword != nil {
		t.Fatal("example must not embed a QuestDB password")
	}
}
