package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestValidateCompleteConfig ensures a fully populated config passes validation.
func TestValidateCompleteConfig(t *testing.T) {
	pass := "secret"
	cfg := Config{
		Port:                  "3000",
		RedisAddr:             "localhost:6379",
		RedisPoolSize:         256,
		MaxSymbolsPerInstance: 500,
		MaxConnectionsPerIP:   3,
		QuestDB:               QuestDBConfig{PGPassword: &pass},
		CandleEngine:          CandleEngineConfig{Timeframes: []string{"5m"}},
	}

	if err := Validate(cfg); err != nil {
		t.Errorf("expected no error for complete config, got: %v", err)
	}
}

func TestValidateQuestDBPGPasswordNil(t *testing.T) {
	cfg := Config{
		Port:      "3000",
		RedisAddr: "localhost:6379",
		QuestDB:   QuestDBConfig{Enabled: true},
		// QuestDB.PGPassword is nil
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected error for nil PGPassword, got nil")
	}
	wantMsg := "QuestDB PG password must be provided via GOWHALE_QUESTDB_PG_PASSWORD env var"
	if err.Error() != wantMsg {
		t.Errorf("unexpected error message: got %q, want %q", err.Error(), wantMsg)
	}
}

// TestValidateQuestDBPGPasswordEmptyString ensures empty-string PGPassword is treated as missing.
func TestValidateQuestDBPGPasswordEmptyString(t *testing.T) {
	empty := ""
	cfg := Config{
		Port:      "3000",
		RedisAddr: "localhost:6379",
		QuestDB:   QuestDBConfig{Enabled: true, PGPassword: &empty},
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected error for empty-string PGPassword, got nil")
	}
	wantMsg := "QuestDB PG password must be provided via GOWHALE_QUESTDB_PG_PASSWORD env var"
	if err.Error() != wantMsg {
		t.Errorf("unexpected error message: got %q, want %q", err.Error(), wantMsg)
	}
}

// TestValidatePortEmpty ensures empty Port returns error in strict mode.
func TestValidatePortEmpty(t *testing.T) {
	pass := "secret"
	cfg := Config{
		RedisAddr: "localhost:6379",
		QuestDB:   QuestDBConfig{PGPassword: &pass},
		// Port is empty
	}

	if err := Validate(cfg); err == nil {
		t.Error("expected error for empty Port in strict mode, got nil")
	}
}

// TestValidateDuplicateExchanges ensures duplicate exchange+market pairs return error in strict mode.
func TestValidateDuplicateExchanges(t *testing.T) {
	pass := "secret"
	cfg := Config{
		Port:         "3000",
		RedisAddr:    "localhost:6379",
		QuestDB:      QuestDBConfig{PGPassword: &pass},
		CandleEngine: CandleEngineConfig{Timeframes: []string{"5m"}},
		Exchanges: []ExchangeConfig{
			{ID: "binance", MarketType: "future"},
			{ID: "binance", MarketType: "future"}, // duplicate
		},
	}

	if err := Validate(cfg); err == nil {
		t.Error("expected error for duplicate exchange config, got nil")
	}
}

// TestValidateOutOfRangeNegativeValues ensures negative values return error in strict mode.
func TestValidateOutOfRangeNegativeValues(t *testing.T) {
	pass := "secret"
	cfg := Config{
		Port:                  "3000",
		RedisAddr:             "localhost:6379",
		QuestDB:               QuestDBConfig{PGPassword: &pass},
		MaxSymbolsPerInstance: -1,
		MaxConnectionsPerIP:   3,
	}

	if err := Validate(cfg); err == nil {
		t.Error("expected error for negative MaxSymbolsPerInstance, got nil")
	}
}

// TestApplyDefaults ensures applyDefaults populates zero-value fields with expected values.
func TestApplyDefaults(t *testing.T) {
	cfg := Config{}
	applyDefaults(&cfg)

	tests := []struct {
		name     string
		got      interface{}
		expected interface{}
	}{
		{"Port", cfg.Port, "3001"},
		{"LogLevel", cfg.LogLevel, "info"},
		{"LogFormat", cfg.LogFormat, "text"},
		{"RedisAddr", cfg.RedisAddr, "localhost:6379"},
		{"RedisPoolSize", cfg.RedisPoolSize, 256},
		{"RedisMinIdleConns", cfg.RedisMinIdleConns, 32},
		{"RedisDialTimeoutS", cfg.RedisDialTimeoutS, 5},
		{"RedisReadTimeoutS", cfg.RedisReadTimeoutS, 3},
		{"RedisWriteTimeoutS", cfg.RedisWriteTimeoutS, 3},
		{"RedisPoolTimeoutS", cfg.RedisPoolTimeoutS, 4},
		{"RedisMaxRetries", cfg.RedisMaxRetries, 2},
		{"MaxSymbolsPerInstance", cfg.MaxSymbolsPerInstance, 50},
		{"MaxConnectionsPerIP", cfg.MaxConnectionsPerIP, 60},
		{"TradeBufferSize", cfg.TradeBufferSize, 5000},
		{"Derivatives.PollIntervalSec", cfg.Derivatives.PollIntervalSec, 300},
		// The assertion points at the SINGLE SOURCE instead of copying the number — copying it
		// here would rebuild the divergence on the other side of the test.
		{"Derivatives.MaxSeriesSize", cfg.Derivatives.MaxSeriesSize, DefaultResidentDepth},
		{"Derivatives.BootstrapLimit", cfg.Derivatives.BootstrapLimit, DefaultResidentDepth},
		{"Derivatives.BootstrapConcurrency", cfg.Derivatives.BootstrapConcurrency, 10},
		{"CandleEngine.SnapshotIntervalMS", cfg.CandleEngine.SnapshotIntervalMS, 5000},
		{"CandleEngine.BingxMinIntervalMS", cfg.CandleEngine.BingxMinIntervalMS, 300},
		{"QuestDB.Table", cfg.QuestDB.Table, "candles"},
		{"QuestDB.PartitionBy", cfg.QuestDB.PartitionBy, "DAY"},
		{"QuestDB.PGPoolSize", cfg.QuestDB.PGPoolSize, 5},
		{"QuestDB.RetentionDays", cfg.QuestDB.RetentionDays, 90},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s = %v, expected %v", tt.name, tt.got, tt.expected)
			}
		})
	}

	// Verify timeframes default
	expectedTFs := []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"}
	if len(cfg.CandleEngine.Timeframes) != len(expectedTFs) {
		t.Errorf("Timeframes length = %d, expected %d", len(cfg.CandleEngine.Timeframes), len(expectedTFs))
	}
	for i, tf := range expectedTFs {
		if cfg.CandleEngine.Timeframes[i] != tf {
			t.Errorf("Timeframes[%d] = %s, expected %s", i, cfg.CandleEngine.Timeframes[i], tf)
		}
	}
}

// TestApplyDefaultsDoesNotOverwriteExisting ensures applyDefaults preserves explicitly set values.
func TestApplyDefaultsDoesNotOverwriteExisting(t *testing.T) {
	cfg := Config{
		Port:     "8080",
		LogLevel: "debug",
		CandleEngine: CandleEngineConfig{
			Timeframes: []string{"1m", "5m"},
		},
	}
	applyDefaults(&cfg)

	if cfg.Port != "8080" {
		t.Errorf("Port = %s, expected 8080 (preserved)", cfg.Port)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %s, expected debug (preserved)", cfg.LogLevel)
	}
	if len(cfg.CandleEngine.Timeframes) != 2 || cfg.CandleEngine.Timeframes[0] != "1m" {
		t.Errorf("Timeframes not preserved: %v", cfg.CandleEngine.Timeframes)
	}
}

func TestLoadEnvOnly(t *testing.T) {
	t.Setenv("GOWHALE_CONFIG_FILE", "")
	t.Setenv("GOWHALE_QUESTDB_PG_PASSWORD", "envpass")
	t.Setenv("GOWHALE_SERVER_PORT", "3010")

	loaded, err := Load("")
	if err != nil {
		t.Fatalf("Load env-only: %v", err)
	}
	if loaded.Port != "3010" {
		t.Errorf("Port from environment = %s, want 3010", loaded.Port)
	}
	if loaded.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr = %q, want local default", loaded.RedisAddr)
	}
}

func TestLogFormat_EnvOverrideAndValidation(t *testing.T) {
	t.Setenv("GOWHALE_CONFIG_FILE", "")
	t.Setenv("GOWHALE_QUESTDB_PG_PASSWORD", "envpass")
	t.Setenv("GOWHALE_LOG_FORMAT", "JSON")

	loaded, err := Load("")
	if err != nil {
		t.Fatalf("Load env-only: %v", err)
	}
	if loaded.LogFormat != "json" {
		t.Errorf("LogFormat = %q, want json (normalized from the environment)", loaded.LogFormat)
	}

	t.Setenv("GOWHALE_LOG_FORMAT", "yaml")
	if _, err := Load(""); err == nil {
		t.Fatal("an invalid GOWHALE_LOG_FORMAT was accepted")
	}

	if err := Validate(Config{Port: "3000", RedisAddr: "localhost:6379", LogFormat: "yaml"}); err == nil {
		t.Error("Validate accepted server.log_format=yaml")
	}
}

func TestLoadRejectsExplicitMissingFile(t *testing.T) {
	t.Setenv("GOWHALE_QUESTDB_PG_PASSWORD", "envpass")
	path := filepath.Join(t.TempDir(), "missing.yaml")

	if _, err := Load(path); err == nil {
		t.Fatal("Load accepted an explicitly selected missing file")
	}
}

func TestLoadRejectsUnknownYAMLField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	const document = `server:
  port: "3000"
redis:
  addr: "localhost:6379"
unknown_field: true
`
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "unknown_field") {
		t.Fatalf("Load did not reject the unknown YAML field: %v", err)
	}
}

func TestValidateRejectsInvalidTimeframe(t *testing.T) {
	pass := "secret"
	cfg := Config{
		Port:         "3000",
		RedisAddr:    "localhost:6379",
		QuestDB:      QuestDBConfig{PGPassword: &pass},
		CandleEngine: CandleEngineConfig{Timeframes: []string{"5m", "5x"}},
	}

	err := Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), `"5x"`) {
		t.Fatalf("expected invalid timeframe error naming %q, got %v", "5x", err)
	}
}

func TestValidateRejectsUnknownMarketType(t *testing.T) {
	pass := "secret"
	cfg := Config{
		Port:         "3000",
		RedisAddr:    "localhost:6379",
		QuestDB:      QuestDBConfig{PGPassword: &pass},
		CandleEngine: CandleEngineConfig{Timeframes: []string{"5m"}},
		Exchanges:    []ExchangeConfig{{ID: "binance", MarketType: "options"}},
	}

	err := Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "market type") {
		t.Fatalf("expected unknown market type error, got %v", err)
	}
}

func TestValidateRejectsUnknownExchange(t *testing.T) {
	pass := "secret"
	cfg := Config{
		Port:         "3000",
		RedisAddr:    "localhost:6379",
		QuestDB:      QuestDBConfig{PGPassword: &pass},
		CandleEngine: CandleEngineConfig{Timeframes: []string{"5m"}},
		Exchanges:    []ExchangeConfig{{ID: "bybit", MarketType: "future"}},
	}

	err := Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "unknown exchange") {
		t.Fatalf("expected unknown exchange error, got %v", err)
	}
}

func TestValidateRejectsInvalidSymbol(t *testing.T) {
	tests := []struct {
		name   string
		symbol string
	}{
		{name: "empty", symbol: ""},
		{name: "no slash", symbol: "BTCUSDT"},
		{name: "multiple slashes", symbol: "BTC/USDT/ETH"},
		{name: "whitespace", symbol: " BTC/USDT"},
		{name: "venue suffix", symbol: "BTC/USDT:USDT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pass := "secret"
			cfg := Config{
				Port:         "3000",
				RedisAddr:    "localhost:6379",
				QuestDB:      QuestDBConfig{PGPassword: &pass},
				CandleEngine: CandleEngineConfig{Timeframes: []string{"5m"}},
				Exchanges:    []ExchangeConfig{{ID: "binance", MarketType: "future", Symbols: []string{tt.symbol}}},
			}

			err := Validate(cfg)
			if err == nil || !strings.Contains(err.Error(), "symbols[0]") {
				t.Fatalf("expected invalid symbol error naming symbols[0], got %v", err)
			}
		})
	}
}

func TestValidateAcceptsKnownExchangeAndSymbols(t *testing.T) {
	pass := "secret"
	cfg := Config{
		Port:         "3000",
		RedisAddr:    "localhost:6379",
		QuestDB:      QuestDBConfig{PGPassword: &pass},
		CandleEngine: CandleEngineConfig{Timeframes: []string{"5m"}},
		Exchanges: []ExchangeConfig{
			{ID: "binance", MarketType: "future", Symbols: []string{"BTC/USDT", "ETH/USDC"}},
			{ID: "bingx", MarketType: "swap", Symbols: []string{"SOL/USDT"}},
		},
	}

	if err := Validate(cfg); err != nil {
		t.Fatalf("expected no error for known exchange and canonical symbols, got: %v", err)
	}
}
