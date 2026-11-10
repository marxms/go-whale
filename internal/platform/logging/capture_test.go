package logger

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNewCapture_RecordsTheLevelMessageAndAttrs(t *testing.T) {
	log, sink := NewCapture(LevelDebug)
	log.Error("durable write failed", "points", 3)
	log.Warn("partial coverage", "symbol", "BTCUSDT")
	log.Info("cycle finished")
	log.Debug("detail")

	records := sink.Records()
	if len(records) != 4 {
		t.Fatalf("captured %d records, want 4: %v", len(records), records)
	}
	if records[0].Level != LevelError || records[0].Message != "durable write failed" {
		t.Errorf("first record = %s, want ERROR durable write failed", records[0])
	}
	if value, ok := records[0].Attr("points"); !ok || value.Int64() != 3 {
		t.Errorf("points attr = %v (present=%v), want 3", value, ok)
	}
	if records[1].Level != LevelWarn || records[2].Level != LevelInfo || records[3].Level != LevelDebug {
		t.Errorf("level sequence lost: %v", records)
	}
}

func TestCapture_LevelOfSearchesMessageAndAttrs(t *testing.T) {
	log, sink := NewCapture(LevelDebug)
	log.Warn("symbol refused", "symbol", "ETHUSDT", "reason", "not listed")

	if level, ok := sink.LevelOf("symbol refused"); !ok || level != LevelWarn {
		t.Errorf("message lookup = %s, %v; want WARN true", level, ok)
	}
	if level, ok := sink.LevelOf("ETHUSDT"); !ok || level != LevelWarn {
		t.Errorf("attribute lookup = %s, %v; want WARN true", level, ok)
	}
	if _, ok := sink.LevelOf("absent"); ok {
		t.Error("LevelOf matched an absent substring")
	}
}

func TestCapture_HonoursTheRequestedThreshold(t *testing.T) {
	log, sink := NewCapture(LevelWarn)
	log.Info("filtered")
	log.Warn("kept")

	records := sink.Records()
	if len(records) != 1 || records[0].Message != "kept" {
		t.Fatalf("records = %v, want only the warning", records)
	}
}

func TestCapture_RecordsKeepEmissionOrder(t *testing.T) {
	log, sink := NewCapture(LevelDebug)
	log.Info("first")
	log.Error("second")

	records := sink.Records()
	if len(records) != 2 {
		t.Fatalf("captured %d records, want 2", len(records))
	}
	if records[0].Message != "first" || records[1].Message != "second" {
		t.Errorf("order changed: %v", records)
	}
}

func TestNew_TextFormatWritesStructuredAttributes(t *testing.T) {
	var out bytes.Buffer
	log, err := New(Config{Level: LevelInfo, Format: FormatText, Writer: &out})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	log.Warn("refused", "symbol", "BTCUSDT")

	line := out.String()
	if !strings.Contains(line, "level=WARN") || !strings.Contains(line, `msg=refused`) || !strings.Contains(line, "symbol=BTCUSDT") {
		t.Errorf("text line %q lacks level/message/attribute", line)
	}
}

func TestNew_JSONFormatWritesStructuredAttributes(t *testing.T) {
	var out bytes.Buffer
	log, err := New(Config{Level: LevelDebug, Format: FormatJSON, Writer: &out})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	log.Debug("detail", "exchange", "binance")

	var decoded map[string]any
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("output is not JSON: %v (%q)", err, out.String())
	}
	if decoded["msg"] != "detail" || decoded["level"] != "DEBUG" || decoded["exchange"] != "binance" {
		t.Errorf("decoded = %v, want msg/level/exchange", decoded)
	}
}

func TestNew_RejectsInvalidLevelAndFormat(t *testing.T) {
	if _, err := New(Config{Level: Level("verbose"), Format: FormatText}); err == nil {
		t.Error("invalid level accepted")
	}
	if _, err := New(Config{Level: LevelInfo, Format: "yaml"}); err == nil {
		t.Error("invalid format accepted")
	}
}

func TestNew_DefaultsWriterAndEmptyValues(t *testing.T) {
	log, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if log == nil {
		t.Fatal("New returned a nil logger")
	}
}

func TestConfigFromProcess_DefaultsAndOverrides(t *testing.T) {
	t.Setenv("GOWHALE_LOG_LEVEL", "")
	t.Setenv("GOWHALE_LOG_FORMAT", "")
	cfg, err := ConfigFromProcess()
	if err != nil {
		t.Fatalf("ConfigFromProcess: %v", err)
	}
	if cfg.Level != LevelInfo || cfg.Format != FormatText {
		t.Errorf("defaults = %s/%s, want INFO/text", cfg.Level, cfg.Format)
	}

	t.Setenv("GOWHALE_LOG_LEVEL", "warning")
	t.Setenv("GOWHALE_LOG_FORMAT", "JSON")
	cfg, err = ConfigFromProcess()
	if err != nil {
		t.Fatalf("ConfigFromProcess: %v", err)
	}
	if cfg.Level != LevelWarn || cfg.Format != FormatJSON {
		t.Errorf("overrides = %s/%s, want WARN/json", cfg.Level, cfg.Format)
	}

	t.Setenv("GOWHALE_LOG_LEVEL", "chatty")
	if _, err := ConfigFromProcess(); err == nil {
		t.Error("invalid GOWHALE_LOG_LEVEL accepted")
	}
}

func TestOrDiscard_NilLoggerIsSafe(t *testing.T) {
	var nilLogger *slog.Logger
	got := OrDiscard(nilLogger)
	if got == nil {
		t.Fatal("OrDiscard(nil) returned nil")
	}
	got.Error("must not panic", "key", "value")

	injected := slog.New(slog.DiscardHandler)
	if OrDiscard(nil, injected) != injected {
		t.Error("OrDiscard did not return the injected logger")
	}
}

func TestLevel_StringNamesEachLevel(t *testing.T) {
	for level, want := range map[Level]string{
		LevelDebug: "DEBUG", LevelInfo: "INFO", LevelWarn: "WARN", LevelError: "ERROR",
	} {
		if got := level.String(); got != want {
			t.Errorf("Level(%q).String() = %q, want %q", level, got, want)
		}
	}
}
