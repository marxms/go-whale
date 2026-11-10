package health_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/inbound/httpapi"
	"github.com/marxms/go-whale/internal/health"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

type shapeHybridManager struct {
	stats []streaming.ConnectionStats
}

func (m shapeHybridManager) GetConnectionStats() []streaming.ConnectionStats {
	return m.stats
}

// TestHealthResponseShapeUnchanged proves the additive rule of HLT-05: every existing
// /health field keeps its name, JSON shape and omitempty behavior, while candle_freshness
// is added alongside them.
func TestHealthResponseShapeUnchanged(t *testing.T) {
	mc := httpapi.NewMetricsCollector(shapeHybridManager{stats: []streaming.ConnectionStats{{
		ExchangeID: "binance",
		MarketType: "future",
		Connected:  true,
		Healthy:    true,
		Symbols:    2,
	}}})
	mc.SetFleetLiveness(nil, []health.CandleFreshness{{
		Scope: "binance:future", Symbols: 2, Stale: 1,
		FreshestAtMs: time.Now().Add(-time.Minute).UnixMilli(), HaveData: true,
	}})

	rec := httptest.NewRecorder()
	mc.Health(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("HTTP code = %d, want 200", rec.Code)
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode /health: %v", err)
	}

	// Existing required fields keep their names and JSON types.
	for key, decode := range map[string]func([]byte) error{
		"status":             func(b []byte) error { var v string; return json.Unmarshal(b, &v) },
		"uptime":             func(b []byte) error { var v string; return json.Unmarshal(b, &v) },
		"goroutines":         func(b []byte) error { var v int; return json.Unmarshal(b, &v) },
		"derivatives_poller": func(b []byte) error { var v bool; return json.Unmarshal(b, &v) },
	} {
		raw, ok := payload[key]
		if !ok {
			t.Fatalf("existing field %q disappeared from /health", key)
		}
		if err := decode(raw); err != nil {
			t.Errorf("existing field %q changed shape: %v", key, err)
		}
	}

	// Existing optional fields stay omitted when empty.
	for _, key := range []string{
		"timeframe_checksum", "missing_scopes", "stale_scopes", "coverage_deficits",
		"cold_write_age_seconds", "cold_write_stale", "gc_headroom_ratio", "gc_headroom_tight",
	} {
		if _, ok := payload[key]; ok {
			t.Errorf("optional field %q must stay omitted when empty", key)
		}
	}

	var connections []map[string]json.RawMessage
	if err := json.Unmarshal(payload["connections"], &connections); err != nil {
		t.Fatalf("connections shape changed: %v", err)
	}
	if len(connections) != 1 {
		t.Fatalf("connections = %d, want 1", len(connections))
	}
	for _, key := range []string{"exchange", "market_type", "connected", "healthy", "symbols"} {
		if _, ok := connections[0][key]; !ok {
			t.Errorf("connection summary lost field %q", key)
		}
	}

	if _, ok := payload["components"]; !ok {
		t.Error("components field disappeared from /health")
	}

	// The additive field is present with the documented shape.
	var freshness []map[string]json.RawMessage
	if err := json.Unmarshal(payload["candle_freshness"], &freshness); err != nil {
		t.Fatalf("candle_freshness missing or malformed: %v", err)
	}
	if len(freshness) != 1 {
		t.Fatalf("candle_freshness = %d scopes, want 1", len(freshness))
	}
	for _, key := range []string{"scope", "symbols", "stale", "stale_fraction", "freshest_ago_ms"} {
		if _, ok := freshness[0][key]; !ok {
			t.Errorf("candle_freshness lost field %q", key)
		}
	}
}
