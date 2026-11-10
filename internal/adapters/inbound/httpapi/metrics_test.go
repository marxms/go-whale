package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/bingx"
	"github.com/marxms/go-whale/internal/health"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// ─── Test doubles ─────────────────────────────────────────────────────────

type mockHybridManager struct {
	connStats []streaming.ConnectionStats
}

func (m *mockHybridManager) GetConnectionStats() []streaming.ConnectionStats {
	return m.connStats
}

// ─── Tests ────────────────────────────────────────────────────────────────

func TestHealthEndpoint_Healthy(t *testing.T) {
	mc := NewMetricsCollector(
		&mockHybridManager{
			connStats: []streaming.ConnectionStats{
				{ExchangeID: "binance", MarketType: "future", Connected: true, Healthy: true, Symbols: 200},
				{ExchangeID: "bingx", MarketType: "swap", Connected: true, Healthy: true, Symbols: 150},
			},
		},
	)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/health", nil)
	w := httptest.NewRecorder()
	mc.Health(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.Status != "healthy" {
		t.Errorf("expected status=healthy, got %s", resp.Status)
	}
	if len(resp.Connections) != 2 {
		t.Errorf("expected 2 connections, got %d", len(resp.Connections))
	}
}

func TestHealthEndpoint_Degraded(t *testing.T) {
	mc := NewMetricsCollector(
		&mockHybridManager{
			connStats: []streaming.ConnectionStats{
				{ExchangeID: "binance", Connected: true, Healthy: true},
				{ExchangeID: "bingx", Connected: true, Healthy: false},
			},
		},
	)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/health", nil)
	w := httptest.NewRecorder()
	mc.Health(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for degraded, got %d", w.Code)
	}

	var resp HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if resp.Status != "degraded" {
		t.Errorf("expected degraded, got %s", resp.Status)
	}
}

func TestHealthEndpoint_Unhealthy(t *testing.T) {
	mc := NewMetricsCollector(
		&mockHybridManager{
			connStats: []streaming.ConnectionStats{
				{ExchangeID: "binance", Connected: false, Healthy: false},
				{ExchangeID: "bingx", Connected: false, Healthy: false},
			},
		},
	)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/health", nil)
	w := httptest.NewRecorder()
	mc.Health(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for unhealthy, got %d", w.Code)
	}

	var resp HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if resp.Status != "unhealthy" {
		t.Errorf("expected unhealthy, got %s", resp.Status)
	}
}

func TestHealthEndpoint_Starting(t *testing.T) {
	mc := NewMetricsCollector(
		&mockHybridManager{connStats: nil},
	)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/health", nil)
	w := httptest.NewRecorder()
	mc.Health(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for starting, got %d", w.Code)
	}

	var resp HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if resp.Status != "starting" {
		t.Errorf("expected starting, got %s", resp.Status)
	}
}

// callHealth runs the /health handler against the given collector and decodes
// the response, returning the HTTP status code and parsed body.
func callHealth(t *testing.T, mc *MetricsCollector) (int, HealthResponse) {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), "GET", "/health", nil)
	w := httptest.NewRecorder()
	mc.Health(w, req)
	var resp HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	return w.Code, resp
}

func containsScope(scopes []string, want string) bool {
	for _, s := range scopes {
		if s == want {
			return true
		}
	}
	return false
}

// TestHealthEndpoint_ExpectedScopes verifies the core outage-visibility fix:
// after the startup grace elapses, a missing/unconnected/unhealthy expected
// scope drives /health to 503/"unhealthy" with the offending scope reported,
// while during grace the lenient "starting"/200 behavior is preserved.
func TestHealthEndpoint_ExpectedScopes(t *testing.T) {
	const elapsedGrace = health.StartupGrace + time.Minute

	tests := []struct {
		name           string
		expectedScopes []string
		connStats      []streaming.ConnectionStats
		startedAgo     time.Duration // 0 => just started (within grace)
		wantHTTP       int
		wantStatus     string
		wantMissing    []string // scopes that must appear in missing_scopes
	}{
		{
			name:           "within grace empty stats stays starting",
			expectedScopes: []string{"binance:future", "bingx:future"},
			connStats:      nil,
			startedAgo:     0,
			wantHTTP:       http.StatusOK,
			wantStatus:     "starting",
		},
		{
			name:           "post grace missing expected scope is unhealthy",
			expectedScopes: []string{"binance:future", "bingx:future"},
			connStats: []streaming.ConnectionStats{
				// bingx reports raw "swap"; canonicalizes to bingx:future and is live.
				{ExchangeID: "bingx", MarketType: "swap", Connected: true, Healthy: true, Symbols: 150},
				// binance:future is entirely absent -> outage.
			},
			startedAgo:  elapsedGrace,
			wantHTTP:    http.StatusServiceUnavailable,
			wantStatus:  "unhealthy",
			wantMissing: []string{"binance:future"},
		},
		{
			name:           "post grace connected but unhealthy scope is unhealthy",
			expectedScopes: []string{"binance:future"},
			connStats: []streaming.ConnectionStats{
				{ExchangeID: "binance", MarketType: "future", Connected: true, Healthy: false, Symbols: 200},
			},
			startedAgo:  elapsedGrace,
			wantHTTP:    http.StatusServiceUnavailable,
			wantStatus:  "unhealthy",
			wantMissing: []string{"binance:future"},
		},
		{
			name:           "post grace all expected scopes live is healthy",
			expectedScopes: []string{"binance:future", "bingx:swap"},
			connStats: []streaming.ConnectionStats{
				{ExchangeID: "binance", MarketType: "future", Connected: true, Healthy: true, Symbols: 200},
				{ExchangeID: "bingx", MarketType: "swap", Connected: true, Healthy: true, Symbols: 150},
			},
			startedAgo: elapsedGrace,
			wantHTTP:   http.StatusOK,
			wantStatus: "healthy",
		},
		{
			name:           "within grace missing expected scope still lenient",
			expectedScopes: []string{"binance:future", "bingx:future"},
			connStats: []streaming.ConnectionStats{
				{ExchangeID: "bingx", MarketType: "swap", Connected: true, Healthy: true, Symbols: 150},
			},
			startedAgo: 0,
			wantHTTP:   http.StatusOK,
			wantStatus: "healthy", // anyConnected + allHealthy, expected check skipped during grace
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mc := NewMetricsCollector(&mockHybridManager{connStats: tc.connStats})
			mc.SetExpectedScopes(tc.expectedScopes)
			if tc.startedAgo > 0 {
				mc.startTime = time.Now().Add(-tc.startedAgo)
			}

			code, resp := callHealth(t, mc)

			if code != tc.wantHTTP {
				t.Fatalf("HTTP code: got %d, want %d", code, tc.wantHTTP)
			}
			if resp.Status != tc.wantStatus {
				t.Errorf("status: got %q, want %q", resp.Status, tc.wantStatus)
			}
			for _, want := range tc.wantMissing {
				if !containsScope(resp.MissingScopes, want) {
					t.Errorf("missing_scopes %v does not contain %q", resp.MissingScopes, want)
				}
			}
			if tc.wantStatus == "healthy" && len(resp.MissingScopes) != 0 {
				t.Errorf("expected no missing_scopes when healthy, got %v", resp.MissingScopes)
			}
		})
	}
}

// TestCanonicalScope locks the exchange-agnostic scope normalization so a
// healthy BingX (reported as "swap") satisfies an expected "future" scope.
func TestCanonicalScope(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"binance:future", "binance:future"},
		{"bingx:swap", "bingx:future"},
		{"binance:spot", "binance:spot"},
		{"binance:perpetual", "binance:future"},
		{"noscope", "noscope"},
	}
	for _, tc := range tests {
		if got := health.CanonicalScope(tc.in); got != tc.want {
			t.Errorf("health.CanonicalScope(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestReadyEndpoint_Ready(t *testing.T) {
	mc := NewMetricsCollector(
		&mockHybridManager{
			connStats: []streaming.ConnectionStats{
				{Connected: true},
			},
		},
	)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/ready", nil)
	w := httptest.NewRecorder()
	mc.Ready(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for ready, got %d", w.Code)
	}
}

func TestReadyEndpoint_NotReady(t *testing.T) {
	mc := NewMetricsCollector(
		&mockHybridManager{
			connStats: []streaming.ConnectionStats{
				{Connected: false},
			},
		},
	)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/ready", nil)
	w := httptest.NewRecorder()
	mc.Ready(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for not ready, got %d", w.Code)
	}
}

func TestConnectionStatsInterface(_ *testing.T) {
	// Verify BingXClient implements StatsProvider at compile time
	var _ streaming.StatsProvider = (*bingx.BingXClient)(nil)
}
