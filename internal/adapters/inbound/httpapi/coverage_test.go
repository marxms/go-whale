package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/health"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// coverageStubManager is a HybridManagerStats whose connection stats the test controls.
type coverageStubManager struct{ stats []streaming.ConnectionStats }

func (m *coverageStubManager) GetConnectionStats() []streaming.ConnectionStats { return m.stats }

// TestHealth_CoverageShortfallIsUnhealthy is the RED guard for
//
// Production ran 200 of 500 configured symbols for ~11 hours — consistent across four
// independent sources — while /health reported "status":"healthy". Nothing in the system
// compared EFFECTIVE coverage against CONFIGURED coverage, so a 60% shortfall was invisible
// to every probe and dashboard.
func TestHealth_CoverageShortfallIsUnhealthy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		configured  int
		effective   int
		wantStatus  int
		wantDeficit bool
	}{
		{
			name:       "shortfall is reported as unhealthy with the deficit named",
			configured: 500, effective: 200,
			wantStatus: http.StatusServiceUnavailable, wantDeficit: true,
		},
		{
			name:       "full coverage reports healthy",
			configured: 500, effective: 500,
			wantStatus: http.StatusOK, wantDeficit: false,
		},
		{
			name:       "coverage above configured is not a deficit",
			configured: 500, effective: 501,
			wantStatus: http.StatusOK, wantDeficit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mgr := &coverageStubManager{stats: []streaming.ConnectionStats{{
				ExchangeID: "binance", MarketType: "future",
				Connected: true, Healthy: true, Symbols: tt.effective,
			}}}
			mc := NewMetricsCollector(mgr)
			mc.SetExpectedScopes([]string{"binance:future"})
			mc.SetConfiguredCoverage(map[string]int{"binance:future": tt.configured})
			// Past the startup grace: a boot that never finished must not hide behind it.
			mc.startTime = time.Now().Add(-2 * health.StartupGrace)

			rec := httptest.NewRecorder()
			mc.Health(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil))

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d (body: %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}

			var resp HealthResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("health body is not valid JSON: %v", err)
			}
			if tt.wantDeficit && len(resp.CoverageDeficits) == 0 {
				t.Error("deficit not named in the response: an operator must see WHICH scope is short without reading logs")
			}
			if !tt.wantDeficit && len(resp.CoverageDeficits) != 0 {
				t.Errorf("unexpected deficit reported: %v", resp.CoverageDeficits)
			}
		})
	}
}
