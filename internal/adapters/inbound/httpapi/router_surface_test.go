package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

func surfaceRouter() http.Handler {
	store := &mockStore{candles: makeCandles(2, 1_700_000_000_000)}
	mds := &mockDerivativesStore{}
	return NewRouterWithMetrics(
		ReadPorts{
			Candles:      store,
			BatchCandles: store,
			Symbols:      store,
			LastTrades:   store,
		},
		"binance", "future", nil,
		mds, mds, mds,
		candle.NewTimeframeRegistry(testSupportedTimeframes()), nil, nil, nil,
	)
}

func TestRouterSurfaceRejectsRemovedRoutes(t *testing.T) {
	router := surfaceRouter()
	removed := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v2/batch/prices?symbols=BTC%2FUSDT"},
		{http.MethodGet, "/api/v2/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe=5m"},
		{http.MethodGet, "/api/timeseries/BTC%2FUSDT/5m"},
		{http.MethodGet, "/api/timeseries/BTC%2FUSDT"},
		{http.MethodGet, "/api/trades/BTC%2FUSDT"},
		{http.MethodPost, "/api/subscribe"},
		{http.MethodPost, "/api/unsubscribe"},
		{http.MethodGet, "/api/derivatives/BTC%2FUSDT"},
		{http.MethodGet, "/api/derivatives/BTC%2FUSDT/series?timeframe=5m"},
		{http.MethodGet, "/stats"},
		{http.MethodPost, "/admin/backfill"},
		{http.MethodGet, "/swagger/index.html"},
	}
	for _, route := range removed {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), route.method, route.path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s = %d, want 404", route.method, route.path, rec.Code)
		}
	}
}

func TestRouterSurfaceServesCanonicalContracts(t *testing.T) {
	router := surfaceRouter()
	kept := []string{
		"/api/timeseries?symbol=BTC%2FUSDT&timeframe=5m",
		"/api/subscriptions",
		"/api/batch/prices?symbols=BTC%2FUSDT",
		"/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe=5m",
		"/health",
		"/openapi.json",
	}
	for _, path := range kept {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200: %s", path, rec.Code, rec.Body.String())
		}
	}
}

func TestOpenAPIIsGeneratedFromTheMountedRoutes(t *testing.T) {
	router := surfaceRouter()
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/openapi.json", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /openapi.json = %d", rec.Code)
	}

	var spec struct {
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &spec); err != nil {
		t.Fatalf("decode spec: %v", err)
	}
	if len(spec.Paths) == 0 {
		t.Fatal("OpenAPI paths are empty: the handler is walking a fresh router instead of the mounted one")
	}
	for _, path := range []string{"/api/timeseries", "/api/batch/prices", "/api/batch/derivatives-series", "/health"} {
		methods, ok := spec.Paths[path]
		if !ok {
			t.Errorf("OpenAPI is missing %s", path)
			continue
		}
		if methods["get"].OperationID == "" {
			t.Errorf("OpenAPI operationId is empty for GET %s", path)
		}
	}
}
