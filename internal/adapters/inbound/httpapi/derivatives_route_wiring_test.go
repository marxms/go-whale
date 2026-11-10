package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
)

// THE COMPOSITION ROOT, MEASURED THROUGH THE ROUTE — not through the private function.
//
// The independent verification left three LIVE mutants here, all with the same cause: the
// cases exercised `derivativesSeries` (private) and none went through the handler `router.go`
// assembles. Swapping `a.derivWindowReader()` for `nil`, or making `GetDerivativesSeries`
// call the ZSET directly again, kept the suite GREEN.
//
// It is the third time this feature reintroduces the same class: the sensor built and the
// cable unplugged. A unit test of the part can say nothing about the assembly, and the
// assembly is what the user receives.

// routeRouter builds the REAL router with both tiers doubled.
func routeRouter(t *testing.T, hot *windowHot, cold *windowCold, zset []*derivatives.DerivativesSnapshot) http.Handler {
	t.Helper()
	mds := &mockDerivativesStore{series: zset}
	return NewRouterWithMetrics(
		ReadPorts{}, "binance", "future", nil,
		mds, mds, mds,
		nil, nil, nil,
		derivatives.NewDerivativesWindowReader(hot, cold),
	)
}

func routeGet(t *testing.T, h http.Handler, url string) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, url, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s returned %d: %s", url, rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unreadable response: %v", err)
	}
	return body
}

// TestRoute_DeepWindowReachesTheBatchRoute is the DASHBOARD route, and it must go through the
// reader. MEASURED live: 1,200 buckets requested, 200 served, with no coverage marker — the
// long history reached the chart and did not reach the panel.
func TestRoute_DeepWindowReachesTheBatchRoute(t *testing.T) {
	now := time.Now().UTC()
	deepTs := now.Add(-600 * 24 * time.Hour).UnixMilli()

	hot := &windowHot{series: dailySeries(now, 30, "venue")}
	cold := &windowCold{series: []*derivatives.DerivativesSnapshot{
		derivWindowPoint(deepTs, derivatives.SourceCoinalyze, 1234),
	}}

	body := routeGet(t, routeRouter(t, hot, cold, hot.series),
		"/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe=1d&limit=800")

	data, _ := body["data"].(map[string]any)
	entradas, _ := data["BTC/USDT"].([]any)
	if len(entradas) == 0 {
		t.Fatalf("the batch served no point: %v", body)
	}
	var achou bool
	for _, e := range entradas {
		m, _ := e.(map[string]any)
		if ts, ok := m["timestamp"].(float64); ok && int64(ts) == deepTs {
			achou = true
		}
	}
	if !achou {
		t.Fatalf("the point from 600 days ago did not cross the BATCH (%d points) — the dashboard route does not go through the window reader", len(entradas))
	}
	if len(cold.scopes) == 0 {
		t.Error("the durable tier was not queried by the route")
	}
}

// NEGATIVE CONTROL: without a window reader the assembly keeps answering, with what the ZSET
// holds. Without this case, "the route serves points" would not distinguish a wired assembly
// from any assembly.
func TestRoute_WithoutTheWindowTheRouteStillServesTheZset(t *testing.T) {
	now := time.Now().UTC()
	zset := dailySeries(now, 30, "venue")
	mds := &mockDerivativesStore{series: zset}
	h := NewRouterWithMetrics(ReadPorts{}, "binance", "future", nil,
		mds, mds, mds, nil, nil, nil, nil)

	body := routeGet(t, h, "/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe=1d&limit=800")
	allData, _ := body["data"].(map[string]any)
	entries, _ := allData["BTC/USDT"].([]any)
	if len(entries) != len(zset) {
		t.Fatalf("without a reader the route served %d points, expected the ZSET's %d", len(entries), len(zset))
	}
}

// THE ROUTE'S ORDER IS PART OF THE CONTRACT, and breaking it moved the scored bar by five
// years.
//
// `GetSnapshotSeries` always returns DESCENDING — it is in the port — and
// `normalizedFromSeries` depends on it: `scoredPair` takes the first CLOSED bucket of the
// slice as "the newest". The window reader returns ascending, for an equally good reason.
// Two coherent pieces with opposite contracts.
//
// MEASURED live in the `1d` response with 2,000 points: `normalized.timestamp` came out as
// 2021-03-03, with `state: "mature"` and not a single line of error. With 30 recent points the
// coincidence does not show; a deep window is what exposes it.
func TestRoute_SeriesIsServedNewestFirst(t *testing.T) {
	now := time.Now().UTC()
	deepTs := now.Add(-600 * 24 * time.Hour).UnixMilli()

	hot := &windowHot{series: dailySeries(now, 30, "venue")}
	cold := &windowCold{series: []*derivatives.DerivativesSnapshot{
		derivWindowPoint(deepTs, derivatives.SourceCoinalyze, 1234),
	}}

	body := routeGet(t, routeRouter(t, hot, cold, hot.series),
		"/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe=1d&limit=800")

	allData, _ := body["data"].(map[string]any)
	pontos, _ := allData["BTC/USDT"].([]any)
	if len(pontos) < 2 {
		t.Fatalf("the route served %d point(s)", len(pontos))
	}
	stamp := func(i int) int64 {
		m, _ := pontos[i].(map[string]any)
		v, _ := m["timestamp"].(float64)
		return int64(v)
	}
	if stamp(0) <= stamp(len(pontos)-1) {
		t.Fatalf("the series came out ASCENDING (first %d, last %d); the route promises newest first, and `scoredPair` takes the first closed bucket as the scored bar",
			stamp(0), stamp(len(pontos)-1))
	}
	for i := 1; i < len(pontos); i++ {
		if stamp(i) > stamp(i-1) {
			t.Fatalf("the order breaks at index %d: %d after %d", i, stamp(i), stamp(i-1))
		}
	}
	// AND THE DEEP POINT IS THE LAST ONE — the depth is still served, from the other side.
	if got := stamp(len(pontos) - 1); got != deepTs {
		t.Errorf("the last point is %d, expected the deepest (%d)", got, deepTs)
	}
}

// AND THE SCORED BAR FOLLOWS: the normalized block has to speak of the RECENT bar.
func TestRoute_TheScoredBarIsTheRecentOne(t *testing.T) {
	now := time.Now().UTC()
	deepTs := now.Add(-600 * 24 * time.Hour).UnixMilli()

	hot := &windowHot{series: dailySeries(now, 30, "venue")}
	cold := &windowCold{series: []*derivatives.DerivativesSnapshot{
		derivWindowPoint(deepTs, derivatives.SourceCoinalyze, 1234),
	}}

	body := routeGet(t, routeRouter(t, hot, cold, hot.series),
		"/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe=1d&limit=800")

	normalized, _ := body["normalized"].(map[string]any)
	norm, _ := normalized["BTC/USDT"].(map[string]any)
	ts, _ := norm["timestamp"].(float64)
	if int64(ts) == deepTs {
		t.Fatalf("the decision axis was computed over the bar from 600 days ago")
	}
	if limite := now.Add(-45 * 24 * time.Hour).UnixMilli(); int64(ts) < limite {
		t.Errorf("the scored bar is %d, older than 45 days — the reading is about the instant, not the archive", int64(ts))
	}
}

// THE DURABLE TIER IS ONLY ASKED FOR WHAT IT HOLDS.
//
// MEASURED on the LAN, shortly after wiring the batch to the reader: the
// dashboard started asking `30m` for 30 symbols per probe, against a table holding only
// `1h`, `4h` and `1d` — the collector does not write `30m`. Each of those reads opened
// partitions to prove there was nothing, and the effect showed up as durable reads timing
// out in cascade in the log.
//
// A timeframe the collector does not write is not a deep window; it is guaranteed-empty work.
func TestRoute_TheDurableTierIsOnlyAskedForWhatItHolds(t *testing.T) {
	now := time.Now().UTC()
	// The warm tier covers little, so the REACH rule alone would send it to the durable tier.
	hot := &windowHot{series: dailySeries(now, 2, "venue")}

	for _, tc := range []struct {
		timeframe string
		esperado  int
		porque    string
	}{
		{"1d", 1, "the collector writes `1d`"},
		{"30m", 0, "the collector does NOT write `30m` — querying opens a partition for nothing"},
	} {
		cold := &windowCold{}
		mds := &mockDerivativesStore{series: hot.series}
		h := NewRouterWithMetrics(ReadPorts{}, "binance", "future", nil,
			mds, mds, mds,
			nil, nil, nil,
			derivatives.NewDerivativesWindowReader(hot, cold),
			"1h", "4h", "1d",
		)
		routeGet(t, h, "/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe="+tc.timeframe+"&limit=800")

		if len(cold.scopes) != tc.esperado {
			t.Errorf("%s: durable queried %d time(s), expected %d — %s",
				tc.timeframe, len(cold.scopes), tc.esperado, tc.porque)
		}
	}
}

// WITHOUT A DECLARATION, the decision falls back to reach alone — the reach-only behavior.
// Without this case, "never queries" would satisfy the case above.
func TestRoute_WithoutADeclarationEveryTimeframeMayReachTheDurableTier(t *testing.T) {
	now := time.Now().UTC()
	hot := &windowHot{series: dailySeries(now, 2, "venue")}
	cold := &windowCold{}

	mds := &mockDerivativesStore{series: hot.series}
	h := NewRouterWithMetrics(ReadPorts{}, "binance", "future", nil,
		mds, mds, mds,
		nil, nil, nil,
		derivatives.NewDerivativesWindowReader(hot, cold))
	routeGet(t, h, "/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe=30m&limit=800")

	if len(cold.scopes) != 1 {
		t.Errorf("without a declaration the durable tier was queried %d time(s), expected 1", len(cold.scopes))
	}
}

// THE COVERAGE RULE APPLIES TO BOTH ROUTES, and this is the dashboard one.
//
// MEASURED live by independent verification: `/api/batch/derivatives-series&limit=3000`
// served 2,000 points per symbol with an envelope carrying no `coverage` at all. The singular
// route was exemplary and the batch one — the address more people read — kept the silent
// truncation.
func TestRoute_TheBatchEnvelopeDeclaresCoveragePerSymbol(t *testing.T) {
	now := time.Now().UTC()
	hot := &windowHot{series: dailySeries(now, 30, "venue")}
	h := routeRouter(t, hot, &windowCold{}, hot.series)

	body := routeGet(t, h, "/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe=1d&limit=3000")
	cov, _ := body["coverage"].(map[string]any)
	if cov == nil {
		t.Fatal("the batch envelope does not declare coverage")
	}
	porSimbolo, _ := cov["BTC/USDT"].(map[string]any)
	if porSimbolo == nil {
		t.Fatalf("coverage is not per symbol: %v", cov)
	}
	if pedido, _ := porSimbolo["requested"].(float64); int(pedido) != 3000 {
		t.Errorf("requested=%v, expected 3000", porSimbolo["requested"])
	}
	if servido, _ := porSimbolo["served"].(float64); int(servido) != 30 {
		t.Errorf("served=%v, expected 30", porSimbolo["served"])
	}
	if parcial, _ := porSimbolo["partial"].(bool); !parcial {
		t.Error("30 of 3000 and the batch did not mark itself partial")
	}

	// NEGATIVE CONTROL, and it is what keeps the marker from accusing always.
	body = routeGet(t, h, "/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe=1d&limit=30")
	cov, _ = body["coverage"].(map[string]any)
	porSimbolo, _ = cov["BTC/USDT"].(map[string]any)
	if parcial, _ := porSimbolo["partial"].(bool); parcial {
		t.Errorf("30 de 30 marcado como parcial: %v", porSimbolo)
	}
}
