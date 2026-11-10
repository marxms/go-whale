package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
)

// ─────────────────────────────────────────────────────────────────────────────
// WHAT THE ROUTE OWES THE WEEKLY TIMEFRAME.
//
// The requirement says "`1w` SHALL be declared as a structural absence on the three
// quantities". Satisfying it by MAKING THE ROUTE STOP SERVING `1w` is the trap. Measured on
// the LAN, the price of that reading:
//
//	`1w` in QuestDB... 0 rows (correct — Coinalyze has no weekly)
//	`1w` via the route. 30 weekly points, 27 with `fundingRate8h`
//
// A test that verified "1w returns nothing" would be a 200-day regression of weekly funding,
// with the gate green. The two cases below demand the conjunction: the declaration appears
// AND the data keeps arriving.
// ─────────────────────────────────────────────────────────────────────────────

// weeklySeries builds `n` ascending weekly buckets ending at `end`.
func weeklySeries(end time.Time, n int) []*derivatives.DerivativesSnapshot {
	out := make([]*derivatives.DerivativesSnapshot, 0, n)
	for i := n - 1; i >= 0; i-- {
		out = append(out, derivWindowPoint(end.Add(-time.Duration(i)*7*24*time.Hour).UnixMilli(), "venue", 1000+float64(i)))
	}
	return out
}

// routerWithDurableCoverage builds the REAL router declaring which timeframes have durable data.
func routerWithDurableCoverage(zset []*derivatives.DerivativesSnapshot, durable ...string) http.Handler {
	mds := &mockDerivativesStore{series: zset}
	return NewRouterWithMetrics(
		ReadPorts{}, "binance", "future", nil,
		mds, mds, mds,
		nil, nil, nil,
		derivatives.NewDerivativesWindowReader(&windowHot{series: zset}, &windowCold{}),
		durable...,
	)
}

func decodeBody(t *testing.T, h http.Handler, url string) map[string]any {
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

// THE CONJUNCTION: the weekly stays served AND declares the durable absence on the three
// quantities.
//
// THE MUTANT THIS CASE KILLS is the one that "satisfies" the requirement by refusing the
// timeframe — returning 404, an empty list or an error. The first half of the case fails
// immediately.
func TestRoute_WeeklyKeepsBeingServedAndDeclaresTheDurableAbsence(t *testing.T) {
	semanal := weeklySeries(time.Now().UTC(), 30)
	router := routerWithDurableCoverage(semanal, "5m", "15m", "30m", "1h", "2h", "4h", "1d")

	body := decodeBody(t, router, "/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe=1w&limit=30")

	// HALF ONE — THE DATA KEEPS ARRIVING. 30 weekly points exist in the existing source, and
	// that is what the chart client reads today.
	allData, ok := body["data"].(map[string]any)
	if !ok {
		t.Fatalf("the batch brought no data: %v", body["data"])
	}
	data, ok := allData["BTC/USDT"].([]any)
	if !ok || len(data) == 0 {
		t.Fatalf("1w came back with no data (%v points) — the existing source serves the weekly and the declaration must not erase it", len(data))
	}
	if len(data) != 30 {
		t.Errorf("points = %d, want 30 — the points the existing source has", len(data))
	}

	// HALF TWO — THE ABSENCE REACHES THE CLIENT, with the three quantities and the reason.
	measurement, ok := body["measurement"].(map[string]any)
	if !ok {
		t.Fatal("the response brought no measurement declaration")
	}
	absences, ok := measurement["durableAbsences"].([]any)
	if !ok {
		t.Fatalf("the declaration brought no durable absences: %v", measurement["durableAbsences"])
	}
	if len(absences) != 3 {
		t.Fatalf("declared %d absences, want 3 — one per quantity", len(absences))
	}
	grandezas := map[string]bool{}
	for _, raw := range absences {
		cell := raw.(map[string]any)
		grandezas[cell["feed"].(string)] = true
		if cell["timeframe"] != "1w" {
			t.Errorf("absence with timeframe %v, want 1w", cell["timeframe"])
		}
		if cell["source"] != "coinalyze" {
			t.Errorf("absence with source %v, want coinalyze", cell["source"])
		}
		if reason, _ := cell["reason"].(string); reason == "" {
			t.Errorf("absence of %v came without a reason — a declaration without a reason is decoration", cell["feed"])
		}
	}
	for _, feed := range []string{"open_interest", "long_short_ratio", "funding_rate"} {
		if !grandezas[feed] {
			t.Errorf("%s has no absence declaration", feed)
		}
	}
}

// NEGATIVE CONTROL: a COVERED timeframe declares no absence at all. Without it, an
// implementation that declared the three on every response would pass the case above.
func TestRoute_ACoveredTimeframeDeclaresNoAbsence(t *testing.T) {
	router := routerWithDurableCoverage(dailySeries(time.Now().UTC(), 10, "venue"),
		"5m", "15m", "30m", "1h", "2h", "4h", "1d")

	body := decodeBody(t, router, "/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe=1d&limit=10")

	measurement := body["measurement"].(map[string]any)
	if got, present := measurement["durableAbsences"]; present {
		t.Errorf("1d has a durable tier and declared an absence: %v", got)
	}
}
