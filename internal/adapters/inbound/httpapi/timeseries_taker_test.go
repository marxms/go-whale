package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// Regression for feature 002 (trade-signal-clarity) G3: the HTTP DTO must
// surface all taker-aggregate fields populated by the candle aggregator so
// the analyzer's streamer-native order-flow path is reachable in production.
// Before this guard, toCandleDTO silently stripped takerBuyVolume / cvdIncrement
// / aggressionRatio etc., forcing every consumer onto the clv-proxy fallback.
func TestTimeseries_TakerAggregatesPropagateToDTO(t *testing.T) {
	ms := &mockStore{candles: []candle.Candle{
		{
			Timestamp: 1_700_000_000_000,
			Open:      100, High: 101, Low: 99, Close: 100.5, Volume: 10,
			TakerBuyVolume:   6,
			TakerSellVolume:  4,
			Source:           "binance-trade-stream",
			AggregateMethod:  "streamer-native",
			SideUnknownCount: 1,
			LateTradeCount:   2,
			HasGap:           false,
		},
	}}
	h := newTestHandler(ms)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/timeseries?symbol=BTC%2FUSDT&timeframe=5m", nil)
	w := httptest.NewRecorder()
	h.GetTimeseriesQuery(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}

	raw := w.Body.String()
	var resp TimeseriesResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 1 {
		t.Fatalf("expected 1 candle, got %d", resp.Count)
	}
	c := resp.Data[0]
	if c.TakerBuyVolume != 6 || c.TakerSellVolume != 4 {
		t.Errorf("taker volumes lost: buy=%v sell=%v", c.TakerBuyVolume, c.TakerSellVolume)
	}
	if c.Source != "binance-trade-stream" {
		t.Errorf("source lost: %q", c.Source)
	}
	if c.AggregateMethod != "streamer-native" {
		t.Errorf("aggregateMethod lost: %q", c.AggregateMethod)
	}
	if c.SideUnknownCount != 1 || c.LateTradeCount != 2 {
		t.Errorf("diagnostic counters lost: unknown=%v late=%v", c.SideUnknownCount, c.LateTradeCount)
	}

	// Raw JSON must contain the analyzer-expected keys verbatim — guards
	// against future struct-tag renames silently breaking the contract.
	for _, key := range []string{
		`"takerBuyVolume":6`,
		`"takerSellVolume":4`,
		`"source":"binance-trade-stream"`,
		`"aggregateMethod":"streamer-native"`,
	} {
		if !contains(raw, key) {
			t.Errorf("raw JSON missing key fragment %s\nbody=%s", key, raw)
		}
	}
}

// TestTimeseries_FlowVoidCandleStillCarriesThePair pins that a candle with no order flow
// still serializes WITH the taker keys.
//
// Omitting them would erase a legitimate zero: a bar where all aggression sat on one side
// reads identically to a bar nobody measured. The derived fields once offered a second copy;
// they are gone, so the pair is always emitted and 0/0 is what consumers read as "volume
// traded, no side classified".
//
// The DERIVED keys must still be absent: they moved to the consumer.
func TestTimeseries_FlowVoidCandleStillCarriesThePair(t *testing.T) {
	ms := &mockStore{candles: []candle.Candle{
		{Timestamp: 1_700_000_000_000, Open: 100, High: 101, Low: 99, Close: 100.5, Volume: 10},
	}}
	h := newTestHandler(ms)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/timeseries?symbol=BTC%2FUSDT&timeframe=5m", nil)
	w := httptest.NewRecorder()
	h.GetTimeseriesQuery(w, req)

	raw := w.Body.String()
	for _, key := range []string{`"takerBuyVolume":0`, `"takerSellVolume":0`} {
		if !contains(raw, key) {
			t.Errorf("flow-void candle must still carry %s — an erased zero is indistinguishable "+
				"from an unmeasured bar: %s", key, raw)
		}
	}
	for _, key := range []string{"cvdIncrement", "cvdCumulative", "aggressionRatio", "source", "aggregateMethod"} {
		if contains(raw, `"`+key+`"`) {
			t.Errorf("candle should omit %q but body contains it: %s", key, raw)
		}
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	n, m := len(s), len(sub)
	for i := 0; i+m <= n; i++ {
		if s[i:i+m] == sub {
			return i
		}
	}
	return -1
}
