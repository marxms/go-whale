package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// The analyzer's cold start walks the whole fleet; the per-symbol route costs one round trip each.
// This route must return ONE timeframe for MANY symbols in a single call, carrying the same candle
// contract — including the gap cause that decides whether a bar may reach a decision.
func TestGetBatchTimeseriesReturnsOneTimeframeForManySymbols(t *testing.T) {
	candles := makeCandles(3, 1_790_000_000_000)
	candles[2].HasGap = true
	candles[2].GapKind = candle.GapProvisional
	handler := newTestHandler(&mockStore{candles: candles})

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet,
		"/api/batch/timeseries?timeframe=5m&limit=3&symbols=BTC/USDT,ETH/USDT", nil)
	recorder := httptest.NewRecorder()
	handler.GetBatchTimeseries(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	var body struct {
		Timeframe string                 `json:"timeframe"`
		Data      map[string][]CandleDTO `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Timeframe != "5m" {
		t.Fatalf("timeframe = %q, want 5m", body.Timeframe)
	}
	if len(body.Data) != 2 {
		t.Fatalf("symbols = %d, want 2", len(body.Data))
	}
	for _, symbol := range []string{"BTC/USDT", "ETH/USDT"} {
		rows := body.Data[symbol]
		if len(rows) != 3 {
			t.Fatalf("%s rows = %d, want 3", symbol, len(rows))
		}
		if rows[2].GapCause != "provisional" {
			t.Fatalf("%s gapCause = %q, want provisional", symbol, rows[2].GapCause)
		}
	}
}

func TestGetBatchTimeseriesRejectsBadRequests(t *testing.T) {
	handler := newTestHandler(&mockStore{candles: makeCandles(3, 1_790_000_000_000)})

	manySymbols := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		manySymbols = append(manySymbols, "S"+strconv.Itoa(i)+"/USDT")
	}
	for _, target := range []string{
		"/api/batch/timeseries?timeframe=5m&symbols=",                 // no symbol
		"/api/batch/timeseries?timeframe=5m&limit=0&symbols=BTC/USDT", // limit below the floor
		"/api/batch/timeseries?timeframe=5m&limit=abc&symbols=BTC/USDT",
		"/api/batch/timeseries?timeframe=3m&symbols=BTC/USDT",                                    // unmodeled timeframe
		"/api/batch/timeseries?timeframe=5m&limit=500&symbols=" + strings.Join(manySymbols, ","), // 50k candles
	} {
		recorder := httptest.NewRecorder()
		handler.GetBatchTimeseries(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, target, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("%s → status %d, want 400", target, recorder.Code)
		}
	}
}
