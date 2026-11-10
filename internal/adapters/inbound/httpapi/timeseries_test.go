package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

func testSupportedTimeframes() []string {
	return []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"}
}

// mockStore implements every narrow read port the handler tests exercise.
type mockStore struct {
	candles      []candle.Candle
	rangeCandles []candle.Candle
	rangeCalled  bool
	rangeFrom    time.Time
	rangeTo      time.Time
	rangeLimit   int
	err          error
}

func (m *mockStore) AddActiveSymbol(_ context.Context, _, _, _ string) error {
	return nil
}

func (m *mockStore) GetActiveSymbols(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}

func (m *mockStore) GetTimeframes(_ context.Context, _, _, _ string) ([]string, error) {
	return nil, nil
}

func (m *mockStore) GetCandlesDesc(_ context.Context, _, _, _, _ string, limit int) ([]candle.Candle, error) {
	if m.err != nil {
		return nil, m.err
	}
	if limit < len(m.candles) {
		return m.candles[:limit], nil
	}
	return m.candles, nil
}

func (m *mockStore) GetLastTrade(_ context.Context, _, _, _ string) (*candle.LastTrade, error) {
	return nil, nil
}

// GetBatchCandlesDesc mirrors GetCandlesDesc per request. On m.err it skips
// every request (empty map), matching the real store's skip-on-error contract.
func (m *mockStore) GetBatchCandlesDesc(_ context.Context, _, _ string, reqs []candle.CandleReq) map[candle.CandleReq][]candle.Candle {
	out := make(map[candle.CandleReq][]candle.Candle, len(reqs))
	if m.err != nil {
		return out
	}
	for _, rq := range reqs {
		if rq.Limit > 0 && rq.Limit < len(m.candles) {
			out[rq] = m.candles[:rq.Limit]
			continue
		}
		out[rq] = m.candles
	}
	return out
}

// GetCandlesRange satisfies the range-query interface assertion in the handler.
func (m *mockStore) GetCandlesRange(_ context.Context, _, _, _, _ string, from, to time.Time, limit int) ([]candle.Candle, error) {
	m.rangeCalled = true
	m.rangeFrom = from
	m.rangeTo = to
	m.rangeLimit = limit
	if m.err != nil {
		return nil, m.err
	}
	if m.rangeCandles != nil {
		return m.rangeCandles, nil
	}
	return m.candles, nil
}

func newTestHandler(ms *mockStore) *Handler {
	return &Handler{
		candles:           ms,
		batchCandles:      ms,
		symbols:           ms,
		lastTrades:        ms,
		defaultExchange:   "binance",
		defaultMarketType: "future",
		tfReg:             candle.NewTimeframeRegistry(testSupportedTimeframes()),
		datetimeCache:     newRFC3339Cache(),
	}
}

func makeCandles(n int, startMs int64) []candle.Candle {
	out := make([]candle.Candle, n)
	for i := range out {
		out[i] = candle.Candle{
			Timestamp: startMs + int64(i)*60_000,
			Open:      100 + float64(i),
			High:      101 + float64(i),
			Low:       99 + float64(i),
			Close:     100.5 + float64(i),
			Volume:    10 + float64(i),
		}
	}
	return out
}

func doTimeseriesRequest(t *testing.T, handler *Handler, symbol, timeframe, queryString string) *httptest.ResponseRecorder {
	t.Helper()
	values := url.Values{"symbol": {symbol}, "timeframe": {timeframe}}
	target := "/api/timeseries?" + values.Encode()
	if queryString != "" {
		target += "&" + queryString
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)

	w := httptest.NewRecorder()
	handler.GetTimeseriesQuery(w, req)
	return w
}

// --- Tests ---

func TestTimeseries_DefaultLimit(t *testing.T) {
	ms := &mockStore{candles: makeCandles(300, 1000000)}
	h := newTestHandler(ms)

	w := doTimeseriesRequest(t, h, "BTC/USDT", "5m", "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp TimeseriesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	// Default limit = 800, mock has 300 → should return all available candle.
	if resp.Count != 300 {
		t.Errorf("expected 300 candles, got %d", resp.Count)
	}
}

func TestTimeseries_CustomLimit(t *testing.T) {
	ms := &mockStore{candles: makeCandles(500, 1000000)}
	h := newTestHandler(ms)

	w := doTimeseriesRequest(t, h, "BTC/USDT", "5m", "limit=50")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp TimeseriesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Count != 50 {
		t.Errorf("expected 50 candles, got %d", resp.Count)
	}
}

func TestTimeseries_LimitCappedAt5000(t *testing.T) {
	ms := &mockStore{candles: makeCandles(100, 1000000)}
	h := newTestHandler(ms)

	w := doTimeseriesRequest(t, h, "BTC/USDT", "5m", "limit=9999")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	// The handler should cap limit to 5000, but mock only has 100
	// Verify no error and we get what's available
	var resp TimeseriesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Count != 100 {
		t.Errorf("expected 100 candles (mock size), got %d", resp.Count)
	}
	// Verify range was NOT called (no from/to)
	if ms.rangeCalled {
		t.Error("GetCandlesRange should not be called without from/to")
	}
}

func TestTimeseries_FromToCallsGetCandlesRange(t *testing.T) {
	rangeData := makeCandles(10, 2000000)
	ms := &mockStore{
		candles:      makeCandles(50, 1000000),
		rangeCandles: rangeData,
	}
	h := newTestHandler(ms)

	w := doTimeseriesRequest(t, h, "BTC/USDT", "5m", "from=2000000&to=3000000&limit=10")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if !ms.rangeCalled {
		t.Fatal("expected GetCandlesRange to be called")
	}
	if ms.rangeFrom != time.UnixMilli(2000000) {
		t.Errorf("expected from=2000000ms, got %v", ms.rangeFrom)
	}
	if ms.rangeTo != time.UnixMilli(3000000) {
		t.Errorf("expected to=3000000ms, got %v", ms.rangeTo)
	}
	if ms.rangeLimit != 10 {
		t.Errorf("expected limit=10, got %d", ms.rangeLimit)
	}

	var resp TimeseriesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Count != 10 {
		t.Errorf("expected 10 candles, got %d", resp.Count)
	}
}

func TestTimeseries_FromOnlyCallsRange(t *testing.T) {
	ms := &mockStore{candles: makeCandles(20, 1000000)}
	h := newTestHandler(ms)

	w := doTimeseriesRequest(t, h, "BTC/USDT", "5m", "from=1500000")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !ms.rangeCalled {
		t.Fatal("expected GetCandlesRange when from is provided")
	}
	if ms.rangeTo.IsZero() == false {
		t.Error("expected to be zero when not provided")
	}
}

func TestTimeseries_InvalidLimitIgnored(t *testing.T) {
	ms := &mockStore{candles: makeCandles(300, 1000000)}
	h := newTestHandler(ms)

	w := doTimeseriesRequest(t, h, "BTC/USDT", "5m", "limit=abc")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp TimeseriesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	// Invalid limit falls back to default 800.
	if resp.Count != 300 {
		t.Errorf("expected 300 (default fallback), got %d", resp.Count)
	}
}

func TestTimeseries_InvalidFromIgnored(t *testing.T) {
	ms := &mockStore{candles: makeCandles(20, 1000000)}
	h := newTestHandler(ms)

	w := doTimeseriesRequest(t, h, "BTC/USDT", "5m", "from=notanumber")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	// Invalid from => no range query, falls back to standard
	if ms.rangeCalled {
		t.Error("GetCandlesRange should not be called with invalid from")
	}
}

// TestTimeseries_TimeframeGuardTypedCode covers CON-06 on the streamer end: a timeframe outside
// the served set responds 400 with the `invalid_timeframe` code in addition to the `error`
// prose.
//
// BOTH paths are asserted on purpose, and each one kills a different guard defect: the
// not-served path kills a guard that lets everything through (`if false` instead of
// `!IsSupported`), and the served path kills a guard that rejects everything (`if true`, or the
// inverted condition). One path alone does not discriminate between the two.
func TestTimeseries_TimeframeGuardTypedCode(t *testing.T) {
	tests := []struct {
		name       string
		timeframe  string
		wantStatus int
		wantCode   string
		wantError  string
	}{
		{
			name:       "served timeframe answers 200 and carries no typed error",
			timeframe:  "5m",
			wantStatus: http.StatusOK,
			wantCode:   "",
			wantError:  "",
		},
		{
			name:       "timeframe outside the set answers 400 invalid_timeframe",
			timeframe:  "3h",
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_timeframe",
			wantError:  "unsupported timeframe",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := &mockStore{candles: makeCandles(10, 1000000)}
			h := newTestHandler(ms)

			w := doTimeseriesRequest(t, h, "BTC/USDT", tt.timeframe, "")
			if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", w.Code, tt.wantStatus, w.Body.String())
			}

			var body map[string]any
			if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
				t.Fatalf("decoding body: %v", err)
			}
			gotCode, _ := body["code"].(string)
			if gotCode != tt.wantCode {
				t.Errorf("code = %q, want %q", gotCode, tt.wantCode)
			}
			gotError, _ := body["error"].(string)
			if gotError != tt.wantError {
				t.Errorf("error = %q, want %q", gotError, tt.wantError)
			}
		})
	}
}

func TestTimeseries_NoParamsBackwardCompatible(t *testing.T) {
	ms := &mockStore{candles: makeCandles(50, 1000000)}
	h := newTestHandler(ms)

	w := doTimeseriesRequest(t, h, "BTC/USDT", "5m", "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ms.rangeCalled {
		t.Error("GetCandlesRange should NOT be called when no from/to params")
	}
	var resp TimeseriesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Count != 50 {
		t.Errorf("expected 50 candles, got %d", resp.Count)
	}
}
