package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"

	"github.com/go-chi/chi/v5"
)

// testHandlerLogger returns the stderr logger only in the observation child process. Ordinary
// tests pass nil, which keeps the handler silent.
func testHandlerLogger() *slog.Logger {
	if os.Getenv("STREAMER_TEST_BATCH_OBSERVATION") != "1" {
		return nil
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// newMarketDataHandlerWith builds the handler from one fixture that implements the three
// derivatives ports. The production constructor takes them separately so each route
// depends only on the capability it uses; the fixtures here all implement all three.
func newMarketDataHandlerWith(
	store interface {
		derivatives.Reader
		derivatives.BaselineReader
		derivatives.PriceReader
	},
	defaultExchange string,
	defaultMarketType string,
	tfReg *candle.TimeframeRegistry,
) *MarketDataHandler {
	return NewMarketDataHandler(store, store, store, defaultExchange, defaultMarketType, tfReg, testHandlerLogger())
}

type mockDerivativesStore struct {
	series []*derivatives.DerivativesSnapshot
	latest *derivatives.DerivativesSnapshot
	err    error

	// priceIncrementValue / priceIncrementReason / priceIncrementErr stand in for the
	// candle series the real store reads. The zero value is "no candle for this bar",
	// which is the honest default for a fixture that declares no price at all.
	priceIncrementValue  *float64
	priceIncrementReason string
	priceIncrementErr    error
	// priceIncrementBar records the bar the handler asked about, so a test can assert
	// the price axis is scored on the SAME bar as the other two.
	priceIncrementBar int64
	priceIncrementMu  sync.Mutex
}

func (m *mockDerivativesStore) SaveSnapshot(_ context.Context, _ *derivatives.DerivativesSnapshot) error {
	return nil
}

func (m *mockDerivativesStore) SaveSeriesOnly(_ context.Context, _ *derivatives.DerivativesSnapshot, _ string) error {
	return nil
}

func (m *mockDerivativesStore) GetLatestSnapshot(_ context.Context, _, _, _ string) (*derivatives.DerivativesSnapshot, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.latest, nil
}

func (m *mockDerivativesStore) GetSnapshotSeries(_ context.Context, _, _, _, _ string, limit int) ([]*derivatives.DerivativesSnapshot, error) {
	if m.err != nil {
		return nil, m.err
	}
	if limit <= 0 || limit >= len(m.series) {
		return m.series, nil
	}
	return m.series[:limit], nil
}

// GetBaseline satisfies the store interface. The envelope declares WHAT a field
// means, not what its ruler says, so a nil baseline is the honest fixture here —
// and it is the state the real store reports before the first recompute.
func (m *mockDerivativesStore) GetBaseline(context.Context, string, string, string, string) (*derivatives.DerivativesBaseline, error) {
	return nil, nil
}

// PriceIncrement stands in for the candle series. It records the bar it was asked
// about — the price axis has to be scored on the same bar as the other two, and a
// silent drift there would be invisible in the response alone.
func (m *mockDerivativesStore) PriceIncrement(_ context.Context, _, _, _, _ string, barTs int64) (*float64, string, error) {
	m.priceIncrementMu.Lock()
	defer m.priceIncrementMu.Unlock()
	m.priceIncrementBar = barTs
	if m.priceIncrementErr != nil {
		return nil, "", m.priceIncrementErr
	}
	if m.priceIncrementValue == nil && m.priceIncrementReason == "" {
		return nil, "no candle is stored for this bar or the one before it", nil
	}
	return m.priceIncrementValue, m.priceIncrementReason, nil
}

type derivativesSeriesPoint struct {
	Timestamp       int64                            `json:"timestamp"`
	SourceTimestamp int64                            `json:"sourceTimestamp"`
	Snapshot        *derivatives.DerivativesSnapshot `json:"snapshot"`
}

type derivativesSeriesResponse struct {
	Status     string                   `json:"status"`
	Symbol     string                   `json:"symbol"`
	Exchange   string                   `json:"exchange"`
	MarketType string                   `json:"marketType"`
	Timeframe  string                   `json:"timeframe"`
	Count      int                      `json:"count"`
	Data       []derivativesSeriesPoint `json:"data"`
}

func attachSymbolParam(path string) *chi.Context {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("symbol", path)
	return rctx
}

func TestGetDerivativesSeries_DirectReturnAndOICoverage(t *testing.T) {
	// Simulate per-timeframe ZSET data: 12 entries at 30m intervals (bucket-aligned)
	baseTs := int64(1700000000000) // arbitrary base
	bucketMs := int64(1800000)     // 30m in ms
	series := make([]*derivatives.DerivativesSnapshot, 0, 12)
	for i := 0; i < 12; i++ {
		oi := 100.0 + float64(i*10)
		series = append(series, &derivatives.DerivativesSnapshot{
			Exchange:     "binance",
			MarketType:   "future",
			Symbol:       "BTC/USDT",
			Timestamp:    baseTs + int64(i)*bucketMs,
			OpenInterest: &oi,
		})
	}

	// Reverse to newest-first (like ZREVRANGE returns)
	reversed := make([]*derivatives.DerivativesSnapshot, len(series))
	for i, s := range series {
		reversed[len(series)-1-i] = s
	}

	h := newMarketDataHandlerWith(&mockDerivativesStore{series: reversed}, "binance", "future", nil)

	t.Run("returns_all_entries_directly", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/derivatives/BTC%2FUSDT/series?exchange=binance&marketType=future&limit=200&timeframe=30m", nil)
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTC/USDT")))
		w := httptest.NewRecorder()

		h.GetDerivativesSeries(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}

		var resp derivativesSeriesResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}

		if resp.Timeframe != "30m" {
			t.Fatalf("expected timeframe 30m, got %s", resp.Timeframe)
		}

		// All 12 entries should be returned directly
		if resp.Count != 12 {
			t.Fatalf("expected count 12, got %d", resp.Count)
		}

		// All entries should have OI
		oiNonNull := 0
		oidNonNull := 0
		for _, point := range resp.Data {
			if point.Snapshot == nil {
				t.Fatalf("snapshot should not be nil")
			}
			if point.Snapshot.OpenInterest != nil {
				oiNonNull++
			}
			if point.Snapshot.OIChangePct != nil {
				oidNonNull++
			}
		}

		if oiNonNull != 12 {
			t.Fatalf("expected OpenInterest non-null 12/12, got %d/12", oiNonNull)
		}

		// The read path does NOT derive OI change: the fixture stores none, so none may
		// appear. Deriving it from the NOTIONAL field would use contracts × price — a column
		// that moves with the price by identity.
		if oidNonNull != 0 {
			t.Fatalf("expected OIChangePct to stay absent, got %d/12 populated", oidNonNull)
		}

		// Step between consecutive points should be 30m (1800000ms)
		if len(resp.Data) >= 2 {
			step := resp.Data[0].Timestamp - resp.Data[1].Timestamp
			if step < 0 {
				step = -step
			}
			if step != bucketMs {
				t.Fatalf("expected step %d ms, got %d ms", bucketMs, step)
			}
		}
	})
}

func TestGetDerivativesSeries_StoreError(t *testing.T) {
	h := newMarketDataHandlerWith(&mockDerivativesStore{err: errors.New("boom")}, "binance", "future", nil)
	req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/derivatives/BTC%2FUSDT/series?exchange=binance&marketType=future&timeframe=15m", nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTC/USDT")))
	w := httptest.NewRecorder()

	h.GetDerivativesSeries(w, req)

	if w.Code != 500 {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}
