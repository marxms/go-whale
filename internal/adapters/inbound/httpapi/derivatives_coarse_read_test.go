package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"

	"github.com/go-chi/chi/v5"
)

type canonicalRouteStore struct {
	mockDerivativesStore
	baseline                *derivatives.DerivativesBaseline
	failure                 error
	bundleCalls, splitCalls int
}

func (s *canonicalRouteStore) HasCanonicalCoarse(tf string) bool { return tf == "1d" || tf == "1w" }
func (s *canonicalRouteStore) GetCanonicalReading(context.Context, string, string, string, string) (*derivatives.DerivativesBaseline, error) {
	s.bundleCalls++
	return s.baseline, s.failure
}

func (s *canonicalRouteStore) GetBaseline(context.Context, string, string, string, string) (*derivatives.DerivativesBaseline, error) {
	s.splitCalls++
	return nil, errors.New("split read must not be used")
}

func TestCanonicalCoarsePublicReadsUseAtomicBundle(t *testing.T) {
	for _, tf := range []string{"1d", "1w"} {
		t.Run(tf, func(t *testing.T) {
			step := candle.ParseTimeframeToSeconds(tf) * 1000
			current := candle.BucketTimestamp(time.Now().UnixMilli(), tf)
			a, b, c := 120.0, 110.0, 100.0
			ls := 1.2
			price := .01
			points := []*derivatives.DerivativesSnapshot{{Timestamp: current, Source: derivatives.SourceCoinalyze, OpenInterestBase: &a, LongShortRatio: &ls}, {Timestamp: current - step, Source: derivatives.SourceCoinalyze, OpenInterestBase: &b, LongShortRatio: &ls}, {Timestamp: current - 2*step, Source: derivatives.SourceCoinalyze, OpenInterestBase: &c, LongShortRatio: &ls}}
			ruler := &derivatives.Baseline{Median: 0, MAD: .01, N: 100}
			store := &canonicalRouteStore{mockDerivativesStore: mockDerivativesStore{priceIncrementValue: &price}, baseline: &derivatives.DerivativesBaseline{Source: derivatives.SourceCoinalyze, SeriesTail: points, WindowN: 288, NMin: 30, UpdatedAt: time.Now().UnixMilli(), OpenInterest: ruler, LongShortRatio: ruler, Price: ruler}}
			cold := &windowCold{}
			hot := &windowHot{}
			h := newMarketDataHandlerWith(store, "binance", "future", nil).WithDerivativesWindow(derivatives.NewDerivativesWindowReader(hot, cold))
			batch := batchRequest(t, h, "/api/batch/derivatives-series?timeframe="+tf+"&limit=3&symbols=BTC/USDT")
			var response BatchDerivativesSeriesEnvelope
			if err := json.Unmarshal(batch.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if batch.Code != 200 || len(response.Data["BTC/USDT"]) != 3 || response.Data["BTC/USDT"][1].Source != derivatives.SourceCoinalyze || response.Normalized["BTC/USDT"].OpenInterest.Z == nil {
				t.Fatalf("batch=%s", batch.Body.String())
			}
			req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/derivatives/BTC%2FUSDT/series?timeframe="+tf+"&limit=3", nil)
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTC/USDT")))
			w := httptest.NewRecorder()
			h.GetDerivativesSeries(w, req)
			var single DerivativesSeriesResponse
			if err := json.Unmarshal(w.Body.Bytes(), &single); err != nil {
				t.Fatal(err)
			}
			if w.Code != 200 || single.Count != 3 || single.Data[1].Snapshot.Source != derivatives.SourceCoinalyze || single.Normalized.OpenInterest.Z == nil {
				t.Fatalf("single=%s", w.Body.String())
			}
			if tf == "1w" && (len(single.Measurement.DurableAbsences) != 0 || len(response.Measurement.DurableAbsences) != 0) {
				t.Fatal("derived weekly observations incorrectly declared unavailable")
			}
			if store.bundleCalls != 2 || store.splitCalls != 0 || hot.calls != 0 || len(cold.scopes) != 0 {
				t.Fatalf("queries bundle=%d split=%d hot=%d cold=%d", store.bundleCalls, store.splitCalls, hot.calls, len(cold.scopes))
			}
			store.baseline.SeriesTail = nil
			empty := batchRequest(t, h, "/api/batch/derivatives-series?timeframe="+tf+"&limit=3&symbols=BTC/USDT")
			var emptyResponse BatchDerivativesSeriesEnvelope
			if err := json.Unmarshal(empty.Body.Bytes(), &emptyResponse); err != nil {
				t.Fatal(err)
			}
			if emptyResponse.Normalized["BTC/USDT"].OpenInterest.State != derivatives.BaselineImmature {
				t.Fatalf("empty canonical history not declared immature: %+v", emptyResponse.Normalized)
			}
			store.failure = errors.New("canonical unavailable")
			failed := batchRequest(t, h, "/api/batch/derivatives-series?timeframe="+tf+"&limit=3&symbols=BTC/USDT")
			response = BatchDerivativesSeriesEnvelope{}
			if err := json.Unmarshal(failed.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Normalized["BTC/USDT"].UnavailableReason != "series_unavailable" || len(response.Data["BTC/USDT"]) != 0 {
				t.Fatalf("fallback hidden: %s", failed.Body.String())
			}
		})
	}
}

func TestCanonicalCoarseNormalizationIndependentOfPayloadLimit(t *testing.T) {
	for _, tf := range []string{"1d", "1w"} {
		t.Run(tf, func(t *testing.T) {
			step := candle.ParseTimeframeToSeconds(tf) * 1000
			current := candle.BucketTimestamp(time.Now().UnixMilli(), tf)
			oi := []float64{120, 110, 100}
			ls := []float64{1.4, 1.3, 1.2}
			points := make([]*derivatives.DerivativesSnapshot, 3)
			for i := range points {
				points[i] = &derivatives.DerivativesSnapshot{Timestamp: current - int64(i)*step, Source: derivatives.SourceCoinalyze, OpenInterestBase: &oi[i], LongShortRatio: &ls[i]}
			}
			price := .01
			ruler := &derivatives.Baseline{Median: 0, MAD: .01, N: 100}
			store := &canonicalRouteStore{mockDerivativesStore: mockDerivativesStore{priceIncrementValue: &price}, baseline: &derivatives.DerivativesBaseline{Source: derivatives.SourceCoinalyze, SeriesTail: points, WindowN: 288, NMin: 30, OpenInterest: ruler, LongShortRatio: ruler, Price: ruler}}
			cold, hot := &windowCold{}, &windowHot{}
			h := newMarketDataHandlerWith(store, "binance", "future", nil).WithDerivativesWindow(derivatives.NewDerivativesWindowReader(hot, cold))
			assertReading := func(got NormalizedReading) {
				t.Helper()
				if got.Timestamp != current-step || got.WindowN != 288 {
					t.Fatalf("wrong scored reading: %+v", got)
				}
				for _, tc := range []struct {
					axis     NormalizedAxis
					value, z float64
				}{
					{got.OpenInterest, 0.09531017980432477, 6.428583556206986},
					{got.LongShortRatio, 0.08004270767353656, 5.398806668928677},
					{got.Price, .01, 0.6744907594765952},
				} {
					if tc.axis.Value == nil || tc.axis.Z == nil || math.Abs(*tc.axis.Value-tc.value) > 1e-12 || math.Abs(*tc.axis.Z-tc.z) > 1e-12 || tc.axis.State != derivatives.BaselineMature || tc.axis.N != 100 || tc.axis.NMin != 30 {
						t.Fatalf("axis=%+v want value=%g z=%g", tc.axis, tc.value, tc.z)
					}
				}
			}
			for _, limit := range []int{1, 2, 3} {
				batch := batchRequest(t, h, fmt.Sprintf("/api/batch/derivatives-series?timeframe=%s&limit=%d&symbols=BTC/USDT", tf, limit))
				var response BatchDerivativesSeriesEnvelope
				if err := json.Unmarshal(batch.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if batch.Code != 200 || len(response.Data["BTC/USDT"]) != limit {
					t.Fatalf("batch limit=%d: %s", limit, batch.Body.String())
				}
				reading := response.Normalized["BTC/USDT"]
				assertReading(reading)
				req := httptest.NewRequestWithContext(t.Context(), "GET", fmt.Sprintf("/api/derivatives/BTC%%2FUSDT/series?timeframe=%s&limit=%d", tf, limit), nil)
				req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTC/USDT")))
				w := httptest.NewRecorder()
				h.GetDerivativesSeries(w, req)
				var single DerivativesSeriesResponse
				if err := json.Unmarshal(w.Body.Bytes(), &single); err != nil {
					t.Fatal(err)
				}
				if w.Code != 200 || single.Count != limit {
					t.Fatalf("single limit=%d: %s", limit, w.Body.String())
				}
				assertReading(single.Normalized)
			}
			if store.bundleCalls != 6 || store.splitCalls != 0 || hot.calls != 0 || len(cold.scopes) != 0 {
				t.Fatalf("queries bundle=%d split=%d hot=%d cold=%d", store.bundleCalls, store.splitCalls, hot.calls, len(cold.scopes))
			}
		})
	}
}
