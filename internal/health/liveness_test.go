package health

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/market"
)

type fakeLivenessProvider struct {
	latest map[string]int64
}

func (f *fakeLivenessProvider) key(exchange, marketType, symbol, tf string) string {
	return exchange + "|" + marketType + "|" + symbol + "|" + tf
}

func (f *fakeLivenessProvider) GetLatestTimestamp(_ context.Context, exchange, marketType, symbol, tf string) (int64, bool) {
	ts, ok := f.latest[f.key(exchange, marketType, symbol, tf)]
	return ts, ok
}

type recordingLivenessMetrics struct {
	total map[string]float64
	stale map[string]float64
}

func (m *recordingLivenessMetrics) SetSymbolsTotal(exchange, marketType string, n float64) {
	if m.total == nil {
		m.total = map[string]float64{}
	}
	m.total[exchange+"|"+marketType] = n
}

func (m *recordingLivenessMetrics) SetSymbolsStale(exchange, marketType string, n float64) {
	if m.stale == nil {
		m.stale = map[string]float64{}
	}
	m.stale[exchange+"|"+marketType] = n
}

// TestSampleFleetLiveness proves that per-symbol fleet freshness is computed from
// the persisted provider, the gauges are set, and a scope whose stale fraction
// exceeds LivenessStaleFraction is reported as failing.
func TestSampleFleetLiveness(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	const tf = "1m"
	intervalMs := int64(60_000)
	staleLagMs := int64(3) * intervalMs
	fresh := now.UnixMilli() - intervalMs
	stale := now.UnixMilli() - 10*intervalMs

	scope := LivenessScope{ExchangeID: "binance", MarketType: market.Future}
	internalMarket := scope.MarketType.String()
	scopeKey := scope.ExchangeID + "|" + internalMarket
	failingKey := scope.ExchangeID + ":" + internalMarket

	p := &fakeLivenessProvider{}
	mkKey := func(sym string) string {
		return p.key(scope.ExchangeID, internalMarket, market.NormalizeSymbol(sym), tf)
	}

	tests := []struct {
		name      string
		latest    map[string]int64
		subs      []string
		wantTotal float64
		wantStale float64
		wantFail  bool
	}{
		{
			name: "majority frozen -> scope fails liveness",
			latest: map[string]int64{
				mkKey("BTC/USDT"):  fresh,
				mkKey("ETH/USDT"):  stale,
				mkKey("SOL/USDT"):  stale,
				mkKey("DOGE/USDT"): stale,
			},
			subs:      []string{"binance:future:BTC/USDT", "binance:future:ETH/USDT", "binance:future:SOL/USDT", "binance:future:DOGE/USDT"},
			wantTotal: 4,
			wantStale: 3,
			wantFail:  true,
		},
		{
			name: "all fresh -> no failing scope",
			latest: map[string]int64{
				mkKey("BTC/USDT"): fresh,
				mkKey("ETH/USDT"): fresh,
			},
			subs:      []string{"binance:future:BTC/USDT", "binance:future:ETH/USDT"},
			wantTotal: 2,
			wantStale: 0,
			wantFail:  false,
		},
		{
			name: "below 20% threshold -> not failing",
			latest: func() map[string]int64 {
				m := map[string]int64{}
				for _, s := range []string{"A", "B", "C", "D", "E", "F", "G", "H", "I"} {
					m[mkKey(s+"/USDT")] = fresh
				}
				m[mkKey("J/USDT")] = stale
				return m
			}(),
			subs: []string{
				"binance:future:A/USDT", "binance:future:B/USDT", "binance:future:C/USDT",
				"binance:future:D/USDT", "binance:future:E/USDT", "binance:future:F/USDT",
				"binance:future:G/USDT", "binance:future:H/USDT", "binance:future:I/USDT",
				"binance:future:J/USDT",
			},
			wantTotal: 10,
			wantStale: 1,
			wantFail:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingLivenessMetrics{}
			provider := &fakeLivenessProvider{latest: tt.latest}
			isStale := func(latest int64, have bool) bool {
				if !have {
					return true
				}
				return now.UnixMilli()-latest >= staleLagMs
			}

			failing, freshness := SampleFleetLiveness(t.Context(), []LivenessScope{scope}, provider, tt.subs, tf, isStale, rec)

			if got := rec.total[scopeKey]; got != tt.wantTotal {
				t.Errorf("StreamSymbolsTotal = %v, want %v", got, tt.wantTotal)
			}
			if got := rec.stale[scopeKey]; got != tt.wantStale {
				t.Errorf("StreamSymbolsStale = %v, want %v", got, tt.wantStale)
			}

			hasFailing := false
			for _, s := range failing {
				if s == failingKey {
					hasFailing = true
				}
			}
			if hasFailing != tt.wantFail {
				t.Errorf("scope %q failing=%v, want %v (failing=%v)", failingKey, hasFailing, tt.wantFail, failing)
			}

			if len(freshness) != 1 {
				t.Fatalf("freshness detail = %+v, want exactly one scope", freshness)
			}
			detail := freshness[0]
			if detail.Scope != failingKey {
				t.Errorf("freshness scope = %q, want %q", detail.Scope, failingKey)
			}
			if detail.Symbols != int(tt.wantTotal) || detail.Stale != int(tt.wantStale) {
				t.Errorf("freshness = %+v, want symbols=%v stale=%v", detail, tt.wantTotal, tt.wantStale)
			}
			if !detail.HaveData || detail.FreshestAtMs != fresh {
				t.Errorf("freshness watermark = (have=%v, %d); every case has at least one fresh symbol %d",
					detail.HaveData, detail.FreshestAtMs, fresh)
			}
		})
	}
}

func TestSampleFleetLiveness_NilMetricsIsSafe(t *testing.T) {
	provider := &fakeLivenessProvider{latest: map[string]int64{}}
	scopes := []LivenessScope{{ExchangeID: "binance", MarketType: market.Future}}
	failing, freshness := SampleFleetLiveness(t.Context(), scopes, provider, []string{"binance:future:BTC/USDT"}, "1m",
		func(int64, bool) bool { return false }, nil)
	if len(failing) != 0 {
		t.Fatalf("unexpected failing scopes: %v", failing)
	}
	if len(freshness) != 1 || freshness[0].Symbols != 1 || freshness[0].HaveData {
		t.Fatalf("freshness = %+v, want one monitored symbol with no readable data", freshness)
	}
}
