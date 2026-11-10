package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

func TestExpectedBuckets(t *testing.T) {
	const h = int64(3600000)
	tests := []struct {
		name               string
		min, max, tf, want int64
	}{
		{"single bucket", 1000, 1000, h, 1},
		{"two 1h buckets", 0, h, h, 2},
		{"six 1h buckets", 0, 5 * h, h, 6},
		{"invalid tf returns 0 (no phantom gaps)", 0, h, 0, 0},
		{"max<min returns 0", 5 * h, 0, h, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := candle.ExpectedBuckets(tt.min, tt.max, tt.tf); got != tt.want {
				t.Fatalf("candle.ExpectedBuckets(%d,%d,%d) = %d, want %d", tt.min, tt.max, tt.tf, got, tt.want)
			}
		})
	}
}

type fakeScanner struct {
	calls   int
	missing int64
	gaps    int
	present int
	err     error
}

func (f *fakeScanner) ScanScopeCompleteness(_ context.Context, _, _, _ string) (candle.ScopeCompleteness, error) {
	f.calls++
	return candle.ScopeCompleteness{
		MissingBuckets:  f.missing,
		SymbolsWithGaps: f.gaps,
		SymbolsPresent:  f.present,
	}, f.err
}

type fakeLister struct {
	symbols []redisstore.SymbolInfo
	err     error
}

func (f *fakeLister) GetAllActiveSymbols(_ context.Context) ([]redisstore.SymbolInfo, error) {
	return f.symbols, f.err
}

func TestColdCompletenessMonitor_scanOnce(t *testing.T) {
	tfReg := candle.NewTimeframeRegistry([]string{"1h", "1d"})

	t.Run("scans each DISTINCT scope x timeframe (symbols dedup by scope)", func(t *testing.T) {
		scanner := &fakeScanner{missing: 3, gaps: 1}
		lister := &fakeLister{symbols: []redisstore.SymbolInfo{
			{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT"},
			{Exchange: "binance", MarketType: "future", Symbol: "ETH/USDT"}, // same scope
			{Exchange: "bingx", MarketType: "swap", Symbol: "SOL/USDT"},     // distinct scope
		}}
		m := NewColdCompletenessMonitor(scanner, lister, tfReg, time.Minute)
		if err := m.scanOnce(context.Background()); err != nil {
			t.Fatalf("scanOnce: %v", err)
		}
		if scanner.calls != 4 { // 2 distinct scopes x 2 timeframes
			t.Fatalf("want 4 scope scans, got %d", scanner.calls)
		}
	})

	t.Run("lister error aborts the scan", func(t *testing.T) {
		m := NewColdCompletenessMonitor(&fakeScanner{}, &fakeLister{err: errors.New("redis down")}, tfReg, time.Minute)
		if err := m.scanOnce(context.Background()); err == nil {
			t.Fatal("expected error from lister")
		}
	})

	t.Run("per-scope scan error is skipped, not fatal", func(t *testing.T) {
		scanner := &fakeScanner{err: errors.New("query failed")}
		lister := &fakeLister{symbols: []redisstore.SymbolInfo{{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT"}}}
		m := NewColdCompletenessMonitor(scanner, lister, tfReg, time.Minute)
		if err := m.scanOnce(context.Background()); err != nil {
			t.Fatalf("per-scope errors should not abort scanOnce, got %v", err)
		}
	})
}
