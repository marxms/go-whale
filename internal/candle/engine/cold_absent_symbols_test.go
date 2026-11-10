package engine

import (
	"context"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// TestColdCompleteness_EmptyScopeIsNotPerfect is the regression for the thirteen-hour
// blind spot: a cold tier that answers with NOTHING is otherwise indistinguishable from a
// cold tier with no gaps, because missing buckets are only measurable BETWEEN rows that
// exist. cold_missing_buckets is 0 in both cases and always will be — the discrimination
// has to come from the present-symbol count.
func TestColdCompleteness_EmptyScopeIsNotPerfect(t *testing.T) {
	tfReg := candle.NewTimeframeRegistry([]string{"1h"})
	lister := &fakeLister{symbols: []redisstore.SymbolInfo{
		{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT"},
		{Exchange: "binance", MarketType: "future", Symbol: "ETH/USDT"},
		{Exchange: "binance", MarketType: "future", Symbol: "SOL/USDT"},
	}}

	t.Run("cold tier holds nothing at all", func(t *testing.T) {
		// The exact outage shape: no rows anywhere, so the scan finds no groups.
		scanner := &fakeScanner{missing: 0, gaps: 0, present: 0}
		m := NewColdCompletenessMonitor(scanner, lister, tfReg, time.Minute)
		rec := newEngineTestMetrics()
		m.SetMetrics(rec.port())
		if err := m.scanOnce(context.Background()); err != nil {
			t.Fatalf("scanOnce: %v", err)
		}

		if got := rec.coldMissing[key("binance", "future", "1h")]; got != 0 {
			t.Fatalf("cold_missing_buckets is structurally 0 for an empty tier, got %v", got)
		}
		if got := rec.coldExpected[key("binance", "future", "1h")]; got != 3 {
			t.Fatalf("cold_symbols_expected = %v, want 3", got)
		}
		if got := rec.coldAbsent[key("binance", "future", "1h")]; got != 3 {
			t.Fatalf("cold_symbols_absent = %v, want 3 — an empty cold tier MUST be distinguishable from a complete one", got)
		}
	})

	t.Run("partial coverage reports only the absent share", func(t *testing.T) {
		scanner := &fakeScanner{missing: 7, gaps: 1, present: 2}
		m := NewColdCompletenessMonitor(scanner, lister, tfReg, time.Minute)
		rec := newEngineTestMetrics()
		m.SetMetrics(rec.port())
		if err := m.scanOnce(context.Background()); err != nil {
			t.Fatalf("scanOnce: %v", err)
		}
		if got := rec.coldAbsent[key("binance", "future", "1h")]; got != 1 {
			t.Fatalf("cold_symbols_absent = %v, want 1", got)
		}
		if got := rec.coldMissing[key("binance", "future", "1h")]; got != 7 {
			t.Fatalf("cold_missing_buckets = %v, want 7 (interior holes still reported)", got)
		}
	})

	t.Run("more present than subscribed clamps at zero", func(t *testing.T) {
		// A symbol deranked minutes ago still has rows in the 48h window.
		scanner := &fakeScanner{present: 9}
		m := NewColdCompletenessMonitor(scanner, lister, tfReg, time.Minute)
		rec := newEngineTestMetrics()
		m.SetMetrics(rec.port())
		if err := m.scanOnce(context.Background()); err != nil {
			t.Fatalf("scanOnce: %v", err)
		}
		if got := rec.coldAbsent[key("binance", "future", "1h")]; got != 0 {
			t.Fatalf("cold_symbols_absent = %v, want 0 (never negative)", got)
		}
	})
}
