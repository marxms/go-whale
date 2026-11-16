package app

import (
	"context"
	"reflect"
	"testing"
	"time"

	binancesdk "github.com/marxms/go-whale/internal/adapters/outbound/binance"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
)

// Regression: one silent partition made the watchdog restart the whole fleet, closing the healthy
// sister and marking every symbol. Only the silent partition may be touched, and only its members
// may be marked.
func TestWatchdogRestartsOnlyTheSilentPartition(t *testing.T) {
	ce := engine.NewCandleEngine(map[string]int{"default": 16})
	const bucket = int64(1_790_000_100_000)
	symbols := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}
	for _, s := range symbols {
		ce.UpdateCandle("binance", "future", s, binancesdk.KlineFeedInterval, watchdogCandle(bucket, 8))
	}

	old := &controlledKlineFeed{
		have:            true,
		silent:          90,
		partitionSilent: true,
		victim:          binancesdk.PartitionStat{ID: 7, Symbols: 2, LastFrameAge: 90 * time.Second},
		victimMembers:   []string{"BTC/USDT", "ETH/USDT"},
	}
	a := &App{
		candleEngine: ce, streamCtx: context.Background(), klineFeed: old,
		klineScope: "binance:future", klineSymbols: symbols,
	}

	if a.restartKlineFeedIfSilent(t.Context(), "binance", "future") {
		t.Fatal("watchdog asked the loop to stop")
	}
	if old.stopped {
		t.Fatal("the fleet was stopped for a single partition's fault")
	}
	if a.klineFeed != old {
		t.Fatal("the feed was replaced for a partition-scoped fault")
	}
	if !reflect.DeepEqual(old.restartedParts, []int{7}) {
		t.Fatalf("restarted partitions = %v, want exactly [7]", old.restartedParts)
	}
	for _, s := range symbols {
		got := ce.GetCandlesAsc("binance", "future", s, binancesdk.KlineFeedInterval, 0)
		if len(got) != 1 {
			t.Fatalf("%s candles = %d, want 1", s, len(got))
		}
		wantMarked := s == "BTC/USDT" || s == "ETH/USDT"
		if got[0].HasGap != wantMarked {
			t.Fatalf("%s HasGap = %v, want %v — only the silent partition's symbols lost their producer",
				s, got[0].HasGap, wantMarked)
		}
	}
}

// When every partition is silent the partition-scoped path must decline and the fleet restart must
// run: healing one partition per tick would leave the rest dead behind a healthy-looking log.
func TestWatchdogFallsBackToFleetRestartWhenNoSisterIsAlive(t *testing.T) {
	ce := engine.NewCandleEngine(map[string]int{"default": 16})
	const bucket = int64(1_790_000_100_000)
	symbols := []string{"BTC/USDT", "ETH/USDT"}
	for _, s := range symbols {
		ce.UpdateCandle("binance", "future", s, binancesdk.KlineFeedInterval, watchdogCandle(bucket, 8))
	}

	old := &controlledKlineFeed{
		have:            true,
		silent:          90,
		partitionSilent: false, // all partitions silent: SilentPartition declines
	}
	a := &App{
		candleEngine: ce, streamCtx: context.Background(), klineFeed: old,
		klineScope: "binance:future", klineSymbols: symbols,
	}
	fresh := &controlledKlineFeed{have: true}
	a.newKlineFeed = func(_ context.Context, _, _, _ string, _ func(string, candle.Candle)) klineFeed {
		return fresh
	}

	if a.restartKlineFeedIfSilent(t.Context(), "binance", "future") {
		t.Fatal("watchdog asked the loop to stop")
	}
	if !old.stopped {
		t.Fatal("a fleet-wide silence must still restart the fleet")
	}
	if a.klineFeed != fresh {
		t.Fatal("the feed was not replaced after a fleet-wide silence")
	}
}
