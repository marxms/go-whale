package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	binancesdk "github.com/marxms/go-whale/internal/adapters/outbound/binance"
	"github.com/marxms/go-whale/internal/adapters/outbound/binancews"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/recovery"
)

// The transport is controlled; the watchdog, candle engine and close callback are real.
// SDK/partition tests separately prove how terminal transport events set the latch.
type controlledKlineFeed struct {
	restart bool
	have    bool
	silent  float64
	stopped bool
	start   func([]string) error
	add     func([]string) error
}

func (f *controlledKlineFeed) Start(symbols []string) error {
	if f.start != nil {
		return f.start(symbols)
	}
	return nil
}

func (f *controlledKlineFeed) AddSymbols(symbols []string) error {
	if f.add != nil {
		return f.add(symbols)
	}
	return nil
}
func (f *controlledKlineFeed) Mute([]string) {}
func (f *controlledKlineFeed) Stop() {
	f.stopped = true
	f.have = false
}

func (f *controlledKlineFeed) SecondsSinceLastFrame() (float64, bool) {
	return f.silent, f.have
}
func (f *controlledKlineFeed) QuietestPartition() (float64, int) { return f.silent, 1 }
func (f *controlledKlineFeed) NeedsRestart() bool                { return f.restart }

func watchdogCandle(ts int64, volume float64) candle.Candle {
	return candle.Candle{
		Timestamp: ts, Open: 10, High: 12, Low: 9, Close: 11,
		Volume: volume, TakerBuyVolume: volume / 2, TakerSellVolume: volume / 2,
		Source: "ws_kline",
	}
}

func TestWatchdogLifecycleTerminalEventOverridesFreshClockAndMarksBeforeFrame(t *testing.T) {
	for _, cause := range []string{"eof", "normal_close", "server_shutdown", "renewal"} {
		t.Run(cause, func(t *testing.T) {
			ce := engine.NewCandleEngine(map[string]int{"default": 16})
			symbols := []string{"BTC/USDT", "ETH/USDT"}
			const bucket = int64(1_790_000_100_000)
			closed := make(map[string]candle.Candle)
			ce.OnCandleClose(func(key engine.CompositeKey, c candle.Candle) {
				if key.Timeframe == binancesdk.KlineFeedInterval {
					closed[key.Symbol] = c
				}
			})
			for _, s := range symbols {
				ce.UpdateCandle("binance", "future", s, binancesdk.KlineFeedInterval, watchdogCandle(bucket, 8))
			}
			old := &controlledKlineFeed{have: true, silent: 0, restart: true}
			a := &App{
				candleEngine: ce, streamCtx: context.Background(), klineFeed: old,
				klineScope: "binance:future", klineSymbols: symbols,
			}
			fresh := &controlledKlineFeed{have: true}
			a.newKlineFeed = func(_ context.Context, _, _, _ string, sink func(string, candle.Candle)) klineFeed {
				fresh.start = func(attached []string) error {
					for _, s := range attached {
						// This closes the old bucket BEFORE Start returns.
						sink(s, watchdogCandle(bucket+300_000, 10))
					}
					return nil
				}
				return fresh
			}
			if a.restartKlineFeedIfSilent(t.Context(), "binance", "future") {
				t.Fatal("watchdog stopped while a healthy replacement was available")
			}
			if a.klineFeed != fresh || !old.stopped {
				t.Fatal("terminal event with fresh clock did not replace the old producer")
			}
			for _, s := range symbols {
				c, ok := closed[s]
				if !ok || !c.HasGap || c.Timestamp != bucket || c.Volume != 8 || c.Source != "ws_kline" {
					t.Fatalf("%s old close = %+v (present=%v), want original volume and marked gap", s, c, ok)
				}
				got := ce.GetCandlesAsc("binance", "future", s, binancesdk.KlineFeedInterval, 0)
				if len(got) != 2 || got[1].Timestamp != bucket+300_000 || got[1].HasGap || got[1].Volume != 10 || got[1].Source != "ws_kline" {
					t.Fatalf("%s replacement candles = %+v, want one clean new bucket", s, got)
				}
			}
			if a.restartKlineFeedIfSilent(t.Context(), "binance", "future") || fresh.stopped {
				t.Fatal("healthy replacement was restarted again without a terminal event")
			}
		})
	}
}

func TestWatchdogLifecycleRetriesCurrentFleetIncludingPendingWithoutDerankedSymbols(t *testing.T) {
	ce := engine.NewCandleEngine(map[string]int{"default": 16})
	old := &controlledKlineFeed{have: true, restart: true, add: func([]string) error { return errors.New("dial failed") }}
	a := &App{
		candleEngine: ce, streamCtx: context.Background(), klineFeed: old,
		klineScope: "binance:future", klineSymbols: []string{"BTC/USDT", "ETH/USDT"},
		klineBackoff: binancews.Backoff{Max: time.Second}, // cadence covered by the backoff test
	}
	a.extendKlineFeed("binance", "future", []string{"SOL/USDT", "BAD/USDT", "SOL/USDT"})
	a.dropFromKlineFeed("binance", "future", []string{"ETH/USDT", "BAD/USDT"})
	want := []string{"BTC/USDT", "SOL/USDT"}
	var attempts [][]string
	var fresh *controlledKlineFeed
	a.newKlineFeed = func(_ context.Context, _, _, _ string, sink func(string, candle.Candle)) klineFeed {
		fresh = &controlledKlineFeed{have: true}
		fresh.start = func(symbols []string) error {
			attempts = append(attempts, append([]string(nil), symbols...))
			if len(attempts) <= 2 {
				return errors.New("replacement unavailable")
			}
			for _, s := range symbols {
				sink(s, watchdogCandle(1_790_000_100_000, 10))
			}
			return nil
		}
		return fresh
	}
	for i := range 3 {
		if a.restartKlineFeedIfSilent(t.Context(), "binance", "future") {
			t.Fatal("retry was disarmed before shutdown")
		}
		if len(attempts) != i+1 {
			t.Fatalf("attempts = %d, want %d", len(attempts), i+1)
		}
		if !reflect.DeepEqual(attempts[i], want) {
			t.Fatalf("attempt %d fleet = %v, want %v", i, attempts[i], want)
		}
		if i < 2 {
			if a.klineFeed != old || !fresh.stopped || !reflect.DeepEqual(a.klinePendingAttach, []string{"SOL/USDT"}) {
				t.Fatal("failed redial published a producer or lost pending fleet/cleanup")
			}
		}
	}
	if a.klineFeed != fresh || !reflect.DeepEqual(a.klineSymbols, want) || len(a.klinePendingAttach) != 0 {
		t.Fatal("successful recovery did not commit current fleet and clear pending attach")
	}
	for _, s := range want {
		got := ce.GetCandlesAsc("binance", "future", s, binancesdk.KlineFeedInterval, 0)
		if len(got) != 1 || got[0].Volume != 10 {
			t.Fatalf("%s did not resume exactly once: %+v", s, got)
		}
	}
	for _, s := range []string{"ETH/USDT", "BAD/USDT"} {
		if got := ce.GetCandlesAsc("binance", "future", s, binancesdk.KlineFeedInterval, 0); len(got) != 0 {
			t.Fatalf("deranked %s returned after recovery: %+v", s, got)
		}
	}
}

func TestWatchdogLifecycleDiscoveryKeepsUniqueRestartFleet(t *testing.T) {
	feed := &controlledKlineFeed{have: true}
	a := &App{klineFeed: feed, klineScope: "binance:future", klineSymbols: []string{"BTC/USDT"}}
	for range 2 {
		a.extendKlineFeed("binance", "future", []string{"BTC/USDT", "ETH/USDT", "ETH/USDT"})
	}
	if want := []string{"BTC/USDT", "ETH/USDT"}; !reflect.DeepEqual(a.klineSymbols, want) {
		t.Fatalf("restart fleet = %v, want %v", a.klineSymbols, want)
	}
}

func TestWatchdogLifecycleCancellationAbortsUnpublishedReplacement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	old := &controlledKlineFeed{have: true, restart: true}
	fresh := &controlledKlineFeed{}
	a := &App{streamCtx: ctx, klineFeed: old, klineScope: "binance:future", klineSymbols: []string{"BTC/USDT"}}
	entered := make(chan struct{})
	a.newKlineFeed = func(parent context.Context, _, _, _ string, _ func(string, candle.Candle)) klineFeed {
		fresh.start = func([]string) error {
			close(entered)
			<-parent.Done()
			return parent.Err()
		}
		return fresh
	}
	done := make(chan bool, 1)
	go func() { done <- a.restartKlineFeedIfSilent(ctx, "binance", "future") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("terminal event did not start replacement")
	}
	cancel()
	select {
	case stopped := <-done:
		if !stopped || a.klineFeed != old || !fresh.stopped {
			t.Fatal("canceled replacement was published or watchdog/transport did not stop")
		}
	case <-time.After(time.Second):
		t.Fatal("application cancellation did not abort unpublished replacement dial")
	}
}

func TestWatchdogLifecycleCancellationAfterStartRefusesPublication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	old := &controlledKlineFeed{have: true, restart: true}
	fresh := &controlledKlineFeed{have: true, start: func([]string) error { cancel(); return nil }}
	a := &App{streamCtx: ctx, klineFeed: old, klineScope: "binance:future"}
	a.newKlineFeed = func(context.Context, string, string, string, func(string, candle.Candle)) klineFeed {
		return fresh
	}
	if !a.restartKlineFeedIfSilent(ctx, "binance", "future") || a.klineFeed != old || !fresh.stopped {
		t.Fatal("successful dial racing shutdown published a replacement or left its resources active")
	}
}

func TestWatchdogLifecycleEmptyDesiredFleetRemainsIdleForDiscovery(t *testing.T) {
	feed := &controlledKlineFeed{}
	a := &App{streamCtx: context.Background(), klineFeed: feed, klineScope: "binance:future"}
	a.newKlineFeed = func(context.Context, string, string, string, func(string, candle.Candle)) klineFeed {
		t.Fatal("empty desired fleet opened a replacement transport")
		return nil
	}
	for range 3 {
		if a.restartKlineFeedIfSilent(t.Context(), "binance", "future") {
			t.Fatal("empty fleet stopped watching before discovery can add symbols")
		}
	}
	if a.klineFeed != feed || feed.stopped || len(a.klineSymbols) != 0 || len(a.klinePendingAttach) != 0 {
		t.Fatal("idle empty fleet was changed instead of retained for discovery")
	}
}

func TestWatchdogLifecycleGapAdmissionRetainsHeadAcrossPartialFailure(t *testing.T) {
	ce := engine.NewCandleEngine(map[string]int{"default": 16})
	manager := recovery.NewManager(nil, 1, candle.NewTimeframeRegistry([]string{"5m"}))
	defer manager.Stop() // Workers are never started; observe actual queue admission only.
	oldHead := time.Now().Add(-71 * time.Second).UnixMilli()
	ce.UpdateCandle("binance", "future", "BTC/USDT", "5m", watchdogCandle(oldHead, 8))
	old := &controlledKlineFeed{have: true, restart: true}
	a := &App{
		streamCtx:    context.Background(),
		klineBackoff: binancews.Backoff{Max: time.Second}, // cadence covered by the backoff test
		candleEngine: ce, gapRecovery: manager,
		klineFeed: old, klineScope: "binance:future", klineSymbols: []string{"BTC/USDT"},
	}
	attempt := 0
	var newHead int64
	a.newKlineFeed = func(_ context.Context, _, _, _ string, sink func(string, candle.Candle)) klineFeed {
		return &controlledKlineFeed{have: true, start: func([]string) error {
			attempt++
			newHead = time.Now().UnixMilli()
			// Model a head in the policy's fresh (<5s) branch, delivered before dial returns.
			sink("BTC/USDT", watchdogCandle(newHead, 10))
			if attempt == 1 {
				return errors.New("later partition failed after first partition delivered")
			}
			return nil
		}}
	}
	if a.restartKlineFeedIfSilent(t.Context(), "binance", "future") {
		t.Fatal("failed attempt disarmed retry")
	}
	if head, ok := a.klineGapHeads.GetLatestTimestamp(t.Context(), "binance", "future", "BTC/USDT", "5m"); !ok || head != oldHead {
		t.Fatalf("reserved head = %d (present=%v), want %d before partial delivery", head, ok, oldHead)
	}
	if a.klineFeed != old || manager.Stats().Queued != 0 {
		t.Fatal("partial failure published replacement or admitted bulk recovery prematurely")
	}
	if a.restartKlineFeedIfSilent(t.Context(), "binance", "future") {
		t.Fatal("successful retry stopped watchdog")
	}
	if time.Since(time.UnixMilli(newHead)) >= recovery.BootGapMinWindow {
		t.Fatal("fixture no longer exercises the policy's fresh-head skip branch")
	}
	if got := manager.Stats().Queued; got != 1 {
		t.Fatalf("bulk recovery queue = %d, want one reserved hole despite fresh head", got)
	}
	if len(a.klineGapHeads) != 0 || a.klineFeed == old {
		t.Fatal("successful admission retained the old reservation or failed to publish replacement")
	}
}
