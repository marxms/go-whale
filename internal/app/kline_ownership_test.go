package app

import (
	"context"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/ingestion"
)

type captureKlineFeed struct {
	sink    func(string, candle.Candle)
	started []string
}

func (f *captureKlineFeed) Start(symbols []string) error { f.started = symbols; return nil }
func (f *captureKlineFeed) AddSymbols([]string) error    { return nil }
func (f *captureKlineFeed) Mute([]string)                {}
func (f *captureKlineFeed) Stop()                        {}
func (f *captureKlineFeed) SecondsSinceLastFrame() (float64, bool) {
	return 0, true
}
func (f *captureKlineFeed) QuietestPartition() (float64, int) { return 0, 1 }
func (f *captureKlineFeed) NeedsRestart() bool                { return false }

func newOwnershipApp(t *testing.T) (*App, *ingestion.StreamTradeHandler, *captureKlineFeed) {
	t.Helper()
	tfReg := candle.NewTimeframeRegistry([]string{"5m", "15m"})
	ce := engine.NewCandleEngine(map[string]int{"default": 16})
	handler := ingestion.NewStreamTradeHandler(nil, "binance", "future", tfReg)
	a := &App{candleEngine: ce, handler: handler, tfReg: tfReg}
	feed := &captureKlineFeed{}
	a.newKlineFeed = func(_ context.Context, _, _, _ string, sink func(string, candle.Candle)) klineFeed {
		feed.sink = sink
		return feed
	}
	return a, handler, feed
}

func TestOwnershipDeclaredOnFirstFrame(t *testing.T) {
	a, handler, feed := newOwnershipApp(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a.startKlineFeed(ctx, "binance", "future", []string{"BTC/USDT"})
	if !handler.TapeWritesCandles("binance", "future") {
		t.Fatal("ownership was declared on a successful dial, before any frame")
	}

	feed.sink("BTC/USDT", watchdogCandle(1_790_000_100_000, 1))
	if handler.TapeWritesCandles("binance", "future") {
		t.Fatal("the first delivered frame did not hand candle ownership to the venue")
	}
}

func TestOwnershipDeferredWhenNoFrame(t *testing.T) {
	a, handler, _ := newOwnershipApp(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a.startKlineFeed(ctx, "binance", "future", []string{"BTC/USDT"})
	if !handler.TapeWritesCandles("binance", "future") {
		t.Fatal("the tape stopped writing without a single venue frame")
	}
}
