package app

import (
	"context"
	"testing"
	"time"

	binancesdk "github.com/marxms/go-whale/internal/adapters/outbound/binance"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
)

// A FAILED REDIAL MUST NOT DISARM THE WATCHDOG.
//
// restartKlineFeedIfSilent stops the feed, redials, and only installs the new one on success. On
// failure a.klineFeed still points at the STOPPED feed — and Stop() clears the partition set, so
// SecondsSinceLastFrame reports (0, false). Reading that as "nothing to report" made every later
// tick return early; startKlineFeed will not re-run either, because it checks a non-nil pointer.
// Every symbol in the scope then has zero candle producers, permanently, with the tape silenced
// behind them.
//
// The liveness state must survive Stop() or live outside it: moving liveness onto the partitions
// put that state into the thing Stop() destroys. A fleet-global clock that survives Stop() keeps
// reporting growing silence, and the next tick retries.
//
// The watchdog may only report "done" when there is no feed at all — i.e. shutdown.
func TestWatchdogKeepsWatchingAfterAFailedRedial(t *testing.T) {
	a := &App{candleEngine: engine.NewCandleEngine(map[string]int{"default": 100})}
	feed := binancesdk.NewKlineFeed(context.Background(), "binance", "future", binancesdk.KlineFeedInterval, func(string, candle.Candle) {})
	feed.Stop() // exactly the state a failed redial leaves behind: no partitions

	a.klineFeed = feed
	a.klineScope = "binance:future"

	// THE DISCRIMINATOR IS WHETHER A RESTART WAS ATTEMPTED, NOT THE RETURN VALUE.
	//
	// Both the correct and the regressed version return "not done" here, so asserting on that
	// proves nothing. What differs is observable on the feed pointer: a restart replaces it, an
	// early return leaves the stopped corpse in place. With no symbols the redial is a local
	// no-op, so the swap happens without touching the network.
	if done := a.restartKlineFeedIfSilent(t.Context(), "binance", "future"); done {
		t.Fatal("the watchdog declared itself done while a feed was still installed")
	}

	a.klineMu.Lock()
	defer a.klineMu.Unlock()
	if a.klineFeed == feed {
		t.Fatal("no restart was attempted on a feed with no partitions: the stopped feed is still " +
			"installed, every later tick will read (0,false) and return early, and all 500 symbols " +
			"stay frozen on all eight timeframes for the life of the process")
	}
}

// It DOES stop when there is genuinely no feed, which is shutdown.
func TestWatchdogStopsWhenTheFeedIsGone(t *testing.T) {
	a := &App{}
	if done := a.restartKlineFeedIfSilent(t.Context(), "binance", "future"); !done {
		t.Fatal("the watchdog kept running with no feed at all")
	}
}

type configFaultFeed struct {
	controlledKlineFeed
	starts int
}

func (f *configFaultFeed) ConfigFault(time.Time) bool { return true }
func (f *configFaultFeed) Start([]string) error       { f.starts++; return nil }

func TestWatchdogDoesNotRedialAConfigFault(t *testing.T) {
	feed := &configFaultFeed{controlledKlineFeed: controlledKlineFeed{have: true, silent: 600}}
	a := &App{klineFeed: feed, klineScope: "binance:future", klineSymbols: []string{"BTC/USDT"}}

	if done := a.restartKlineFeedIfSilent(t.Context(), "binance", "future"); done {
		t.Fatal("the watchdog declared itself done with a feed installed")
	}
	if feed.starts != 0 || feed.stopped {
		t.Fatalf("a config fault was redialled: starts=%d stopped=%v", feed.starts, feed.stopped)
	}
}
