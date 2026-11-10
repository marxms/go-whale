package app

import (
	"context"
	"sync"
	"testing"

	binancesdk "github.com/marxms/go-whale/internal/adapters/outbound/binance"
	"github.com/marxms/go-whale/internal/candle"
)

// THE FEED POINTER IS SHARED BY THREE GOROUTINES AND MUST BE GUARDED.
//
// Boot writes App.klineFeed, the watchdog replaces it when a partition goes silent, and periodic
// discovery reads it to attach newly found symbols. Unguarded, this is a genuine data race under
// `-race`.
//
// The race detector is the cheap part of the finding. The expensive part is the interleaving it
// stands for: discovery reads the pointer, the watchdog stops that feed and swaps in a fresh one,
// and discovery then attaches its symbols to the stopped feed. In a scope where ownership has
// silenced the tape, those symbols end up with NO candle producer at all — the exact state
// AddSymbols was written to prevent, reintroduced through the back door.
//
// This test drives all three accessors concurrently. It fails under `-race` if any of them touches
// klineFeed or klineSymbols outside klineMu.
// THE WRITER IS THE POINT. Three readers against a nil field would pass with the mutex removed —
// concurrent reads are not a race, so such a test proves nothing. There must be a writer.
//
// The writer here holds klineMu, mirroring boot and the watchdog. A locked write still races an
// unlocked read — there is no happens-before edge between them — so the detector fires the moment
// an accessor drops the lock.
func TestKlineFeedPointerIsSynchronised(t *testing.T) {
	a := &App{}
	// NewKlineFeed does not dial; Start does. Constructing one is free and touches no network.
	feed := binancesdk.NewKlineFeed(context.Background(), "binance", "future", binancesdk.KlineFeedInterval, func(string, candle.Candle) {})

	var wg sync.WaitGroup
	const rounds = 500

	// The writer: boot and the watchdog both install a feed pointer this way.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range rounds {
			a.klineMu.Lock()
			if i%2 == 0 {
				a.klineFeed = feed
			} else {
				a.klineFeed = nil
			}
			a.klineMu.Unlock()
		}
	}()

	// Discovery's accessor. Empty symbols so it returns before dialing — but Go evaluates the
	// condition left to right, so it still READS a.klineFeed first, which is the access under test.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range rounds {
			a.extendKlineFeed("binance", "future", nil)
		}
	}()

	// The watchdog's accessor, reading the same pointer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range rounds {
			a.restartKlineFeedIfSilent(t.Context(), "binance", "future")
		}
	}()

	wg.Wait()
}

// A RESTART MUST BRING THE DISCOVERED SYMBOLS WITH IT.
//
// The watchdog used to close over the boot symbol list and restart from it. Every symbol periodic
// discovery had attached since was silently dropped — and because ownership silences the tape for
// the whole scope, a dropped symbol has no candle producer at all, permanently, until the process
// restarts. The live fleet is tracked so the restart reinstates what is actually subscribed.
func TestKlineSymbolsTrackTheLiveFleet(t *testing.T) {
	a := &App{}

	a.klineMu.Lock()
	a.klineSymbols = []string{"BTC/USDT", "ETH/USDT"}
	a.klineMu.Unlock()

	// extendKlineFeed returns early on a nil feed, so append the way it does to pin the contract
	// the watchdog depends on: the restart list grows, it is not replaced.
	a.klineMu.Lock()
	a.klineSymbols = append(a.klineSymbols, "SOL/USDT")
	got := append([]string(nil), a.klineSymbols...)
	a.klineMu.Unlock()

	want := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}
	if len(got) != len(want) {
		t.Fatalf("live fleet = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("live fleet = %v, want %v", got, want)
		}
	}
}

// extendKlineFeed MUST NOT grow the restart list when the attach failed.
//
// With a nil feed there is nothing to attach to and the function returns before AddSymbols. If it
// appended anyway, a later watchdog restart would dial symbols that were never subscribed.
func TestExtendKlineFeedDoesNotRecordSymbolsItDidNotAttach(t *testing.T) {
	a := &App{}
	a.extendKlineFeed("binance", "future", []string{"BTC/USDT"})

	a.klineMu.Lock()
	defer a.klineMu.Unlock()
	if len(a.klineSymbols) != 0 {
		t.Fatalf("recorded %v as attached, but there was no feed to attach to", a.klineSymbols)
	}
}

// THE FEED OWNS ONE SCOPE, AND ONLY startKlineFeed USED TO KNOW IT.
//
// startKlineFeed refuses anything that is not binance:future. extendKlineFeed took the same two
// arguments and used them only in log messages, and App.klineFeed is a single field, not a map
// keyed by scope. Turning on discovery for another exchange — one line of YAML, no code — would
// have attached that venue's symbols to this Binance-futures feed and recorded them under
// exchange="binance". A rule stated at one site and absent from its sibling.
// The observable is dropFromKlineFeed's restart list rather than extendKlineFeed's, because it is
// deterministic without a socket: extendKlineFeed reaches AddSymbols, which dials, and a dial that
// fails leaves the restart list untouched too — so asserting on it would pass with the guard
// deleted and prove nothing. Both functions apply the identical scope predicate.
func TestKlineFeedRefusesForeignScope(t *testing.T) {
	newApp := func() *App {
		a := &App{}
		a.klineFeed = binancesdk.NewKlineFeed(context.Background(), "binance", "future", binancesdk.KlineFeedInterval, func(string, candle.Candle) {})
		a.klineScope = "binance:future"
		a.klineSymbols = []string{"BTC/USDT"}
		return a
	}

	foreign := newApp()
	foreign.dropFromKlineFeed("bingx", "future", []string{"BTC/USDT"})
	foreign.klineMu.Lock()
	remaining := len(foreign.klineSymbols)
	foreign.klineMu.Unlock()
	if remaining != 1 {
		t.Fatalf("a foreign scope reached this feed's state: klineSymbols=%d, want 1 untouched", remaining)
	}

	own := newApp()
	own.dropFromKlineFeed("binance", "future", []string{"BTC/USDT"})
	own.klineMu.Lock()
	remaining = len(own.klineSymbols)
	own.klineMu.Unlock()
	if remaining != 0 {
		t.Fatalf("the owning scope was refused: klineSymbols=%d, want 0", remaining)
	}
}

// A DERANK MUST STICK AGAINST THE VENUE FEED.
//
// Discovery deranks a symbol by unsubscribing and purging its engine buffer and Redis keys. The
// feed heard none of it and keeps pushing the candle, and UpdateCandle recreates the buffer on
// arrival. Muting stops the live frames; dropping it from the restart list stops the watchdog
// re-dialing it. Tracking the live fleet WITHOUT this would have made the restart path carry every
// deranked symbol forward — strictly worse than the boot list it replaced.
func TestDropFromKlineFeedRemovesSymbolFromRestartList(t *testing.T) {
	a := &App{}
	a.klineFeed = binancesdk.NewKlineFeed(context.Background(), "binance", "future", binancesdk.KlineFeedInterval, func(string, candle.Candle) {})
	a.klineScope = "binance:future"
	a.klineSymbols = []string{"BTC/USDT", "DEAD/USDT", "ETH/USDT"}

	a.dropFromKlineFeed("binance", "future", []string{"DEAD/USDT"})

	a.klineMu.Lock()
	defer a.klineMu.Unlock()
	for _, s := range a.klineSymbols {
		if s == "DEAD/USDT" {
			t.Fatalf("deranked symbol still in the restart list: %v", a.klineSymbols)
		}
	}
	if len(a.klineSymbols) != 2 {
		t.Fatalf("restart list = %v, want the two survivors", a.klineSymbols)
	}
}
