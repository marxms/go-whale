package collector

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

func scope(id string, mt market.MarketType, symbols ...string) CoinalyzeScope {
	return CoinalyzeScope{ExchangeID: id, MarketType: mt, Symbols: symbols}
}

func TestCoinalyzeSymbolsAreTheConfiguredFutureSymbols(t *testing.T) {
	symbols := CoinalyzeSymbolsOf([]CoinalyzeScope{
		scope("binance", market.Spot, "BTC/USDT"),
		scope("binance", market.Future, "BTC/USDT", "ETH/USDT"),
	})

	if len(symbols) != 2 {
		t.Fatalf("expected 2 futures symbols, got %v", symbols)
	}
	for _, s := range symbols {
		if s != "BTC/USDT" && s != "ETH/USDT" {
			t.Fatalf("unexpected symbol: %s", s)
		}
	}
}

func TestCoinalyzeSymbolsAreDeduplicated(t *testing.T) {
	symbols := CoinalyzeSymbolsOf([]CoinalyzeScope{
		scope("binance", market.Future, "BTC/USDT", "ETH/USDT"),
		scope("bingx", market.Future, "BTC/USDT"),
	})
	if len(symbols) != 2 {
		t.Fatalf("the repeated symbol was not deduplicated: %v", symbols)
	}
}

func TestCoinalyzeSymbolsAreEmptyWithoutFutures(t *testing.T) {
	if got := CoinalyzeSymbolsOf([]CoinalyzeScope{scope("binance", market.Spot, "BTC/USDT")}); len(got) != 0 {
		t.Fatalf("with no futures configured the list is empty, got %v", got)
	}
}

func TestCoinalyzeDoesNotStartWhenDisabled(t *testing.T) {
	got := DecideCoinalyze(false, true, []CoinalyzeScope{scope("binance", market.Future, "BTC/USDT")})
	if got.Start {
		t.Fatal("disabled by configuration cannot start")
	}
	if got.Reason == "" {
		t.Fatal("the refusal must say why")
	}
}

func TestCoinalyzeDegradesWithoutTheKeyInsteadOfFailing(t *testing.T) {
	got := DecideCoinalyze(true, false, []CoinalyzeScope{scope("binance", market.Future, "BTC/USDT")})
	if got.Start {
		t.Fatal("without a key the collection cannot start")
	}
	if !strings.Contains(got.Reason, "GOWHALE_COINALYZE_API_KEY") {
		t.Fatalf("the refusal must name the missing variable: %q", got.Reason)
	}
}

func TestCoinalyzeDoesNotStartWithoutFutureSymbols(t *testing.T) {
	got := DecideCoinalyze(true, true, []CoinalyzeScope{scope("binance", market.Spot, "BTC/USDT")})
	if got.Start {
		t.Fatal("without a futures symbol there is nothing to collect")
	}
}

func TestCoinalyzeStartsWhenEverythingIsInPlace(t *testing.T) {
	got := DecideCoinalyze(true, true,
		[]CoinalyzeScope{scope("binance", market.Future, "BTC/USDT", "ETH/USDT")})
	if !got.Start {
		t.Fatalf("it should have started, it refused with %q", got.Reason)
	}
	if len(got.Symbols) != 2 {
		t.Fatalf("expected the 2 configured symbols, got %v", got.Symbols)
	}
}

func TestCoinalyzeDeclaredCadence_ComesFromTheLoad(t *testing.T) {
	const symbols, timeframes = 503, 7
	want := 4*time.Hour + 24*time.Minute + 4*time.Second + 500*time.Millisecond

	got := CoinalyzeDeclaredCadence(time.Hour, symbols, timeframes)
	if delta := got - want; delta < -time.Microsecond || delta > time.Microsecond {
		t.Fatalf("declared cadence = %s, wanted %s", got, want)
	}
	if got <= time.Hour {
		t.Fatalf("the declaration became the constant again (%s)", got)
	}
}

func TestCoinalyzeDeclaredCadence_ALoadThatFitsKeepsTheConfiguredInterval(t *testing.T) {
	if got := CoinalyzeDeclaredCadence(time.Hour, 20, 3); got != time.Hour {
		t.Fatalf("a load that fits declared a cadence of %s", got)
	}
	if got := CoinalyzeDeclaredCadence(time.Hour, 0, 7); got != time.Hour {
		t.Fatalf("an empty load declared a cadence of %s", got)
	}
	if got := CoinalyzeDeclaredCadence(0, 503, 7); got != 0 {
		t.Fatalf("without a configured interval the declaration came out %s", got)
	}
}

func TestCoinalyzeCadenceReport_DeclaresBothNumbersAtWarn(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	declared := CoinalyzeDeclaredCadence(time.Hour, 503, 7)
	ReportCoinalyzeCadence(time.Hour, declared, 503, 7, log)

	line := lineContaining(t, sink, "declared cadence")
	if !strings.Contains(line, "4h24m") {
		t.Errorf("the line does not declare the derived pass duration (4h24m): %q", line)
	}
	if !strings.Contains(line, time.Hour.String()) {
		t.Errorf("the line does not declare the configured interval (%s): %q", time.Hour, line)
	}
	if level, _ := sink.LevelOf("declared cadence"); level != logger.LevelWarn {
		t.Errorf("the impossible cadence came out at %s", level)
	}
}

func TestCoinalyzeCadenceReport_ALoadThatFitsIsNotAnAlarm(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	ReportCoinalyzeCadence(time.Hour, time.Hour, 20, 3, log)

	level, found := sink.LevelOf("declared cadence")
	if !found {
		t.Fatalf("the cadence was not declared; captured: %v", sink.Records())
	}
	if level != logger.LevelInfo {
		t.Errorf("a load that fits came out at %s", level)
	}
	if _, alarmed := sink.LevelOf("does not fit"); alarmed {
		t.Errorf("a load that fits was reported as impossible: %v", sink.Records())
	}
}

func fleetScope(id string, mt market.MarketType, discovery bool, maxAssets int) FleetScope {
	return FleetScope{ExchangeID: id, MarketType: mt, DiscoveryEnabled: discovery, MaxAssets: maxAssets}
}

func TestFleetReady_WaitsForTheDiscoveredExchangeSpecifically(t *testing.T) {
	exchanges := []FleetScope{
		fleetScope("binance", market.Future, true, 30),
		fleetScope("bingx", market.Future, false, 0),
		fleetScope("binance", market.Spot, true, 0),
	}
	withoutTarget := append(append([]FleetScope{}, exchanges...),
		fleetScope("semAlvo", market.Future, true, 0))

	for _, tc := range []struct {
		name   string
		counts map[string]int
		want   bool
	}{
		{"the measured instant: only the static one published", map[string]int{"bingx": 1}, false},
		{"the static one repeated", map[string]int{"bingx": 1}, false},
		{"discovery published the whole target", map[string]int{"bingx": 1, "binance": 30}, true},
		{"only discovery, without the static one", map[string]int{"binance": 30}, true},
		{"nothing published", map[string]int{}, false},
		{"discovery published TWO of thirty", map[string]int{"bingx": 1, "binance": 2}, false},
		{"discovery passed the target", map[string]int{"bingx": 1, "binance": 31}, true},
		{"undeclared target, nothing published", map[string]int{"semAlvo": 0}, false},
		{"undeclared target, published one", map[string]int{"semAlvo": 1}, true},
	} {
		list := exchanges
		if _, has := tc.counts["semAlvo"]; has {
			list = withoutTarget
			tc.counts["binance"], tc.counts["bingx"] = 30, 1
		}
		if got := FleetReady(list, tc.counts); got != tc.want {
			t.Errorf("%s: FleetReady=%v, expected %v", tc.name, got, tc.want)
		}
	}
}

func TestFleetReady_WithoutDiscoveryThereIsNothingToWaitFor(t *testing.T) {
	exchanges := []FleetScope{fleetScope("bingx", market.Future, false, 0)}
	if !FleetReady(exchanges, map[string]int{"bingx": 1}) {
		t.Error("static fleet published and the wait continued")
	}
	spot := []FleetScope{fleetScope("binance", market.Spot, true, 0)}
	if !FleetReady(spot, map[string]int{}) {
		t.Error("a discovered SPOT exchange held up a futures collector's wait")
	}
	if !FleetReady(nil, map[string]int{}) {
		t.Error("with no exchange at all the wait continued")
	}
}

func TestSortedFleet(t *testing.T) {
	got := SortedFleet([]string{"ETH/USDT", "BTC/USDT", "ETH/USDT"})
	if fmt.Sprint(got) != "[BTC/USDT ETH/USDT]" {
		t.Fatalf("SortedFleet = %v", got)
	}
}

func TestCoinalyzeCallsPerBatchIsStillTheDeclaredUnit(t *testing.T) {
	if CoinalyzeCallsPerBatch <= 0 {
		t.Fatalf("CoinalyzeCallsPerBatch = %d", CoinalyzeCallsPerBatch)
	}
	if coinalyze.CoinalyzeCallsPerMinute != 40 {
		t.Fatalf("the measured budget is 40/min, it is %d", coinalyze.CoinalyzeCallsPerMinute)
	}
}
