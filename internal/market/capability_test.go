package market

import (
	"reflect"
	"testing"
)

func allCapabilities() []Capability {
	return []Capability{
		CapabilityKlineFeed,
		CapabilityCVD,
		CapabilityFunding,
		CapabilityOpenInterest,
		CapabilityOpenInterestHistory,
		CapabilityLongShortRatio,
		CapabilityMarkets,
		CapabilityListingFloors,
	}
}

// TestCapabilitiesTableFourScopes pins CAP-01/CAP-04: every supported scope declares all
// eight capabilities, every absence carries a reason, and the reference scope provides all
// of them.
func TestCapabilitiesTableFourScopes(t *testing.T) {
	scopes := []struct {
		name       string
		exchange   string
		marketType MarketType
	}{
		{name: "binance spot", exchange: "binance", marketType: Spot},
		{name: "binance future", exchange: "binance", marketType: Future},
		{name: "bingx spot", exchange: "bingx", marketType: Spot},
		{name: "bingx swap", exchange: "bingx", marketType: Future},
	}

	for _, scope := range scopes {
		t.Run(scope.name, func(t *testing.T) {
			caps := CapabilitiesFor(scope.exchange, string(scope.marketType))
			if len(caps) != len(allCapabilities()) {
				t.Fatalf("%s declares %d capabilities, want %d", scope.name, len(caps), len(allCapabilities()))
			}
			for _, capability := range allCapabilities() {
				support, ok := caps[capability]
				if !ok {
					t.Errorf("%s does not declare %s", scope.name, capability)
					continue
				}
				if !support.Present && support.Reason == "" {
					t.Errorf("%s declares %s absent without a reason", scope.name, capability)
				}
				if support.Present && support.Reason != "" {
					t.Errorf("%s declares %s present with absence reason %q", scope.name, capability, support.Reason)
				}
			}
		})
	}

	reference := CapabilitiesFor("binance", "future")
	for _, capability := range allCapabilities() {
		if !reference.Supports(capability) {
			t.Errorf("reference scope does not provide %s", capability)
		}
	}

	binanceSpot := CapabilitiesFor("binance", "spot")
	spotAbsences := []Capability{
		CapabilityFunding,
		CapabilityKlineFeed,
		CapabilityListingFloors,
		CapabilityLongShortRatio,
		CapabilityOpenInterest,
		CapabilityOpenInterestHistory,
	}
	if got, want := binanceSpot.DivergesFrom(reference), spotAbsences; !reflect.DeepEqual(got, want) {
		t.Errorf("binance spot diverges from the reference on %v, want %v", got, want)
	}

	bingXSwapAbsences := []Capability{
		CapabilityCVD,
		CapabilityKlineFeed,
		CapabilityListingFloors,
		CapabilityLongShortRatio,
		CapabilityOpenInterestHistory,
	}
	if got, want := CapabilitiesFor("bingx", "swap").DivergesFrom(reference), bingXSwapAbsences; !reflect.DeepEqual(got, want) {
		t.Errorf("bingx swap diverges from the reference on %v, want %v", got, want)
	}

	if caps := CapabilitiesFor("unknown", "future"); len(caps) != 0 {
		t.Errorf("unknown exchange declared capabilities: %v", caps)
	}
}

// TestListingFloorsFromMatrix pins CAP-09: the floor decision is the matrix row, not a
// second list, and it reaches VerifiedListingFloors.
func TestListingFloorsFromMatrix(t *testing.T) {
	scopes := [][2]string{
		{"binance", "spot"},
		{"binance", "future"},
		{"bingx", "spot"},
		{"bingx", "swap"},
	}
	for _, scope := range scopes {
		want := CapabilitiesFor(scope[0], scope[1]).Supports(CapabilityListingFloors)
		if got := SupportsListingFloors(scope[0], scope[1]); got != want {
			t.Errorf("SupportsListingFloors(%s, %s) = %v, matrix says %v", scope[0], scope[1], got, want)
		}
	}

	if !SupportsListingFloors("binance", "future") {
		t.Fatal("binance futures must publish listing floors")
	}
	for _, scope := range [][2]string{{"binance", "spot"}, {"bingx", "spot"}, {"bingx", "swap"}} {
		if SupportsListingFloors(scope[0], scope[1]) {
			t.Errorf("%s %s must not publish listing floors", scope[0], scope[1])
		}
	}

	symbol, id, usdt, created := "BTC/USDT:USDT", "BTCUSDT", "USDT", int64(1000)
	trueValue := true
	good := MarketInfo{
		Symbol: &symbol, ID: &id, Created: &created,
		Swap: &trueValue, Linear: &trueValue, QuoteCurrency: &usdt, Settle: &usdt,
	}
	wanted := []string{"BTC/USDT"}
	markets := map[string]MarketInfo{symbol: good}

	if floors := VerifiedListingFloors("binance", "future", wanted, markets, 2000); floors["BTC/USDT"] != created {
		t.Errorf("binance future floor = %v, want %d", floors["BTC/USDT"], created)
	}
	if floors := VerifiedListingFloors("bingx", "swap", wanted, markets, 2000); len(floors) != 0 {
		t.Errorf("bingx swap returned floors from a matrix row that declares none: %v", floors)
	}
}
