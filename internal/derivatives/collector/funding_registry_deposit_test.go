package collector

import "testing"

// TestDepositWithoutRegistryIdentifiesItself pins the semantics production violated: a deposit
// that does not land MUST say why. `c == nil` and `Hours <= 0` sharing a silent return made the two
// indistinguishable from outside.
func TestDepositWithoutRegistryIdentifiesItself(t *testing.T) {
	var ausente *FundingProfileRegistry // nil receiver: the suspicious path in production
	scope := SymbolScope{ExchangeID: "sensor-nil", MarketType: "future", Symbol: "BTC/USDT"}
	rec := newCollectorTestMetrics()

	ausente.setMeasured(rec.port(), scope, measuredCadence{Hours: 8, ObservedFromMs: 1})

	if got := rec.measurements[ckey(scope.ExchangeID, "deposit_nil_registry")]; got != 1 {
		t.Fatalf("a nil receiver must identify itself: counter = %v", got)
	}
}

// TestDepositWithZeroCadenceIdentifiesItself separates the OTHER silent exit.
func TestDepositWithZeroCadenceIdentifiesItself(t *testing.T) {
	reg := NewFundingProfileRegistry()
	scope := SymbolScope{ExchangeID: "sensor-zero", MarketType: "future", Symbol: "ETH/USDT"}
	rec := newCollectorTestMetrics()

	reg.setMeasured(rec.port(), scope, measuredCadence{Hours: 0, ObservedFromMs: 1})

	if got := rec.measurements[ckey(scope.ExchangeID, "deposit_zero_hours")]; got != 1 {
		t.Fatalf("zero cadence must identify itself: counter = %v", got)
	}
	if _, ok := reg.measuredFor(scope); ok {
		t.Fatal("a non-positive cadence cannot become a registry entry")
	}
}

// TestValidDepositMovesTheGauge proves the POSITIVE side: when the deposit is good, the size gauge
// rises.
func TestValidDepositMovesTheGauge(t *testing.T) {
	reg := NewFundingProfileRegistry()
	scope := SymbolScope{ExchangeID: "sensor-ok", MarketType: "future", Symbol: "SOL/USDT"}
	rec := newCollectorTestMetrics()

	reg.setMeasured(rec.port(), scope, measuredCadence{Hours: 8, ObservedFromMs: 1})

	if rec.registrySize < 1 {
		t.Fatalf("a valid deposit must move the size gauge, it stayed at %v", rec.registrySize)
	}
	cadence, ok := reg.measuredFor(scope)
	if !ok || cadence.Hours != 8 {
		t.Fatalf("a valid deposit must be readable back: %+v ok=%v", cadence, ok)
	}
}
