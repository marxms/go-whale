package market

import "testing"

func TestVerifiedListingFloorsRequiresExactLinearPerpetual(t *testing.T) {
	symbol, id, usdt := "BTC/USDT:USDT", "BTCUSDT", "USDT"
	trueValue := true
	created := int64(1000)
	good := MarketInfo{Symbol: &symbol, ID: &id, Created: &created, Swap: &trueValue, Linear: &trueValue, QuoteCurrency: &usdt, Settle: &usdt}
	wanted := []string{"BTC/USDT"}
	floors := VerifiedListingFloors("binance", "future", wanted, map[string]MarketInfo{symbol: good}, 2000)
	if floors["BTC/USDT"] != created {
		t.Fatalf("exact market was not bound: %v", floors)
	}
	spotSymbol, otherSymbol, otherSettle := "BTC/USDT", "BTC/USDT:USDC", "USDC"
	spot := MarketInfo{Symbol: &spotSymbol, ID: &id, Spot: &trueValue}
	other := MarketInfo{Symbol: &otherSymbol, ID: &id, Settle: &otherSettle}
	if got := VerifiedListingFloors("binance", "future", wanted, map[string]MarketInfo{"swap": good, "spot": spot, "other": other}, 2000); got["BTC/USDT"] != created {
		t.Fatalf("unrelated markets invalidated exact perpetual: %v", got)
	}
	for name, bad := range map[string]MarketInfo{
		"missing": func() MarketInfo { m := good; m.Created = nil; return m }(),
		"zero":    func() MarketInfo { m := good; n := int64(0); m.Created = &n; return m }(),
		"future":  func() MarketInfo { m := good; n := int64(2001); m.Created = &n; return m }(),
		"inverse": func() MarketInfo { m := good; v := false; m.Linear = &v; return m }(),
	} {
		if got := VerifiedListingFloors("binance", "future", wanted, map[string]MarketInfo{symbol: bad}, 2000); len(got) != 0 {
			t.Errorf("%s: unexpected floor %v", name, got)
		}
	}
	conflict := good
	otherCreated := int64(999)
	conflict.Created = &otherCreated
	if got := VerifiedListingFloors("binance", "future", wanted, map[string]MarketInfo{"one": good, "two": conflict}, 2000); len(got) != 0 {
		t.Fatalf("conflicting market identity accepted: %v", got)
	}
	if got := VerifiedListingFloors("binance", "spot", wanted, map[string]MarketInfo{symbol: good}, 2000); len(got) != 0 {
		t.Fatalf("cross-market floor accepted: %v", got)
	}
}

func TestValidSymbolsFiltered(t *testing.T) {
	spot := "spot"
	usdt := "USDT"
	base := "BTC"
	contract := true
	settle := "USDT"
	markets := map[string]MarketInfo{
		"spot":     {Type: &spot, BaseCurrency: &base, QuoteCurrency: &usdt},
		"contract": {Contract: &contract, BaseID: &base, SettleID: &settle},
	}
	got := ValidSymbolsFiltered(markets, "spot")
	if _, ok := got["BTC/USDT"]; !ok {
		t.Fatalf("spot market missing from filtered set: %v", got)
	}
	if len(got) != 1 {
		t.Fatalf("filter should exclude the contract, got %v", got)
	}
}
