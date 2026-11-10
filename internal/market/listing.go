package market

import (
	"strings"
)

// ValidSymbolsFiltered builds the canonical symbol set from venue metadata,
// optionally filtered by the venue's market type ("spot", "swap", "future").
//
// The filter compares the venue vocabulary on purpose: the caller converts the
// internal market type with ExchangeAdapter.ToExchange, because the metadata
// carries the venue's own word.
func ValidSymbolsFiltered(markets map[string]MarketInfo, marketTypeFilter string) map[string]struct{} {
	valid := make(map[string]struct{})
	for _, info := range markets {
		if marketTypeFilter != "" && info.Type != nil && *info.Type != marketTypeFilter {
			continue
		}

		base := strings.ToUpper(deref(info.BaseCurrency))
		if base == "" {
			base = strings.ToUpper(deref(info.BaseID))
		}

		quote := strings.ToUpper(deref(info.QuoteCurrency))
		if quote == "" {
			quote = strings.ToUpper(deref(info.QuoteID))
		}

		settle := strings.ToUpper(deref(info.Settle))
		if settle == "" {
			settle = strings.ToUpper(deref(info.SettleID))
		}

		isContract := false
		if info.Contract != nil {
			isContract = *info.Contract
		}

		if isContract && settle != "" {
			valid[base+"/"+settle] = struct{}{}
			continue
		}

		if base != "" && quote != "" {
			valid[base+"/"+quote] = struct{}{}
		}
	}

	return valid
}

// SupportsListingFloors reports whether this venue/market publishes a listing time
// usable as an engine floor. The answer is the capability matrix row, not a second list.
func SupportsListingFloors(exchangeID, marketType string) bool {
	return CapabilitiesFor(exchangeID, marketType).Supports(CapabilityListingFloors)
}

// VerifiedListingFloors accepts only an unambiguous USDⓈ-M perpetual market
// already returned by LoadMarkets. It never makes a network request.
func VerifiedListingFloors(exchangeID, marketType string, symbols []string, markets map[string]MarketInfo, now int64) map[string]int64 {
	out := make(map[string]int64)
	if !CapabilitiesFor(exchangeID, marketType).Supports(CapabilityListingFloors) {
		return out
	}
	wanted := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		wanted[symbol] = struct{}{}
	}
	ambiguous := make(map[string]bool)
	for marketKey, m := range markets {
		name := marketKey
		if m.Symbol != nil {
			name = *m.Symbol
		}
		symbol := NormalizeSymbol(name)
		if _, ok := wanted[symbol]; !ok || !strings.HasSuffix(symbol, "/USDT") {
			continue
		}
		if m.ID != nil && *m.ID != strings.ReplaceAll(symbol, "/", "") {
			continue // a distinct contract, such as a dated future
		}
		// LoadMarkets may contain spot and contracts with another settlement in
		// the same map. They are not candidates for this perpetual identity.
		if (m.Spot != nil && *m.Spot) || (m.Swap != nil && !*m.Swap) || (m.Linear != nil && !*m.Linear) || (m.Settle != nil && *m.Settle != quoteUSDT) || (m.Symbol != nil && !strings.HasSuffix(*m.Symbol, ":USDT")) {
			continue
		}
		if m.Symbol == nil || m.ID == nil || m.Created == nil || !strings.HasSuffix(*m.Symbol, ":USDT") || m.Swap == nil || !*m.Swap || m.Linear == nil || !*m.Linear || m.QuoteCurrency == nil || *m.QuoteCurrency != quoteUSDT || m.Settle == nil || *m.Settle != quoteUSDT {
			ambiguous[symbol] = true
			continue
		}
		if *m.Created <= 0 || *m.Created > now {
			ambiguous[symbol] = true
			continue
		}
		if old, ok := out[symbol]; ok && old != *m.Created {
			ambiguous[symbol] = true
		} else {
			out[symbol] = *m.Created
		}
	}
	for symbol := range ambiguous {
		delete(out, symbol)
	}
	return out
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
