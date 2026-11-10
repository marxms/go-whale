package market

import (
	"fmt"
	"strings"
	"unicode"
)

// Symbol is a validated internal market symbol. The internal form is
// BASE/QUOTE (e.g. "BTC/USDT"); venue-native forms such as "BTC-USDT" or
// "BTC/USDT:USDT" are adapter vocabulary and must be normalized before they
// reach the core.
//
// The zero value is invalid, and ParseSymbol is the only way to construct a
// non-zero value.
type Symbol struct {
	text string
}

// ParseSymbol validates the canonical internal symbol form. It is deliberately
// strict: it does not trim, split on other delimiters, or repair the input, so a
// malformed symbol fails at the boundary instead of producing a key that
// silently names a different series.
func ParseSymbol(raw string) (Symbol, error) {
	if raw == "" {
		return Symbol{}, fmt.Errorf("market: symbol is empty")
	}
	if strings.IndexFunc(raw, unicode.IsSpace) >= 0 {
		return Symbol{}, fmt.Errorf("market: symbol %q contains whitespace", raw)
	}
	if strings.Contains(raw, ":") {
		return Symbol{}, fmt.Errorf("market: symbol %q contains a venue suffix", raw)
	}
	parts := strings.Split(raw, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Symbol{}, fmt.Errorf("market: symbol %q is not BASE/QUOTE", raw)
	}
	return Symbol{text: raw}, nil
}

func (s Symbol) String() string {
	return s.text
}

// Valid reports whether the symbol was produced by ParseSymbol.
func (s Symbol) Valid() bool {
	return s.text != ""
}

// quoteUSDT is the settlement quote named in the stable-suffix and swap-listing rules.
const quoteUSDT = "USDT"

// stableQuoteSuffixes are the quotes a concatenated or dash-joined venue
// symbol may carry. The order matters: "USDT" must be tried before the
// shorter spellings when a symbol ends in more than one of them.
func stableQuoteSuffixes() []string {
	return []string{
		quoteUSDT,
		"USDC",
		"BUSD",
		"FDUSD",
		"TUSD",
		"DAI",
	}
}

// NormalizeSymbol standardizes a venue symbol to the internal BASE/QUOTE
// form. It strips a settlement suffix (":USDT" for futures and BingX swaps) and
// converts a dash-joined stable pair (BTC-USDT) to slash form. Anything it
// does not recognize is returned unchanged rather than repaired.
func NormalizeSymbol(symbol string) string {
	if symbol == "" {
		return symbol
	}

	normalized := symbol
	if strings.Contains(normalized, ":") {
		parts := strings.Split(normalized, ":")
		if len(parts) > 0 {
			normalized = parts[0]
		}
	}

	if strings.Contains(normalized, "/") {
		return normalized
	}

	for _, quote := range stableQuoteSuffixes() {
		suffix := "-" + quote
		if !strings.HasSuffix(normalized, suffix) {
			continue
		}
		base := strings.TrimSuffix(normalized, suffix)
		if base == "" {
			return normalized
		}
		return base + "/" + quote
	}

	return normalized
}

// InternalSymbolFromConcatenated turns a venue's concatenated id (BTCUSDT)
// into the internal BASE/QUOTE form. It returns "" for anything it cannot
// address unambiguously — dated futures (ETHUSDT_240329), settled markets
// (CVCUSDTSETTLED) and non-stable quotes.
func InternalSymbolFromConcatenated(symbol string) string {
	if symbol == "" || strings.ContainsAny(symbol, "/-_:") {
		return ""
	}

	for _, quote := range stableQuoteSuffixes() {
		if !strings.HasSuffix(symbol, quote) {
			continue
		}
		base := strings.TrimSuffix(symbol, quote)
		if base == "" {
			continue
		}
		return base + "/" + quote
	}

	return ""
}
