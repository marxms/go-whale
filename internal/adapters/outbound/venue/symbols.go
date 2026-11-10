package venue

import (
	"strings"

	"github.com/marxms/go-whale/internal/market"
)

// binanceSymbol translates an internal BASE/QUOTE symbol to Binance's
// concatenated id. The venue suffix is adapter vocabulary and never reaches the
// core.
func binanceSymbol(symbol string) string {
	normalized := market.NormalizeSymbol(symbol)
	if i := strings.IndexByte(normalized, ':'); i >= 0 {
		normalized = normalized[:i]
	}
	return strings.ReplaceAll(normalized, "/", "")
}

// bingXSymbol translates an internal symbol to BingX's dash form.
func bingXSymbol(symbol string) string {
	normalized := market.NormalizeSymbol(symbol)
	return strings.ReplaceAll(normalized, "/", "-")
}

func quoteOf(symbol string) string {
	parts := strings.SplitN(market.NormalizeSymbol(symbol), "/", 2)
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}
