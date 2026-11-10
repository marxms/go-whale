package venue

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/marxms/go-whale/internal/market"
)

// LoadMarkets returns the venue catalog in canonical form, keyed by the
// unified symbol the rest of the process uses. The façade delegates to the
// resolved driver, so a new venue does not need a branch here.
func (c *Client) LoadMarkets(ctx context.Context) (map[string]market.MarketInfo, error) {
	if c.driver == nil {
		return nil, fmt.Errorf("venue: no driver resolved for %s %s", c.venue, c.marketType)
	}
	return c.driver.LoadMarkets(ctx)
}

func (c *Client) loadBinanceMarkets(ctx context.Context) (map[string]market.MarketInfo, error) {
	path := "/api/v3/exchangeInfo"
	weight := 20
	if c.marketType != market.Spot {
		path = "/fapi/v1/exchangeInfo"
		weight = 1
	}

	payload, err := c.getJSON(ctx, path, nil, func(ctx context.Context) error {
		return c.acquireBinanceWeight(ctx, weight)
	})
	if err != nil {
		return nil, err
	}
	root, ok := asMap(payload)
	if !ok {
		return nil, fmt.Errorf("venue: %s returned %T, want an object", path, payload)
	}
	rows, ok := asSlice(root["symbols"])
	if !ok {
		return nil, fmt.Errorf("venue: %s has no symbols array", path)
	}

	markets := make(map[string]market.MarketInfo, len(rows))
	for _, raw := range rows {
		row, ok := asMap(raw)
		if !ok {
			continue
		}
		var key string
		var info market.MarketInfo
		if c.marketType == market.Spot {
			key, info, ok = binanceSpotMarket(row)
		} else {
			key, info, ok = binanceFuturesMarket(row)
		}
		if ok {
			markets[key] = info
		}
	}
	return markets, nil
}

func binanceFuturesMarket(row map[string]any) (string, market.MarketInfo, bool) {
	id, _ := asString(row["symbol"])
	base, _ := asString(row["baseAsset"])
	quote, _ := asString(row["quoteAsset"])
	margin, _ := asString(row["marginAsset"])
	contractType, _ := asString(row["contractType"])
	status, hasStatus := asString(row["status"])
	if id == "" || base == "" || quote == "" {
		return "", market.MarketInfo{}, false
	}
	if margin == "" {
		margin = quote
	}

	symbol := base + "/" + quote + ":" + margin
	// TRADIFI_PERPETUAL is a perpetual too; treating only the literal
	// "PERPETUAL" as swap would drop every tokenized equity from discovery and
	// from ValidSymbolsFiltered.
	isPerpetual := strings.Contains(contractType, "PERPETUAL")
	marketType := "swap"
	if !isPerpetual {
		marketType = "future"
	}

	info := market.MarketInfo{
		Symbol:         &symbol,
		ID:             &id,
		BaseCurrency:   &base,
		BaseID:         &base,
		QuoteCurrency:  &quote,
		QuoteID:        &quote,
		Settle:         &margin,
		SettleID:       &margin,
		Type:           &marketType,
		UnderlyingType: ptrString(row["underlyingType"]),
		Active:         ptrBool(hasStatus && status == "TRADING"),
		Spot:           ptrBool(false),
		Swap:           ptrBool(isPerpetual),
		Linear:         ptrBool(margin == quote),
		Contract:       ptrBool(true),
		Created:        ptrInt64(row["onboardDate"]),
	}
	// THE MAP KEY IS THE VENUE ID, NOT THE UNIFIED SYMBOL.
	//
	// A perpetual and its quarterly contract share base, quote and margin (BTCUSDT and
	// BTCUSDT_250926 both map to BTC/USDT:USDT). Keying by the unified symbol let the
	// quarterly row overwrite the perpetual, and ValidSymbolsFiltered("swap") then dropped
	// BTC/USDT and ETH/USDT from the seed and discovery universes. The ID is unique per
	// market; consumers that need the unified symbol read MarketInfo.Symbol.
	return id, info, true
}

func binanceSpotMarket(row map[string]any) (string, market.MarketInfo, bool) {
	id, _ := asString(row["symbol"])
	base, _ := asString(row["baseAsset"])
	quote, _ := asString(row["quoteAsset"])
	status, hasStatus := asString(row["status"])
	spotAllowed, hasSpotAllowed := asBool(row["isSpotTradingAllowed"])
	if id == "" || base == "" || quote == "" {
		return "", market.MarketInfo{}, false
	}

	symbol := base + "/" + quote
	marketType := "spot"
	active := hasStatus && status == "TRADING"
	if hasSpotAllowed {
		active = active && spotAllowed
	}

	info := market.MarketInfo{
		Symbol:        &symbol,
		ID:            &id,
		BaseCurrency:  &base,
		BaseID:        &base,
		QuoteCurrency: &quote,
		QuoteID:       &quote,
		Type:          &marketType,
		Active:        ptrBool(active),
		Spot:          ptrBool(true),
		Swap:          ptrBool(false),
		Linear:        ptrBool(false),
		Contract:      ptrBool(false),
	}
	return symbol, info, true
}

func (c *Client) loadBingXMarkets(ctx context.Context) (map[string]market.MarketInfo, error) {
	if c.marketType == market.Spot {
		return c.loadBingXSpotMarkets(ctx)
	}
	return c.loadBingXSwapMarkets(ctx)
}

func (c *Client) loadBingXSwapMarkets(ctx context.Context) (map[string]market.MarketInfo, error) {
	payload, err := c.getJSON(ctx, "/openApi/swap/v2/quote/contracts", nil, func(ctx context.Context) error {
		return c.waitBingX(ctx)
	})
	if err != nil {
		return nil, err
	}
	rows, ok := asSlice(payload)
	if !ok {
		return nil, fmt.Errorf("venue: bingx contracts returned %T, want an array", payload)
	}

	markets := make(map[string]market.MarketInfo, len(rows))
	for _, raw := range rows {
		row, ok := asMap(raw)
		if !ok {
			continue
		}
		id, _ := asString(row["symbol"])
		base, _ := asString(row["asset"])
		quote, _ := asString(row["currency"])
		status, hasStatus := asString(row["status"])
		if id == "" || base == "" || quote == "" {
			continue
		}

		symbol := base + "/" + quote + ":" + quote
		marketType := "swap"
		info := market.MarketInfo{
			Symbol:        &symbol,
			ID:            &id,
			BaseCurrency:  &base,
			BaseID:        &base,
			QuoteCurrency: &quote,
			QuoteID:       &quote,
			Settle:        &quote,
			SettleID:      &quote,
			Type:          &marketType,
			Active:        ptrBool(hasStatus && status == "1"),
			Spot:          ptrBool(false),
			Swap:          ptrBool(true),
			Linear:        ptrBool(true),
			Contract:      ptrBool(true),
			Created:       ptrInt64(row["timeOnline"]),
		}
		markets[symbol] = info
	}
	return markets, nil
}

func (c *Client) loadBingXSpotMarkets(ctx context.Context) (map[string]market.MarketInfo, error) {
	payload, err := c.getJSON(ctx, "/openApi/spot/v1/common/symbols", nil, func(ctx context.Context) error {
		return c.waitBingX(ctx)
	})
	if err != nil {
		return nil, err
	}
	root, ok := asMap(payload)
	if !ok {
		return nil, fmt.Errorf("venue: bingx symbols returned %T, want an object", payload)
	}
	rows, ok := asSlice(root["symbols"])
	if !ok {
		return nil, fmt.Errorf("venue: bingx symbols has no symbols array")
	}

	markets := make(map[string]market.MarketInfo, len(rows))
	for _, raw := range rows {
		row, ok := asMap(raw)
		if !ok {
			continue
		}
		id, _ := asString(row["symbol"])
		base, quote, ok := splitDashSymbol(id)
		if !ok {
			continue
		}
		status, hasStatus := asInt64(row["status"])
		symbol := base + "/" + quote
		marketType := "spot"
		info := market.MarketInfo{
			Symbol:        &symbol,
			ID:            &id,
			BaseCurrency:  &base,
			BaseID:        &base,
			QuoteCurrency: &quote,
			QuoteID:       &quote,
			Type:          &marketType,
			Active:        ptrBool(hasStatus && status == 1),
			Spot:          ptrBool(true),
			Swap:          ptrBool(false),
			Linear:        ptrBool(false),
			Contract:      ptrBool(false),
		}
		markets[symbol] = info
	}
	return markets, nil
}

func splitDashSymbol(symbol string) (string, string, bool) {
	parts := strings.SplitN(symbol, "-", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// marketInfoByID indexes the catalog by the venue id. Discovery uses it to
// match tickers, whose keys are not always the unified symbol.
func marketInfoByID(markets map[string]market.MarketInfo) map[string]market.MarketInfo {
	byID := make(map[string]market.MarketInfo, len(markets))
	for _, info := range markets {
		if info.ID != nil {
			byID[*info.ID] = info
		}
	}
	return byID
}

// queryValues is a small helper that keeps call sites readable.
func queryValues(pairs ...string) url.Values {
	values := make(url.Values, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			values.Set(pairs[i], pairs[i+1])
		}
	}
	return values
}
