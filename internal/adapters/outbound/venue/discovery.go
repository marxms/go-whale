package venue

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// Discovery is the concrete market.DiscoverySource. It is stateless; each call
// builds the REST client it needs.
type Discovery struct {
	Log     *slog.Logger
	Options Options
}

// DiscoverSymbols resolves the top-N markets by quote-denominated liquidity.
// The result is internal BASE/QUOTE, in liquidity-descending order.
func (d Discovery) DiscoverSymbols(
	ctx context.Context,
	exchangeID market.Exchange,
	marketType market.MarketType,
	limit int,
) ([]string, error) {
	if limit <= 0 {
		limit = 50
	}

	opts := d.Options
	opts.Log = logger.OrDiscard(d.Log, opts.Log)
	client, err := NewClientWithOptions(exchangeID.String(), marketType, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.Close() }()

	switch exchangeID {
	case market.Binance:
		return client.discoverBinance(ctx, marketType, limit)
	case market.BingX:
		return client.discoverBingX(ctx, marketType, limit)
	default:
		return nil, fmt.Errorf("venue: unsupported exchange for discovery %q", exchangeID)
	}
}

func (c *Client) discoverBinance(ctx context.Context, marketType market.MarketType, limit int) ([]string, error) {
	markets, err := c.LoadMarkets(ctx)
	if err != nil {
		return nil, err
	}
	byID := marketInfoByID(markets)

	path := "/api/v3/ticker/24hr"
	weight := 80
	if marketType != market.Spot {
		path = "/fapi/v1/ticker/24hr"
		weight = 40
	}
	payload, err := c.getJSON(ctx, path, nil, func(ctx context.Context) error {
		return c.acquireBinanceWeight(ctx, weight)
	})
	if err != nil {
		return nil, err
	}
	rows, ok := asSlice(payload)
	if !ok {
		return nil, fmt.Errorf("venue: %s returned %T, want an array", path, payload)
	}

	candidates := make(map[string]float64, len(rows))
	for _, raw := range rows {
		row, ok := asMap(raw)
		if !ok {
			continue
		}
		id, _ := asString(row["symbol"])
		info, ok := byID[id]
		if !ok || !eligibleMarket(info, marketType) {
			continue
		}
		internal := market.InternalSymbolFromConcatenated(id)
		if internal == "" || quoteOf(internal) != "USDT" {
			continue
		}
		volume := tickerVolume(row)
		if current, seen := candidates[internal]; !seen || volume > current {
			candidates[internal] = volume
		}
	}

	return rankSymbols(candidates, limit, marketType), nil
}

func (c *Client) discoverBingX(ctx context.Context, marketType market.MarketType, limit int) ([]string, error) {
	markets, err := c.LoadMarkets(ctx)
	if err != nil {
		return nil, err
	}
	byID := marketInfoByID(markets)

	path := "/openApi/spot/v1/ticker/24hr"
	if marketType != market.Spot {
		path = "/openApi/swap/v2/quote/ticker"
	}
	payload, err := c.getJSON(ctx, path, nil, func(ctx context.Context) error {
		return c.waitBingX(ctx)
	})
	if err != nil {
		return nil, err
	}
	rows, ok := asSlice(payload)
	if !ok {
		return nil, fmt.Errorf("venue: %s returned %T, want an array", path, payload)
	}

	candidates := make(map[string]float64, len(rows))
	for _, raw := range rows {
		row, ok := asMap(raw)
		if !ok {
			continue
		}
		id, _ := asString(row["symbol"])
		info, ok := byID[id]
		if !ok || !eligibleMarket(info, marketType) {
			continue
		}
		internal := market.NormalizeSymbol(id)
		if internal == "" || quoteOf(internal) != "USDT" {
			continue
		}
		volume := tickerVolume(row)
		if current, seen := candidates[internal]; !seen || volume > current {
			candidates[internal] = volume
		}
	}

	return rankSymbols(candidates, limit, marketType), nil
}

func eligibleMarket(info market.MarketInfo, marketType market.MarketType) bool {
	if info.Active != nil && !*info.Active {
		return false
	}
	if marketType == market.Spot {
		return info.Spot != nil && *info.Spot
	}
	return info.Swap != nil && *info.Swap
}

func tickerVolume(row map[string]any) float64 {
	if volume, ok := asFloat(row["quoteVolume"]); ok {
		return volume
	}
	volume, volumeOK := asFloat(row["volume"])
	price, priceOK := asFloat(row["lastPrice"])
	if !priceOK {
		price, priceOK = asFloat(row["last"])
	}
	if volumeOK && priceOK {
		return volume * price
	}
	return 0
}

func rankSymbols(candidates map[string]float64, limit int, marketType market.MarketType) []string {
	type ranked struct {
		symbol string
		volume float64
	}
	rows := make([]ranked, 0, len(candidates))
	for symbol, volume := range candidates {
		rows = append(rows, ranked{symbol: symbol, volume: volume})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].volume == rows[j].volume {
			return rows[i].symbol < rows[j].symbol
		}
		return rows[i].volume > rows[j].volume
	})

	out := make([]string, 0, limit)
	for _, row := range rows {
		out = append(out, row.symbol)
		if len(out) >= limit {
			break
		}
	}
	if marketType == market.Future {
		out = ensureMajorFutures(out, limit)
	}
	return out
}

func ensureMajorFutures(out []string, limit int) []string {
	if limit <= 0 {
		return out
	}
	majors := []string{"BTC/USDT", "ETH/USDT"}
	present := make(map[string]struct{}, len(out))
	for _, symbol := range out {
		present[symbol] = struct{}{}
	}

	for _, major := range majors {
		if _, ok := present[major]; ok {
			continue
		}
		if len(out) < limit {
			out = append(out, major)
			present[major] = struct{}{}
			continue
		}
		for index := len(out) - 1; index >= 0; index-- {
			if out[index] == "BTC/USDT" || out[index] == "ETH/USDT" {
				continue
			}
			delete(present, out[index])
			out[index] = major
			present[major] = struct{}{}
			break
		}
	}
	return out
}
