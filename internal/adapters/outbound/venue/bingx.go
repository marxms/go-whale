package venue

import (
	"context"
	"errors"
	"fmt"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/market"
)

// bingXDriver owns every BingX request path, the per-IP pacer and the keepalive encoding.
// It shares the Client's transport and absence bookkeeping; the request shape stays here
// so a switch never returns to the façade.
type bingXDriver struct {
	client *Client
}

var _ driver = (*bingXDriver)(nil)

// Capabilities reads the declared matrix row for this scope.
func (d *bingXDriver) Capabilities() market.Capabilities {
	return market.CapabilitiesFor(market.BingX.String(), d.client.marketType.String())
}

// FetchOHLCV returns OHLCV-only candles for one internal symbol.
func (d *bingXDriver) FetchOHLCV(
	ctx context.Context,
	symbol, timeframe string,
	sinceMs, limit int64,
) ([]candle.Candle, error) {
	rows, err := d.fetchKlineRows(ctx, symbol, timeframe, sinceMs, limit)
	if err != nil {
		return nil, err
	}

	out := make([]candle.Candle, 0, len(rows))
	for _, row := range rows {
		if parsed, ok := parseOHLCVRow(row); ok {
			out = append(out, parsed)
		}
	}
	return out, nil
}

// FetchKlinesWithCVD reports the declared absence: BingX public klines carry no taker-buy
// volume, so the caller falls back to FetchOHLCV.
func (d *bingXDriver) FetchKlinesWithCVD(
	context.Context,
	string, string,
	int,
) ([]candle.Candle, bool, error) {
	return nil, false, d.client.unsupported(market.CapabilityCVD)
}

func (d *bingXDriver) fetchKlineRows(
	ctx context.Context,
	symbol, timeframe string,
	sinceMs, limit int64,
) ([]any, error) {
	clamped := clampKlineLimit(limit)
	query := queryValues(
		"symbol", bingXSymbol(symbol),
		"interval", timeframe,
		"limit", fmt.Sprintf("%d", clamped),
	)
	if sinceMs > 0 {
		query.Set("startTime", fmt.Sprintf("%d", sinceMs))
	}

	path := "/openApi/swap/v3/quote/klines"
	if d.client.marketType == market.Spot {
		path = "/openApi/spot/v1/market/kline"
	}
	payload, err := d.client.getJSON(ctx, path, query, func(ctx context.Context) error {
		return d.client.waitBingX(ctx)
	})
	if err != nil {
		if errors.Is(err, exchange.ErrRateLimited) {
			d.client.budget.PenalizeForRefusal()
		}
		return nil, err
	}

	rows, ok := asSlice(payload)
	if !ok {
		return nil, fmt.Errorf("venue: %s returned %T, want an array", path, payload)
	}
	return rows, nil
}

// FundingRate returns the current funding reading for one contract.
func (d *bingXDriver) FundingRate(ctx context.Context, symbol string) (derivatives.FundingRate, error) {
	if d.client.marketType == market.Spot {
		return derivatives.FundingRate{}, d.client.unsupported(market.CapabilityFunding)
	}

	path := "/openApi/swap/v2/quote/premiumIndex"
	query := queryValues("symbol", bingXSymbol(symbol))

	payload, err := d.client.getJSON(ctx, path, query, func(ctx context.Context) error {
		return d.client.waitBingX(ctx)
	})
	if err != nil {
		return derivatives.FundingRate{}, err
	}

	row, ok := fundingRow(payload)
	if !ok {
		return derivatives.FundingRate{}, fmt.Errorf("venue: %s returned %T, want an object", path, payload)
	}

	return derivatives.FundingRate{
		Rate:         ptrFloat(row["lastFundingRate"]),
		MarkPrice:    ptrFloat(row["markPrice"]),
		InterestRate: ptrFloat(row["interestRate"]),
	}, nil
}

// FundingRateHistory returns settlements in venue order.
func (d *bingXDriver) FundingRateHistory(
	ctx context.Context,
	symbol string,
	limit int64,
) ([]derivatives.FundingRateHistoryPoint, error) {
	if d.client.marketType == market.Spot {
		return nil, d.client.unsupported(market.CapabilityFunding)
	}

	path := "/openApi/swap/v2/quote/fundingRate"
	query := queryValues("symbol", bingXSymbol(symbol))
	if limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", clampHistoryLimit(limit)))
	}

	payload, err := d.client.getJSON(ctx, path, query, func(ctx context.Context) error {
		return d.client.waitBingX(ctx)
	})
	if err != nil {
		return nil, err
	}

	rows, ok := asSlice(payload)
	if !ok {
		return nil, fmt.Errorf("venue: %s returned %T, want an array", path, payload)
	}

	out := make([]derivatives.FundingRateHistoryPoint, 0, len(rows))
	for _, raw := range rows {
		row, ok := asMap(raw)
		if !ok {
			continue
		}
		out = append(out, derivatives.FundingRateHistoryPoint{
			Timestamp: ptrInt64(row["fundingTime"]),
			Rate:      ptrFloat(row["fundingRate"]),
		})
	}
	return out, nil
}

// OpenInterest returns the current reading. BingX publishes a base-asset value, not a
// contract count, so Amount stays absent rather than being labeled with the wrong unit.
func (d *bingXDriver) OpenInterest(ctx context.Context, symbol string) (derivatives.OpenInterest, error) {
	if d.client.marketType == market.Spot {
		return derivatives.OpenInterest{}, d.client.unsupported(market.CapabilityOpenInterest)
	}

	path := "/openApi/swap/v2/quote/openInterest"
	query := queryValues("symbol", bingXSymbol(symbol))

	payload, err := d.client.getJSON(ctx, path, query, func(ctx context.Context) error {
		return d.client.waitBingX(ctx)
	})
	if err != nil {
		return derivatives.OpenInterest{}, err
	}

	row, ok := fundingRow(payload)
	if !ok {
		return derivatives.OpenInterest{}, fmt.Errorf("venue: %s returned %T, want an object", path, payload)
	}

	return derivatives.OpenInterest{
		Timestamp: ptrInt64(firstNonZero(row, "time", "timestamp")),
	}, nil
}

// OpenInterestHistory reports the declared absence: BingX publishes no open interest
// history.
func (d *bingXDriver) OpenInterestHistory(
	context.Context,
	string, string,
	int64, int64, int64,
) ([]derivatives.OpenInterestHistoryPoint, error) {
	return nil, d.client.unsupported(market.CapabilityOpenInterestHistory)
}

// LongShortRatioHistory reports the declared absence: BingX publishes no long/short account
// ratio history.
func (d *bingXDriver) LongShortRatioHistory(
	context.Context,
	string, string,
	int64, int64, int64,
) ([]derivatives.LongShortRatioPoint, error) {
	return nil, d.client.unsupported(market.CapabilityLongShortRatio)
}

// LoadMarkets returns the BingX catalog in canonical form.
func (d *bingXDriver) LoadMarkets(ctx context.Context) (map[string]market.MarketInfo, error) {
	return d.client.loadBingXMarkets(ctx)
}

// Close releases the driver's own resources. The transport belongs to the façade.
func (d *bingXDriver) Close() []error { return nil }

// keepaliveEncoding is how the adapter must answer BingX's application ping: JSON on spot,
// text on swap. It reads the declared policy instead of a local constant.
func (d *bingXDriver) keepaliveEncoding() market.PingEncoding {
	policy, ok := market.ConnPolicyFor(market.BingX.String(), d.client.marketType)
	if !ok {
		return ""
	}
	return policy.PingEncoding
}
