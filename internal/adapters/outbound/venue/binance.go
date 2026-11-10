package venue

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/market"
)

// binanceDriver owns every Binance request path and its request weight. It shares the
// Client's transport, budget and absence bookkeeping; only the venue-specific request shape
// lives here, so a new venue adds a driver instead of editing a switch in the hot path.
type binanceDriver struct {
	client *Client
}

var _ driver = (*binanceDriver)(nil)

// Capabilities reads the declared matrix row for this scope.
func (d *binanceDriver) Capabilities() market.Capabilities {
	return market.CapabilitiesFor(market.Binance.String(), d.client.marketType.String())
}

// FetchOHLCV returns OHLCV-only candles for one internal symbol.
func (d *binanceDriver) FetchOHLCV(
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

// FetchKlinesWithCVD returns candles carrying the taker-buy volume. Binance serves the
// field both in spot and in USD-M, so the capability is present on both scopes.
func (d *binanceDriver) FetchKlinesWithCVD(
	ctx context.Context,
	symbol, timeframe string,
	limit int,
) ([]candle.Candle, bool, error) {
	rows, err := d.fetchKlineRows(ctx, symbol, timeframe, 0, int64(limit))
	if err != nil {
		return nil, false, err
	}

	out := make([]candle.Candle, 0, len(rows))
	for _, row := range rows {
		parsed, ok := parseOHLCVRow(row)
		if !ok {
			continue
		}
		if takerBuy, ok := klineTakerBuy(row); ok {
			parsed.TakerBuyVolume = takerBuy
			candle.DeriveTakerSell(&parsed)
		}
		out = append(out, parsed)
	}
	return out, true, nil
}

func (d *binanceDriver) fetchKlineRows(
	ctx context.Context,
	symbol, timeframe string,
	sinceMs, limit int64,
) ([]any, error) {
	clamped := clampKlineLimit(limit)
	query := queryValues(
		"symbol", binanceSymbol(symbol),
		"interval", timeframe,
		"limit", fmt.Sprintf("%d", clamped),
	)
	if sinceMs > 0 {
		query.Set("startTime", fmt.Sprintf("%d", sinceMs))
	}

	path := "/fapi/v1/klines"
	if d.client.marketType == market.Spot {
		path = "/api/v3/klines"
	}
	payload, err := d.client.getJSON(ctx, path, query, func(ctx context.Context) error {
		return d.client.acquireKlines(ctx, int(clamped))
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
func (d *binanceDriver) FundingRate(ctx context.Context, symbol string) (derivatives.FundingRate, error) {
	if d.client.marketType == market.Spot {
		return derivatives.FundingRate{}, d.client.unsupported(market.CapabilityFunding)
	}

	path := "/fapi/v1/premiumIndex"
	query := queryValues("symbol", binanceSymbol(symbol))

	payload, err := d.client.getJSON(ctx, path, query, func(ctx context.Context) error {
		return d.client.acquireBinanceWeight(ctx, 1)
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

// FundingRateHistory returns settlements in venue order; absent fields stay absent so the
// caller can distinguish a missing rate from zero.
func (d *binanceDriver) FundingRateHistory(
	ctx context.Context,
	symbol string,
	limit int64,
) ([]derivatives.FundingRateHistoryPoint, error) {
	if d.client.marketType == market.Spot {
		return nil, d.client.unsupported(market.CapabilityFunding)
	}

	path := "/fapi/v1/fundingRate"
	query := queryValues("symbol", binanceSymbol(symbol))
	if limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", clampHistoryLimit(limit)))
	}

	payload, err := d.client.getJSON(ctx, path, query, func(ctx context.Context) error {
		return d.client.acquireBinanceWeight(ctx, 1)
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

// OpenInterest returns the current contract count for one contract.
func (d *binanceDriver) OpenInterest(ctx context.Context, symbol string) (derivatives.OpenInterest, error) {
	if d.client.marketType == market.Spot {
		return derivatives.OpenInterest{}, d.client.unsupported(market.CapabilityOpenInterest)
	}

	path := "/fapi/v1/openInterest"
	query := queryValues("symbol", binanceSymbol(symbol))

	payload, err := d.client.getJSON(ctx, path, query, func(ctx context.Context) error {
		return d.client.acquireBinanceWeight(ctx, 1)
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
		Amount:    ptrFloat(row["openInterest"]),
	}, nil
}

// OpenInterestHistory returns samples inside the explicit page window.
func (d *binanceDriver) OpenInterestHistory(
	ctx context.Context,
	symbol, timeframe string,
	sinceMs, untilMs, limit int64,
) ([]derivatives.OpenInterestHistoryPoint, error) {
	if d.client.marketType == market.Spot {
		return nil, d.client.unsupported(market.CapabilityOpenInterestHistory)
	}

	query := queryValues(
		"symbol", binanceSymbol(symbol),
		"period", timeframe,
		"limit", fmt.Sprintf("%d", clampOIHistoryLimit(limit)),
	)
	if sinceMs > 0 {
		query.Set("startTime", fmt.Sprintf("%d", sinceMs))
	}
	if untilMs > 0 {
		query.Set("endTime", fmt.Sprintf("%d", untilMs))
	}

	payload, err := d.client.getJSON(ctx, "/futures/data/openInterestHist", query, func(ctx context.Context) error {
		return d.client.acquireBinanceWeight(ctx, 1)
	})
	if err != nil {
		return nil, err
	}

	rows, ok := asSlice(payload)
	if !ok {
		return nil, fmt.Errorf("venue: open interest history returned %T, want an array", payload)
	}

	out := make([]derivatives.OpenInterestHistoryPoint, 0, len(rows))
	for _, raw := range rows {
		row, ok := asMap(raw)
		if !ok {
			continue
		}
		out = append(out, derivatives.OpenInterestHistoryPoint{
			Timestamp: ptrInt64(row["timestamp"]),
			Amount:    ptrFloat(row["sumOpenInterest"]),
		})
	}
	return out, nil
}

// LongShortRatioHistory returns global long/short account ratio samples.
func (d *binanceDriver) LongShortRatioHistory(
	ctx context.Context,
	symbol, timeframe string,
	limit int64,
	sinceMs, endMs int64,
) ([]derivatives.LongShortRatioPoint, error) {
	if d.client.marketType == market.Spot {
		return nil, d.client.unsupported(market.CapabilityLongShortRatio)
	}

	marketID := binanceSymbol(symbol)
	query := queryValues(
		"symbol", marketID,
		"period", timeframe,
		"limit", fmt.Sprintf("%d", clampHistoryLimit(limit)),
	)
	if sinceMs > 0 && endMs > sinceMs {
		query.Set("startTime", fmt.Sprintf("%d", sinceMs))
		query.Set("endTime", fmt.Sprintf("%d", endMs))
	}

	payload, err := d.client.getJSON(ctx, "/futures/data/globalLongShortAccountRatio", query, func(ctx context.Context) error {
		return d.client.acquireBinanceWeight(ctx, 1)
	})
	if err != nil {
		return nil, err
	}

	rows, ok := asSlice(payload)
	if !ok {
		return nil, fmt.Errorf("venue: long/short ratio returned %T, want an array", payload)
	}

	out := make([]derivatives.LongShortRatioPoint, 0, len(rows))
	for _, raw := range rows {
		row, ok := asMap(raw)
		if !ok {
			continue
		}
		point := d.parseLSRRow(row, marketID)
		if point.Timestamp > 0 && point.Ratio > 0 {
			out = append(out, point)
		}
	}
	return out, nil
}

func (d *binanceDriver) parseLSRRow(row map[string]any, marketID string) derivatives.LongShortRatioPoint {
	point := derivatives.LongShortRatioPoint{}
	if timestamp, ok := asInt64(row["timestamp"]); ok {
		point.Timestamp = timestamp
	}

	rawRatio, _ := asString(row["longShortRatio"])
	ratio, err := strconv.ParseFloat(rawRatio, 64)
	if err != nil || math.IsNaN(ratio) || math.IsInf(ratio, 0) {
		d.client.log.Warn("long/short ratio discarded: unreadable or non-finite",
			"symbol", marketID, "value", rawRatio)
	} else {
		point.Ratio = ratio
	}

	longFraction, hasLong := parseShareFraction(row["longAccount"])
	shortFraction, hasShort := parseShareFraction(row["shortAccount"])

	ok, reason := validateShareSum(longFraction, shortFraction, hasLong, hasShort)
	if ok {
		point.LongPct = longFraction * 100
		point.ShortPct = shortFraction * 100
		point.HasShares = true
		return point
	}

	d.client.metrics.longShortShareSumViolation("binance", marketID, reason)
	d.client.log.Warn("long/short shares rejected",
		"symbol", marketID,
		"reason", reason,
		"long", row["longAccount"],
		"short", row["shortAccount"])
	return point
}

// LoadMarkets returns the Binance catalog in canonical form.
func (d *binanceDriver) LoadMarkets(ctx context.Context) (map[string]market.MarketInfo, error) {
	return d.client.loadBinanceMarkets(ctx)
}

// Close releases the driver's own resources. The transport belongs to the façade.
func (d *binanceDriver) Close() []error { return nil }
