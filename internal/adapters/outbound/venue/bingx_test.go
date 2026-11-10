package venue

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/marxms/go-whale/internal/market"
)

// TestBingXDriverParity pins PRT-03 for the extracted driver: every request the façade
// served before is answered by bingXDriver with the same typed result, including the
// declared absences, and the catalog keeps its venue ids.
func TestBingXDriverParity(t *testing.T) {
	client := newTestClient(t, market.BingX, market.Future, map[string]http.HandlerFunc{
		"/openApi/swap/v3/quote/klines":       jsonHandler(bingXSwapKlineBody("100.0", "110.0", "90.0", "105.0")),
		"/openApi/swap/v2/quote/contracts":    jsonHandler(bingXSwapMarketsBody),
		"/openApi/swap/v2/quote/premiumIndex": jsonHandler(bingXFundingBody),
		"/openApi/swap/v2/quote/fundingRate": jsonHandler(`{
			"code": 0,
			"data": [{"symbol": "BTC-USDT", "fundingRate": "0.0001", "fundingTime": 1700000000000}]
		}`),
		"/openApi/swap/v2/quote/openInterest": jsonHandler(bingXOpenInterestBody),
	})
	d, ok := client.driver.(*bingXDriver)
	require.True(t, ok, "the BingX scope must resolve the BingX driver")

	ctx := context.Background()

	candles, err := d.FetchOHLCV(ctx, "BTC/USDT", "5m", 0, 1)
	require.NoError(t, err)
	require.Len(t, candles, 1)
	require.Equal(t, int64(1700000000000), candles[0].Timestamp)
	require.InDelta(t, 105.0, candles[0].Close, 1e-9)
	require.InDelta(t, 12.0, candles[0].Volume, 1e-9)

	cvd, cvdOK, err := d.FetchKlinesWithCVD(ctx, "BTC/USDT", "5m", 1)
	require.False(t, cvdOK)
	require.Nil(t, cvd)
	assertCapabilityAbsent(t, err, market.CapabilityCVD)

	rate, err := d.FundingRate(ctx, "BTC/USDT")
	require.NoError(t, err)
	require.InDelta(t, 0.0001, *rate.Rate, 1e-12)
	require.InDelta(t, 105.5, *rate.MarkPrice, 1e-9)
	require.Nil(t, rate.InterestRate, "BingX publishes no interest rate")

	fundingHistory, err := d.FundingRateHistory(ctx, "BTC/USDT", 10)
	require.NoError(t, err)
	require.Len(t, fundingHistory, 1)
	require.Equal(t, int64(1700000000000), *fundingHistory[0].Timestamp)
	require.InDelta(t, 0.0001, *fundingHistory[0].Rate, 1e-12)

	reading, err := d.OpenInterest(ctx, "BTC/USDT")
	require.NoError(t, err)
	require.Nil(t, reading.Amount, "BingX publishes a base-asset value, not a contract count")
	require.Equal(t, int64(1700000000000), *reading.Timestamp)

	_, err = d.OpenInterestHistory(ctx, "BTC/USDT", "5m", 0, 0, 10)
	assertCapabilityAbsent(t, err, market.CapabilityOpenInterestHistory)

	_, err = d.LongShortRatioHistory(ctx, "BTC/USDT", "5m", 10, 0, 0)
	assertCapabilityAbsent(t, err, market.CapabilityLongShortRatio)

	markets, err := d.LoadMarkets(ctx)
	require.NoError(t, err)
	require.Contains(t, markets, "BTC/USDT:USDT")

	require.Equal(t, market.CapabilitiesFor(market.BingX.String(), market.Future.String()), d.Capabilities())

	spot := newTestClient(t, market.BingX, market.Spot, map[string]http.HandlerFunc{
		"/openApi/spot/v1/market/kline": jsonHandler(bingXSpotKlineBody("100.0", "110.0", "90.0", "105.0")),
	})
	spotDriver, ok := spot.driver.(*bingXDriver)
	require.True(t, ok)

	spotCandles, err := spotDriver.FetchOHLCV(ctx, "BTC/USDT", "5m", 0, 1)
	require.NoError(t, err)
	require.Len(t, spotCandles, 1)
	require.InDelta(t, 12.0, spotCandles[0].Volume, 1e-9)

	_, err = spotDriver.FundingRate(ctx, "BTC/USDT")
	assertCapabilityAbsent(t, err, market.CapabilityFunding)

	require.Equal(t, market.CapabilitiesFor(market.BingX.String(), market.Spot.String()), spotDriver.Capabilities())
}

// TestBingXKeepaliveEncoding pins KFEED-05 at the driver: the adapter answers the venue's
// application ping in the encoding the declared policy names, JSON on spot and text on
// swap.
func TestBingXKeepaliveEncodingDeclared(t *testing.T) {
	swap := newTestClient(t, market.BingX, market.Future, nil)
	swapDriver, ok := swap.driver.(*bingXDriver)
	require.True(t, ok)
	require.Equal(t, market.PingEncodingText, swapDriver.keepaliveEncoding())

	spot := newTestClient(t, market.BingX, market.Spot, nil)
	spotDriver, ok := spot.driver.(*bingXDriver)
	require.True(t, ok)
	require.Equal(t, market.PingEncodingJSON, spotDriver.keepaliveEncoding())
}
