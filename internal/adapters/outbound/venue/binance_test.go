package venue

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/marxms/go-whale/internal/market"
)

// TestBinanceDriverParity pins PRT-03/PRT-04 for the extracted driver: every request the
// façade served before is answered by binanceDriver with the same typed result, and every
// capability the matrix declares absent still returns the typed permanent absence.
func TestBinanceDriverParity(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/fapi/v1/klines":       jsonHandler(binanceKlineBody("100.0", "110.0", "90.0", "105.0")),
		"/fapi/v1/exchangeInfo": jsonHandler(binanceFutureMarketsBody),
		"/fapi/v1/premiumIndex": jsonHandler(binanceFundingBody),
		"/fapi/v1/fundingRate": jsonHandler(`[
			{"symbol":"BTCUSDT","fundingRate":"0.0001","fundingTime":1700000000000}
		]`),
		"/fapi/v1/openInterest":                     jsonHandler(binanceOpenInterestBody),
		"/futures/data/openInterestHist":            jsonHandler(binanceOpenInterestHistoryBody),
		"/futures/data/globalLongShortAccountRatio": jsonHandler(binanceLongShortRatioBody),
	})
	d, ok := client.driver.(*binanceDriver)
	require.True(t, ok, "the Binance scope must resolve the Binance driver")

	ctx := context.Background()

	candles, err := d.FetchOHLCV(ctx, "BTC/USDT", "5m", 0, 1)
	require.NoError(t, err)
	require.Len(t, candles, 1)
	require.Equal(t, int64(1700000000000), candles[0].Timestamp)
	require.InDelta(t, 110.0, candles[0].High, 1e-9)
	require.InDelta(t, 12.0, candles[0].Volume, 1e-9)

	cvd, cvdOK, err := d.FetchKlinesWithCVD(ctx, "BTC/USDT", "5m", 1)
	require.NoError(t, err)
	require.True(t, cvdOK)
	require.Len(t, cvd, 1)
	require.InDelta(t, 7.0, cvd[0].TakerBuyVolume, 1e-9)
	require.InDelta(t, 5.0, cvd[0].TakerSellVolume, 1e-9)

	rate, err := d.FundingRate(ctx, "BTC/USDT")
	require.NoError(t, err)
	require.InDelta(t, 0.0001, *rate.Rate, 1e-12)
	require.InDelta(t, 105.5, *rate.MarkPrice, 1e-9)
	require.InDelta(t, 0.0002, *rate.InterestRate, 1e-12)

	fundingHistory, err := d.FundingRateHistory(ctx, "BTC/USDT", 10)
	require.NoError(t, err)
	require.Len(t, fundingHistory, 1)
	require.Equal(t, int64(1700000000000), *fundingHistory[0].Timestamp)
	require.InDelta(t, 0.0001, *fundingHistory[0].Rate, 1e-12)

	reading, err := d.OpenInterest(ctx, "BTC/USDT")
	require.NoError(t, err)
	require.InDelta(t, 12345.5, *reading.Amount, 1e-9)
	require.Equal(t, int64(1700000000000), *reading.Timestamp)

	oiHistory, err := d.OpenInterestHistory(ctx, "BTC/USDT", "5m", 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, oiHistory, 1)
	require.InDelta(t, 100.0, *oiHistory[0].Amount, 1e-9)

	points, err := d.LongShortRatioHistory(ctx, "BTC/USDT", "5m", 10, 0, 0)
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.InDelta(t, 1.5, points[0].Ratio, 1e-9)
	require.True(t, points[0].HasShares)
	require.InDelta(t, 60.0, points[0].LongPct, 1e-9)

	markets, err := d.LoadMarkets(ctx)
	require.NoError(t, err)
	require.Contains(t, markets, "BTCUSDT")

	require.Equal(t, market.CapabilitiesFor(market.Binance.String(), market.Future.String()), d.Capabilities())

	spot := newTestClient(t, market.Binance, market.Spot, map[string]http.HandlerFunc{
		"/api/v3/klines": jsonHandler(binanceKlineBody("100.0", "110.0", "90.0", "105.0")),
	})
	spotDriver, ok := spot.driver.(*binanceDriver)
	require.True(t, ok)

	spotCVD, spotOK, err := spotDriver.FetchKlinesWithCVD(ctx, "BTC/USDT", "5m", 1)
	require.NoError(t, err, "the matrix declares CVD present on Binance spot")
	require.True(t, spotOK)
	require.Len(t, spotCVD, 1)

	_, err = spotDriver.FundingRate(ctx, "BTC/USDT")
	assertCapabilityAbsent(t, err, market.CapabilityFunding)

	_, err = spotDriver.OpenInterest(ctx, "BTC/USDT")
	assertCapabilityAbsent(t, err, market.CapabilityOpenInterest)

	_, err = spotDriver.OpenInterestHistory(ctx, "BTC/USDT", "5m", 0, 0, 10)
	assertCapabilityAbsent(t, err, market.CapabilityOpenInterestHistory)

	_, err = spotDriver.LongShortRatioHistory(ctx, "BTC/USDT", "5m", 10, 0, 0)
	assertCapabilityAbsent(t, err, market.CapabilityLongShortRatio)
}
