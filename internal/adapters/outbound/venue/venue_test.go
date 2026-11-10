package venue

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/market"
)

func newTestClient(
	t *testing.T,
	venueID market.Exchange,
	marketType market.MarketType,
	routes map[string]http.HandlerFunc,
) *Client {
	t.Helper()

	mux := http.NewServeMux()
	for path, handler := range routes {
		mux.HandleFunc(path, handler)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client, err := NewClientWithOptions(venueID.String(), marketType, Options{
		BinanceSpotBaseURL:    server.URL,
		BinanceFuturesBaseURL: server.URL,
		BingXBaseURL:          server.URL,
		HTTPClient:            server.Client(),
	})
	require.NoError(t, err)
	return client
}

func jsonHandler(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

func statusHandler(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func TestLoadMarkets_BinanceFutures(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/fapi/v1/exchangeInfo": jsonHandler(`{
			"symbols": [{
				"symbol": "BTCUSDT",
				"baseAsset": "BTC",
				"quoteAsset": "USDT",
				"marginAsset": "USDT",
				"contractType": "PERPETUAL",
				"underlyingType": "COIN",
				"status": "TRADING",
				"onboardDate": 1569398400000
			}]
		}`),
	})

	markets, err := client.LoadMarkets(context.Background())
	require.NoError(t, err)

	info, ok := markets["BTCUSDT"]
	require.True(t, ok)
	require.Equal(t, "BTCUSDT", *info.ID)
	require.Equal(t, "USDT", *info.Settle)
	require.Equal(t, "swap", *info.Type)
	require.True(t, *info.Swap)
	require.True(t, *info.Linear)
	require.Equal(t, "COIN", *info.UnderlyingType)
	require.Equal(t, int64(1569398400000), *info.Created)
}

func TestLoadMarkets_BinanceTradFiPerpetual(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/fapi/v1/exchangeInfo": jsonHandler(`{
			"symbols": [{
				"symbol": "AAPLUSDT",
				"baseAsset": "AAPL",
				"quoteAsset": "USDT",
				"marginAsset": "USDT",
				"contractType": "TRADIFI_PERPETUAL",
				"underlyingType": "EQUITY",
				"status": "TRADING",
				"onboardDate": 1
			}]
		}`),
	})

	markets, err := client.LoadMarkets(context.Background())
	require.NoError(t, err)

	info, ok := markets["AAPLUSDT"]
	require.True(t, ok)
	require.True(t, *info.Swap, "TRADIFI_PERPETUAL is a perpetual and must stay in the swap universe")
	require.Equal(t, "swap", *info.Type)
	require.Equal(t, "EQUITY", *info.UnderlyingType)
}

// A perpetual and its quarterly contract share base, quote and margin. Keying the markets
// map by the unified symbol let the quarterly row overwrite the perpetual, and the swap
// filter then dropped BTC/USDT from seeding and discovery.
func TestLoadMarkets_PerpetualAndQuarterlyDoNotCollide(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/fapi/v1/exchangeInfo": jsonHandler(`{
			"symbols": [
				{"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","contractType":"PERPETUAL","status":"TRADING"},
				{"symbol":"BTCUSDT_250926","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","contractType":"CURRENT_QUARTER","status":"TRADING"}
			]
		}`),
	})

	markets, err := client.LoadMarkets(context.Background())
	require.NoError(t, err)

	perpetual, ok := markets["BTCUSDT"]
	require.True(t, ok, "the perpetual must survive the quarterly row")
	require.Equal(t, "swap", *perpetual.Type)

	quarterly, ok := markets["BTCUSDT_250926"]
	require.True(t, ok, "the quarterly contract must be kept under its own venue id")
	require.Equal(t, "future", *quarterly.Type)

	valid := market.ValidSymbolsFiltered(markets, "swap")
	require.Contains(t, valid, "BTC/USDT", "the perpetual must remain in the swap universe")
}

func TestLoadMarkets_BinanceSpot(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Spot, map[string]http.HandlerFunc{
		"/api/v3/exchangeInfo": jsonHandler(`{
			"symbols": [{
				"symbol": "BTCUSDT",
				"baseAsset": "BTC",
				"quoteAsset": "USDT",
				"status": "TRADING",
				"isSpotTradingAllowed": true
			}]
		}`),
	})

	markets, err := client.LoadMarkets(context.Background())
	require.NoError(t, err)

	info, ok := markets["BTC/USDT"]
	require.True(t, ok)
	require.Equal(t, "spot", *info.Type)
	require.True(t, *info.Spot)
	require.Nil(t, info.Created)
}

func TestLoadMarkets_BingXSwap(t *testing.T) {
	client := newTestClient(t, market.BingX, market.Future, map[string]http.HandlerFunc{
		"/openApi/swap/v2/quote/contracts": jsonHandler(`{
			"code": 0,
			"data": [{
				"symbol": "BTC-USDT",
				"asset": "BTC",
				"currency": "USDT",
				"status": "1"
			}]
		}`),
	})

	markets, err := client.LoadMarkets(context.Background())
	require.NoError(t, err)

	info, ok := markets["BTC/USDT:USDT"]
	require.True(t, ok)
	require.Equal(t, "BTC-USDT", *info.ID)
	require.True(t, *info.Swap)
	require.True(t, *info.Active)
}

func TestLoadMarkets_BingXSpot(t *testing.T) {
	client := newTestClient(t, market.BingX, market.Spot, map[string]http.HandlerFunc{
		"/openApi/spot/v1/common/symbols": jsonHandler(`{
			"code": 0,
			"data": {"symbols": [{"symbol": "BTC-USDT", "status": 1}]}
		}`),
	})

	markets, err := client.LoadMarkets(context.Background())
	require.NoError(t, err)

	info, ok := markets["BTC/USDT"]
	require.True(t, ok)
	require.True(t, *info.Spot)
	require.True(t, *info.Active)
}

func TestFetchOHLCV_BinanceFutures(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/fapi/v1/klines": jsonHandler(`[
			[1700000000000,"100.0","110.0","90.0","105.0","12.0",1700000299999,"1260",42,"7.0","735","0"]
		]`),
	})

	candles, err := client.FetchOHLCV(context.Background(), "BTC/USDT", "5m", 0, 1)
	require.NoError(t, err)
	require.Len(t, candles, 1)
	require.Equal(t, candle.Candle{
		Timestamp: 1700000000000,
		Open:      100,
		High:      110,
		Low:       90,
		Close:     105,
		Volume:    12,
	}, candles[0])
}

// fapi.binance.com returns BCHUSDT 1w 2020-01-13 with high 215.69 below its open
// (339.65) and close (339.62). The row is impossible, and rejecting it would leave
// a hole that disables the weekly series downstream; the parser extends the bounds.
func TestFetchOHLCV_NormalizesVenueImpossibleBounds(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/fapi/v1/klines": jsonHandler(`[
			[1578873600000,"339.65","215.69","182.65","339.62","975831.371",1579478399999,1,1,"0","0","0"]
		]`),
	})

	candles, err := client.FetchOHLCV(context.Background(), "BCH/USDT", "1w", 0, 1)
	require.NoError(t, err)
	require.Len(t, candles, 1)
	require.InDelta(t, 339.65, candles[0].High, 1e-9)
	require.InDelta(t, 182.65, candles[0].Low, 1e-9)
	require.InDelta(t, 339.62, candles[0].Close, 1e-9)
}

func TestFetchOHLCV_BinanceSpot(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Spot, map[string]http.HandlerFunc{
		"/api/v3/klines": jsonHandler(`[
			[1700000000000,"100.0","110.0","90.0","105.0","12.0",1700000299999,"1260",42,"7.0","735","0"]
		]`),
	})

	candles, err := client.FetchOHLCV(context.Background(), "BTC/USDT", "5m", 0, 1)
	require.NoError(t, err)
	require.Len(t, candles, 1)
	require.InDelta(t, 105.0, candles[0].Close, 1e-9)
}

func TestFundingRateHistory_BingX(t *testing.T) {
	client := newTestClient(t, market.BingX, market.Future, map[string]http.HandlerFunc{
		"/openApi/swap/v2/quote/fundingRate": jsonHandler(`{
			"code": 0,
			"data": [{"symbol": "BTC-USDT", "fundingRate": "0.0001", "fundingTime": 1700000000000}]
		}`),
	})

	history, err := client.FundingRateHistory(context.Background(), "BTC/USDT", 10)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, int64(1700000000000), *history[0].Timestamp)
	require.InDelta(t, 0.0001, *history[0].Rate, 1e-12)
}

func TestLongShortRatioHistory_BingXIsUnsupported(t *testing.T) {
	client := newTestClient(t, market.BingX, market.Future, nil)

	_, err := client.LongShortRatioHistory(context.Background(), "BTC/USDT", "5m", 10, 0, 0)
	require.ErrorIs(t, err, exchange.ErrPermanent)
}

func TestFetchKlinesWithCVD_Binance(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/fapi/v1/klines": jsonHandler(`[
			[1700000000000,"100.0","110.0","90.0","105.0","12.0",1700000299999,"1260",42,"7.0","735","0"]
		]`),
	})

	candles, ok, err := client.FetchKlinesWithCVD(context.Background(), "BTC/USDT", "5m", 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, candles, 1)
	require.InDelta(t, 7.0, candles[0].TakerBuyVolume, 1e-9)
	require.InDelta(t, 5.0, candles[0].TakerSellVolume, 1e-9)
}

// The caller still falls back to FetchOHLCV, but the absence is now typed and observable
// instead of a silent `nil, false, nil`.
func TestFetchKlinesWithCVD_BingXFallsBack(t *testing.T) {
	client := newTestClient(t, market.BingX, market.Future, nil)

	candles, ok, err := client.FetchKlinesWithCVD(context.Background(), "BTC/USDT", "5m", 1)
	require.False(t, ok)
	require.Nil(t, candles)
	var absent *CapabilityAbsentError
	require.ErrorAs(t, err, &absent)
	require.Equal(t, market.CapabilityCVD, absent.Capability)
	require.ErrorIs(t, err, exchange.ErrPermanent)
}

func TestFetchOHLCV_BingXSwap(t *testing.T) {
	client := newTestClient(t, market.BingX, market.Future, map[string]http.HandlerFunc{
		"/openApi/swap/v3/quote/klines": jsonHandler(`{
			"code": 0,
			"data": [{
				"open": "100.0",
				"high": "110.0",
				"low": "90.0",
				"close": "105.0",
				"volume": "12.0",
				"time": 1700000000000
			}]
		}`),
	})

	candles, err := client.FetchOHLCV(context.Background(), "BTC/USDT", "5m", 0, 1)
	require.NoError(t, err)
	require.Len(t, candles, 1)
	require.Equal(t, int64(1700000000000), candles[0].Timestamp)
	require.InDelta(t, 105.0, candles[0].Close, 1e-9)
}

func TestFetchOHLCV_BingXSpot(t *testing.T) {
	client := newTestClient(t, market.BingX, market.Spot, map[string]http.HandlerFunc{
		"/openApi/spot/v1/market/kline": jsonHandler(`{
			"code": 0,
			"data": [[1700000000000, 100.0, 110.0, 90.0, 105.0, 12.0, 1700000299999, 1260]]
		}`),
	})

	candles, err := client.FetchOHLCV(context.Background(), "BTC/USDT", "5m", 0, 1)
	require.NoError(t, err)
	require.Len(t, candles, 1)
	require.InDelta(t, 12.0, candles[0].Volume, 1e-9)
}

func TestFundingRate_Binance(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/fapi/v1/premiumIndex": jsonHandler(`{
			"symbol": "BTCUSDT",
			"markPrice": "105.5",
			"lastFundingRate": "0.0001",
			"interestRate": "0.0002",
			"nextFundingTime": 1700003600000
		}`),
	})

	rate, err := client.FundingRate(context.Background(), "BTC/USDT")
	require.NoError(t, err)
	require.InDelta(t, 0.0001, *rate.Rate, 1e-12)
	require.InDelta(t, 105.5, *rate.MarkPrice, 1e-9)
	require.InDelta(t, 0.0002, *rate.InterestRate, 1e-12)
	require.Nil(t, rate.NextRate)
}

func TestFundingRate_BingX(t *testing.T) {
	client := newTestClient(t, market.BingX, market.Future, map[string]http.HandlerFunc{
		"/openApi/swap/v2/quote/premiumIndex": jsonHandler(`{
			"code": 0,
			"data": {"symbol": "BTC-USDT", "markPrice": "105.5", "lastFundingRate": "0.0001"}
		}`),
	})

	rate, err := client.FundingRate(context.Background(), "BTC/USDT")
	require.NoError(t, err)
	require.InDelta(t, 0.0001, *rate.Rate, 1e-12)
	require.Nil(t, rate.InterestRate)
}

func TestFundingRateHistory(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/fapi/v1/fundingRate": jsonHandler(`[
			{"symbol":"BTCUSDT","fundingRate":"0.0001","fundingTime":1700000000000}
		]`),
	})

	history, err := client.FundingRateHistory(context.Background(), "BTC/USDT", 10)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, int64(1700000000000), *history[0].Timestamp)
	require.InDelta(t, 0.0001, *history[0].Rate, 1e-12)
}

func TestOpenInterest_Binance(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/fapi/v1/openInterest": jsonHandler(`{"symbol":"BTCUSDT","openInterest":"12345.5","time":1700000000000}`),
	})

	reading, err := client.OpenInterest(context.Background(), "BTC/USDT")
	require.NoError(t, err)
	require.InDelta(t, 12345.5, *reading.Amount, 1e-9)
	require.Equal(t, int64(1700000000000), *reading.Timestamp)
}

func TestOpenInterest_BingXKeepsAmountAbsent(t *testing.T) {
	client := newTestClient(t, market.BingX, market.Future, map[string]http.HandlerFunc{
		"/openApi/swap/v2/quote/openInterest": jsonHandler(`{
			"code": 0,
			"data": {"symbol": "BTC-USDT", "openInterest": "12345.5", "time": 1700000000000}
		}`),
	})

	reading, err := client.OpenInterest(context.Background(), "BTC/USDT")
	require.NoError(t, err)
	require.Nil(t, reading.Amount)
	require.Equal(t, int64(1700000000000), *reading.Timestamp)
}

func TestOpenInterestHistory_Binance(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/futures/data/openInterestHist": jsonHandler(`[
			{"symbol":"BTCUSDT","sumOpenInterest":"100.0","sumOpenInterestValue":"10500","timestamp":1700000000000}
		]`),
	})

	history, err := client.OpenInterestHistory(context.Background(), "BTC/USDT", "5m", 1, 2, 10)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.InDelta(t, 100.0, *history[0].Amount, 1e-9)
}

func TestOpenInterestHistory_BingXIsUnsupported(t *testing.T) {
	client := newTestClient(t, market.BingX, market.Future, nil)

	_, err := client.OpenInterestHistory(context.Background(), "BTC/USDT", "5m", 0, 0, 10)
	require.ErrorIs(t, err, exchange.ErrPermanent)
}

func TestLongShortRatioHistory_Binance(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/futures/data/globalLongShortAccountRatio": jsonHandler(`[
			{"symbol":"BTCUSDT","longAccount":"0.6","shortAccount":"0.4","longShortRatio":"1.5","timestamp":1700000000000}
		]`),
	})

	points, err := client.LongShortRatioHistory(context.Background(), "BTC/USDT", "5m", 10, 1, 2)
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.InDelta(t, 1.5, points[0].Ratio, 1e-9)
	require.True(t, points[0].HasShares)
	require.InDelta(t, 60.0, points[0].LongPct, 1e-9)
}

func TestLongShortRatioHistory_RejectsNonFiniteShares(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
		"/futures/data/globalLongShortAccountRatio": jsonHandler(`[
			{"symbol":"BTCUSDT","longAccount":"NaN","shortAccount":"0.4","longShortRatio":"1.5","timestamp":1700000000000}
		]`),
	})

	var violations int
	var reason string
	client.SetMetrics(Metrics{
		LongShortShareSumViolation: func(_, _, recorded string) {
			violations++
			reason = recorded
		},
	})

	points, err := client.LongShortRatioHistory(context.Background(), "BTC/USDT", "5m", 10, 0, 0)
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.False(t, points[0].HasShares)
	require.Equal(t, 1, violations)
	require.Equal(t, "share_absent", reason)
}

func TestValidateShareSum_DistinguishesNonFiniteFromAbsent(t *testing.T) {
	ok, reason := validateShareSum(math.NaN(), 0.5, true, true)
	require.False(t, ok)
	require.Equal(t, "share_not_finite", reason)
}

func TestErrorClassification(t *testing.T) {
	t.Run("rate limited", func(t *testing.T) {
		client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
			"/fapi/v1/openInterest": statusHandler(http.StatusTooManyRequests, `{"code":-1003,"msg":"Too many requests."}`),
		})
		_, err := client.OpenInterest(context.Background(), "BTC/USDT")
		require.ErrorIs(t, err, exchange.ErrRateLimited)
	})

	t.Run("transient", func(t *testing.T) {
		client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
			"/fapi/v1/openInterest": statusHandler(http.StatusServiceUnavailable, `service unavailable`),
		})
		_, err := client.OpenInterest(context.Background(), "BTC/USDT")
		require.ErrorIs(t, err, exchange.ErrTransient)
	})

	t.Run("permanent", func(t *testing.T) {
		client := newTestClient(t, market.Binance, market.Future, map[string]http.HandlerFunc{
			"/fapi/v1/openInterest": statusHandler(http.StatusBadRequest, `{"code":-1121,"msg":"Invalid symbol."}`),
		})
		_, err := client.OpenInterest(context.Background(), "BTC/USDT")
		require.ErrorIs(t, err, exchange.ErrPermanent)
	})

	t.Run("bingx code", func(t *testing.T) {
		client := newTestClient(t, market.BingX, market.Future, map[string]http.HandlerFunc{
			"/openApi/swap/v2/quote/premiumIndex": jsonHandler(`{"code":100413,"msg":"invalid symbol"}`),
		})
		_, err := client.FundingRate(context.Background(), "BTC/USDT")
		require.ErrorIs(t, err, exchange.ErrPermanent)
	})
}

func newRouteServer(t *testing.T, routes map[string]string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	for path, body := range routes {
		mux.HandleFunc(path, jsonHandler(body))
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func TestDiscoverSymbols_BinanceFutures(t *testing.T) {
	server := newRouteServer(t, map[string]string{
		"/fapi/v1/exchangeInfo": `{
			"symbols": [
				{"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","contractType":"PERPETUAL","status":"TRADING","onboardDate":1},
				{"symbol":"ETHUSDT","baseAsset":"ETH","quoteAsset":"USDT","marginAsset":"USDT","contractType":"PERPETUAL","status":"TRADING","onboardDate":1},
				{"symbol":"SOLUSDT","baseAsset":"SOL","quoteAsset":"USDT","marginAsset":"USDT","contractType":"PERPETUAL","status":"TRADING","onboardDate":1}
			]
		}`,
		"/fapi/v1/ticker/24hr": `[
			{"symbol":"BTCUSDT","quoteVolume":"100"},
			{"symbol":"ETHUSDT","quoteVolume":"300"},
			{"symbol":"SOLUSDT","quoteVolume":"200"}
		]`,
	})

	discovery := Discovery{Options: Options{
		BinanceFuturesBaseURL: server.URL,
		HTTPClient:            server.Client(),
	}}
	symbols, err := discovery.DiscoverSymbols(context.Background(), market.Binance, market.Future, 3)
	require.NoError(t, err)
	require.Equal(t, []string{"ETH/USDT", "SOL/USDT", "BTC/USDT"}, symbols)
}

func TestDiscoverSymbols_BingXSwap(t *testing.T) {
	server := newRouteServer(t, map[string]string{
		"/openApi/swap/v2/quote/contracts": `{
			"code": 0,
			"data": [
				{"symbol":"BTC-USDT","asset":"BTC","currency":"USDT","status":"1"},
				{"symbol":"ETH-USDT","asset":"ETH","currency":"USDT","status":"1"},
				{"symbol":"SOL-USDT","asset":"SOL","currency":"USDT","status":"1"}
			]
		}`,
		"/openApi/swap/v2/quote/ticker": `{
			"code": 0,
			"data": [
				{"symbol":"BTC-USDT","quoteVolume":"100"},
				{"symbol":"ETH-USDT","quoteVolume":"300"},
				{"symbol":"SOL-USDT","quoteVolume":"200"}
			]
		}`,
	})

	discovery := Discovery{Options: Options{
		BingXBaseURL: server.URL,
		HTTPClient:   server.Client(),
	}}
	symbols, err := discovery.DiscoverSymbols(context.Background(), market.BingX, market.Future, 3)
	require.NoError(t, err)
	require.Equal(t, []string{"ETH/USDT", "SOL/USDT", "BTC/USDT"}, symbols)
}
