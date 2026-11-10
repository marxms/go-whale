package venue

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/market"
)

// contractScope is one row of the porting contract. Every scope runs the same assertions,
// so a new venue is certified by adding a row, not by writing a second harness.
type contractScope struct {
	name       string
	exchange   market.Exchange
	marketType market.MarketType
	routes     map[string]http.HandlerFunc

	// klinePath and impossibleBody carry the venue's own wire shape for the
	// impossible-OHLC fixture: the contract is about the row, not the encoding.
	klinePath      string
	impossibleBody string
}

// TestVenueContractFourScopes pins CAP-07/CAP-08/SD-06/SD-07: every capability the matrix
// declares present answers a valid fixture with a typed, non-empty result; every absent
// driver capability answers with CapabilityAbsentError (never a silent empty result); listing
// floors come from the matrix; and the impossible-OHLC class is normalized on every venue.
//
// This is the executable certification the go-whale porting checklist points at, together
// with the rest of venue_test.go.
func TestVenueContractFourScopes(t *testing.T) {
	scopes := []contractScope{
		{
			name:       "binance spot",
			exchange:   market.Binance,
			marketType: market.Spot,
			routes: map[string]http.HandlerFunc{
				"/api/v3/klines":       jsonHandler(binanceKlineBody("100.0", "110.0", "90.0", "105.0")),
				"/api/v3/exchangeInfo": jsonHandler(binanceSpotMarketsBody),
			},
			klinePath:      "/api/v3/klines",
			impossibleBody: binanceKlineBody("339.65", "215.69", "182.65", "339.62"),
		},
		{
			name:       "binance future",
			exchange:   market.Binance,
			marketType: market.Future,
			routes: map[string]http.HandlerFunc{
				"/fapi/v1/klines":                           jsonHandler(binanceKlineBody("100.0", "110.0", "90.0", "105.0")),
				"/fapi/v1/exchangeInfo":                     jsonHandler(binanceFutureMarketsBody),
				"/fapi/v1/premiumIndex":                     jsonHandler(binanceFundingBody),
				"/fapi/v1/openInterest":                     jsonHandler(binanceOpenInterestBody),
				"/futures/data/openInterestHist":            jsonHandler(binanceOpenInterestHistoryBody),
				"/futures/data/globalLongShortAccountRatio": jsonHandler(binanceLongShortRatioBody),
			},
			klinePath:      "/fapi/v1/klines",
			impossibleBody: binanceKlineBody("339.65", "215.69", "182.65", "339.62"),
		},
		{
			name:       "bingx spot",
			exchange:   market.BingX,
			marketType: market.Spot,
			routes: map[string]http.HandlerFunc{
				"/openApi/spot/v1/market/kline":   jsonHandler(bingXSpotKlineBody("100.0", "110.0", "90.0", "105.0")),
				"/openApi/spot/v1/common/symbols": jsonHandler(bingXSpotMarketsBody),
			},
			klinePath:      "/openApi/spot/v1/market/kline",
			impossibleBody: bingXSpotKlineBody("339.65", "215.69", "182.65", "339.62"),
		},
		{
			name:       "bingx swap",
			exchange:   market.BingX,
			marketType: market.Future,
			routes: map[string]http.HandlerFunc{
				"/openApi/swap/v3/quote/klines":       jsonHandler(bingXSwapKlineBody("100.0", "110.0", "90.0", "105.0")),
				"/openApi/swap/v2/quote/contracts":    jsonHandler(bingXSwapMarketsBody),
				"/openApi/swap/v2/quote/premiumIndex": jsonHandler(bingXFundingBody),
				"/openApi/swap/v2/quote/openInterest": jsonHandler(bingXOpenInterestBody),
			},
			klinePath:      "/openApi/swap/v3/quote/klines",
			impossibleBody: bingXSwapKlineBody("339.65", "215.69", "182.65", "339.62"),
		},
	}

	driverCapabilities := []market.Capability{
		market.CapabilityCVD,
		market.CapabilityFunding,
		market.CapabilityOpenInterest,
		market.CapabilityOpenInterestHistory,
		market.CapabilityLongShortRatio,
	}

	for _, scope := range scopes {
		scope := scope
		t.Run(scope.name, func(t *testing.T) {
			ctx := context.Background()
			caps := market.CapabilitiesFor(scope.exchange.String(), scope.marketType.String())
			client := newTestClient(t, scope.exchange, scope.marketType, scope.routes)

			var recorded []market.Capability
			client.SetMetrics(Metrics{CapabilityAbsent: func(ex, mt, capability string) {
				require.Equal(t, scope.exchange.String(), ex)
				require.Equal(t, scope.marketType.String(), mt)
				recorded = append(recorded, market.Capability(capability))
			}})

			// kline_feed is a transport capability, so the contract reads the same policy
			// the WebSocket transport derives its route from.
			if caps.Supports(market.CapabilityKlineFeed) {
				policy, ok := market.ConnPolicyFor(scope.exchange.String(), scope.marketType)
				require.True(t, ok)
				require.Equal(t, "/market", policy.Routes["kline"],
					"a scope that declares a kline feed must route it on /market")
			} else {
				require.NotEmpty(t, caps.AbsenceReason(market.CapabilityKlineFeed))
			}

			candles, ok, err := client.FetchKlinesWithCVD(ctx, "BTC/USDT", "5m", 1)
			if caps.Supports(market.CapabilityCVD) {
				require.NoError(t, err)
				require.True(t, ok)
				require.NotEmpty(t, candles)
				require.Greater(t, candles[0].TakerBuyVolume, 0.0,
					"the CVD capability must carry the venue's taker-buy volume")
			} else {
				require.False(t, ok)
				require.Nil(t, candles)
				assertCapabilityAbsent(t, err, market.CapabilityCVD)
			}

			rate, err := client.FundingRate(ctx, "BTC/USDT")
			if caps.Supports(market.CapabilityFunding) {
				require.NoError(t, err)
				require.NotNil(t, rate.Rate)
			} else {
				assertCapabilityAbsent(t, err, market.CapabilityFunding)
			}

			reading, err := client.OpenInterest(ctx, "BTC/USDT")
			if caps.Supports(market.CapabilityOpenInterest) {
				require.NoError(t, err)
				require.NotNil(t, reading.Timestamp)
				require.Positive(t, *reading.Timestamp)
			} else {
				assertCapabilityAbsent(t, err, market.CapabilityOpenInterest)
			}

			history, err := client.OpenInterestHistory(ctx, "BTC/USDT", "5m", 0, 0, 10)
			if caps.Supports(market.CapabilityOpenInterestHistory) {
				require.NoError(t, err)
				require.NotEmpty(t, history)
				require.NotNil(t, history[0].Amount)
			} else {
				assertCapabilityAbsent(t, err, market.CapabilityOpenInterestHistory)
			}

			points, err := client.LongShortRatioHistory(ctx, "BTC/USDT", "5m", 10, 0, 0)
			if caps.Supports(market.CapabilityLongShortRatio) {
				require.NoError(t, err)
				require.NotEmpty(t, points)
				require.NotZero(t, points[0].Ratio)
			} else {
				assertCapabilityAbsent(t, err, market.CapabilityLongShortRatio)
			}

			markets, err := client.LoadMarkets(ctx)
			require.True(t, caps.Supports(market.CapabilityMarkets),
				"every supported scope lists markets; a future absence needs a typed path here")
			require.NoError(t, err)
			require.NotEmpty(t, markets)

			// listing_floors is derived from LoadMarkets by VerifiedListingFloors, not a
			// driver method, so the harness proves it through that function.
			listingMarkets, wanted, created := contractListingFixture()
			floors := market.VerifiedListingFloors(scope.exchange.String(), scope.marketType.String(), wanted, listingMarkets, created+1000)
			if caps.Supports(market.CapabilityListingFloors) {
				require.Equal(t, created, floors["BTC/USDT"])
			} else {
				require.Empty(t, floors)
			}

			var wantRecorded []market.Capability
			for _, capability := range driverCapabilities {
				if !caps.Supports(capability) {
					wantRecorded = append(wantRecorded, capability)
				}
			}
			require.ElementsMatch(t, wantRecorded, recorded,
				"every absent driver capability must be refused with a typed, counted absence")

			// SD-06/SD-07: the same integrity contract on every venue, with no exchange
			// branch. The row is impossible as published and must be repaired, not dropped.
			broken := newTestClient(t, scope.exchange, scope.marketType, map[string]http.HandlerFunc{
				scope.klinePath: jsonHandler(scope.impossibleBody),
			})
			repaired, err := broken.FetchOHLCV(ctx, "BCH/USDT", "1w", 0, 1)
			require.NoError(t, err)
			require.Len(t, repaired, 1)
			require.InDelta(t, 339.65, repaired[0].High, 1e-9)
			require.InDelta(t, 182.65, repaired[0].Low, 1e-9)
			require.InDelta(t, 339.65, repaired[0].Open, 1e-9)
			require.InDelta(t, 339.62, repaired[0].Close, 1e-9)
		})
	}
}

func assertCapabilityAbsent(t *testing.T, err error, capability market.Capability) {
	t.Helper()
	var absent *CapabilityAbsentError
	require.ErrorAs(t, err, &absent)
	require.Equal(t, capability, absent.Capability)
	require.ErrorIs(t, err, exchange.ErrPermanent)
}

func contractListingFixture() (map[string]market.MarketInfo, []string, int64) {
	symbol, id, usdt := "BTC/USDT:USDT", "BTCUSDT", "USDT"
	created := int64(1000)
	trueValue := true
	return map[string]market.MarketInfo{
		symbol: {
			Symbol: &symbol, ID: &id, Created: &created,
			Swap: &trueValue, Linear: &trueValue, QuoteCurrency: &usdt, Settle: &usdt,
		},
	}, []string{"BTC/USDT"}, created
}

const binanceSpotMarketsBody = `{"symbols":[{"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","status":"TRADING","isSpotTradingAllowed":true}]}`

const binanceFutureMarketsBody = `{"symbols":[{"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","contractType":"PERPETUAL","underlyingType":"COIN","status":"TRADING","onboardDate":1569398400000}]}`

const bingXSpotMarketsBody = `{"code":0,"data":{"symbols":[{"symbol":"BTC-USDT","status":1}]}}`

const bingXSwapMarketsBody = `{"code":0,"data":[{"symbol":"BTC-USDT","asset":"BTC","currency":"USDT","status":"1"}]}`

const binanceFundingBody = `{"symbol":"BTCUSDT","markPrice":"105.5","lastFundingRate":"0.0001","interestRate":"0.0002","nextFundingTime":1700003600000}`

const bingXFundingBody = `{"code":0,"data":{"symbol":"BTC-USDT","markPrice":"105.5","lastFundingRate":"0.0001"}}`

const binanceOpenInterestBody = `{"symbol":"BTCUSDT","openInterest":"12345.5","time":1700000000000}`

const bingXOpenInterestBody = `{"code":0,"data":{"symbol":"BTC-USDT","openInterest":"12345.5","time":1700000000000}}`

const binanceOpenInterestHistoryBody = `[{"symbol":"BTCUSDT","sumOpenInterest":"100.0","sumOpenInterestValue":"10500","timestamp":1700000000000}]`

const binanceLongShortRatioBody = `[{"symbol":"BTCUSDT","longAccount":"0.6","shortAccount":"0.4","longShortRatio":"1.5","timestamp":1700000000000}]`

func binanceKlineBody(open, high, low, closePrice string) string {
	return `[[1700000000000,"` + open + `","` + high + `","` + low + `","` + closePrice + `","12.0",1700000299999,"1260",42,"7.0","735","0"]]`
}

func bingXSwapKlineBody(open, high, low, closePrice string) string {
	return `{"code":0,"data":[{"open":"` + open + `","high":"` + high + `","low":"` + low + `","close":"` + closePrice + `","volume":"12.0","time":1700000000000}]}`
}

func bingXSpotKlineBody(open, high, low, closePrice string) string {
	return `{"code":0,"data":[[1700000000000,` + open + `,` + high + `,` + low + `,` + closePrice + `,12.0,1700000299999,1260]]}`
}
