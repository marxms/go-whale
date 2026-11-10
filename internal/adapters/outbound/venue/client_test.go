package venue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/market"
)

// TestClientFacadeDelegates pins PRT-04/PRT-06 on the façade: swapping the resolved driver
// changes every request answer, which is only possible because the Client holds no venue
// request body of its own. The httptest server behind the client has no routes, so a method
// that still issued its own request would fail with a transport error instead of returning
// the fake's values.
func TestClientFacadeDelegates(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, nil)

	rate := 0.123
	amount := 777.0
	fake := &fakeDriver{
		capabilities: market.Capabilities{market.CapabilityMarkets: {Present: true}},
		candles:      []candle.Candle{{Timestamp: 1111111111, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 3}},
		cvdOK:        true,
		funding:      derivatives.FundingRate{Rate: &rate},
		fundingHist:  []derivatives.FundingRateHistoryPoint{{Rate: &rate}},
		openInterest: derivatives.OpenInterest{Amount: &amount},
		oiHistory:    []derivatives.OpenInterestHistoryPoint{{Amount: &amount}},
		lsr:          []derivatives.LongShortRatioPoint{{Ratio: 2.5}},
		markets:      map[string]market.MarketInfo{"BTC/USDT": {}},
	}
	client.driver = fake

	ctx := context.Background()

	candles, err := client.FetchOHLCV(ctx, "BTC/USDT", "5m", 0, 1)
	require.NoError(t, err)
	require.Equal(t, fake.candles, candles)

	cvd, ok, err := client.FetchKlinesWithCVD(ctx, "BTC/USDT", "5m", 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, fake.candles, cvd)

	funding, err := client.FundingRate(ctx, "BTC/USDT")
	require.NoError(t, err)
	require.InDelta(t, rate, *funding.Rate, 1e-9)

	fundingHistory, err := client.FundingRateHistory(ctx, "BTC/USDT", 1)
	require.NoError(t, err)
	require.Equal(t, fake.fundingHist, fundingHistory)

	reading, err := client.OpenInterest(ctx, "BTC/USDT")
	require.NoError(t, err)
	require.InDelta(t, amount, *reading.Amount, 1e-9)

	oiHistory, err := client.OpenInterestHistory(ctx, "BTC/USDT", "5m", 0, 0, 1)
	require.NoError(t, err)
	require.Equal(t, fake.oiHistory, oiHistory)

	points, err := client.LongShortRatioHistory(ctx, "BTC/USDT", "5m", 1, 0, 0)
	require.NoError(t, err)
	require.Equal(t, fake.lsr, points)

	markets, err := client.LoadMarkets(ctx)
	require.NoError(t, err)
	require.Equal(t, fake.markets, markets)

	require.False(t, fake.closed)
	require.Empty(t, client.Close())
	require.True(t, fake.closed, "Close must reach the resolved driver")
}
