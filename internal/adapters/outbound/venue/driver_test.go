package venue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/market"
)

// fakeDriver is an implementation outside the Binance and BingX drivers. It proves the
// interface is satisfiable, and it is the stand-in a ported venue uses in tests.
type fakeDriver struct {
	capabilities market.Capabilities
	candles      []candle.Candle
	cvdOK        bool
	funding      derivatives.FundingRate
	fundingHist  []derivatives.FundingRateHistoryPoint
	openInterest derivatives.OpenInterest
	oiHistory    []derivatives.OpenInterestHistoryPoint
	lsr          []derivatives.LongShortRatioPoint
	markets      map[string]market.MarketInfo
	closed       bool
}

var _ driver = (*fakeDriver)(nil)

func (f *fakeDriver) FetchOHLCV(context.Context, string, string, int64, int64) ([]candle.Candle, error) {
	return f.candles, nil
}

func (f *fakeDriver) FetchKlinesWithCVD(context.Context, string, string, int) ([]candle.Candle, bool, error) {
	return f.candles, f.cvdOK, nil
}

func (f *fakeDriver) FundingRate(context.Context, string) (derivatives.FundingRate, error) {
	return f.funding, nil
}

func (f *fakeDriver) FundingRateHistory(context.Context, string, int64) ([]derivatives.FundingRateHistoryPoint, error) {
	return f.fundingHist, nil
}

func (f *fakeDriver) OpenInterest(context.Context, string) (derivatives.OpenInterest, error) {
	return f.openInterest, nil
}

func (f *fakeDriver) OpenInterestHistory(context.Context, string, string, int64, int64, int64) ([]derivatives.OpenInterestHistoryPoint, error) {
	return f.oiHistory, nil
}

func (f *fakeDriver) LongShortRatioHistory(context.Context, string, string, int64, int64, int64) ([]derivatives.LongShortRatioPoint, error) {
	return f.lsr, nil
}

func (f *fakeDriver) LoadMarkets(context.Context) (map[string]market.MarketInfo, error) {
	return f.markets, nil
}

func (f *fakeDriver) Capabilities() market.Capabilities { return f.capabilities }

func (f *fakeDriver) Close() []error {
	f.closed = true
	return nil
}

// TestDriverInterfaceContract pins PRT-01: one interface carries every request the façade
// exposes, and a provider outside the two venue implementations can satisfy it.
func TestDriverInterfaceContract(t *testing.T) {
	caps := market.Capabilities{
		market.CapabilityMarkets: {Present: true},
		market.CapabilityCVD:     {Reason: "not published"},
	}
	rate := 0.0001
	amount := 12345.5
	fake := &fakeDriver{
		capabilities: caps,
		candles:      []candle.Candle{{Timestamp: 1700000000000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 12}},
		cvdOK:        true,
		funding:      derivatives.FundingRate{Rate: &rate},
		fundingHist:  []derivatives.FundingRateHistoryPoint{{Rate: &rate}},
		openInterest: derivatives.OpenInterest{Amount: &amount},
		oiHistory:    []derivatives.OpenInterestHistoryPoint{{Amount: &amount}},
		lsr:          []derivatives.LongShortRatioPoint{{Ratio: 1.5}},
		markets:      map[string]market.MarketInfo{"BTC/USDT": {}},
	}

	var contract driver = fake
	ctx := context.Background()

	candles, err := contract.FetchOHLCV(ctx, "BTC/USDT", "5m", 0, 1)
	require.NoError(t, err)
	require.Equal(t, fake.candles, candles)

	cvd, ok, err := contract.FetchKlinesWithCVD(ctx, "BTC/USDT", "5m", 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, fake.candles, cvd)

	funding, err := contract.FundingRate(ctx, "BTC/USDT")
	require.NoError(t, err)
	require.InDelta(t, 0.0001, *funding.Rate, 1e-9)

	history, err := contract.FundingRateHistory(ctx, "BTC/USDT", 1)
	require.NoError(t, err)
	require.Len(t, history, 1)

	reading, err := contract.OpenInterest(ctx, "BTC/USDT")
	require.NoError(t, err)
	require.InDelta(t, 12345.5, *reading.Amount, 1e-9)

	oiHistory, err := contract.OpenInterestHistory(ctx, "BTC/USDT", "5m", 0, 0, 1)
	require.NoError(t, err)
	require.Len(t, oiHistory, 1)

	points, err := contract.LongShortRatioHistory(ctx, "BTC/USDT", "5m", 1, 0, 0)
	require.NoError(t, err)
	require.Len(t, points, 1)

	markets, err := contract.LoadMarkets(ctx)
	require.NoError(t, err)
	require.Contains(t, markets, "BTC/USDT")

	require.Equal(t, caps, contract.Capabilities())
	require.True(t, contract.Capabilities().Supports(market.CapabilityMarkets))
	require.Equal(t, "not published", contract.Capabilities().AbsenceReason(market.CapabilityCVD))

	require.Empty(t, contract.Close())
	require.True(t, fake.closed)
}
