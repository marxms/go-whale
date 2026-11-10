package venue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/market"
)

// TestCapabilityAbsenceIsTypedAndCounted pins CAP-02/CAP-03/CAP-06: asking for a
// capability the scope does not provide returns a typed error and records the metric. It
// must never look like a successful empty answer.
func TestCapabilityAbsenceIsTypedAndCounted(t *testing.T) {
	type sample struct{ exchange, marketType, capability string }

	var got []sample
	client := newTestClient(t, market.BingX, market.Future, nil)
	client.SetMetrics(Metrics{CapabilityAbsent: func(exchange, marketType, capability string) {
		got = append(got, sample{exchange, marketType, capability})
	}})

	candles, ok, err := client.FetchKlinesWithCVD(context.Background(), "BTC/USDT", "5m", 1)
	require.False(t, ok)
	require.Nil(t, candles)
	var absent *CapabilityAbsentError
	require.ErrorAs(t, err, &absent)
	require.Equal(t, market.CapabilityCVD, absent.Capability)
	require.Equal(t, market.BingX, absent.Exchange)
	require.Equal(t, market.Future, absent.MarketType)
	require.ErrorIs(t, err, exchange.ErrPermanent,
		"an absence must stay a permanent failure for the retry and circuit policy")

	_, err = client.LongShortRatioHistory(context.Background(), "BTC/USDT", "5m", 10, 0, 0)
	require.ErrorAs(t, err, &absent)
	require.Equal(t, market.CapabilityLongShortRatio, absent.Capability)

	require.Equal(t, []sample{
		{"bingx", "future", "cvd"},
		{"bingx", "future", "long_short_ratio"},
	}, got)

	spot := newTestClient(t, market.Binance, market.Spot, nil)
	_, err = spot.FundingRate(context.Background(), "BTC/USDT")
	require.ErrorAs(t, err, &absent)
	require.Equal(t, market.CapabilityFunding, absent.Capability)
	require.Equal(t, market.Spot, absent.MarketType)
}
