package venue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/recovery"
)

// TestDriverRegistryResolvesScopes pins PRT-01: the production registry answers each of the
// four scopes with the driver that owns it, and the driver declares the scope's matrix row.
func TestDriverRegistryResolvesScopes(t *testing.T) {
	scopes := []struct {
		name       string
		exchange   market.Exchange
		marketType market.MarketType
		want       driver
	}{
		{name: "binance spot", exchange: market.Binance, marketType: market.Spot, want: &binanceDriver{}},
		{name: "binance future", exchange: market.Binance, marketType: market.Future, want: &binanceDriver{}},
		{name: "bingx spot", exchange: market.BingX, marketType: market.Spot, want: &bingXDriver{}},
		{name: "bingx swap", exchange: market.BingX, marketType: market.Future, want: &bingXDriver{}},
	}

	for _, scope := range scopes {
		scope := scope
		t.Run(scope.name, func(t *testing.T) {
			client := newTestClient(t, scope.exchange, scope.marketType, nil)
			require.NotNil(t, client.driver, "every supported scope must resolve a driver")
			require.IsType(t, scope.want, client.driver)
			require.Equal(t,
				market.CapabilitiesFor(scope.exchange.String(), scope.marketType.String()),
				client.driver.Capabilities())
		})
	}
}

// TestDriverRegistryRejectsUnknown pins PRT-03: a scope with no registered driver fails
// with a typed permanent error, never with a silent fallback to another venue.
func TestDriverRegistryRejectsUnknown(t *testing.T) {
	registry := driverRegistry{}

	_, err := registry.resolve(venueScope{exchange: market.Exchange("kraken"), marketType: market.Spot}, nil)
	var unknown *UnknownVenueScopeError
	require.ErrorAs(t, err, &unknown)
	require.Equal(t, market.Exchange("kraken"), unknown.Exchange)
	require.Equal(t, market.Spot, unknown.MarketType)
	require.ErrorIs(t, err, exchange.ErrPermanent)

	_, err = registry.resolve(venueScope{exchange: market.Binance, marketType: market.MarketType("quarterly")}, nil)
	require.ErrorAs(t, err, &unknown)
	require.Equal(t, market.Binance, unknown.Exchange)
	require.Equal(t, market.MarketType("quarterly"), unknown.MarketType)
}

// TestRegistryAcceptsFakeDriverWithoutRecoveryChange pins CAP-05/PRT-04: a new venue is one
// registry row plus its driver, and the driver answers through the same port the recovery,
// health and seed paths consume. This test imports no recovery code on purpose.
func TestRegistryAcceptsFakeDriverWithoutRecoveryChange(t *testing.T) {
	fakeScope := venueScope{exchange: market.Exchange("fake"), marketType: market.Future}
	registry := driverRegistry{
		fakeScope: func(*Client) driver {
			return &fakeDriver{
				capabilities: market.Capabilities{market.CapabilityMarkets: {Present: true}},
				candles:      []candle.Candle{{Timestamp: 1, Open: 1, High: 1, Low: 1, Close: 1}},
			}
		},
	}

	port, err := registry.resolve(fakeScope, nil)
	require.NoError(t, err)
	require.IsType(t, &fakeDriver{}, port)

	candles, err := port.FetchOHLCV(context.Background(), "BTC/USDT", "5m", 0, 1)
	require.NoError(t, err)
	require.Len(t, candles, 1)
	require.True(t, port.Capabilities().Supports(market.CapabilityMarkets))
	require.Empty(t, port.Close())
}

// TestFakeVenueRunsRecoveryFiller exercises a newly registered venue through the recovery
// fetch layer: the manager talks to the HistorySource port, the façade resolves the driver,
// and the filler receives the fake venue's candles. Health and seed consume the same port,
// so a new venue does not get a private recovery path.
func TestFakeVenueRunsRecoveryFiller(t *testing.T) {
	client := newTestClient(t, market.Binance, market.Future, nil)
	const bucket = int64(1_790_000_100_000)
	client.driver = &fakeDriver{candles: []candle.Candle{{
		Timestamp: bucket, Open: 10, High: 11, Low: 9, Close: 10.5, Volume: 1,
	}}}

	result, err := recovery.NewBasicOHLCVFiller().Fill(context.Background(), client, recovery.FillRequest{
		ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m",
		SinceMs: 0, FetchLimit: 1, GapStart: bucket, CurrentBucket: bucket + 300_000,
	})
	require.NoError(t, err)
	require.Len(t, result.Historical, 1)
	require.Equal(t, bucket, result.Historical[0].Timestamp)
}
