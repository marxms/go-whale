//go:build smoke

package venue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/market"
)

// TestSmokeVenue exercises the adapter against the live public endpoints. It is
// behind the `smoke` build tag so the normal suite never touches the network.
//
//	go test -tags=smoke -run TestSmokeVenue -v ./internal/adapters/outbound/venue
func TestSmokeVenue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cases := []struct {
		exchange   market.Exchange
		marketType market.MarketType
	}{
		{market.Binance, market.Future},
		{market.Binance, market.Spot},
		{market.BingX, market.Future},
		{market.BingX, market.Spot},
	}

	for _, tc := range cases {
		t.Run(tc.exchange.String()+"/"+tc.marketType.String(), func(t *testing.T) {
			client, err := NewClient(tc.exchange.String(), tc.marketType)
			if err != nil {
				t.Fatalf("new client: %v", err)
			}
			defer func() { _ = client.Close() }()

			markets, err := client.LoadMarkets(ctx)
			if err != nil {
				t.Fatalf("load markets: %v", err)
			}
			if len(markets) == 0 {
				t.Fatal("load markets returned an empty catalog")
			}
			t.Logf("markets: %d", len(markets))

			candles, err := client.FetchOHLCV(ctx, "BTC/USDT", "5m", 0, 5)
			if err != nil {
				t.Fatalf("fetch OHLCV: %v", err)
			}
			if len(candles) == 0 {
				t.Fatal("fetch OHLCV returned no candles")
			}
			last := candles[len(candles)-1]
			if last.Timestamp <= 0 || last.Close <= 0 || last.Volume < 0 {
				t.Fatalf("candle shape is unusable: %+v", last)
			}
			t.Logf("last candle: ts=%d close=%.4f volume=%.4f", last.Timestamp, last.Close, last.Volume)

			discovered, err := Discovery{}.DiscoverSymbols(ctx, tc.exchange, tc.marketType, 5)
			if err != nil {
				t.Fatalf("discover symbols: %v", err)
			}
			if len(discovered) != 5 {
				t.Fatalf("discovery returned %d symbols, want 5: %v", len(discovered), discovered)
			}
			t.Logf("discovered: %v", discovered)

			if tc.marketType == market.Spot {
				_, err := client.FundingRate(ctx, "BTC/USDT")
				if !errors.Is(err, exchange.ErrPermanent) {
					t.Fatalf("spot funding rate = %v, want ErrPermanent", err)
				}
				return
			}

			rate, err := client.FundingRate(ctx, "BTC/USDT")
			if err != nil {
				t.Fatalf("funding rate: %v", err)
			}
			if rate.Rate == nil {
				t.Fatal("funding rate returned no rate")
			}
			t.Logf("funding: rate=%.8f mark=%v", *rate.Rate, floatValue(rate.MarkPrice))

			history, err := client.FundingRateHistory(ctx, "BTC/USDT", 5)
			if err != nil {
				t.Fatalf("funding history: %v", err)
			}
			if len(history) == 0 {
				t.Fatal("funding history returned no rows")
			}
			t.Logf("funding history: %d rows, last ts=%v rate=%v",
				len(history), intValue(history[len(history)-1].Timestamp), floatValue(history[len(history)-1].Rate))

			oi, err := client.OpenInterest(ctx, "BTC/USDT")
			if err != nil {
				t.Fatalf("open interest: %v", err)
			}
			t.Logf("open interest: amount=%v ts=%v", floatValue(oi.Amount), intValue(oi.Timestamp))

			if tc.exchange == market.Binance {
				oiHistory, err := client.OpenInterestHistory(ctx, "BTC/USDT", "5m", 0, 0, 5)
				if err != nil {
					t.Fatalf("open interest history: %v", err)
				}
				if len(oiHistory) == 0 {
					t.Fatal("open interest history returned no rows")
				}
				t.Logf("OI history: %d rows, last amount=%v",
					len(oiHistory), floatValue(oiHistory[len(oiHistory)-1].Amount))

				lsr, err := client.LongShortRatioHistory(ctx, "BTC/USDT", "5m", 5, 0, 0)
				if err != nil {
					t.Fatalf("long/short ratio: %v", err)
				}
				if len(lsr) == 0 {
					t.Fatal("long/short ratio returned no rows")
				}
				t.Logf("LSR: %d rows, last ratio=%.4f shares=%v", len(lsr), lsr[len(lsr)-1].Ratio, lsr[len(lsr)-1].HasShares)
				return
			}

			_, err = client.OpenInterestHistory(ctx, "BTC/USDT", "5m", 0, 0, 5)
			if !errors.Is(err, exchange.ErrPermanent) {
				t.Fatalf("bingx OI history = %v, want ErrPermanent", err)
			}
			_, err = client.LongShortRatioHistory(ctx, "BTC/USDT", "5m", 5, 0, 0)
			if !errors.Is(err, exchange.ErrPermanent) {
				t.Fatalf("bingx LSR = %v, want ErrPermanent", err)
			}
		})
	}
}

// TestSmokeKlinesWithCVD checks the Binance CVD path against the live kline.
func TestSmokeKlinesWithCVD(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	client, err := NewClient(market.Binance.String(), market.Future)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer func() { _ = client.Close() }()

	candles, ok, err := client.FetchKlinesWithCVD(ctx, "BTC/USDT", "5m", 5)
	if err != nil {
		t.Fatalf("fetch klines with CVD: %v", err)
	}
	if !ok {
		t.Fatal("CVD path returned ok=false for Binance")
	}
	if len(candles) == 0 {
		t.Fatal("CVD path returned no candles")
	}
	last := candles[len(candles)-1]
	if last.TakerBuyVolume <= 0 || last.TakerSellVolume < 0 {
		t.Fatalf("CVD fields are unusable: buy=%v sell=%v", last.TakerBuyVolume, last.TakerSellVolume)
	}
	t.Logf("CVD: ts=%d buy=%.4f sell=%.4f volume=%.4f", last.Timestamp, last.TakerBuyVolume, last.TakerSellVolume, last.Volume)
}

func floatValue(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

func intValue(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}
