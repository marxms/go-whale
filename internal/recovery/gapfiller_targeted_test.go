package recovery

import (
	"context"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

type targetOHLCVClient struct {
	items []candle.Candle
}

func (c targetOHLCVClient) FetchOHLCV(context.Context, string, string, int64, int64) ([]candle.Candle, error) {
	return c.items, nil
}

func (c targetOHLCVClient) FetchKlinesWithCVD(context.Context, string, string, int) ([]candle.Candle, bool, error) {
	return nil, false, nil
}

func (c targetOHLCVClient) Close() []error { return nil }

func TestBasicFiller_TargetedResultExcludesLaterBuckets(t *testing.T) {
	const target = int64(1_700_000_100_000)
	const next = target + 60_000
	client := targetOHLCVClient{items: []candle.Candle{
		{Timestamp: target, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1},
		{Timestamp: next, Open: 2, High: 2, Low: 2, Close: 2, Volume: 2},
	}}
	f := NewBasicOHLCVFiller()
	res, err := f.Fill(context.Background(), client, FillRequest{
		ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT",
		Timeframe: "1m", SinceMs: target, GapStart: target, CurrentBucket: next + 60_000,
		UntilMs: target, FetchLimit: 2,
	})
	if err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if res.Current != nil || len(res.Historical) != 1 || res.Historical[0].Timestamp != target {
		t.Errorf("targeted result = %+v, want only the named bucket", res)
	}
	if res.TakerVolumesAuthoritative {
		t.Error("Basic OHLCV has no measured taker pair")
	}

	bulk, err := f.Fill(context.Background(), client, FillRequest{
		ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT",
		Timeframe: "1m", SinceMs: target, GapStart: target, CurrentBucket: next,
		FetchLimit: 2,
	})
	if err != nil {
		t.Fatalf("bulk Fill: %v", err)
	}
	if len(bulk.Historical) != 1 || bulk.Current == nil || bulk.Current.Timestamp != next {
		t.Errorf("bulk result = %+v, want historical target and real open bucket", bulk)
	}
}
