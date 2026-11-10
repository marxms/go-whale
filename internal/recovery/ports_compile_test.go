package recovery

import (
	"context"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// minimalSeedStore implements ONLY the two methods MultiExchangeStore declares. A store that
// cannot split the seed batch is the documented fallback, so this fake must be accepted.
type minimalSeedStore struct{}

func (minimalSeedStore) AddStreamingCandle(context.Context, string, string, string, string, candle.Candle) error {
	return nil
}

func (minimalSeedStore) SeedCandlesBatch(context.Context, string, string, string, string, []candle.Candle) error {
	return nil
}

var _ MultiExchangeStore = minimalSeedStore{}

// minimalSplitSeedStore adds exactly the two splitter methods; together they prove the
// seeder's optional three-step path needs nothing beyond candle.SeedSplitter.
type minimalSplitSeedStore struct{}

func (minimalSplitSeedStore) AddStreamingCandle(context.Context, string, string, string, string, candle.Candle) error {
	return nil
}

func (minimalSplitSeedStore) SeedCandlesBatch(context.Context, string, string, string, string, []candle.Candle) error {
	return nil
}

func (minimalSplitSeedStore) FoldSeedBatch(string, string, string, string, []candle.Candle) []candle.Candle {
	return nil
}

func (minimalSplitSeedStore) PersistSeedBatch(context.Context, string, string, string, string, []candle.Candle, []candle.Candle) error {
	return nil
}

var (
	_ MultiExchangeStore  = minimalSplitSeedStore{}
	_ candle.SeedSplitter = minimalSplitSeedStore{}
)

func TestMinimalStoreSatisfiesTheSeeder(t *testing.T) {
	if err := applySeedWindow(context.Background(), Metrics{}, minimalSeedStore{}, "binance", "future", "BTC/USDT", "5m", &seedWindow{}); err != nil {
		t.Fatalf("the seeder rejected the minimal store: %v", err)
	}
	if err := applySeedWindow(context.Background(), Metrics{}, minimalSplitSeedStore{}, "binance", "future", "BTC/USDT", "5m", &seedWindow{}); err != nil {
		t.Fatalf("the seeder rejected the minimal splitter: %v", err)
	}
}
