package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

type durableColdFake struct {
	barMs   int64
	visible atomic.Bool
}

func (f *durableColdFake) WriteBatches(context.Context, map[candle.SeriesKey][]candle.Candle) error {
	return nil
}

func (f *durableColdFake) QueryRange(context.Context, string, string, string, string, time.Time, time.Time, int) ([]candle.Candle, error) {
	if f.visible.Load() {
		return []candle.Candle{{Timestamp: f.barMs}}, nil
	}
	return nil, nil
}

func (f *durableColdFake) GetOldestTimestamp(context.Context, string, string, string, string) (time.Time, error) {
	return time.Time{}, nil
}

func (f *durableColdFake) Close() error { return nil }

func (f *durableColdFake) Stats() candle.ColdStoreStats { return candle.ColdStoreStats{} }

func (f *durableColdFake) CaptureApplyTarget(context.Context) (int64, error) { return 1, nil }

func (f *durableColdFake) WaitApplied(ctx context.Context, _ int64) error {
	for !f.visible.Load() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return nil
}

func TestColdDurableRequiresWriterTxnAtTarget(t *testing.T) {
	fake := &durableColdFake{barMs: 1_700_000_000_000}
	store := &TieredStore{cold: fake}

	bounded, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	durable, err := store.ColdDurable(bounded, "binance", "future", "BTC/USDT", "5m", fake.barMs)
	if durable {
		t.Fatal("a bar below the WAL target must not be durable")
	}
	if err == nil {
		t.Fatal("the barrier timeout must surface instead of becoming a silent false")
	}

	fake.visible.Store(true)
	durable, err = store.ColdDurable(context.Background(), "binance", "future", "BTC/USDT", "5m", fake.barMs)
	if err != nil || !durable {
		t.Fatalf("after the apply: durable=%v err=%v, want true/nil", durable, err)
	}
}
