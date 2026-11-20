package engine

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

func hotWriterKey(symbol string) CompositeKey {
	return CompositeKey{Exchange: "binance", MarketType: "future", Symbol: symbol, Timeframe: "5m"}
}

// The owner coalesces the LATEST snapshot per key: the hot write is a total, so replacing the
// pending value is lossless. The first flush is held open so the second/third writes land while
// the owner is busy, which is the production boundary shape.
func TestHotWriterCoalescesLatestPerKey(t *testing.T) {
	flushed := make(chan []hotWrite, 4)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	writer := NewHotWriter(
		func(ctx context.Context, entries []hotWrite) error {
			flushed <- entries
			started <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		WithHotWriterInterval(time.Hour),
	)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		_ = writer.Stop(context.Background())
	}()

	keyA, keyB := hotWriterKey("A"), hotWriterKey("B")
	if !writer.Enqueue(keyA, candle.Candle{Close: 1}) {
		t.Fatal("the first A must be admitted")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the owner did not start the first flush")
	}
	if !writer.Enqueue(keyA, candle.Candle{Close: 2}) {
		t.Fatal("a newer A must be admitted")
	}
	if !writer.Enqueue(keyA, candle.Candle{Close: 3}) {
		t.Fatal("a coalesced A must be admitted")
	}
	if !writer.Enqueue(keyB, candle.Candle{Close: 4}) {
		t.Fatal("B must be admitted")
	}
	close(release)

	latest := map[string]float64{}
	deadline := time.After(2 * time.Second)
	for len(latest) < 2 || latest["A"] != 3 || latest["B"] != 4 {
		select {
		case batch := <-flushed:
			for _, entry := range batch {
				latest[entry.key.Symbol] = entry.candle.Close
			}
		case <-deadline:
			t.Fatalf("flushes seen = %v, want A=3 and B=4", latest)
		}
	}
	stats := writer.Stats()
	if stats.Coalesced != 1 {
		t.Fatalf("coalesced = %d, want 1", stats.Coalesced)
	}
	if stats.Dropped != 0 {
		t.Fatalf("dropped = %d, want 0", stats.Dropped)
	}
}

// A new key beyond the bounded capacity is dropped, counted, and reported with the age of the
// oldest accepted entry; an existing key would have been coalesced instead.
func TestHotWriterRecordsDropAndOldestPending(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	writer := NewHotWriter(
		func(_ context.Context, _ []hotWrite) error {
			started <- struct{}{}
			<-release
			return nil
		},
		WithHotWriterCapacity(2),
		WithHotWriterInterval(time.Millisecond),
	)
	defer func() {
		close(release)
		_ = writer.Stop(context.Background())
	}()

	if !writer.Enqueue(hotWriterKey("A"), candle.Candle{Close: 1}) {
		t.Fatal("A must be admitted")
	}
	if !writer.Enqueue(hotWriterKey("B"), candle.Candle{Close: 2}) {
		t.Fatal("B must be admitted")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the owner did not start the flush")
	}
	time.Sleep(2 * time.Millisecond)
	if writer.Enqueue(hotWriterKey("C"), candle.Candle{Close: 3}) {
		t.Fatal("a NEW key beyond the capacity must be dropped")
	}

	stats := writer.Stats()
	if stats.Dropped != 1 {
		t.Fatalf("dropped = %d, want 1", stats.Dropped)
	}
	if stats.Depth != 2 {
		t.Fatalf("depth = %d, want 2 (in flight)", stats.Depth)
	}
	if stats.OldestPendingAge <= 0 {
		t.Fatalf("oldest pending age = %v, want > 0", stats.OldestPendingAge)
	}
}
