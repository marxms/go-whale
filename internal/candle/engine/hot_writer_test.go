package engine

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	logger "github.com/marxms/go-whale/internal/platform/logging"
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
	writer := newHotWriter(
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
	log, sink := logger.NewCapture(logger.LevelWarn)
	writer := newHotWriter(
		func(_ context.Context, _ []hotWrite) error {
			started <- struct{}{}
			<-release
			return nil
		},
		WithHotWriterCapacity(2),
		WithHotWriterInterval(time.Millisecond),
		WithHotWriterLogger(log),
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
	// O PRIMEIRO drop é nomeado, não só contado: o símbolo e o motivo saem no log.
	level, found := sink.LevelOf("dropped a closed bucket")
	if !found || level != logger.LevelWarn {
		t.Fatalf("first drop log = %s (found=%v), want WARN", level, found)
	}
	if _, found := sink.LevelOf("symbol=C"); !found {
		t.Fatalf("first drop log does not name the dropped symbol: %v", sink.Records())
	}
}

// The reader keeps producing frames while the hot owner is blocked on Redis: the counter and the
// last-frame clock advance, because Enqueue never waits for the flush.
func TestRedisStallDoesNotChangeFramesOrLastFrameAge(t *testing.T) {
	release := make(chan struct{})
	writer := newHotWriter(
		func(ctx context.Context, _ []hotWrite) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		WithHotWriterInterval(time.Millisecond),
	)
	defer func() {
		close(release)
		_ = writer.Stop(context.Background())
	}()

	var frames atomic.Int64
	var lastFrameAt atomic.Int64
	lastFrameAt.Store(time.Now().UnixMilli())
	for i := 0; i < 200; i++ {
		frames.Add(1)
		lastFrameAt.Store(time.Now().UnixMilli())
		if !writer.Enqueue(hotWriterKey(fmt.Sprintf("STALL%03d", i)), candle.Candle{Close: float64(i)}) {
			t.Fatalf("enqueue %d was dropped below the capacity", i)
		}
	}
	if frames.Load() != 200 {
		t.Fatalf("frames = %d, want 200", frames.Load())
	}
	if age := time.Since(time.UnixMilli(lastFrameAt.Load())); age > time.Second {
		t.Fatalf("last frame age = %v; the reader stalled on the flush", age)
	}
}

// Liveness is independent of the queue's age: while the oldest pending entry grows, the last
// frame stays recent. The hot tier is best-effort; Redis latency is not a partition property.
func TestRedisStallKeepsLivenessIndependent(t *testing.T) {
	release := make(chan struct{})
	writer := newHotWriter(
		func(ctx context.Context, _ []hotWrite) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		WithHotWriterInterval(time.Millisecond),
	)
	defer func() {
		close(release)
		_ = writer.Stop(context.Background())
	}()

	var lastFrameAt atomic.Int64
	for i := 0; i < 200; i++ {
		if i == 1 {
			// Separate the oldest pending entry from the newest frame below the 1 ms clock
			// resolution, so the two ages cannot collapse into the same millisecond.
			time.Sleep(2 * time.Millisecond)
		}
		lastFrameAt.Store(time.Now().UnixMilli())
		if !writer.Enqueue(hotWriterKey(fmt.Sprintf("LIVE%03d", i)), candle.Candle{Close: float64(i)}) {
			t.Fatalf("enqueue %d was dropped below the capacity", i)
		}
	}
	time.Sleep(2 * time.Millisecond)
	pendingAge := writer.Stats().OldestPendingAge
	livenessAge := time.Since(time.UnixMilli(lastFrameAt.Load()))
	if pendingAge <= 0 {
		t.Fatalf("pending age = %v; the queue did not retain the in-flight entry", pendingAge)
	}
	if livenessAge >= pendingAge {
		t.Fatalf("liveness age = %v, pending age = %v; the Redis stall reached the reader", livenessAge, pendingAge)
	}
}
