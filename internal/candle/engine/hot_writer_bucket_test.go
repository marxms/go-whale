package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

func hotCandleAt(ts int64, close float64) candle.Candle {
	return candle.Candle{Timestamp: ts, LastUpdate: ts, Close: close, Open: close, High: close, Low: close, Volume: 1}
}

// Five distinct closed buckets of ONE series must all reach the hot tier. Keying by the series
// alone collapsed them into the last one (the lifecycle test failed 2/8 runs on this).
func TestHotWriterKeepsDistinctClosedBuckets(t *testing.T) {
	var mu sync.Mutex
	var written []candle.Candle
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	first := true
	writer := NewHotWriter(
		func(_ context.Context, entries []hotWrite) error {
			if first {
				first = false
				close(started)
				<-release
			}
			mu.Lock()
			for _, e := range entries {
				written = append(written, e.candle)
			}
			mu.Unlock()
			return nil
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

	key := hotWriterKey("A")
	if !writer.Enqueue(key, hotCandleAt(1000, 1)) {
		t.Fatal("bucket 1000 must be admitted")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the owner did not start the first flush")
	}
	for _, ts := range []int64{2000, 3000, 4000} {
		if !writer.Enqueue(key, hotCandleAt(ts, float64(ts))) {
			t.Fatalf("bucket %d must be admitted", ts)
		}
	}
	close(release)

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		got := map[int64]bool{}
		for _, c := range written {
			got[c.Timestamp] = true
		}
		mu.Unlock()
		if len(got) == 4 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("buckets written = %v, want 1000,2000,3000,4000", got)
		case <-time.After(5 * time.Millisecond):
		}
	}
	if stats := writer.Stats(); stats.Coalesced != 0 || stats.Dropped != 0 {
		t.Fatalf("stats = %+v, want no coalesce/drop for distinct bars", stats)
	}
}

// A failed flush is requeued, and a NEWER snapshot of the same bar wins the retry.
func TestHotWriterRequeuesFailedBatchKeepingNewerSnapshot(t *testing.T) {
	var mu sync.Mutex
	written := map[string]float64{}
	fail := true
	attempted := make(chan struct{}, 8)
	var failed int
	writer := NewHotWriter(
		func(_ context.Context, entries []hotWrite) error {
			mu.Lock()
			defer mu.Unlock()
			if fail {
				fail = false
				attempted <- struct{}{}
				return errors.New("redis refused")
			}
			for _, e := range entries {
				written[e.key.Symbol+"@"+time.UnixMilli(e.candle.Timestamp).UTC().String()] = e.candle.Close
			}
			return nil
		},
		WithHotWriterInterval(time.Millisecond),
		WithHotWriterMetrics(Metrics{HotWriterFailed: func() { mu.Lock(); failed++; mu.Unlock() }}),
	)
	defer func() { _ = writer.Stop(context.Background()) }()

	key := hotWriterKey("A")
	writer.Enqueue(key, hotCandleAt(1000, 1))
	select {
	case <-attempted:
	case <-time.After(time.Second):
		t.Fatal("the failing flush did not run")
	}
	// A newer snapshot of the SAME bar arrives before the retry: the retry must write 2, not 1.
	writer.Enqueue(key, hotCandleAt(1000, 2))

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		v, ok := written["A@"+time.UnixMilli(1000).UTC().String()]
		mu.Unlock()
		if ok {
			if v != 2 {
				t.Fatalf("retried value = %v, want the newer 2", v)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("the failed batch was never requeued and retried")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if stats := writer.Stats(); stats.Failed != 1 {
		t.Fatalf("failed = %d, want 1", stats.Failed)
	}
	mu.Lock()
	exported := failed
	mu.Unlock()
	if exported != 1 {
		t.Fatalf("exported failures = %d, want 1", exported)
	}
}

// A sustained drop/coalesce must be visible in the metrics, not only in the first log line.
func TestHotWriterExportsCountersAndDepth(t *testing.T) {
	var mu sync.Mutex
	var dropped, coalesced int
	depth := -1.0
	oldest := -1.0
	started := make(chan struct{})
	release := make(chan struct{})
	writer := NewHotWriter(
		func(context.Context, []hotWrite) error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return nil
		},
		WithHotWriterCapacity(2),
		WithHotWriterInterval(time.Hour),
		WithHotWriterMetrics(Metrics{
			HotWriterDropped:       func() { mu.Lock(); dropped++; mu.Unlock() },
			HotWriterCoalesced:     func() { mu.Lock(); coalesced++; mu.Unlock() },
			HotWriterDepth:         func(v float64) { mu.Lock(); depth = v; mu.Unlock() },
			HotWriterOldestPending: func(s float64) { mu.Lock(); oldest = s; mu.Unlock() },
		}),
	)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		_ = writer.Stop(context.Background())
	}()

	key := hotWriterKey("A")
	if !writer.Enqueue(key, hotCandleAt(1000, 1)) {
		t.Fatal("A must be admitted")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the flush did not start")
	}
	if !writer.Enqueue(key, hotCandleAt(1000, 2)) { // same bar: a newer snapshot pending
		t.Fatal("the newer A must be admitted")
	}
	if !writer.Enqueue(key, hotCandleAt(1000, 3)) { // the pending entry is COALESCED
		t.Fatal("the coalesced A must be admitted")
	}
	if writer.Enqueue(hotWriterKey("B"), hotCandleAt(1000, 4)) { // in flight 1 + pending 1 = capacity 2
		t.Fatal("B beyond the capacity must be dropped")
	}
	mu.Lock()
	gotDropped, gotCoalesced, gotDepth, gotOldest := dropped, coalesced, depth, oldest
	mu.Unlock()
	if gotDropped != 1 {
		t.Fatalf("dropped = %d, want 1", gotDropped)
	}
	if gotCoalesced != 1 {
		t.Fatalf("coalesced = %d, want 1", gotCoalesced)
	}
	if gotDepth < 1 || gotOldest < 0 {
		t.Fatalf("depth/oldest not exported: depth=%v oldest=%v", gotDepth, gotOldest)
	}
}
