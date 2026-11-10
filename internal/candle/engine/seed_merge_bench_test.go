package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// Production shape, so the numbers here mean something operationally:
// candle.buffer_sizes.default is 800 in config.yaml, and the seeder asks the
// exchange for 500 bars (the weight-2 tier). A boot therefore folds a 500-bar batch into an
// 800-slot ring, 4,000 times (500 symbols x 8 timeframes).
const (
	benchDepth = 800
	benchBatch = 500
)

// benchSeedFixture builds a buffer holding `depth` contiguous closed bars and the repair
// batch that overlaps its newest `benchBatch` buckets, with `holes` of those buckets ABSENT
// from the buffer — the exact shape a restart leaves behind when the live stream never
// delivered an interior bucket.
//
// Holes are spread evenly rather than clustered: a clustered hole is the cheap case for an
// insert-at-position implementation (later inserts land near the end), and benchmarking the
// cheap case would understate the cost this measurement exists to expose.
func benchSeedFixture(tb testing.TB, holes int) (*CandleEngine, CompositeKey, []candle.Candle) {
	tb.Helper()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "BTC/USDT"
		tf       = "5m"
		step     = int64(5 * 60 * 1000)
	)
	base := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC).UnixMilli()

	missing := make(map[int64]bool, holes)
	if holes > 0 {
		// Spread across the batch's span, which is the newest benchBatch buckets.
		firstBatchIdx := benchDepth - benchBatch
		for h := range holes {
			idx := firstBatchIdx + (h*benchBatch)/holes
			missing[base+int64(idx)*step] = true
		}
	}

	seeded := make([]candle.Candle, 0, benchDepth)
	for i := range benchDepth {
		ts := base + int64(i)*step
		if missing[ts] {
			continue
		}
		seeded = append(seeded, benchCandle(ts, float64(i)))
	}

	ce := NewCandleEngine(map[string]int{"default": benchDepth})
	ce.SeedCandles(exchange, market, symbol, tf, seeded)

	batch := make([]candle.Candle, 0, benchBatch)
	for i := benchDepth - benchBatch; i < benchDepth; i++ {
		batch = append(batch, benchCandle(base+int64(i)*step, float64(i)))
	}

	return ce, CompositeKey{exchange, market, symbol, tf}, batch
}

func benchCandle(ts int64, seq float64) candle.Candle {
	return candle.Candle{
		Timestamp:       ts,
		Open:            100 + seq,
		High:            101 + seq,
		Low:             99 + seq,
		Close:           100.5 + seq,
		Volume:          10 + seq,
		TakerBuyVolume:  6 + seq,
		TakerSellVolume: 4,
		Source:          string(sourceRESTTotal),
	}
}

// BenchmarkMergeSeedIntoBuffer measures the SHIPPED fold as a function of how many buckets
// the buffer is MISSING. Compare it against BenchmarkFoldBatchPerCandle below, which
// measures the rejected alternative on the identical fixture.
//
// What matters here is the SLOPE, not the absolute number. This implementation is a
// merge-join over two sorted sequences, so its cost is O(n+m) and should stay flat as holes
// grow; the per-candle fold rebuilds the ring once per inserted bucket, so its cost is
// O(holes x depth). A flat line here and a rising line there is the whole justification for
// the design — if that ever inverts, these two benchmarks are what will say so.
//
// This runs the fold under no lock at all. In production it runs inside the single global
// RWMutex every reader shares, up to 4,000 times at boot, which is why the slope is a
// liveness property and not just a throughput one.
func BenchmarkMergeSeedIntoBuffer(b *testing.B) {
	for _, holes := range []int{0, 1, 10, 50, 250, 500} {
		b.Run(fmt.Sprintf("holes=%d", holes), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				ce, key, batch := benchSeedFixture(b, holes)
				buf := ce.buffers[key]
				b.StartTimer()

				candle.MergeBatchIntoBuffer(buf, batch, int64(5*60*1000), time.Now().UnixMilli())
			}
		})
	}
}

// BenchmarkFoldBatchPerCandle measures the alternative that was rejected: folding the batch
// through mergeHistorical, the per-candle entry point the live late-trade path uses.
//
// It is kept as a live benchmark rather than described in a comment so the design decision
// stays FALSIFIABLE. mergeHistorical is production code on the live path, so this measures a
// real function, not a museum piece — and if a future change ever makes the per-candle fold
// competitive, this benchmark is what will say so instead of a stale claim in a doc comment.
func BenchmarkFoldBatchPerCandle(b *testing.B) {
	for _, holes := range []int{0, 1, 10, 50, 250, 500} {
		b.Run(fmt.Sprintf("holes=%d", holes), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				ce, key, batch := benchSeedFixture(b, holes)
				buf := ce.buffers[key]
				now := time.Now().UnixMilli()
				b.StartTimer()

				for i := range batch {
					ce.mergeHistorical(key, buf, batch[i], now)
				}
			}
		})
	}
}
