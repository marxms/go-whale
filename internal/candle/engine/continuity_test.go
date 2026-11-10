package engine

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

type targetedRepair struct {
	symbol string
	bucket int64
}

type batchRepair struct {
	exchangeID, marketType string
	symbols                []string
	since, until           time.Time
}

type recordingRepairer struct {
	targeted []targetedRepair
	batches  []batchRepair
}

func (r *recordingRepairer) EnqueueBatch(exchangeID, marketType string, symbols []string, since, until time.Time) {
	r.batches = append(r.batches, batchRepair{exchangeID, marketType, symbols, since, until})
}

func (r *recordingRepairer) EnqueueTargetedBucket(_, _, symbol, _ string, bucketOpenTime int64) {
	r.targeted = append(r.targeted, targetedRepair{symbol, bucketOpenTime})
}

type countingRepairMetrics struct {
	bootSettle   int
	gappedRepair int
}

func (m *countingRepairMetrics) IncBootBucketSettle(_, _, _ string) { m.bootSettle++ }
func (m *countingRepairMetrics) IncGappedBucketRepair(_, _, _ string) {
	m.gappedRepair++
}

func TestBootBucket_IsSettledByTheExchangeWhenItCloses(t *testing.T) {
	t.Parallel()

	const step = int64(5 * 60 * 1000)
	key := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "DOGE/USDT", Timeframe: "5m"}
	base := int64(1_700_000_000_000) / step * step

	// The process started INSIDE bucket `base`, which is therefore the boot bucket.
	tracker := NewContinuityTracker(time.UnixMilli(base+90_000), nil)
	orch := &recordingRepairer{}

	// A close for the bucket BEFORE the boot bucket — the seed's ordering produces it first.
	tracker.Observe(orch, key, candle.Candle{Timestamp: base - step})
	if len(orch.targeted) != 0 {
		t.Fatalf("the bucket BEFORE the boot bucket was settled (%+v)", orch.targeted)
	}

	tracker.Observe(orch, key, candle.Candle{Timestamp: base})
	if len(orch.targeted) != 1 {
		t.Fatalf("enqueued %d settle requests for the boot bucket, want exactly 1", len(orch.targeted))
	}
	got := orch.targeted[0]
	if got.symbol != key.Symbol || got.bucket != base {
		t.Errorf("settled %q bucket %d, want %q bucket %d", got.symbol, got.bucket, key.Symbol, base)
	}
}

func TestBootBucket_SettlesExactlyOncePerKey(t *testing.T) {
	t.Parallel()

	const step = int64(5 * 60 * 1000)
	key := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}
	base := int64(1_700_000_000_000) / step * step

	tracker := NewContinuityTracker(time.UnixMilli(base+90_000), nil)
	orch := &recordingRepairer{}

	for i := range 6 {
		tracker.Observe(orch, key, candle.Candle{Timestamp: base + int64(i)*step})
	}

	if len(orch.targeted) != 1 {
		t.Errorf("enqueued %d times, want exactly 1", len(orch.targeted))
	}
}

func TestBootBucket_GapDetectionStillWorks(t *testing.T) {
	t.Parallel()

	const step = int64(5 * 60 * 1000)
	key := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "ETH/USDT", Timeframe: "5m"}
	base := int64(1_700_000_000_000) / step * step

	tracker := NewContinuityTracker(time.UnixMilli(base+90_000), nil)
	orch := &recordingRepairer{}

	tracker.Observe(orch, key, candle.Candle{Timestamp: base})          // boot settle
	tracker.Observe(orch, key, candle.Candle{Timestamp: base + 4*step}) // 3-bucket gap

	if len(orch.targeted) != 1 || len(orch.batches) != 1 {
		t.Fatalf("targeted=%d batch=%d, want 1 settle + 1 gap", len(orch.targeted), len(orch.batches))
	}
	gap := orch.batches[0]
	if gap.since.UnixMilli() != base+step || gap.until.UnixMilli() != base+4*step {
		t.Errorf("gap window [%d, %d], want [%d, %d]",
			gap.since.UnixMilli(), gap.until.UnixMilli(), base+step, base+4*step)
	}
}

func TestBootBucket_NoEnqueuerIsSafe(t *testing.T) {
	t.Parallel()

	tracker := NewContinuityTracker(time.Now(), nil)
	tracker.Observe(nil, CompositeKey{
		Exchange: "binance", MarketType: "future", Symbol: "SOL/USDT", Timeframe: "5m",
	}, candle.Candle{Timestamp: 1_700_000_000_000})
}

func TestGappedBucket_ClosesAskingForAnAuthoritativeReRead(t *testing.T) {
	t.Parallel()

	orch := &recordingRepairer{}
	tracker := NewContinuityTracker(time.Time{}, nil)
	key := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "XRP/USDT", Timeframe: "5m"}

	closed := candle.Candle{
		Timestamp:       1_785_728_700_000, // 03:45 UTC, the incident bucket
		Close:           1,
		Volume:          5_614_460.40,
		TakerBuyVolume:  1_906_756.60,
		TakerSellVolume: 3_707_703.80,
		HasGap:          true,
	}
	tracker.Observe(orch, key, closed)

	if len(orch.targeted) != 1 || orch.targeted[0].bucket != closed.Timestamp {
		t.Fatalf("targeted repairs %+v, want exactly the gapped bucket %d", orch.targeted, closed.Timestamp)
	}
}

func TestCompleteBucket_AsksForNothing(t *testing.T) {
	t.Parallel()

	orch := &recordingRepairer{}
	tracker := NewContinuityTracker(time.Time{}, nil)
	key := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}

	tracker.Observe(orch, key, candle.Candle{Timestamp: 1_785_728_700_000, Close: 1, Volume: 10})

	if len(orch.targeted) != 0 {
		t.Fatalf("a complete bucket enqueued %d repairs", len(orch.targeted))
	}
}

// TestOpenTimeContinuity_EnqueuesMissingRange verifies a TARGETED backfill only when
// a closed candle's open-time is not contiguous with the previously-seen close for
// the same key, and only for the missing range and that single symbol.
func TestOpenTimeContinuity_EnqueuesMissingRange(t *testing.T) {
	const tf = "5m"
	intervalMs := candle.ParseTimeframeToSeconds(tf) * 1000 // 300_000
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixMilli()
	base = (base / intervalMs) * intervalMs // align to bucket

	key := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: tf}

	tests := []struct {
		name        string
		closes      []int64
		wantCalls   int
		wantSince   int64
		wantUntil   int64
		wantSymbols []string
	}{
		{name: "first close alone is not a gap", closes: []int64{base}, wantCalls: 0},
		{name: "contiguous sequence enqueues nothing", closes: []int64{base, base + intervalMs, base + 2*intervalMs}, wantCalls: 0},
		{
			name:        "one missing bucket enqueues exactly the missing range",
			closes:      []int64{base, base + 2*intervalMs},
			wantCalls:   1,
			wantSince:   base + intervalMs,
			wantUntil:   base + 2*intervalMs,
			wantSymbols: []string{"BTC/USDT"},
		},
		{
			name:        "multi-bucket gap enqueues the whole missing range only",
			closes:      []int64{base, base + 3*intervalMs},
			wantCalls:   1,
			wantSince:   base + intervalMs,
			wantUntil:   base + 3*intervalMs,
			wantSymbols: []string{"BTC/USDT"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := NewContinuityTracker(time.Time{}, nil)
			fake := &recordingRepairer{}

			for _, openTime := range tt.closes {
				tracker.Observe(fake, key, candle.Candle{Timestamp: openTime})
			}

			if len(fake.batches) != tt.wantCalls {
				t.Fatalf("EnqueueBatch calls = %d, want %d (%+v)", len(fake.batches), tt.wantCalls, fake.batches)
			}
			if tt.wantCalls == 0 {
				return
			}
			c := fake.batches[0]
			if c.exchangeID != key.Exchange || c.marketType != key.MarketType {
				t.Errorf("scope = %s:%s, want %s:%s", c.exchangeID, c.marketType, key.Exchange, key.MarketType)
			}
			if !sameStringSet(c.symbols, tt.wantSymbols) {
				t.Errorf("symbols = %v, want %v", c.symbols, tt.wantSymbols)
			}
			if c.since.UnixMilli() != tt.wantSince {
				t.Errorf("since = %d, want %d", c.since.UnixMilli(), tt.wantSince)
			}
			if c.until.UnixMilli() != tt.wantUntil {
				t.Errorf("until = %d, want %d", c.until.UnixMilli(), tt.wantUntil)
			}
		})
	}
}

// TestOpenTimeContinuity_PerKeyIsolation verifies the last-seen open-time is tracked
// per (exchange, market_type, symbol, timeframe): a gap on one key never leaks into
// another, and only the affected symbol/TF is enqueued.
func TestOpenTimeContinuity_PerKeyIsolation(t *testing.T) {
	const tf = "1m"
	intervalMs := candle.ParseTimeframeToSeconds(tf) * 1000
	base := (time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixMilli() / intervalMs) * intervalMs

	btc := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: tf}
	eth := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "ETH/USDT", Timeframe: tf}

	tracker := NewContinuityTracker(time.Time{}, nil)
	fake := &recordingRepairer{}

	tracker.Observe(fake, btc, candle.Candle{Timestamp: base})
	tracker.Observe(fake, eth, candle.Candle{Timestamp: base})
	tracker.Observe(fake, btc, candle.Candle{Timestamp: base + intervalMs})
	tracker.Observe(fake, eth, candle.Candle{Timestamp: base + intervalMs})
	if len(fake.batches) != 0 {
		t.Fatalf("contiguous interleaved streams must not enqueue, got %+v", fake.batches)
	}

	tracker.Observe(fake, btc, candle.Candle{Timestamp: base + 2*intervalMs})
	tracker.Observe(fake, eth, candle.Candle{Timestamp: base + 3*intervalMs})

	if len(fake.batches) != 1 {
		t.Fatalf("expected exactly 1 enqueue (ETH gap only), got %d (%+v)", len(fake.batches), fake.batches)
	}
	if !sameStringSet(fake.batches[0].symbols, []string{"ETH/USDT"}) {
		t.Errorf("symbols = %v, want only ETH/USDT", fake.batches[0].symbols)
	}
	if fake.batches[0].since.UnixMilli() != base+2*intervalMs {
		t.Errorf("since = %d, want %d", fake.batches[0].since.UnixMilli(), base+2*intervalMs)
	}
	if fake.batches[0].until.UnixMilli() != base+3*intervalMs {
		t.Errorf("until = %d, want %d", fake.batches[0].until.UnixMilli(), base+3*intervalMs)
	}
}

// TestPruneClosedOpenTime proves the in-memory continuity map is bounded to the
// live set: orphan baselines are dropped, every timeframe of a live symbol kept.
func TestPruneClosedOpenTime(t *testing.T) {
	live5m := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}
	live1h := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "1h"}
	orphan := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "DEAD/USDT", Timeframe: "5m"}

	tracker := NewContinuityTracker(time.Time{}, nil)
	orch := &recordingRepairer{}
	tracker.Observe(orch, live5m, candle.Candle{Timestamp: 111})
	tracker.Observe(orch, live1h, candle.Candle{Timestamp: 222})
	tracker.Observe(orch, orphan, candle.Candle{Timestamp: 333})

	tracker.Prune(map[string]struct{}{"binance:future:BTC/USDT": {}})

	if _, ok := tracker.LatestClosed(orphan); ok {
		t.Error("orphan baseline was not pruned")
	}
	if _, ok := tracker.LatestClosed(live5m); !ok {
		t.Error("live baseline (5m) was wrongly pruned")
	}
	if _, ok := tracker.LatestClosed(live1h); !ok {
		t.Error("live baseline (1h) was wrongly pruned")
	}
	if tracker.Len() != 2 {
		t.Errorf("tracker holds %d baselines, want 2", tracker.Len())
	}
}

// TestContinuityTracker_RepairMetrics proves both repair triggers reach the
// observability port, and that a nil port is a safe no-op.
func TestContinuityTracker_RepairMetrics(t *testing.T) {
	metrics := &countingRepairMetrics{}
	tracker := NewContinuityTracker(time.Time{}, metrics)
	orch := &recordingRepairer{}
	key := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}

	tracker.Observe(orch, key, candle.Candle{Timestamp: 1, HasGap: true})
	if metrics.gappedRepair != 1 || metrics.bootSettle != 0 {
		t.Fatalf("metrics after gapped close = gapped:%d boot:%d, want 1/0", metrics.gappedRepair, metrics.bootSettle)
	}

	bootKey := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "ETH/USDT", Timeframe: "5m"}
	bootAt := time.UnixMilli(candle.BucketTimestamp(1_700_000_000_000, "5m") + 90_000)
	bootTracker := NewContinuityTracker(bootAt, metrics)
	bootTracker.Observe(orch, bootKey, candle.Candle{Timestamp: candle.BucketTimestamp(bootAt.UnixMilli(), "5m")})
	if metrics.bootSettle != 1 {
		t.Fatalf("boot settle metric = %d, want 1", metrics.bootSettle)
	}
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]int, len(a))
	for _, s := range a {
		set[s]++
	}
	for _, s := range b {
		set[s]--
		if set[s] < 0 {
			return false
		}
	}
	return true
}
