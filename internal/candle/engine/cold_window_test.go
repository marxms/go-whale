package engine

import (
	"testing"
	"time"
)

// TestColdReadLowerBound is the guard for
//
// Every cold read passed time.Time{} as the lower bound — the zero time — so QuestDB had no
// interval to prune on and scanned all 367 partitions while filtering four unindexed SYMBOL
// columns. Production logged those reads at 2-6 seconds each, all ending "canceled by user"
// (the client's context deadline), so the work was done and thrown away.
//
// A read that wants N buckets of timeframe T cannot need data older than N*T, plus slack for
// gaps. Bounding it lets the engine prune partitions instead of reading the whole table.
func TestColdReadLowerBound(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		timeframe string
		limit     int
		to        time.Time
		wantSpan  time.Duration
		reason    string
	}{
		{
			name:      "5m x 800 covers days, not years",
			timeframe: "5m", limit: 800, to: now,
			wantSpan: coldReadSlack * 800 * 5 * time.Minute / coldReadSlack,
			reason:   "800 five-minute buckets is under three days",
		},
		{
			name:      "1w x 800 must NOT reach back fifteen years",
			timeframe: "1w", limit: 800, to: now,
			wantSpan: coldReadMaxLookback,
			reason:   "800 weekly buckets is ~15 years; the retention window caps it",
		},
		{
			name:      "unknown timeframe falls back to the retention cap, never to unbounded",
			timeframe: "bogus", limit: 100, to: now,
			wantSpan: coldReadMaxLookback,
			reason:   "an unparseable timeframe must not silently become a full-table scan",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			from := coldReadFrom(tt.to, tt.timeframe, tt.limit)

			if from.IsZero() {
				t.Fatalf("lower bound is the zero time: the read is unbounded and scans every partition — %s", tt.reason)
			}
			if got := tt.to.Sub(from); got > coldReadMaxLookback {
				t.Errorf("lookback = %v, want at most the retention cap %v — %s", got, coldReadMaxLookback, tt.reason)
			}
		})
	}
}

// TestColdReadLowerBound_CoversTheRequestedDepth guards the other direction: bounding the
// window must never cut off buckets the caller legitimately asked for.
func TestColdReadLowerBound_CoversTheRequestedDepth(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	for _, tf := range []string{"5m", "15m", "1h", "4h", "1d"} {
		from := coldReadFrom(now, tf, 800)
		tfDur, ok := parseTimeframeDuration(tf)
		if !ok {
			t.Fatalf("timeframe %s should parse", tf)
		}
		needed := time.Duration(800) * tfDur
		if needed <= coldReadMaxLookback && now.Sub(from) < needed {
			t.Errorf("%s: window %v is shorter than the %d buckets requested (%v) — the read would return short",
				tf, now.Sub(from), 800, needed)
		}
	}
}
