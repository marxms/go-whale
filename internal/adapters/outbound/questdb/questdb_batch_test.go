package questdb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// countingColdStore records how many times the pipeline asks the cold store to COMMIT.
type countingColdStore struct {
	commits atomic.Int64
	rows    atomic.Int64
}

func (s *countingColdStore) WriteBatches(_ context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	s.commits.Add(1)
	for _, batch := range groups {
		s.rows.Add(int64(len(batch)))
	}
	return nil
}

func (s *countingColdStore) QueryRange(context.Context, string, string, string, string, time.Time, time.Time, int) ([]candle.Candle, error) {
	return nil, nil
}

func (s *countingColdStore) GetOldestTimestamp(context.Context, string, string, string, string) (time.Time, error) {
	return time.Time{}, nil
}
func (s *countingColdStore) Close() error                 { return nil }
func (s *countingColdStore) Stats() candle.ColdStoreStats { return candle.ColdStoreStats{} }

// TestPipeline_OneCommitPerSweep is the guard for , the anchor finding.
//
// flushBatches grouped by candle.SeriesKey and called WriteBatch PER GROUP, and each group holds
// exactly one candle because a series closes one bucket at a time. Every candle therefore
// became its own ILP request and its own WAL transaction — measured at 1.0000 rows per
// commit across four independent production windows, against a client default of 75.000
// rows per flush.
//
// The cost is not the transaction count itself but what each one triggers: a WAL-apply with
// a dedup merge over five columns, on a DAY-partitioned table whose timestamps are
// out-of-order by construction (a weekly candle closing today carries a timestamp six days
// back, in an earlier partition). 3,91 TB written to hold 3,6 GB.
func TestPipeline_OneCommitPerSweep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		series  int
		wantMax int64
	}{
		{name: "a 5m boundary: every symbol closes at once", series: 203, wantMax: 1},
		{name: "the 00:00 UTC boundary: every symbol on every timeframe", series: 1624, wantMax: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := &countingColdStore{}
			p := NewPipeline(store, 8192, time.Hour) // long interval: we flush explicitly
			p.Start()

			for i := range tt.series {
				p.Enqueue("binance", "future", symbolFor(i), "5m", candle.Candle{
					Timestamp: 1_700_000_000_000, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1,
				})
			}
			p.Stop(t.Context()) // drains and flushes everything still buffered

			if got := store.commits.Load(); got > tt.wantMax {
				t.Errorf("cold store committed %d times for %d series, want at most %d — "+
					"one transaction per series is what produced 1,0000 rows per commit and ~1.200x write amplification",
					got, tt.series, tt.wantMax)
			}
			if got := store.rows.Load(); got != int64(tt.series) {
				t.Errorf("rows written = %d, want %d: batching must not lose or duplicate a candle", got, tt.series)
			}
		})
	}
}

func symbolFor(i int) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	return string(alphabet[i%26]) + string(alphabet[(i/26)%26]) + string(alphabet[(i/676)%26]) + "/USDT"
}
