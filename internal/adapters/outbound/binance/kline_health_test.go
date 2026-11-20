package binancesdk

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/binancews"
)

// TestPartitionStatsExposeSilentSymbols proves the per-partition count the health reports: a
// partition past the silence limit carries ALL its symbols as unproduced; a fresh one carries
// none. Exposure in /health is asserted by the health package's test.
func TestPartitionStatsExposeSilentSymbols(t *testing.T) {
	now := time.Now().UnixMilli()
	fresh := &klinePartition{client: &binancews.Connection{ID: 1}, symbols: 10}
	fresh.lastFrameAt.Store(now)
	silent := &klinePartition{client: &binancews.Connection{ID: 2}, symbols: 7}
	silent.lastFrameAt.Store(now - int64((KlineSilenceLimitSeconds+1)*1000))
	feed := &KlineFeed{parts: []*klinePartition{fresh, silent}}

	stats := feed.PartitionStats()
	if len(stats) != 2 {
		t.Fatalf("stats = %+v, want one row per partition", stats)
	}
	if stats[0].SilentSymbols != 0 {
		t.Fatalf("fresh partition silent symbols = %d, want 0", stats[0].SilentSymbols)
	}
	if stats[1].SilentSymbols != 7 {
		t.Fatalf("silent partition silent symbols = %d, want all 7", stats[1].SilentSymbols)
	}
}
