package pruning

import (
	"context"
	"testing"
	"time"
)

// pagedMockRedis serves keys one PAGE at a time, the way Redis SCAN actually behaves:
// it returns a non-zero cursor while more of the keyspace remains, and 0 only on the
// last page.
//
// The pre-existing mock always returned cursor 0, which is why five green tests sat on
// top of an infinite loop for as long as this code has existed.
type pagedMockRedis struct {
	pages    map[string][][]string // pattern -> pages of keys
	unlinked []string
	calls    int
}

func (m *pagedMockRedis) ScanKeys(_ context.Context, cursor uint64, pattern string) ([]string, uint64, error) {
	m.calls++
	pages := m.pages[pattern]
	if int(cursor) >= len(pages) { //nolint:gosec // G115: cursor is this fake's own page index, bounded by len(pages).
		return nil, 0, nil
	}
	next := cursor + 1
	if int(next) >= len(pages) { //nolint:gosec // G115: next is cursor+1, bounded by the same page count.
		next = 0 // last page: Redis signals completion with cursor 0
	}
	return pages[cursor], next, nil
}

func (m *pagedMockRedis) UnlinkKeys(_ context.Context, keys ...string) (int64, error) {
	m.unlinked = append(m.unlinked, keys...)
	return int64(len(keys)), nil
}

// TestPruneRedis_TerminatesOnPaginatedKeyspace is the RED guard for the infinite loop.
//
// The redisClient contract declared `scanKeys(ctx, pattern) ([]string, uint64, error)` —
// it RETURNED a cursor but did not ACCEPT one, and the implementation hardcoded
// Scan(ctx, 0, pattern, 100). The caller paginated correctly (cursor = nextCursor; break
// when 0), so on any keyspace larger than one page the same first page came back forever
// with the same non-zero cursor.
//
// Measured in production: the three scanned patterns hold 201, 1.680 and 203 keys against
// a COUNT of 100, so the first call already returns a non-zero cursor for every one of
// them. The goroutine at app/lifecycle.go spins on SCAN and never reaches the housekeeping
// that follows the call — PurgeStaleSymbols, PurgeStaleSymbolData, PurgeStaleDerivatives.
//
// The test is bounded by a timeout because the failure mode is a hang, not a wrong value:
// without the bound a regression would stall the suite instead of failing it.
func TestPruneRedis_TerminatesOnPaginatedKeyspace(t *testing.T) {
	t.Parallel()

	mock := &pagedMockRedis{pages: map[string][][]string{
		"derivatives:snapshot:*:*:*": {
			{"derivatives:snapshot:latest:binance:future:AAVE/USDT"},
			{"derivatives:snapshot:latest:binance:future:STALE/USDT"},
		},
		"derivatives:series:*:*:*:*": {
			{"derivatives:series:binance:future:AAVE/USDT:1h"},
			{"derivatives:series:binance:future:STALE/USDT:5m"},
		},
		"last_trade:*:*:*": {
			{"last_trade:binance:future:AAVE/USDT"},
			{"last_trade:binance:future:STALE/USDT"},
		},
	}}

	done := make(chan error, 1)
	go func() { done <- newTestPruner(mock).PruneRedis(context.Background(), []string{"AAVE/USDT"}) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("PruneRedis: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("PruneRedis did not terminate on a multi-page keyspace (%d scan calls) — the cursor is not being carried", mock.calls)
	}

	// Every page must be visited: a fix that terminates by ignoring pages past the first
	// would pass the timeout check while silently pruning nothing.
	if len(mock.unlinked) != 3 {
		t.Errorf("unlinked %d keys, want 3 (the STALE/USDT key on each pattern's SECOND page): %v", len(mock.unlinked), mock.unlinked)
	}
	for _, k := range mock.unlinked {
		if sym := extractSymbolFromKey(k); sym != "STALE/USDT" {
			t.Errorf("unlinked a key for %q — only symbols outside the active set may be removed: %s", sym, k)
		}
	}
}

// TestExtractSymbolFromKey_MatchesProductionKeyShapes pins the parser against key formats
// read from the live Redis, not from the doc comment.
//
// This matters more than it looks: the parser decides what gets UNLINKed. A shape it
// misparses yields a symbol that is not in the active set, and the pruner deletes live
// data. Before the pagination fix the parser was never reached past page one, so it was
// effectively untested against real traffic.
func TestExtractSymbolFromKey_MatchesProductionKeyShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key  string
		want string
	}{
		{"derivatives:snapshot:latest:binance:future:AAVE/USDT", "AAVE/USDT"},
		{"derivatives:series:binance:future:IBM/USDT:1h", "IBM/USDT"},
		{"derivatives:series:binance:future:NATGAS/USDT:5m", "NATGAS/USDT"},
		{"last_trade:binance:future:PENGU/USDT", "PENGU/USDT"},
		// Unrecognized shapes must yield "" so the caller SKIPS them. Returning a wrong
		// symbol here would delete a live key.
		{"candles:binance:future:BTC/USDT:5m", ""},
		{"totally:unknown", ""},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			t.Parallel()
			if got := extractSymbolFromKey(tt.key); got != tt.want {
				t.Errorf("extractSymbolFromKey(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}
