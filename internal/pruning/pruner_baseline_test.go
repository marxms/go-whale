package pruning

import (
	"context"
	"testing"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
)

// TestPruner_PruneRedis_CoversBaselineKeys asserts the THIRD sweep walks the baseline
// family too.
//
// The pruner is the sweep that is easiest to forget: it lives in another package from
// the writer and parses the symbol positionally out of a glob it types itself. It now
// takes both the patterns and the parse from storage, and this test is what fails if
// someone types them here again.
func TestPruner_PruneRedis_CoversBaselineKeys(t *testing.T) {
	t.Parallel()

	staleBaseline := "derivatives:baseline:binance:future:XRP/USDT:1h:v1"
	activeBaseline := "derivatives:baseline:binance:future:BTC/USDT:1h:v1"

	keys := make(map[string][]string)
	for _, pattern := range redisstore.DerivativesKeyPatterns() {
		keys[pattern] = nil
	}
	baselinePattern := "derivatives:baseline:*:*:*:*:*"
	if _, ok := keys[baselinePattern]; !ok {
		t.Fatalf("the baseline family is not in redisstore.DerivativesKeyPatterns(): %v", redisstore.DerivativesKeyPatterns())
	}
	keys[baselinePattern] = []string{activeBaseline, staleBaseline}

	redis := &mockRedis{keys: keys}
	p := newTestPruner(redis)

	if err := p.PruneRedis(context.Background(), []string{"BTC/USDT"}); err != nil {
		t.Fatalf("PruneRedis: %v", err)
	}

	if len(redis.unlinked) != 1 || redis.unlinked[0] != staleBaseline {
		t.Fatalf("unlinked = %v, want only %q", redis.unlinked, staleBaseline)
	}
}

// TestExtractSymbolFromKey_Families pins the parse the pruner uses to decide what to
// UNLINK. A wrong symbol here deletes live data, so an unknown shape must return ""
// rather than whichever segment happens to sit at the expected index.
func TestExtractSymbolFromKey_Families(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		want string
	}{
		{"snapshot", "derivatives:snapshot:latest:binance:future:BTC/USDT", "BTC/USDT"},
		{"series", "derivatives:series:binance:future:BTC/USDT:1h", "BTC/USDT"},
		{"baseline", "derivatives:baseline:binance:future:BTC/USDT:1h:v1", "BTC/USDT"},
		{"last trade", "last_trade:binance:future:BTC/USDT", "BTC/USDT"},
		{"unknown family", "derivatives:mystery:binance:future:BTC/USDT:1h", ""},
		{"unrelated key", "candles:binance:future:BTC/USDT:1h", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractSymbolFromKey(tc.key); got != tc.want {
				t.Fatalf("extractSymbolFromKey(%q) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}
