package bingx

import (
	"reflect"
	"testing"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// TestWrapperPreservesInnerCapabilities is the RED guard for silent capability loss
// through a composing wrapper.
//
// The port carries OPTIONAL capabilities discovered by type assertion (BufferFillAware,
// SharedSubscriptionClient, StatsProvider). A wrapper — pool, hybrid, hot-swap — that
// composes an inner client and forgets to forward one of those methods does not fail to
// compile and does not log: the consumer's assertion simply returns ok=false and the
// system silently falls back.
//
// This actually happened. BingXClient implements GetBufferFillRatio, the factory returns
// BingXConnectionPool, and the pool did not forward it — so every BingX symbol dropped out
// of adaptive polling into the conservative delay, with no error and no metric, and the
// aggregate avg_buffer_fill_ratio was computed from the Binance clients alone.
//
// The test asserts the RELATIONSHIP (wrapper preserves what the inner type implements)
// rather than a hardcoded list, so a capability added later is covered without editing it.
func TestWrapperPreservesInnerCapabilities(t *testing.T) {
	t.Parallel()

	capabilities := []struct {
		name string
		typ  reflect.Type
	}{
		{"BufferFillAware", reflect.TypeOf((*streaming.BufferFillAware)(nil)).Elem()},
		{"SharedSubscriptionClient", reflect.TypeOf((*streaming.SharedSubscriptionClient)(nil)).Elem()},
		{"StatsProvider", reflect.TypeOf((*streaming.StatsProvider)(nil)).Elem()},
	}

	// Each pair is (wrapper, inner) as they are composed in production.
	pairs := []struct {
		name    string
		wrapper any
		inner   any
	}{
		{"BingXConnectionPool wraps BingXClient", (*BingXConnectionPool)(nil), (*BingXClient)(nil)},
	}

	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()
			wrapperT := reflect.TypeOf(p.wrapper)
			innerT := reflect.TypeOf(p.inner)

			for _, c := range capabilities {
				innerHas := innerT.Implements(c.typ)
				wrapperHas := wrapperT.Implements(c.typ)
				if innerHas && !wrapperHas {
					t.Errorf("%s implements %s but %s does not forward it — "+
						"the consumer's type assertion will silently fail and the venue loses the capability with no error and no log",
						innerT, c.name, wrapperT)
				}
			}
		})
	}
}
