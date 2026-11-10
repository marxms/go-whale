package httpapi

import (
	"testing"
	"time"
)

// TestRFC3339Cache_MatchesDirectFormat proves neutrality: the cached string
// equals time.UnixMilli(ts).UTC().Format(RFC3339) for representative timestamps,
// on both the cold (miss) and warm (hit) read.
func TestRFC3339Cache_MatchesDirectFormat(t *testing.T) {
	cache := newRFC3339Cache()

	timestamps := []int64{
		0,
		60_000,
		1_749_800_000_000, // typical recent ms
		1_749_800_060_000,
		1_700_000_123_000,
	}

	for _, ts := range timestamps {
		want := time.UnixMilli(ts).UTC().Format(time.RFC3339)

		if got := cache.format(ts); got != want { // cold (miss)
			t.Fatalf("cold format(%d) = %q, want %q", ts, got, want)
		}
		if got := cache.format(ts); got != want { // warm (hit)
			t.Fatalf("warm format(%d) = %q, want %q", ts, got, want)
		}
	}
}

// TestRFC3339Cache_Bounded proves the cache never grows past the cap: feeding
// far more distinct timestamps than rfc3339CacheMaxEntries keeps len(m) within
// the bound, and every value still matches the direct format.
func TestRFC3339Cache_Bounded(t *testing.T) {
	cache := newRFC3339Cache()

	const distinct = rfc3339CacheMaxEntries * 3
	base := int64(1_700_000_000_000)
	for i := 0; i < distinct; i++ {
		ts := base + int64(i)*60_000
		want := time.UnixMilli(ts).UTC().Format(time.RFC3339)
		if got := cache.format(ts); got != want {
			t.Fatalf("format(%d) = %q, want %q", ts, got, want)
		}
		// white-box: single-goroutine test, direct map read needs no lock.
		if len(cache.m) > rfc3339CacheMaxEntries {
			t.Fatalf("cache grew to %d entries, exceeds bound %d", len(cache.m), rfc3339CacheMaxEntries)
		}
	}
}

// BenchmarkRFC3339Format documents the win: a cache hit avoids the
// time.UnixMilli().UTC().Format() work that the direct path pays per candle.
func BenchmarkRFC3339Format(b *testing.B) {
	const ts int64 = 1_749_800_000_000

	b.Run("direct_Format", func(b *testing.B) {
		b.ReportAllocs()
		var sink string
		for i := 0; i < b.N; i++ {
			sink = time.UnixMilli(ts).UTC().Format(time.RFC3339)
		}
		_ = sink
	})

	b.Run("cached_format", func(b *testing.B) {
		cache := newRFC3339Cache()
		cache.format(ts) // warm
		b.ReportAllocs()
		var sink string
		for i := 0; i < b.N; i++ {
			sink = cache.format(ts)
		}
		_ = sink
	})
}
