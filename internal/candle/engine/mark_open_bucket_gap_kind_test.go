package engine

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// A restart hole that crosses a bucket's CLOSE is permanently lossy: the venue never resends the
// finished bucket, so its prices are frozen at the last frame and cannot be called exact.
func TestMarkOpenBucketGappedClosedBucketIsProvisional(t *testing.T) {
	ce := NewCandleEngine(map[string]int{"default": 8})
	const ex, mt, sym, tf = "binance", "future", "CLOSED/USDT", "5m"
	ts := time.Now().Add(-10 * time.Minute).UnixMilli()
	ce.UpdateCandle(ex, mt, sym, tf, candle.Candle{Timestamp: ts, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 10})
	if n := ce.MarkOpenBucketGapped(ex, mt, tf, []string{sym}); len(n) != 1 {
		t.Fatalf("marked %d buckets, want 1", len(n))
	}
	got := ce.GetCandleByTimestamp(ex, mt, sym, tf, ts)
	if got == nil || !got.HasGap {
		t.Fatalf("bucket not marked: %+v", got)
	}
	if got.GapKind != candle.GapProvisional {
		t.Fatalf("gap kind = %v, want provisional for a bucket that closed during the hole", got.GapKind)
	}
}

// A bucket still open at the marking instant will be completed by the next live frame: only the
// volume total is short, so the mark stays volume-only and the parent fold can proceed.
func TestMarkOpenBucketGappedOpenBucketIsVolumeOnly(t *testing.T) {
	ce := NewCandleEngine(map[string]int{"default": 8})
	const ex, mt, sym, tf = "binance", "future", "OPEN/USDT", "5m"
	ts := time.Now().Add(-time.Minute).UnixMilli()
	ce.UpdateCandle(ex, mt, sym, tf, candle.Candle{Timestamp: ts, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 10})
	if n := ce.MarkOpenBucketGapped(ex, mt, tf, []string{sym}); len(n) != 1 {
		t.Fatalf("marked %d buckets, want 1", len(n))
	}
	got := ce.GetCandleByTimestamp(ex, mt, sym, tf, ts)
	if got == nil || !got.HasGap {
		t.Fatalf("bucket not marked: %+v", got)
	}
	if got.GapKind != candle.GapVolumeIncomplete {
		t.Fatalf("gap kind = %v, want volume_incomplete for a bucket still open", got.GapKind)
	}
}
