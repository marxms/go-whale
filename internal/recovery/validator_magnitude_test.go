package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

func TestBucketsBetween(t *testing.T) {
	const m = int64(60_000)
	tests := []struct {
		name         string
		from, to, tf int64
		want         int
	}{
		{"adjacent buckets = no hole", 0, m, m, 1},
		{"four missing buckets", 0, 4 * m, m, 4},
		{"a whole 4h hole on 1m", 0, 240 * m, m, 240},
		{"backwards span yields 0", 4 * m, 0, m, 0},
		{"zero timeframe yields 0", 0, 4 * m, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bucketsBetween(tt.from, tt.to, tt.tf); got != tt.want {
				t.Fatalf("bucketsBetween(%d,%d,%d) = %d, want %d", tt.from, tt.to, tt.tf, got, tt.want)
			}
		})
	}
}

// magnitudeWriter serves a fixed candle sequence to the validator.
type magnitudeWriter struct {
	StorageWriter
	out []candle.Candle
}

func (w *magnitudeWriter) GetCandlesByScoreRange(_ context.Context, _, _, _, _ string, _, _ int64) ([]candle.Candle, error) {
	return w.out, nil
}

// TestValidate_ReportsBucketsNotDiscontinuities pins the second return value to a MAGNITUDE.
//
// Counting `missing++` per discontinuity reports both a four-hour hole and a one-bucket blip
// as 1. The value is logged as "N missing candle(s)" and handed to a caller, so a count that
// cannot size or rank a failing symbol is useless.
func TestValidate_ReportsBucketsNotDiscontinuities(t *testing.T) {
	const m = int64(60_000)
	// Anchor the fixtures so the newest stored candle is the previous closed bucket: the
	// coverage check must not add a tail hole to the magnitude these cases assert.
	current := candle.BucketTimestamp(time.Now().UnixMilli(), "1m")
	previousClosed := current - m

	t.Run("one hole of 240 buckets reports 240, not 1", func(t *testing.T) {
		gapStart := previousClosed - 241*m
		w := &magnitudeWriter{out: []candle.Candle{
			{Timestamp: gapStart},
			{Timestamp: gapStart + m},
			{Timestamp: gapStart + 241*m}, // 239 buckets absent between them
		}}
		ok, missing := NewRedisValidator(w).Validate(context.Background(), "binance", "future", "BTC/USDT", "1m", gapStart, current)
		if ok {
			t.Fatal("expected validation failure")
		}
		if missing != 239 {
			t.Fatalf("missing = %d, want 239 buckets (the old code returned 1 — one discontinuity)", missing)
		}
	})

	t.Run("an absent head is measured, not reported as zero", func(t *testing.T) {
		// First stored candle is 10 buckets past gapStart: the whole prefix is missing.
		gapStart := previousClosed - 11*m
		w := &magnitudeWriter{out: []candle.Candle{
			{Timestamp: gapStart + 10*m},
			{Timestamp: gapStart + 11*m},
		}}
		ok, missing := NewRedisValidator(w).Validate(context.Background(), "binance", "future", "BTC/USDT", "1m", gapStart, current)
		if ok {
			t.Fatal("expected validation failure")
		}
		if missing != 10 {
			t.Fatalf("missing = %d, want 10 (the old code returned 0 for the LARGEST hole it can detect)", missing)
		}
	})

	t.Run("a contiguous sequence is clean", func(t *testing.T) {
		gapStart := previousClosed - 2*m
		w := &magnitudeWriter{out: []candle.Candle{
			{Timestamp: gapStart},
			{Timestamp: gapStart + m},
			{Timestamp: gapStart + 2*m},
		}}
		ok, missing := NewRedisValidator(w).Validate(context.Background(), "binance", "future", "BTC/USDT", "1m", gapStart, current)
		if !ok || missing != 0 {
			t.Fatalf("ok=%v missing=%d, want true/0", ok, missing)
		}
	})
}
