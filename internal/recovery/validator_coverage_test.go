package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// TestValidateRequiresPreviousClosedBucket pins REC-07: interior continuity is not enough.
// A series that simply stops before the present passes the sequence check, and that is the
// exact shape a silent feed leaves behind. The previous closed bucket is required; the open
// one may legitimately be absent.
func TestValidateRequiresPreviousClosedBucket(t *testing.T) {
	const tf = "1h"
	const tfMs = int64(3600_000)
	current := candle.BucketTimestamp(time.Now().UnixMilli(), tf)
	previousClosed := current - tfMs

	series := func(from, to int64) []candle.Candle {
		var out []candle.Candle
		for at := from; at <= to; at += tfMs {
			out = append(out, candle.Candle{Timestamp: at})
		}
		return out
	}

	tests := []struct {
		name        string
		candles     []candle.Candle
		gapStart    int64
		wantOK      bool
		wantMissing int
	}{
		{
			name:        "stops one bucket before the previous closed bucket",
			candles:     series(current-4*tfMs, current-2*tfMs),
			gapStart:    current - 4*tfMs,
			wantOK:      false,
			wantMissing: 1,
		},
		{
			name:        "stops three buckets before the previous closed bucket",
			candles:     series(current-6*tfMs, current-4*tfMs),
			gapStart:    current - 6*tfMs,
			wantOK:      false,
			wantMissing: 3,
		},
		{
			name:     "reaches the previous closed bucket",
			candles:  series(current-4*tfMs, previousClosed),
			gapStart: current - 4*tfMs,
			wantOK:   true,
		},
		{
			name:     "reaches the open bucket",
			candles:  series(current-4*tfMs, current),
			gapStart: current - 4*tfMs,
			wantOK:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &magnitudeWriter{out: tt.candles}
			ok, missing := NewRedisValidator(w).Validate(
				context.Background(), "binance", "future", "BTC/USDT", tf, tt.gapStart, current)
			if ok != tt.wantOK || missing != tt.wantMissing {
				t.Fatalf("Validate = (%v,%d), want (%v,%d)", ok, missing, tt.wantOK, tt.wantMissing)
			}
		})
	}
}
