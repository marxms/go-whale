package market

import (
	"testing"
	"time"
)

func TestParseTimeframeValid(t *testing.T) {
	tests := []struct {
		raw      string
		wantText string
		want     time.Duration
	}{
		{raw: "5m", wantText: "5m", want: 5 * time.Minute},
		{raw: " 1H ", wantText: "1h", want: time.Hour},
		{raw: "1d", wantText: "1d", want: 24 * time.Hour},
		{raw: "1w", wantText: "1w", want: 7 * 24 * time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := ParseTimeframe(tt.raw)
			if err != nil {
				t.Fatalf("ParseTimeframe(%q): %v", tt.raw, err)
			}
			if got.String() != tt.wantText || got.Duration() != tt.want {
				t.Fatalf("ParseTimeframe(%q) = %q/%s, want %q/%s",
					tt.raw, got, got.Duration(), tt.wantText, tt.want)
			}
			if got.Seconds() != int64(tt.want/time.Second) || got.Milliseconds() != tt.want.Milliseconds() {
				t.Fatalf("duration conversions disagree for %q", tt.raw)
			}
		})
	}
}

func TestParseTimeframeInvalid(t *testing.T) {
	for _, raw := range []string{"", "m", "0m", "-1h", "5x", "5", "1.5h"} {
		t.Run(raw, func(t *testing.T) {
			if _, err := ParseTimeframe(raw); err == nil {
				t.Fatalf("ParseTimeframe(%q) unexpectedly succeeded", raw)
			}
		})
	}
}

func TestTimeframeBucketStart(t *testing.T) {
	// 2026-11-05T12:34:56.789Z.
	const instant = int64(1793882096789)
	tests := []struct {
		tf   string
		want int64
	}{
		{tf: "1m", want: int64(1793882040000)},
		{tf: "1h", want: int64(1793880000000)},
		{tf: "1d", want: int64(1793836800000)},
		// Monday 2026-11-02T00:00:00Z.
		{tf: "1w", want: int64(1793577600000)},
	}
	for _, tt := range tests {
		t.Run(tt.tf, func(t *testing.T) {
			interval, err := ParseTimeframe(tt.tf)
			if err != nil {
				t.Fatal(err)
			}
			if got := interval.BucketStart(instant); got != tt.want {
				t.Fatalf("BucketStart(%s) = %d, want %d", tt.tf, got, tt.want)
			}
		})
	}
}

func TestZeroTimeframeIsInvalid(t *testing.T) {
	var zero Timeframe
	if zero.Valid() {
		t.Fatal("zero Timeframe must be invalid")
	}
	if got := zero.BucketStart(1793872496789); got != 0 {
		t.Fatalf("zero.BucketStart() = %d, want 0", got)
	}
}
