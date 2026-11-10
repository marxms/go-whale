package market

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Timeframe is a validated candle interval. The zero value is invalid, and
// ParseTimeframe is the only way to construct a non-zero value.
type Timeframe struct {
	text     string
	duration time.Duration
}

// ParseTimeframe accepts the interval vocabulary used by the venues:
// minutes (m), hours (h), days (d), and weeks (w). It is deliberately strict;
// callers must not substitute a default interval for unknown input.
func ParseTimeframe(raw string) (Timeframe, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if len(normalized) < 2 {
		return Timeframe{}, fmt.Errorf("timeframe: invalid interval %q", raw)
	}
	value, err := strconv.ParseInt(normalized[:len(normalized)-1], 10, 64)
	if err != nil || value <= 0 {
		return Timeframe{}, fmt.Errorf("timeframe: invalid interval %q", raw)
	}

	var unit time.Duration
	switch normalized[len(normalized)-1] {
	case 'm':
		unit = time.Minute
	case 'h':
		unit = time.Hour
	case 'd':
		unit = 24 * time.Hour
	case 'w':
		unit = 7 * 24 * time.Hour
	default:
		return Timeframe{}, fmt.Errorf("timeframe: unsupported unit in %q", raw)
	}
	return Timeframe{text: normalized, duration: time.Duration(value) * unit}, nil
}

func (t Timeframe) String() string {
	return t.text
}

// Valid reports whether the timeframe has a positive duration.
func (t Timeframe) Valid() bool {
	return t.duration > 0
}

// Duration returns the timeframe length.
func (t Timeframe) Duration() time.Duration {
	return t.duration
}

// Seconds returns the timeframe length in whole seconds.
func (t Timeframe) Seconds() int64 {
	return int64(t.duration / time.Second)
}

// Milliseconds returns the timeframe length in milliseconds.
func (t Timeframe) Milliseconds() int64 {
	return t.duration.Milliseconds()
}

// BucketStart rounds an instant down to the start of its interval bucket.
// Weekly buckets are aligned to Monday 00:00 UTC, unlike a plain epoch division.
func (t Timeframe) BucketStart(ms int64) int64 {
	seconds := t.Seconds()
	if seconds <= 0 {
		return 0
	}
	tsSeconds := ms / 1000
	if t.text == "1w" {
		const mondayOffset int64 = 3 * 86400
		return ((tsSeconds+mondayOffset)/seconds*seconds - mondayOffset) * 1000
	}
	return (tsSeconds / seconds) * seconds * 1000
}
