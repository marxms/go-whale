package derivatives

import (
	"strconv"
	"strings"
	"time"
)

// The derivatives package is candle-free by contract: it is a core package and
// must not reach into an adapter or the candle engine. These two helpers are the
// minimal arithmetic the window and durable writers need, kept here so the
// boundary types (scope, point, batch) never carry a candle dependency.

// timeframeMilliseconds parses the interval vocabulary the venues use: minutes (m),
// hours (h), days (d) and weeks (w). The result is false for an unknown interval, so
// callers can refuse instead of substituting an hour-long bucket.
func timeframeMilliseconds(raw string) (int64, bool) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if len(normalized) < 2 {
		return 0, false
	}
	value, err := strconv.ParseInt(normalized[:len(normalized)-1], 10, 64)
	if err != nil || value <= 0 {
		return 0, false
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
		return 0, false
	}
	return (time.Duration(value) * unit).Milliseconds(), true
}

// bucketStartMs rounds an instant down to the start of its interval bucket.
// Weekly buckets align to Monday 00:00 UTC. Invalid intervals return 0, matching
// the convention the durable read uses for a timeframe it cannot measure.
func bucketStartMs(ts int64, timeframe string) int64 {
	bucketMs, ok := timeframeMilliseconds(timeframe)
	if !ok || bucketMs <= 0 {
		return 0
	}
	seconds := bucketMs / 1000
	if strings.ToLower(strings.TrimSpace(timeframe)) == "1w" {
		const mondayOffset int64 = 3 * 86400
		return ((ts/1000+mondayOffset)/seconds*seconds - mondayOffset) * 1000
	}
	return (ts / 1000 / seconds) * seconds * 1000
}
