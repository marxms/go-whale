package redisstore

import (
	"testing"

	"github.com/marxms/go-whale/internal/derivatives"
)

// The same interval, from two feeds, lands in the SAME bucket.
//
// Bucketing a derivatives point answers "which interval is this?". A feed that labels the END
// and another that labels the START describe the same window with timestamps one bucket
// apart; aligning both with the same arithmetic puts them in different buckets, and then
// funding from one source and open interest from the other no longer describe the same
// minute. Nothing blows up. The read is silently wrong, which is a failure a chart cannot
// show.
//
// The feeds are START-labeled: Binance `openInterestHist?period=1d` and
// `globalLongShortAccountRatio?period=1d` return `t` = today 00:00Z, the SAME instant as
// Coinalyze — a bucket still OPEN, which is the signature of a START label. If they labeled
// the end, today's point would only exist tomorrow.
//
// Therefore the internal anchor is the START of the interval, and the floor is already the
// right answer for a start-labeled feed. What needs correction is the END label: the instant
// at which `[S, E)` closed belongs to `S`, one period BELOW.
func TestBucketAlignAgreesAcrossConventions(t *testing.T) {
	const hourMs int64 = 3600 * 1000
	// A one-hour interval that starts at 10:00 and closes at 11:00 UTC.
	const start int64 = 1_700_000_000_000 / hourMs * hourMs
	end := start + hourMs

	porFim := bucketAlignMsWithConvention(end, "1h", derivatives.LabelIntervalEnd)
	porInicio := bucketAlignMsWithConvention(start, "1h", derivatives.LabelIntervalStart)
	// And BOTH land on the START of the interval, which is the internal anchor.
	if porInicio != start {
		t.Fatalf("the start label should anchor at %d, anchored at %d", start, porInicio)
	}

	if porFim != porInicio {
		t.Fatalf("the same interval fell into different buckets: end=%d start=%d", porFim, porInicio)
	}
}

// The feeds that already write cannot have a single bucket moved, and they are START-labeled.
// bucketAlignMs is the test-only mirror of the removed start-aligned wrapper. The fixtures
// below were written in that vocabulary; production now has a single convention-aware entry
// point, and keeping the helper here makes the compatibility assertion explicit.
func bucketAlignMs(timestampMs int64, timeframe string) int64 {
	return bucketAlignMsWithConvention(timestampMs, timeframe, derivatives.LabelIntervalStart)
}

// The floor they have always received is exactly what the start convention returns.
//
// The case compares against LITERALS, not against the old function: a literal would age with
// the arithmetic and would start asserting what the code does instead of what it promised not
// to change.
func TestStartLabelledKeepsExactlyTheBucketsItAlreadyProduced(t *testing.T) {
	// LITERALS, NOT THE OLD FUNCTION.
	//
	// Comparing the old start-aligned wrapper with
	// `bucketAlignMsWithConvention(..., LabelIntervalStart)` would be a function compared with
	// itself through a wrapper: an error in the shared arithmetic would change both sides
	// together and the case would pass asserting that nothing changed.
	//
	// The values below come from the DEFINITION of alignment, calculated outside Go:
	// `floor(t/P)*P`, with `P` the timeframe period in milliseconds. For `1w` the 3-day offset
	// enters because Binance's week starts on Monday and the Unix epoch was a Thursday:
	// `floor((t+3d)/P)*P - 3d`.
	instants := []int64{
		1_700_000_000_000, // in the middle of a bucket
		1_699_999_200_000, // exactly on the hour boundary
		1_699_999_200_001, // one millisecond after the boundary
		1_699_999_999_999, // one millisecond before the middle instant
		0,                 // the epoch itself
	}
	for _, tc := range []struct {
		timeframe string
		want      []int64
	}{
		{"5m", []int64{1699999800000, 1699999200000, 1699999200000, 1699999800000, 0}},
		{"15m", []int64{1699999200000, 1699999200000, 1699999200000, 1699999200000, 0}},
		{"1h", []int64{1699999200000, 1699999200000, 1699999200000, 1699999200000, 0}},
		{"4h", []int64{1699992000000, 1699992000000, 1699992000000, 1699992000000, 0}},
		{"1d", []int64{1699920000000, 1699920000000, 1699920000000, 1699920000000, 0}},
		{"1w", []int64{1699833600000, 1699833600000, 1699833600000, 1699833600000, -259200000}},
	} {
		for i, instant := range instants {
			got := bucketAlignMsWithConvention(instant, tc.timeframe, derivatives.LabelIntervalStart)
			if got != tc.want[i] {
				t.Errorf("%s @ %d: bucket %d, expected %d", tc.timeframe, instant, got, tc.want[i])
			}
		}
	}
}

// THE END CONVENTION, ALSO AGAINST LITERALS. An end label is the PREVIOUS bucket: the point
// stamped at 12:00 describes the period that ENDED there, that is, 11:00 on `1h`. Without that
// subtraction Coinalyze history would be written one bucket in the future.
func TestEndLabelledLandsOnThePeriodThatClosed(t *testing.T) {
	for _, tc := range []struct {
		timeframe string
		stamp     int64
		want      int64
	}{
		{"1h", 1_699_999_200_000, 1_699_995_600_000}, // 12:00 labels 11:00
		// 1ms AFTER the boundary still describes the period that CLOSED at 12:00 — one whole
		// period is subtracted and only then floored. It is the case that separates the two
		// conventions: under a START label the same stamp falls at 12:00, under an END label at
		// 11:00.
		{"1h", 1_699_999_200_001, 1_699_995_600_000},
		{"5m", 1_699_999_200_000, 1_699_998_900_000},
		{"1d", 1_699_920_000_000, 1_699_833_600_000},
		{"1w", 1_699_833_600_000, 1_699_228_800_000},
	} {
		got := bucketAlignMsWithConvention(tc.stamp, tc.timeframe, derivatives.LabelIntervalEnd)
		if got != tc.want {
			t.Errorf("%s @ %d with END label: bucket %d, expected %d", tc.timeframe, tc.stamp, got, tc.want)
		}
		// And it is STRICTLY earlier than what the start convention would give for the same
		// stamp, on every boundary — the difference between the two conventions is exactly
		// what a sign error would erase.
		if inicio := bucketAlignMsWithConvention(tc.stamp, tc.timeframe, derivatives.LabelIntervalStart); got >= inicio {
			t.Errorf("%s @ %d: end=%d is not earlier than start=%d", tc.timeframe, tc.stamp, got, inicio)
		}
	}
}

// Binance's week starts on Monday, and the Unix epoch was a Thursday. The offset applies to
// BOTH conventions, otherwise `1w` would agree on every timeframe except the one where the
// arithmetic differs — which is where an error would hide.
func TestWeeklyAgreesAcrossConventionsToo(t *testing.T) {
	const weekMs int64 = 7 * 86400 * 1000
	const mondayOffsetMs int64 = 3 * 86400 * 1000
	start := (1_700_000_000_000+mondayOffsetMs)/weekMs*weekMs - mondayOffsetMs

	porFim := bucketAlignMsWithConvention(start+weekMs, "1w", derivatives.LabelIntervalEnd)
	porInicio := bucketAlignMsWithConvention(start, "1w", derivatives.LabelIntervalStart)

	if porFim != porInicio {
		t.Fatalf("the week disagreed: end=%d start=%d", porFim, porInicio)
	}
}

// AN UNRECOGNIZED TIMEFRAME DOES NOT SILENTLY BECOME AN HOUR.
//
// Invalid input arrives as zero, and zero is the sentinel callers must refuse; neither
// convention tries to align it.
func TestUnrecognisedTimeframeDoesNotInventAnHour(t *testing.T) {
	const start int64 = 1_700_000_000_000

	for _, convention := range []derivatives.LabelConvention{
		derivatives.LabelIntervalStart,
		derivatives.LabelIntervalEnd,
	} {
		if got := bucketAlignMsWithConvention(start, "not-a-timeframe", convention); got != 0 {
			t.Fatalf("convention %s invented bucket %d for an invalid timeframe", convention, got)
		}
	}
}
