package redisstore

import (
	"testing"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
)

// PARITY, on the eight timeframes the product actually serves.
//
// The timeframes come from `config.example.yaml` (`candle.timeframes`), not from a list invented
// here: a parity proved on three periods and broken on the fourth is a parity that does not
// exist, and the fourth would be exactly what Coinalyze brings anew.
//
// THE ASSERTION IS ABOUT SEMANTICS, not arithmetic: the same INTERVAL, described by two feeds
// that label opposite ends, must land on the same ZSET score. If it lands on two, funding from
// one source and open interest from the other stop describing the same minute — and nothing
// blows up, because both scores are valid.
func registeredTimeframes() []string {
	return []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"}
}

func TestBucketParityAcrossConventionsForEveryRegisteredTimeframe(t *testing.T) {
	for _, timeframe := range registeredTimeframes() {
		t.Run(timeframe, func(t *testing.T) {
			periodMs := bucketPeriodMsForTest(t, timeframe)
			// The REAL start of a bucket, not an arbitrary instant: aligning from the middle of
			// the interval would hide a boundary error, which is where this defect lives.
			start := bucketAlignMs(1_700_000_000_000, timeframe)
			end := start + periodMs

			porInicio := bucketAlignMsWithConvention(start, timeframe, derivatives.LabelIntervalStart)
			porFim := bucketAlignMsWithConvention(end, timeframe, derivatives.LabelIntervalEnd)

			if porInicio != porFim {
				t.Fatalf(
					"the same interval [%d, %d) fell into two buckets: start->%d, end->%d",
					start, end, porInicio, porFim,
				)
			}
		})
	}
}

// THE PER-FEED TERM IS WHAT HOLDS PARITY, and this case proves it is not decoration: without
// it, the two labels diverge on ALL eight. A test that only asserted equality would pass with
// the two conventions collapsed into one.
func TestParityDependsOnTheFeedTerm(t *testing.T) {
	for _, timeframe := range registeredTimeframes() {
		periodMs := bucketPeriodMsForTest(t, timeframe)
		start := bucketAlignMs(1_700_000_000_000, timeframe)

		// What the OLD alignment would do: ignore the convention and floor. That is CORRECT for
		// the start label — the internal anchor IS the start — and WRONG for the end one, which
		// must step back one period. That is where the per-feed term must show up.
		semTermo := bucketAlignMs(start, timeframe)
		comTermoFim := bucketAlignMsWithConvention(start, timeframe, derivatives.LabelIntervalEnd)

		if semTermo == comTermoFim {
			t.Fatalf(
				"%s: the per-feed term changed nothing — end-labeled must go down one bucket (%d)",
				timeframe, periodMs,
			)
		}
	}
}

// And the feed that already writes — START-labeled, measured on both providers — does not
// move on any of the eight.
//
// LITERAL, NOT f(x)==f(x). Comparing the old start-aligned wrapper with
// `bucketAlignMsWithConvention(..., LabelIntervalStart)` would compare a function with itself
// through a wrapper: an error in the shared arithmetic moves both sides together and the case
// passes. The expectation comes from the DEFINITION — `floor(t/P)*P`, with the 3-day offset on
// `1w` because the Unix epoch was a Thursday — using the period table that
// `TestBucketPeriodTableMatchesTheRegistry` guards against the parser.
func TestStartLabelledUnchangedForEveryRegisteredTimeframe(t *testing.T) {
	const mondayOffsetMs int64 = 3 * 86400 * 1000
	instants := []int64{0, 1_699_999_999_999, 1_700_000_000_000, 1_700_000_000_001}

	for _, timeframe := range registeredTimeframes() {
		periodMs := bucketPeriodMsForTest(t, timeframe)
		for _, instant := range instants {
			esperado := instant / periodMs * periodMs
			if timeframe == "1w" {
				esperado = (instant+mondayOffsetMs)/periodMs*periodMs - mondayOffsetMs
			}

			got := bucketAlignMsWithConvention(instant, timeframe, derivatives.LabelIntervalStart)
			if got != esperado {
				t.Errorf("%s @ %d: bucket %d, expected %d by definition", timeframe, instant, got, esperado)
			}
			// And the OLD function still agrees — that is what guarantees the buckets already
			// written to the ZSET stay addressable. It only has value AFTER the literal
			// assertion.
			if antes := bucketAlignMs(instant, timeframe); antes != got {
				t.Errorf("%s @ %d: bucket moved from %d to %d", timeframe, instant, antes, got)
			}
		}
	}
}

// bucketPeriodMs is a TABLE OF LITERALS; deriving the period from `bucketAlignMs` itself would
// make the period a mirror of the implementation. An error in alignment would move the period
// with it, the test interval would follow, and parity between conventions would pass asserting
// nothing.
//
// Silent aging is handled elsewhere: `TestBucketPeriodTableMatchesTheRegistry` compares this
// table with the project's timeframe registry, and fails if someone adds a timeframe without
// adding the literal.
func bucketPeriodMs() map[string]int64 {
	return map[string]int64{
		"5m":  300_000,
		"15m": 900_000,
		"30m": 1_800_000,
		"1h":  3_600_000,
		"2h":  7_200_000,
		"4h":  14_400_000,
		"1d":  86_400_000,
		"1w":  604_800_000,
	}
}

func bucketPeriodMsForTest(t *testing.T, timeframe string) int64 {
	t.Helper()
	periodMs, ok := bucketPeriodMs()[timeframe]
	if !ok {
		t.Fatalf("no literal period declared for %s", timeframe)
	}
	return periodMs
}

// THE GUARD AGAINST AGING. It reads the period from the project's PARSER — an independent
// source from alignment — and not from `bucketAlignMs`. If a timeframe enters the registry
// without a literal, or a literal diverges from the parser, this case fails.
func TestBucketPeriodTableMatchesTheRegistry(t *testing.T) {
	for _, timeframe := range registeredTimeframes() {
		want := candle.ParseTimeframeToSeconds(timeframe) * 1000
		if got := bucketPeriodMs()[timeframe]; got != want {
			t.Errorf("%s: literal %d, parser %d", timeframe, got, want)
		}
	}
	if len(bucketPeriodMs()) != len(registeredTimeframes()) {
		t.Errorf("the table has %d entries and the registry has %d timeframes",
			len(bucketPeriodMs()), len(registeredTimeframes()))
	}
}
