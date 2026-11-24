package binancews

import "testing"

// The combined-stream query must be a valid ASCII request target. The venue publishes
// symbols outside ASCII (measured 2026-11-24: 币安人生USDT); raw UTF-8 in the query is
// percent-encoded here, while the documented "@" separator stays literal.
func TestJoinStreamsPercentEncodesNonASCII(t *testing.T) {
	t.Parallel()

	got := joinStreams([]string{"btcusdt@aggTrade", "币安人生usdt@aggTrade"})
	want := "btcusdt@aggTrade/%E5%B8%81%E5%AE%89%E4%BA%BA%E7%94%9Fusdt@aggTrade"
	if got != want {
		t.Fatalf("joinStreams = %q, want %q", got, want)
	}
	for index := 0; index < len(got); index++ {
		if got[index] > 127 {
			t.Fatalf("non-ASCII byte at %d in %q", index, got)
		}
	}
}

// An ASCII-only stream list keeps its documented form; the encoder must not change the
// names the venue already accepts.
func TestJoinStreamsLeavesASCIIUnchanged(t *testing.T) {
	t.Parallel()

	if got := joinStreams([]string{"btcusdt@kline_5m", "ethusdt@aggTrade"}); got != "btcusdt@kline_5m/ethusdt@aggTrade" {
		t.Fatalf("joinStreams = %q", got)
	}
}
