package binancesdk

import "testing"

// TestRecordResubscribeOutcome_EmitsIncomplete proves truthful resubscribe accounting:
// when a reconnect restores only part of the subscribed fleet, the shortfall MUST be
// surfaced rather than hidden behind a "completed" log. This is the metric that makes the
// stage mass-kill (e.g. 289/500 restored) observable.
func TestRecordResubscribeOutcome_EmitsIncomplete(t *testing.T) {
	var got float64
	metrics := Metrics{
		ResubscribeIncompleteTotal: func(exchange, marketType string, n float64) {
			if exchange != "binance" || marketType != "future" {
				t.Errorf("labels = %s/%s, want binance/future", exchange, marketType)
			}
			got += n
		},
	}

	recordResubscribeOutcome(metrics, "future", 500, 289)

	if got != 211 {
		t.Fatalf("expected incomplete +211 (500-289), got %v", got)
	}
}

// TestRecordResubscribeOutcome_NoIncompleteWhenFull proves a fully restored fleet emits no
// incompleteness signal.
func TestRecordResubscribeOutcome_NoIncompleteWhenFull(t *testing.T) {
	var got float64
	metrics := Metrics{
		ResubscribeIncompleteTotal: func(_, _ string, n float64) { got += n },
	}

	recordResubscribeOutcome(metrics, "spot", 500, 500)

	if got != 0 {
		t.Fatalf("expected no incomplete increment when full, got %v", got)
	}
}
