package candle

import "testing"

// TestMergeLateIntoClosed_ProducesLateTradeCount guards the producer for lateTradeCount.
//
// Without a producer on the ingestion path the field is always 0, and omitempty hides it — which
// makes the silent-discard defect it exists to expose unmeasurable. The merge into a closed
// bucket IS the late event, so that is where the count is produced.
func TestMergeLateIntoClosed_ProducesLateTradeCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		why    string
	}{
		{
			name:   "delta branch",
			source: string(CandleMergeSourceWSDelta),
			why:    "a late ws_delta is one late event",
		},
		{
			name:   "total branch",
			source: string(CandleMergeSourceRESTTotal),
			why:    "per-event counters stay additive in BOTH branches — a total MAX-merges its volumes but is still one late event",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stored := Candle{Volume: 100, TakerBuyVolume: 60}
			late := Candle{Volume: 2, TakerBuyVolume: 1, Source: tt.source}

			first := MergeLateIntoClosed(stored, late, 1)
			if first.LateTradeCount != 1 {
				t.Fatalf("lateTradeCount = %d, want 1 — %s", first.LateTradeCount, tt.why)
			}

			// Additive across successive late updates: the counter accumulates, it does
			// not latch at 1 or reset.
			second := MergeLateIntoClosed(first, late, 2)
			if second.LateTradeCount != 2 {
				t.Fatalf("lateTradeCount = %d after a second late update, want 2", second.LateTradeCount)
			}
		})
	}
}

// TestMergeLateIntoClosed_LateTradeCountIsAdditiveNotMax pins the asymmetry against the
// volume merge: on the TOTAL branch volume MAX-merges, but the event counter must NOT.
// MAX-merging the counter would make it stop advancing as soon as a total arrived,
// silently re-creating the "always zero-ish" condition the producer exists to remove.
func TestMergeLateIntoClosed_LateTradeCountIsAdditiveNotMax(t *testing.T) {
	t.Parallel()

	stored := Candle{Volume: 100, LateTradeCount: 5}
	late := Candle{Volume: 140, Source: string(CandleMergeSourceRESTTotal)}

	got := MergeLateIntoClosed(stored, late, 1)

	if got.Volume != 140 {
		t.Errorf("volume = %v, want 140 — a total MAX-merges", got.Volume)
	}
	if got.LateTradeCount != 6 {
		t.Errorf("lateTradeCount = %d, want 6 — the counter is additive even when the volumes MAX-merge", got.LateTradeCount)
	}
}
