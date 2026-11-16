package candle

import "testing"

// A venue frame replaces the bucket's VALUES, but it says nothing about WHY this process marked
// the bucket. If the merge drops the stored cause, a volume-only mark degrades into an unknown
// one and the fold refuses the child — the derived parent freezes again.
// this feature exists to remove. The cause has to survive the LIVE path, not only the mark that
// set it.
func TestMergeOpenVenueFrame_PreservesTheStoredGapCause(t *testing.T) {
	t.Parallel()

	const bucket = int64(1_785_909_600_000)
	stored := Candle{
		Timestamp: bucket, Open: 1, High: 1.1, Low: 0.9, Close: 1.05, Volume: 10,
		HasGap: true, GapKind: GapVolumeIncomplete, KlineEventAt: 10,
	}
	incoming := Candle{
		Timestamp: bucket, Open: 1, High: 1.2, Low: 0.85, Close: 1.1, Volume: 12,
		Source: string(CandleMergeSourceWSKline), KlineEventAt: 11,
	}

	merged, applied := MergeOpenVenueFrame(stored, incoming, bucket+90_000)
	if !applied {
		t.Fatal("a newer venue reading was rejected")
	}
	if !merged.HasGap {
		t.Fatal("the local mark did not survive the venue frame")
	}
	if merged.GapKind != GapVolumeIncomplete {
		t.Fatalf("GapKind = %v after a venue frame, want GapVolumeIncomplete — erasing the cause "+
			"makes the fold refuse this child, so its parent freezes exactly as it did before the fix",
			merged.GapKind)
	}
}

// The opposite direction must NOT be papered over: a provisional mark (an authoritative record
// that erased a live measurement) may be wrong in the prices, so it stays provisional through the
// venue frame and keeps its parent blocked until the targeted re-read confirms it.
func TestMergeOpenVenueFrame_KeepsAProvisionalMarkProvisional(t *testing.T) {
	t.Parallel()

	const bucket = int64(1_785_909_900_000)
	stored := Candle{
		Timestamp: bucket, Open: 1, High: 1.1, Low: 0.9, Close: 1.05, Volume: 10,
		HasGap: true, GapKind: GapProvisional, KlineEventAt: 10,
	}
	incoming := Candle{
		Timestamp: bucket, Open: 1, High: 1.2, Low: 0.85, Close: 1.1, Volume: 12,
		Source: string(CandleMergeSourceWSKline), KlineEventAt: 11,
	}

	merged, applied := MergeOpenVenueFrame(stored, incoming, bucket+90_000)
	if !applied {
		t.Fatal("a newer venue reading was rejected")
	}
	if merged.GapKind != GapProvisional {
		t.Fatalf("GapKind = %v after a venue frame, want GapProvisional — a provisional price "+
			"must not be laundered into a settled one by a later frame", merged.GapKind)
	}
}
