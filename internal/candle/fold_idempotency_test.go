package candle

import "testing"

// The fold and the seed union are REBUILDS, not accumulations. That is the property that removes
// the exactly-once requirement from the transport: a venue resend, a restart or a retry may apply
// the same input twice, and the answer must not move. These tests pin determinism (same input,
// same output) and idempotence (applying the result again is a no-op) for the pure functions that
// document the property.

func foldKids(base int64) []Candle {
	return []Candle{
		{Timestamp: base, Open: 100, High: 110, Low: 95, Close: 105, Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4, Source: string(CandleMergeSourceWSKline), Origin: OriginNative, KlineEventAt: 1},
		{Timestamp: base + 300_000, Open: 105, High: 120, Low: 100, Close: 115, Volume: 20, TakerBuyVolume: 12, TakerSellVolume: 8, Source: string(CandleMergeSourceWSKline), Origin: OriginNative, KlineEventAt: 2},
		{Timestamp: base + 600_000, Open: 115, High: 118, Low: 90, Close: 92, Volume: 30, TakerBuyVolume: 18, TakerSellVolume: 12, Source: string(CandleMergeSourceWSKline), Origin: OriginNative, KlineEventAt: 3},
	}
}

func TestFoldFromChildren_IsDeterministicAndIdempotent(t *testing.T) {
	t.Parallel()

	const base = int64(1_700_000_100_000)
	kids := foldKids(base)

	first := FoldFromChildren(kids, base)
	second := FoldFromChildren(kids, base)
	if first != second {
		t.Fatalf("two folds of the same children differ:\n first=%+v\nsecond=%+v", first, second)
	}

	// Applying the parent as the fold's own input again must be a no-op: the function derives
	// every value from its arguments, and nothing is accumulated into a previous result.
	if again := FoldFromChildren(kids, base); again != first {
		t.Fatalf("repeated fold moved the parent: %+v -> %+v", first, again)
	}
	if !SameFoldValues(first, second) {
		t.Fatalf("fold is not stable under SameFoldValues: %+v vs %+v", first, second)
	}
}

func TestMergeBatchIntoBuffer_IsIdempotent(t *testing.T) {
	t.Parallel()

	buf := NewRingBuffer[Candle](16)
	buf.Push(Candle{Timestamp: 10, Close: 100, Volume: 5})
	buf.Push(Candle{Timestamp: 20, Close: 101, Volume: 6})

	batch := []Candle{
		{Timestamp: 10, Close: 100, Volume: 7, TakerBuyVolume: 4, TakerSellVolume: 3, Source: string(CandleMergeSourceRESTTotal)},
		{Timestamp: 20, Close: 101, Volume: 8, TakerBuyVolume: 5, TakerSellVolume: 3, Source: string(CandleMergeSourceRESTTotal)},
		{Timestamp: 30, Close: 102, Volume: 9, TakerBuyVolume: 6, TakerSellVolume: 3, Source: string(CandleMergeSourceRESTTotal)},
	}
	now := int64(1_000)

	MergeBatchIntoBuffer(buf, batch, KlineSettleGraceMs, now)
	afterFirst := buf.Slice()

	stats := MergeBatchIntoBuffer(buf, batch, KlineSettleGraceMs, now)
	afterSecond := buf.Slice()

	if len(afterFirst) != len(afterSecond) {
		t.Fatalf("second fold changed buffer depth: %d -> %d", len(afterFirst), len(afterSecond))
	}
	for i := range afterFirst {
		if afterFirst[i] != afterSecond[i] {
			t.Fatalf("bucket %d moved on a repeated fold:\n first=%+v\nsecond=%+v",
				i, afterFirst[i], afterSecond[i])
		}
	}
	if stats.Inserted != 0 || stats.Merged != len(batch) {
		t.Fatalf("second fold reported inserted=%d merged=%d, want 0/%d — a rebuild must be a no-op",
			stats.Inserted, stats.Merged, len(batch))
	}
}

func TestUnionSeedInto_IsIdempotent(t *testing.T) {
	t.Parallel()

	const tfMs = int64(5 * 60 * 1000)
	const bucket = int64(1_700_000_100_000)
	stored := Candle{Timestamp: bucket, Open: 1, High: 1.1, Low: 0.9, Close: 1.05, Volume: 100, TakerBuyVolume: 60, TakerSellVolume: 40}
	incoming := Candle{Timestamp: bucket, Open: 1, High: 1.15, Low: 0.85, Close: 1.05, Volume: 120, TakerBuyVolume: 70, TakerSellVolume: 50, Source: string(CandleMergeSourceRESTTotal), FetchedAt: bucket + tfMs + KlineSettleGraceMs}
	now := bucket + tfMs + KlineSettleGraceMs + 1_000

	once := UnionSeedInto(stored, incoming, tfMs, now)
	twice := UnionSeedInto(once, incoming, tfMs, now)
	if once != twice {
		t.Fatalf("union moved on a repeated application:\n once=%+v\ntwice=%+v", once, twice)
	}
}

func TestCollectFoldChildren_ReportsMissingStartedChild(t *testing.T) {
	t.Parallel()

	const base = int64(1_700_000_100_000)
	kids := foldKids(base)
	buf := NewRingBuffer[Candle](16)
	buf.Push(kids[0])
	buf.Push(kids[2]) // middle child started and is missing

	if _, ok := CollectFoldChildren(buf, "5m", 3, base, base+3*300_000, 0); ok {
		t.Fatal("a started-but-missing child was folded over; the parent would be short by construction")
	}
}

func TestCollectFoldChildren_SkipsIntervalsProvenBeforeListing(t *testing.T) {
	t.Parallel()

	const base = int64(1_700_000_100_000)
	buf := NewRingBuffer[Candle](16)
	buf.Push(foldKids(base)[2])

	// The listing floor is the third child's open: the first two intervals predate it entirely
	// and cannot exist at the venue either, so they are skipped rather than treated as missing
	// data.
	floor := base + 600_000
	got, ok := CollectFoldChildren(buf, "5m", 3, base, base+3*300_000, floor)
	if !ok {
		t.Fatal("proven pre-listing intervals blocked the fold")
	}
	if len(got) != 1 || got[0].Timestamp != base+600_000 {
		t.Fatalf("children = %+v, want only the post-listing bucket", got)
	}
}
