package candle

import "testing"

// THE CAUSE IS PART OF THE MARK, SO EVERY MERGE MUST CARRY IT.
//
// Six writers rebuild a bucket from a stored record and an incoming one. Four of them used to
// recompute `HasGap` and write the zero value of `GapKind`, which is `GapUnknown`. The fold refuses
// an unknown cause exactly as it refuses a provisional one, so the first live delta, snapshot or
// late frame on a marked bucket silently re-created the freeze this feature removes — with the
// suite green, because the only assertion was on `HasGap`.
//
// The table drives the same three scenarios through every writer. Scenario B is the discriminating
// one for the writers that copy the stored record: without the shared rule, a stored volume-only
// cause survives and the incoming provisional cause is lost.

func gapWriterNow() int64 { return 1_785_910_000_000 }

func gapStored() Candle {
	return Candle{
		Timestamp: 1_785_909_600_000, Open: 1, High: 1.1, Low: 0.9, Close: 1.05, Volume: 10,
		Source: string(CandleMergeSourceWSKline), KlineEventAt: 10,
	}
}

func gapIncoming() Candle {
	return Candle{
		Timestamp: 1_785_909_600_000, Open: 1, High: 1.2, Low: 0.85, Close: 1.1, Volume: 12,
		Source: string(CandleMergeSourceWSDelta),
	}
}

func TestEveryMergeWriterCarriesTheGapCause(t *testing.T) {
	t.Parallel()

	now := gapWriterNow()
	writers := map[string]func(stored, incoming Candle) Candle{
		"MergeCandleValues/venue": func(stored, incoming Candle) Candle {
			incoming.Source = string(CandleMergeSourceWSKline)
			incoming.KlineEventAt = stored.KlineEventAt + 1
			return MergeCandleValues(stored, incoming, true)
		},
		"MergeCandleValues/values": func(stored, incoming Candle) Candle {
			return MergeCandleValues(stored, incoming, false)
		},
		"MergeOpenVenueFrame": func(stored, incoming Candle) Candle {
			incoming.Source = string(CandleMergeSourceWSKline)
			incoming.KlineEventAt = stored.KlineEventAt + 1
			merged, applied := MergeOpenVenueFrame(stored, incoming, now)
			if !applied {
				t.Fatalf("MergeOpenVenueFrame rejected a newer reading")
			}
			return merged
		},
		"MergeOpenDelta":         func(stored, incoming Candle) Candle { return MergeOpenDelta(stored, incoming, now, false, false) },
		"MergeOpenSnapshot":      func(stored, incoming Candle) Candle { return MergeOpenSnapshot(stored, incoming, now) },
		"MergeDroppedDeltaMarks": func(stored, incoming Candle) Candle { return MergeDroppedDeltaMarks(stored, incoming, now) },
	}

	scenarios := []struct {
		name   string
		mutate func(stored, incoming *Candle)
		want   GapKind
	}{
		{
			name: "volume-only survives an unmarked frame",
			mutate: func(stored, _ *Candle) {
				stored.HasGap, stored.GapKind = true, GapVolumeIncomplete
			},
			want: GapVolumeIncomplete,
		},
		{
			name: "a provisional cause wins over a volume-only one",
			mutate: func(stored, incoming *Candle) {
				stored.HasGap, stored.GapKind = true, GapVolumeIncomplete
				incoming.HasGap, incoming.GapKind = true, GapProvisional
			},
			want: GapProvisional,
		},
		{
			name: "an unrecorded cause is preserved as provisional",
			mutate: func(stored, incoming *Candle) {
				// The stored bucket must NOT be venue-owned here: the ownership veto returns the
				// stored record untouched, and that return says nothing about marks.
				stored.Source = ""
				incoming.HasGap = true
			},
			want: GapProvisional,
		},
	}

	for name, write := range writers {
		for _, sc := range scenarios {
			stored, incoming := gapStored(), gapIncoming()
			sc.mutate(&stored, &incoming)
			got := write(stored, incoming)
			if !got.HasGap {
				t.Errorf("%s / %s: the mark was dropped entirely", name, sc.name)
				continue
			}
			if got.GapKind != sc.want {
				t.Errorf("%s / %s: GapKind = %v, want %v — losing the cause makes the fold refuse "+
					"this child, so its derived parent freezes",
					name, sc.name, got.GapKind, sc.want)
			}
		}
	}
}

// The seed's authoritative branch REPLACES the record, so it is the one path where the cause could
// be taken from the incoming candle by accident. A bucket we marked as volume-short stays that way
// when an unmarked REST total settles it; otherwise the derived parent is blocked forever.
func TestUnionSeedInto_AuthoritativeRecordKeepsAVolumeOnlyCause(t *testing.T) {
	t.Parallel()

	const (
		tfMs   = int64(5 * 60 * 1000)
		bucket = int64(1_785_909_600_000)
	)
	closedAt := bucket + tfMs
	settled := closedAt + KlineSettleGraceMs

	stored := Candle{
		Timestamp: bucket, Open: 1, High: 1.1, Low: 0.9, Close: 1.05, Volume: 10,
		Source: string(CandleMergeSourceWSDelta),
	}
	stored.HasGap, stored.GapKind = true, GapVolumeIncomplete
	incoming := Candle{
		Timestamp: bucket, Open: 1, High: 1.2, Low: 0.85, Close: 1.1, Volume: 12,
		TakerBuyVolume: 7, TakerSellVolume: 5,
		Source: string(CandleMergeSourceRESTTotal), FetchedAt: settled,
	}

	got := UnionSeedInto(stored, incoming, tfMs, settled+1_000)
	if !got.HasGap {
		t.Fatal("the seed dropped the mark")
	}
	if got.GapKind != GapVolumeIncomplete {
		t.Fatalf("GapKind = %v after an unmarked settled record, want GapVolumeIncomplete — "+
			"the cause was taken from the incoming candle, so the bucket stops being foldable", got.GapKind)
	}
}
