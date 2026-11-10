package parity

import "testing"

func tape(entries ...AggTrade) []AggTrade {
	for i := range entries {
		v, err := asFloat(entries[i].Quantity)
		if err != nil {
			panic(err)
		}
		entries[i].qty = v
	}
	return entries
}

// TestSumTapeByBucket_UsesTheProductionAssignmentRule is the whole point of the harness: the
// fold must place a trade exactly where the live aggregator would place it. If this ever stops
// calling candles.BucketTimestamp the comparison silently becomes a test of a copy.
func TestSumTapeByBucket_UsesTheProductionAssignmentRule(t *testing.T) {
	t.Parallel()

	// A REAL 5m boundary. 1_700_000_000_000 is NOT one (1700000000 % 300 != 0): both lookups would
	// land on a zero value, and the assertions would fail for a reason unrelated to the code under
	// test.
	const b0 = int64(1_699_999_800_000)
	got := SumTapeByBucket(tape(
		AggTrade{ID: 1, Time: b0, Quantity: "1"},           // first instant of the bucket
		AggTrade{ID: 2, Time: b0 + 299_999, Quantity: "2"}, // last millisecond of the bucket
		AggTrade{ID: 3, Time: b0 + 300_000, Quantity: "4"}, // first instant of the NEXT bucket
	), "5m")

	if len(got) != 2 {
		t.Fatalf("expected 2 buckets, got %d — the boundary must split at exactly +300000ms", len(got))
	}
	if got[b0].Volume != 3 {
		t.Errorf("bucket %d volume = %v, want 3 (the closing millisecond belongs to it)", b0, got[b0].Volume)
	}
	if got[b0+300_000].Volume != 4 {
		t.Errorf("bucket %d volume = %v, want 4", b0+300_000, got[b0+300_000].Volume)
	}
}

// TestSumTapeByBucket_TakerSideFollowsBinanceMakerFlag pins the convention. `m` is "the BUYER is
// the maker" — so m==true means the taker SOLD. Getting this backwards would make the harness
// report a taker split that is exactly mirrored, and a mirrored split reads as a real defect.
func TestSumTapeByBucket_TakerSideFollowsBinanceMakerFlag(t *testing.T) {
	t.Parallel()

	const b0 = int64(1_699_999_800_000) // a real 5m boundary
	got := SumTapeByBucket(tape(
		AggTrade{ID: 1, Time: b0, Quantity: "10", IsBuyerM: true}, // buyer is maker → taker SELL
		AggTrade{ID: 2, Time: b0, Quantity: "3", IsBuyerM: false}, // buyer is taker → taker BUY
	), "5m")

	b := got[b0]
	if b.TakerSell != 10 || b.TakerBuy != 3 {
		t.Errorf("takerBuy=%v takerSell=%v, want 3 and 10", b.TakerBuy, b.TakerSell)
	}
	// The identity the exchange satisfies by construction: every trade has exactly one taker.
	if b.TakerBuy+b.TakerSell != b.Volume {
		t.Errorf("buy+sell = %v, must equal volume %v on a tape fold", b.TakerBuy+b.TakerSell, b.Volume)
	}
}

// TestTapeIsContiguous_DetectsAHole guards the PRECONDITION. A tape with a missing id makes a
// bucket sum short for a reason unrelated to bucket assignment, and reading that as a convention
// disagreement is precisely the false conclusion this harness exists to avoid.
func TestTapeIsContiguous_DetectsAHole(t *testing.T) {
	t.Parallel()

	ok, missing := TapeIsContiguous(tape(
		AggTrade{ID: 10, Quantity: "1"},
		AggTrade{ID: 11, Quantity: "1"},
		AggTrade{ID: 13, Quantity: "1"},
	))
	if ok {
		t.Fatal("a gap between id 11 and 13 must be reported, not silently summed")
	}
	if missing != 12 {
		t.Errorf("missing = %d, want 12", missing)
	}

	if ok, _ := TapeIsContiguous(tape(
		AggTrade{ID: 10, Quantity: "1"},
		AggTrade{ID: 11, Quantity: "1"},
	)); !ok {
		t.Error("a dense run must be reported contiguous")
	}
}

// TestTapeIsContiguous_EmptyIsNotContiguous pins the precondition that was vacuous.
//
// An empty tape has nothing to be contiguous ABOUT. Returning true made the completeness check
// pass exactly when the fetch had failed, and the comparison then proceeded against zero trades
// and reported an "irreducible floor" of 0 while the test stayed green.
func TestTapeIsContiguous_EmptyIsNotContiguous(t *testing.T) {
	t.Parallel()

	if ok, _ := TapeIsContiguous(nil); ok {
		t.Fatal("an empty tape must NOT report contiguous — that made the precondition vacuous")
	}
	if ok, _ := TapeIsContiguous(tape(AggTrade{ID: 7, Quantity: "1"})); !ok {
		t.Error("a single trade is trivially contiguous")
	}
}
