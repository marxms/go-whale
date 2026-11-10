package candle

import "testing"

// TestBuildOHLCVC_SameMillisecondTieBrokenByTradeID pins the tie-break on the axis no volume
// tolerance covers.
//
// Exchanges emit many aggregate trades inside the same millisecond. Resolving a tie by position
// in the slice made the open whichever trade the batch listed first and the close whichever it
// listed last — neither is a fact about the market. The magnitudes are tiny and every
// volume-shaped check passes, which is why the defect survived; what is wrong is a PRICE the
// exchange never printed for that bar.
//
// The fixture feeds the burst in REVERSE id order, because a test that feeds it in order cannot
// distinguish "elected by id" from "elected by slice position" — which is the entire question.
func TestBuildOHLCVC_SameMillisecondTieBrokenByTradeID(t *testing.T) {
	t.Parallel()

	const bucket = int64(1_785_909_300_000) // CROSS/USDT 5m 12:35
	const sameMs = bucket + 137_000

	// Five aggregate trades, all in one millisecond, delivered NEWEST-id first.
	trades := []Trade{
		{ID: "38750877", Timestamp: sameMs, Price: 0.09735, Amount: 10, Side: "buy"},
		{ID: "38750876", Timestamp: sameMs, Price: 0.09733, Amount: 10, Side: "buy"},
		{ID: "38750875", Timestamp: sameMs, Price: 0.09732, Amount: 10, Side: "sell"},
		{ID: "38750874", Timestamp: sameMs, Price: 0.09731, Amount: 10, Side: "sell"},
		{ID: "38750873", Timestamp: sameMs, Price: 0.09728, Amount: 10, Side: "buy"},
	}

	built := BuildOHLCVC(trades, "5m")
	if len(built) != 1 {
		t.Fatalf("built %d buckets, want 1: %+v", len(built), built)
	}
	got, ok := built[bucket]
	if !ok {
		t.Fatalf("no bucket at %d; got keys %+v", bucket, built)
	}

	// The exchange's order is by aggregate-trade id: 873 opened the burst, 877 closed it.
	if got.Open != 0.09728 {
		t.Errorf("open = %v, want 0.09728 (id 38750873, the LOWEST id in the tie) — the open was "+
			"taken from slice position, so a batch delivered newest-first reports a price the "+
			"exchange never printed as this bar's open", got.Open)
	}
	if got.Close != 0.09735 {
		t.Errorf("close = %v, want 0.09735 (id 38750877, the HIGHEST id in the tie) — `ts >= latestTs` "+
			"keeps the LAST element of the slice, which is the oldest trade when the batch "+
			"arrives newest-first", got.Close)
	}

	// The rest of the bar must be untouched: this changes WHICH trade is elected, never how
	// much was traded.
	if got.Volume != 50 {
		t.Errorf("volume = %v, want 50 — the election must not move any quantity", got.Volume)
	}
	if got.High != 0.09735 || got.Low != 0.09728 {
		t.Errorf("high/low = %v/%v, want 0.09735/0.09728", got.High, got.Low)
	}
}

// TestTradeIDPrecedes_DegradesSafelyWithoutNumericIDs keeps venues whose ids are opaque at the
// fallback behavior rather than at a worse one. A trade the exchange cannot order must never
// displace one it can.
func TestTradeIDPrecedes_DegradesSafelyWithoutNumericIDs(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		a, b string
		want bool
		why  string
	}{
		{"100", "200", true, "plain numeric ordering"},
		{"200", "100", false, "plain numeric ordering, reversed"},
		{"100", "100", false, "equal ids keep the incumbent, so the election is stable"},
		{"100", "abc", true, "an ordered id outranks an unorderable one"},
		{"abc", "100", false, "an unorderable id never displaces an ordered one"},
		{"abc", "def", false, "neither is orderable, so the incumbent stands"},
		{"", "100", false, "an empty id is not an ordering"},
	} {
		if got := tradeIDPrecedes(tc.a, tc.b); got != tc.want {
			t.Errorf("tradeIDPrecedes(%q, %q) = %v, want %v — %s", tc.a, tc.b, got, tc.want, tc.why)
		}
	}
}
