package parity

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"

	// The PRODUCTION bucket rule, imported deliberately.
	//
	// The rest of this package avoids depending on the system it measures, and infers the bucket
	// width from the sample instead (see bucketStep). That isolation is right for G1 and WRONG
	// here: this comparison exists to put OUR ASSIGNMENT RULE on trial. Re-implementing it
	// locally would test the copy and leave the original unexamined, which is how a convention
	// bug survives a gate that appears to cover it.
	"github.com/marxms/go-whale/internal/candle"
)

// TAPE CONSERVATION — the ruler that decides whether our BUCKET ASSIGNMENT agrees with Binance's.
//
// # WHY THIS EXISTS AND WHY NO EXISTING GATE ANSWERS IT
//
// G1 compares our stored candle against Binance's kline. When they disagree it cannot say WHICH
// of two very different things happened:
//
//	(1) we lost or double-counted flow                  — our defect, fixable in our code
//	(2) we assigned a trade to a different bucket than
//	    Binance assigned the same trade               — a CONVENTION disagreement
//
// Those demand opposite responses, and the difference is worth a great deal: if (2) is real then
// a digit-exact mirror is UNREACHABLE by aggregating the tape ourselves, no matter how correct
// our accounting becomes. If (2) is absent, every divergence is ours and therefore fixable.
//
// The comment at the top of this package asserts (2) is real and irreducible — it calls the
// boundary straddle "the resolution limit of the input", measured at ~4,2% of boundaries, and
// that assertion is what justifies WindowTolerance existing at all. It has never been tested
// directly. A tolerance constant that encodes an untested assumption is indistinguishable from
// an off-by-one nobody looked at.
//
// # THE TEST
//
// Binance publishes the raw aggregate-trade tape. Sum it into buckets with OUR rule and compare
// each bucket against Binance's OWN kline for the same bucket:
//
//	Σ q  over aggTrades where BucketTimestamp(T) == B     versus     kline[B].volume
//
// Both sides come from the exchange. WE ARE NOT IN THIS COMPARISON — no candle of ours is read.
// So it isolates the assignment rule and nothing else:
//
//	equal    → Binance buckets by trade time exactly as we do. The straddle is NOT a resolution
//	           limit, no boundary group is displaced, and every G1 divergence is our own defect.
//	unequal  → the conventions genuinely differ. The residual is the real, irreducible floor, and
//	           it is measured instead of assumed.
//
// It is pure observation on public read-only endpoints — no reconciliation, no repair, no write.
// Nothing here may ever be wired into an ingestion path.

// TapeBucket is the tape summed into one bucket by OUR assignment rule.
type TapeBucket struct {
	Timestamp int64
	Volume    float64
	TakerBuy  float64
	TakerSell float64
	Trades    int
	FirstID   int64
	LastID    int64
}

// AggTrade is the subset of Binance's aggregate-trade record this comparison needs.
type AggTrade struct {
	ID       int64   `json:"a"`
	Price    string  `json:"p"`
	Quantity string  `json:"q"`
	First    int64   `json:"f"`
	Last     int64   `json:"l"`
	Time     int64   `json:"T"`
	IsBuyerM bool    `json:"m"`
	qty      float64 // parsed once by parseAggTrades
}

// SumTapeByBucket folds the tape into buckets using the SAME BucketTimestamp rule the live
// aggregator uses. Any divergence from the kline is therefore attributable to that rule and to
// nothing else in our system.
//
// The taker split follows Binance's own convention: `m` is "the buyer is the maker", so m==true
// means the TAKER sold.
func SumTapeByBucket(trades []AggTrade, timeframe string) map[int64]TapeBucket {
	out := make(map[int64]TapeBucket)
	for _, t := range trades {
		b := candle.BucketTimestamp(t.Time, timeframe)
		agg := out[b]
		if agg.Trades == 0 {
			agg.Timestamp = b
			agg.FirstID = t.ID
		}
		agg.Volume += t.qty
		if t.IsBuyerM {
			agg.TakerSell += t.qty
		} else {
			agg.TakerBuy += t.qty
		}
		agg.Trades++
		agg.LastID = t.ID
		out[b] = agg
	}
	return out
}

// TapeIsContiguous reports whether the fetched tape has every aggregate-trade id in its range.
//
// This is a precondition on the MEASUREMENT, not a claim about our ingestion. A tape with a hole
// would make a bucket sum short for a reason that has nothing to do with bucket assignment, and
// reading that as a convention disagreement is exactly the kind of false conclusion this
// investigation has already produced several times.
func TapeIsContiguous(trades []AggTrade) (bool, int64) {
	// An EMPTY tape is not a contiguous one — there is nothing to be contiguous about. Returning
	// true here made the precondition vacuous exactly when it mattered: a fetch that found nothing
	// passed the completeness check and the comparison proceeded against zero trades.
	if len(trades) == 0 {
		return false, 0
	}
	if len(trades) == 1 {
		return true, 0
	}
	for i := 1; i < len(trades); i++ {
		if trades[i].ID != trades[i-1].ID+1 {
			return false, trades[i-1].ID + 1
		}
	}
	return true, 0
}

// FetchAggTrades pages the aggregate-trade tape forward by id until it passes endTime.
//
// Pagination is by fromId rather than by startTime/endTime on every call BECAUSE ONLY THE ID
// WALK IS PROVABLY COMPLETE: a time-windowed request silently truncates at the 1000-row limit
// and returns a prefix that looks like a full answer. Walking ids makes any hole visible as a
// discontinuity, which TapeIsContiguous then asserts on.
func FetchAggTrades(client *http.Client, baseURL, rawSymbol string, startTime, endTime int64, maxPages int) ([]AggTrade, error) {
	const limit = 1000

	// ANCHOR OVER THE REAL WINDOW, NOT A FIXED MINUTE.
	//
	// Asking for the first trade in [startTime, startTime+60s] fails on an illiquid symbol: nothing
	// trades in that minute, the anchor comes back empty, and this function returns (nil, nil) — an
	// EMPTY TAPE WITH NO ERROR. Everything downstream then reads as agreement about nothing:
	// TapeIsContiguous says true for an empty slice, every bucket folds to zero, and the harness
	// reports a displacement rate and an "irreducible floor" of 0 while PASSING. Measured on
	// HIMS/USDT, which had traded — 11 trades, volume 32.72 — but whose first trade fell 83.6 s
	// after startTime, outside the anchor.
	//
	// Both headline outputs of this instrument were fabricable by a fetch artifact, with the test
	// green. The window is now the one actually being measured, capped at 55 minutes because
	// Binance rejects an aggTrades time range beyond an hour.
	anchorEnd := endTime
	if anchorEnd-startTime > 55*60_000 {
		anchorEnd = startTime + 55*60_000
	}
	first, err := fetchAggPage(client, baseURL, rawSymbol, 0, startTime, anchorEnd, 1)
	if err != nil {
		return nil, fmt.Errorf("anchor: %w", err)
	}
	if len(first) == 0 {
		// Genuinely nothing traded in the anchor span. The CALLER decides whether that is
		// legitimate, by checking the exchange's own klines for volume — this function must not
		// guess, and must not present an artifact as a measurement.
		return nil, nil
	}

	out := make([]AggTrade, 0, limit)
	fromID := first[0].ID
	for page := 0; page < maxPages; page++ {
		batch, err := fetchAggPage(client, baseURL, rawSymbol, fromID, 0, 0, limit)
		if err != nil {
			return nil, fmt.Errorf("page %d (fromId=%d): %w", page, fromID, err)
		}
		if len(batch) == 0 {
			break
		}
		done := false
		for _, t := range batch {
			if t.Time >= endTime {
				done = true
				break
			}
			out = append(out, t)
		}
		if done {
			return out, nil
		}
		fromID = batch[len(batch)-1].ID + 1
	}
	return out, fmt.Errorf("tape did not reach endTime within %d pages — the window is INCOMPLETE "+
		"and must not be compared; raise maxPages or narrow the window", maxPages)
}

func fetchAggPage(client *http.Client, baseURL, rawSymbol string, fromID, startTime, endTime int64, limit int) ([]AggTrade, error) {
	q := url.Values{}
	q.Set("symbol", rawSymbol)
	q.Set("limit", fmt.Sprintf("%d", limit))
	if fromID > 0 {
		q.Set("fromId", fmt.Sprintf("%d", fromID))
	}
	if startTime > 0 {
		q.Set("startTime", fmt.Sprintf("%d", startTime))
		q.Set("endTime", fmt.Sprintf("%d", endTime))
	}
	u := fmt.Sprintf("%s/fapi/v1/aggTrades?%s", baseURL, q.Encode())

	var raw []AggTrade
	if err := getJSON(client, u, &raw); err != nil {
		return nil, err
	}
	return parseAggTrades(raw)
}

func parseAggTrades(raw []AggTrade) ([]AggTrade, error) {
	for i := range raw {
		v, err := asFloat(raw[i].Quantity)
		if err != nil {
			return nil, fmt.Errorf("aggTrade %d qty %q: %w", raw[i].ID, raw[i].Quantity, err)
		}
		raw[i].qty = v
	}
	sort.Slice(raw, func(i, j int) bool { return raw[i].ID < raw[j].ID })
	return raw, nil
}
