package candle

import (
	"math"
	"sort"
)

// FoldStep names one timeframe, the one it is built from, and how many of them it takes.
type FoldStep struct {
	TF    string
	Child string
	N     int
}

// FoldChain orders each timeframe after the one it is built from, coarsest-last so a single pass
// propagates a change all the way up.
//
// A week holds 2016 five-minute buckets and the ring holds 800, so 1w cannot be rebuilt from 5m —
// the data is not there. Each timeframe IS an exact multiple of the one below it, so folding step
// by step reads between 2 and 7 buckets per level and reaches 1w with the same exactness:
//
//	15m = 3 x 5m    30m = 2 x 15m    1h = 2 x 30m    2h = 2 x 1h
//	4h  = 2 x 2h    1d  = 6 x 4h     1w = 7 x 1d
func FoldChain() []FoldStep {
	return []FoldStep{
		{"15m", "5m", 3},
		{"30m", "15m", 2},
		{"1h", "30m", 2},
		{"2h", "1h", 2},
		{"4h", "2h", 2},
		{"1d", "4h", 6},
		{"1w", "1d", 7},
	}
}

// FindByTimestamp returns the candle at the exact timestamp, or nil when the ring does not hold
// it.
//
// Rings are ordinarily chronological. A conservative linear fallback also handles an unsorted
// initial seed without treating a missed child as absent.
func FindByTimestamp(buf *RingBuffer[Candle], timestamp int64) *Candle {
	if buf == nil {
		return nil
	}
	lo, hi := 0, buf.Len()
	for lo < hi {
		mid := lo + (hi-lo)/2
		c, _ := buf.At(mid)
		if c.Timestamp == timestamp {
			return &c
		}
		if c.Timestamp < timestamp {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	for i := 0; i < buf.Len(); i++ {
		c, _ := buf.At(i)
		if c.Timestamp == timestamp {
			return &c
		}
	}
	return nil
}

// CollectFoldChildren gathers the children a parent bucket is rebuilt from, and reports whether
// the set is complete.
//
// # TWO KINDS OF PARTIAL, AND ONLY ONE IS A DEFECT
//
// "In progress" and "missing data" both look like a subset of children, and treating them alike
// is wrong in both directions.
//
// A child whose bucket has not STARTED cannot exist, and waiting for it would freeze the open
// parent until it closes — a whole week for 1w, sitting on its boot seed and never moving. That is
// the staleness the fold exists to end.
//
// A child whose bucket HAS started and is absent is missing DATA. Folding over it publishes a
// bucket short by construction and labels it measured — the failure the fold exists to remove,
// arriving dressed as an improvement.
//
// So: every child that has already STARTED must be present; children still in the future are
// simply not part of the parent yet. A missing interval proven to predate the symbol's listing is
// skipped instead of blocking: the venue publishes no candle there either, so it can never arrive.
func CollectFoldChildren(buf *RingBuffer[Candle], childTF string, childCount int, parentTS, now, listingFloor int64) ([]Candle, bool) {
	childMs := ParseTimeframeToSeconds(childTF) * 1000
	if childMs <= 0 {
		return nil, false
	}
	kids := make([]Candle, 0, childCount)
	for i := 0; i < childCount; i++ {
		childTS := parentTS + int64(i)*childMs
		if childTS > now {
			break // not part of the parent yet
		}
		c := FindByTimestamp(buf, childTS)
		if c == nil {
			if listingFloor > 0 && listingFloor <= now && childTS <= listingFloor && listingFloor-childTS >= childMs {
				continue
			}
			// Neither an existing parent nor the ring's oldest child proves when the
			// series began. Preserve the parent until the missing child is known.
			return nil, false
		}
		if c.HasGap {
			return nil, false
		}
		kids = append(kids, *c)
	}
	if len(kids) == 0 {
		return nil, false
	}
	return kids, true
}

// FoldFromChildren rebuilds one parent bucket from its children. It REBUILDS rather than
// accumulates, which is idempotent by construction: running it twice gives the same answer, so no
// delivery guarantee is required of the transport.
//
// PRICES COME FROM CHILDREN THAT TRADED, ALL FOUR OF THEM.
//
// A child in which nothing traded carries the PREVIOUS close on open, high, low and close — four
// prices that never occurred in its own interval. Taking the parent's open from such a child
// reports an opening print that did not happen. Measured against Binance: CRWD/USDT and
// HYUNDAI/USDT 15m failed on PRICE with volume and both taker legs exact to 1e-16 — illiquid
// symbols whose first 5m child of the bucket had no trades.
//
// If NO child traded the bucket genuinely had no activity, and the carried price is then the
// correct thing to report — so the fallback is the first child rather than nothing.
func FoldFromChildren(kids []Candle, parentTS int64) Candle {
	parent := Candle{
		Origin:    OriginDerived,
		Timestamp: parentTS,
		Open:      kids[0].Open,
		Close:     kids[len(kids)-1].Close,
		Low:       math.Inf(1),
		Source:    string(CandleMergeSourceWSKline),
	}
	for _, k := range kids {
		if k.Volume > 0 {
			parent.Open = k.Open
			break
		}
	}
	for i := len(kids) - 1; i >= 0; i-- {
		if kids[i].Volume > 0 {
			parent.Close = kids[i].Close
			break
		}
	}
	for _, k := range kids {
		// An untraded child's high and low are the carried price too, and folding them would drag
		// the parent's range to prints that never happened.
		if k.Volume <= 0 {
			continue
		}
		parent.Volume += k.Volume
		parent.TakerBuyVolume += k.TakerBuyVolume
		parent.TakerSellVolume += k.TakerSellVolume
		parent.SideUnknownCount += k.SideUnknownCount
		parent.High = math.Max(parent.High, k.High)
		// A bucket in which nothing traded carries the previous close on every price field, so its
		// low is a price that never occurred.
		if k.Low > 0 && k.Low < parent.Low {
			parent.Low = k.Low
		}
	}
	// HasGap and the venue clock come from EVERY child, traded or not: an untraded child can still
	// be marked, and its frame still carries a timestamp.
	unknownSource := false
	for _, k := range kids {
		parent.HasGap = parent.HasGap || k.HasGap
		if k.Source == string(CandleMergeSourceRESTTotal) {
			// A REST-derived child cannot become independent WS evidence.
			parent.Source = string(CandleMergeSourceRESTTotal)
		} else if k.Source != string(CandleMergeSourceWSKline) || k.Origin == OriginUnknown {
			unknownSource = true
		}
		if k.KlineEventAt > parent.KlineEventAt {
			parent.KlineEventAt = k.KlineEventAt
		}
	}
	if unknownSource {
		parent.Source = ""
	}
	// No child traded: the bucket had no activity and the carried price is what the venue itself
	// reports for such an interval.
	if math.IsInf(parent.Low, 1) {
		parent.Low = kids[0].Low
		parent.High = kids[0].High
		parent.Open = kids[0].Open
		parent.Close = kids[len(kids)-1].Close
	}
	return parent
}

// SameFoldValues reports whether two candles are equal on the fields a fold preserves, using a
// relative tolerance of 8 ulps. Non-finite values are never equal.
func SameFoldValues(a, b Candle) bool {
	equal := func(x, y float64) bool {
		if math.IsNaN(x) || math.IsNaN(y) || math.IsInf(x, 0) || math.IsInf(y, 0) {
			return false
		}
		return x == y || math.Abs(x-y) <= math.Max(math.Abs(x), math.Abs(y))*8*2.220446049250313e-16
	}
	return equal(a.Open, b.Open) && equal(a.High, b.High) && equal(a.Low, b.Low) && equal(a.Close, b.Close) &&
		equal(a.Volume, b.Volume) && equal(a.TakerBuyVolume, b.TakerBuyVolume) && equal(a.TakerSellVolume, b.TakerSellVolume)
}

// UniqueSortedParentTimestamps maps child timestamps to the parent buckets that contain them, once
// each and oldest-first, so a repair visits every affected parent exactly one time per level.
func UniqueSortedParentTimestamps(parentTF string, timestamps []int64) []int64 {
	unique := make(map[int64]struct{}, len(timestamps))
	for _, ts := range timestamps {
		unique[BucketTimestamp(ts, parentTF)] = struct{}{}
	}
	parents := make([]int64, 0, len(unique))
	for ts := range unique {
		parents = append(parents, ts)
	}
	sort.Slice(parents, func(i, j int) bool { return parents[i] < parents[j] })
	return parents
}
