package candle

import "math"

// VENUE OWNERSHIP.

// VenueHasSpoken reports whether a stored bucket was written by the venue itself, in which case
// no other producer may overwrite it.
//
// It exists as ONE function because the same rule has to hold at every site that merges; stating
// it once per site had already let merge paths diverge, with one path overwriting buckets another
// path protected.
//
// The rule is narrow. It protects a bucket the venue has ALREADY spoken for; it says nothing about
// buckets it has not, so historical seeding is untouched. And it is not symmetric: a venue candle
// replacing a seeded total is correct and wanted, because a snapshot of a number must not overwrite
// the number itself.
// The one exception is a bucket we KNOW we did not hear in full. The venue kline is CUMULATIVE per
// bucket, so an interruption spanning a bucket's close leaves the last total we received standing
// forever — the venue never resends a finished bucket. Its word is then not authoritative about
// that bucket, it is a partial transcript of it, and holding the veto up means the short value is
// final. HasGap is that admission, and the restart path is its only producer.
func VenueHasSpoken(stored, incoming Candle) bool {
	return stored.Source == string(CandleMergeSourceWSKline) &&
		stored.Origin != OriginDerived &&
		incoming.Source != string(CandleMergeSourceWSKline) &&
		!stored.HasGap
}

// IsTotalSource reports whether a candle carries the bucket TOTAL rather than an incremental
// delta. Totals MAX-merge; deltas accumulate. The distinction comes from the DECLARED source,
// never from comparing magnitudes — a delta is routinely smaller than the accumulated bucket, and
// treating "smaller" as "stale" is what silently dropped late trades.
//
// UNLABELLED CANDLES ARE TREATED AS TOTALS, deliberately. Only an explicit ws_delta accumulates.
// The asymmetry is a safety choice: MAX-merging a delta under-counts by that delta, while
// accumulating a total inflates the bucket by the whole total and corrupts taker/CVD downstream.
// Under-count is recoverable; inflation is not. Producers on the live path label ws_delta
// explicitly, so the accumulating branch is reached by intent, never by omission.
func IsTotalSource(source string) bool {
	return CandleMergeSource(source) != CandleMergeSourceWSDelta
}

// MergeCandleValues unions `incoming` into `stored` and returns the result, leaving every field
// that records an EVENT (LateTradeCount, LastUpdate) to the caller.
//
// `total` selects how the monotonic fields combine and is a property of the CALLER's knowledge
// about the payload, not of the values:
//
//	total=true   the payload IS the whole bucket -> MAX (summing would double-count)
//	total=false  the payload is an increment      -> accumulate
//
// CvdCumulative is deliberately untouched: it is an absolute CROSS-bucket level, so merging into
// one bucket must never rewrite the running anchor the later buckets were derived from.
func MergeCandleValues(stored, incoming Candle, total bool) Candle {
	// A late update does not overwrite a bucket the venue itself wrote.
	if VenueHasSpoken(stored, incoming) {
		return stored
	}

	// THE VENUE REPLACES HERE TOO, ORDERED BY ITS OWN CLOCK.
	//
	// A venue frame arriving after the bucket rolled must not be decided by MAGNITUDE: magnitude
	// cannot tell a legitimate restatement from a coincidentally larger number, so a venue
	// revision DOWNWARDS would be discarded in silence. The tail of this function copies
	// `incoming.Source`, so an inflated REST total that won on magnitude could also steal the
	// ws_kline label — after which VenueHasSpoken locks the bucket against every later
	// correction.
	if incoming.Source == string(CandleMergeSourceWSKline) {
		if incoming.KlineEventAt > 0 && incoming.KlineEventAt <= stored.KlineEventAt {
			return stored
		}
		merged := incoming
		merged.CreatedAt = stored.CreatedAt
		merged.HasGap = stored.HasGap || incoming.HasGap
		merged.SideUnknownCount = stored.SideUnknownCount
		merged.LateTradeCount = stored.LateTradeCount
		return merged
	}
	var volume, takerBuy, takerSell float64
	var sideUnknown int64
	if total {
		// THE TRIO IS ELECTED ATOMICALLY, FROM ONE RECORD.
		//
		// Three independent maxima let volume come from one observation and a leg from another,
		// and that combination is the ONLY way takerBuy+takerSell > volume becomes expressible:
		// each producer honors the identity alone. For example, stored{100,60,40} merged with a
		// total{100,45,55} yields volume=100 buy=60 sell=55, a sum of 115 that
		// ValidateCandleAggregates itself rejects.
		volume = math.Max(stored.Volume, incoming.Volume)
		takerBuy, takerSell = incoming.TakerBuyVolume, incoming.TakerSellVolume
		sideUnknown = incoming.SideUnknownCount
		if stored.Volume >= incoming.Volume {
			takerBuy, takerSell = stored.TakerBuyVolume, stored.TakerSellVolume
			sideUnknown = stored.SideUnknownCount
		}
		// A record carrying no split must not erase a measured one.
		if takerBuy == 0 && takerSell == 0 {
			takerBuy, takerSell = stored.TakerBuyVolume, stored.TakerSellVolume
			if takerBuy == 0 && takerSell == 0 {
				takerBuy, takerSell = incoming.TakerBuyVolume, incoming.TakerSellVolume
			}
		}
	} else {
		volume = stored.Volume + incoming.Volume
		takerBuy = stored.TakerBuyVolume + incoming.TakerBuyVolume
		takerSell = stored.TakerSellVolume + incoming.TakerSellVolume
		sideUnknown = stored.SideUnknownCount + incoming.SideUnknownCount
	}

	merged := stored
	merged.Volume = volume
	merged.TakerBuyVolume = takerBuy
	merged.TakerSellVolume = takerSell
	merged.SideUnknownCount = sideUnknown
	// Same rule as the same-bucket merge: a payload that IS the whole bucket carries the
	// bucket's open, so it restates it; an increment never does. `total=true` means "the payload
	// IS the whole bucket" for every monotonic field, Open included.
	merged.Open = FirstNonZero(stored.Open, incoming.Open)
	if total {
		merged.Open = FirstNonZero(incoming.Open, stored.Open)
	}
	merged.High = math.Max(stored.High, incoming.High)
	merged.Low = MinPositive(stored.Low, incoming.Low)
	if incoming.Close != 0 {
		merged.Close = incoming.Close
	}
	merged.HasGap = stored.HasGap || incoming.HasGap
	// THE LABEL IS NOT TRANSFERABLE. Copying incoming.Source blindly would let a REST total that
	// won on magnitude inherit the ws_kline credential from the bucket it overwrote, and
	// VenueHasSpoken would then defend the wrong number as if the venue had stated it.
	if incoming.Source != "" && incoming.Source != string(CandleMergeSourceWSKline) {
		merged.Source = incoming.Source
	}
	return merged
}

// MergeLateIntoClosed merges a late update into an already-closed bucket.
//
// Branch by DECLARED source, never by magnitude:
//   - total (rest_total / snapshot_total): the payload IS the whole bucket, so every monotonic
//     field MAX-merges. MAX (not replace) also protects against a stale total arriving after a
//     better one and regressing the bucket.
//   - delta (ws_delta, or empty which normalises to it): the payload is an increment of this
//     bucket, so the monotonic fields accumulate.
//
// OHLC composes identically in both branches: open is the first non-zero seen, close is the
// newest value, high/low extend outward. Per-bar aggression is not stored here; consumers derive
// it from the merged taker pair.
func MergeLateIntoClosed(stored, late Candle, now int64) Candle {
	merged := MergeCandleValues(stored, late, IsTotalSource(late.Source))
	// THE PRODUCER for lateTradeCount. Additive in BOTH branches on purpose: this merge is one
	// late event regardless of whether the payload was a delta or a total. MAX-merging it
	// alongside the volumes would stop it advancing the moment a total arrived, leaving the
	// counter permanently zero.
	//
	// It lives HERE and not in MergeCandleValues because it counts an EVENT, not a value: a bulk
	// seed repair unions the same fields but is not a late trade, and folding thousands of
	// repaired buckets into this counter would blind it to actual late trades.
	merged.LateTradeCount = stored.LateTradeCount + 1
	merged.LastUpdate = now
	return merged
}

// MergeOpenVenueFrame applies a venue frame that REPLACES the open bucket rather than merging.
//
// A venue candle is not merged, elected or arbitrated — it REPLACES. The venue computed every
// field of the output contract and this process transports them; no other producer writes those
// fields for that symbol, so there is nothing to referee.
//
// Ordering is the venue's OWN clock. Two frames for the same bucket are two readings of one
// running total, so the newer wins — but the SDK dispatches each websocket message on its own
// goroutine, so ARRIVAL order can swap them, and MAGNITUDE cannot tell a legitimate supersession
// from a coincidentally larger number. Equal event timestamps are REJECTED rather than applied:
// a repeat carries no new information.
//
// It reports false when the frame carries no newer reading and the caller must leave the bucket
// alone. Local marks survive because they describe THIS PROCESS's treatment of the bucket, not
// the market.
func MergeOpenVenueFrame(stored, incoming Candle, now int64) (Candle, bool) {
	if incoming.KlineEventAt > 0 && incoming.KlineEventAt <= stored.KlineEventAt {
		return stored, false
	}
	merged := incoming
	merged.CreatedAt = FirstNonZeroInt(stored.CreatedAt, now)
	merged.LastUpdate = now
	// OUR OWN MARKS SURVIVE. They describe this process's treatment of the bucket, not the
	// market, so the venue has nothing to say about them and replacing them with its silence
	// would erase the only evidence the repair path reads.
	merged.HasGap = stored.HasGap || incoming.HasGap
	merged.SideUnknownCount = stored.SideUnknownCount
	merged.LateTradeCount = stored.LateTradeCount
	return merged, true
}

// DeltaInsideTotalHorizon reports whether a live delta is wholly contained by the exchange total
// already standing on the bucket.
//
// An exchange TOTAL reaches up to the instant its snapshot was taken (FetchedAt); our LIVE
// accumulation reaches up to its newest trade (LastTradeAt). The true horizon H is bracketed,
// RequestedAt <= H <= FetchedAt. A delta whose newest trade predates the REQUEST that produced
// the total was certainly counted by it, and adding it again is the double count — the lower end
// is the only sound one here. Using FetchedAt as a lower bound discards trades the total never
// contained and can lose a large share of the volume.
func DeltaInsideTotalHorizon(stored, incoming Candle) bool {
	return stored.RequestedAt > 0 && incoming.LastTradeAt > 0 && incoming.LastTradeAt <= stored.RequestedAt
}

// MergeDroppedDeltaMarks applies the non-flow part of a delta that was already counted by a total.
//
// ONLY THE FLOW IS A DUPLICATE. The rest of the record is not: HasGap is the ONLY trigger of the
// targeted repair, so a batch that was BOTH already-counted AND gap-marked would otherwise
// silence its own repair, and SideUnknownCount is the only evidence that volume was seen with no
// attributable side.
func MergeDroppedDeltaMarks(stored, incoming Candle, now int64) Candle {
	stored.HasGap = stored.HasGap || incoming.HasGap
	stored.SideUnknownCount += incoming.SideUnknownCount
	stored.LastUpdate = now
	return stored
}

// MergeOpenDelta merges a live batch into the bucket that is still OPEN and returns the result.
//
// `supersede` means the payload is a bucket total that replaces the live accumulation instead of
// adding to it; `declaredTotal` is the narrower property "the payload declared itself the whole
// bucket", which also governs the Open election and the tradeless-placeholder rule.
//
// THE TRIO IS ELECTED ATOMICALLY, FROM ONE RECORD. The retained pair always belongs to a record
// whose own volume is <= the elected volume, so takerBuy+takerSell <= volume holds by
// construction rather than by luck.
//
// A TOTAL IS ELECTED, NEVER ADDED. The windows covered by the live accumulation and by the total
// OVERLAP, so summing them counts the overlap twice; max() can under-count the tail, and
// under-counting is a deficit the gate sees, while inflation is permanent.
func MergeOpenDelta(stored, incoming Candle, now int64, supersede, declaredTotal bool) Candle {
	var volume float64
	if supersede {
		volume = math.Max(stored.Volume, incoming.Volume)
	} else {
		volume = stored.Volume + incoming.Volume
	}

	var takerBuyVolume, takerSellVolume float64
	// The horizon travels with the record whose measurement WON, never independently.
	requestedAtMerged := stored.RequestedAt
	if supersede {
		takerBuyVolume, takerSellVolume = incoming.TakerBuyVolume, incoming.TakerSellVolume
		requestedAtMerged = incoming.RequestedAt
		if stored.Volume >= incoming.Volume {
			takerBuyVolume, takerSellVolume = stored.TakerBuyVolume, stored.TakerSellVolume
			requestedAtMerged = stored.RequestedAt
		}
		// A RECORD WITH NO SPLIT MUST NOT ERASE A MEASURED ONE.
		if takerBuyVolume == 0 && takerSellVolume == 0 {
			takerBuyVolume, takerSellVolume = stored.TakerBuyVolume, stored.TakerSellVolume
			if takerBuyVolume == 0 && takerSellVolume == 0 {
				takerBuyVolume, takerSellVolume = incoming.TakerBuyVolume, incoming.TakerSellVolume
			}
		}
	} else {
		takerBuyVolume = stored.TakerBuyVolume + incoming.TakerBuyVolume
		takerSellVolume = stored.TakerSellVolume + incoming.TakerSellVolume
	}

	// Only a DECLARED total may restate the open: within a bucket the first trade sets it, so an
	// increment must leave it alone, and an unlabelled payload has unknown provenance and cannot
	// claim to carry it.
	openMerged := FirstNonZero(stored.Open, incoming.Open)
	if declaredTotal {
		openMerged = FirstNonZero(incoming.Open, stored.Open)
	}
	highMerged := math.Max(stored.High, incoming.High)
	lowMerged := MinPositive(stored.Low, incoming.Low)
	// A BUCKET THAT HAD NO TRADES CANNOT DICTATE THE PRICES OF ONE THAT DOES. A REST kline for a
	// bucket in which nothing traded carries the PREVIOUS close and volume 0; the first REAL
	// trade owns the open, and the placeholder's high/low go with it.
	if stored.Volume <= 0 && !declaredTotal && incoming.Volume > 0 {
		openMerged = incoming.Open
		highMerged, lowMerged = incoming.High, incoming.Low
	}

	sourceMerged := stored.Source
	if incoming.Source != "" {
		sourceMerged = incoming.Source
	}

	return Candle{
		Timestamp:        incoming.Timestamp,
		Open:             openMerged,
		High:             highMerged,
		Low:              lowMerged,
		Close:            incoming.Close,
		Volume:           volume,
		CreatedAt:        FirstNonZeroInt(stored.CreatedAt, now),
		LastUpdate:       now,
		TakerBuyVolume:   takerBuyVolume,
		TakerSellVolume:  takerSellVolume,
		SideUnknownCount: stored.SideUnknownCount + incoming.SideUnknownCount,
		Source:           sourceMerged,
		AggregateMethod:  incoming.AggregateMethod,
		LateTradeCount:   stored.LateTradeCount + incoming.LateTradeCount,
		HasGap:           stored.HasGap || incoming.HasGap,

		// THE TWO CLOCKS, CARRIED FORWARD. Without them the bucket forgets how far the exchange
		// total it already absorbed reached, so the NEXT delta cannot be judged against it and is
		// summed on top — the double count. The horizon must not go backwards when a delta with
		// no horizon arrives, and the reach must not go backwards when a total with no trade
		// time does.
		FetchedAt:   MaxInt64(stored.FetchedAt, incoming.FetchedAt),
		RequestedAt: requestedAtMerged,
		LastTradeAt: MaxInt64(stored.LastTradeAt, incoming.LastTradeAt),
	}
}

// MergeOpenSnapshot merges a REST snapshot into the bucket that is still open. REST candles carry
// the bucket TOTAL, so every side MAX-merges against the accumulated live value and never
// double-counts; the Close prefers the accumulated one because a snapshot is fetched with a delay
// and can be stale.
//
// A MERGE MAY NOT SUBTRACT INFORMATION. HasGap, SideUnknownCount and LateTradeCount describe OUR
// treatment of the bucket, and a REST kline has nothing to say about them, so their accumulated
// values survive.
func MergeOpenSnapshot(stored, incoming Candle, now int64) Candle {
	volumeMerged := math.Max(stored.Volume, incoming.Volume)
	requestedAtMerged := incoming.RequestedAt
	takerBuyVolume, takerSellVolume := incoming.TakerBuyVolume, incoming.TakerSellVolume
	if stored.Volume >= incoming.Volume {
		takerBuyVolume, takerSellVolume = stored.TakerBuyVolume, stored.TakerSellVolume
		requestedAtMerged = stored.RequestedAt
	}
	// A record carrying no split must not erase a measured one.
	if takerBuyVolume == 0 && takerSellVolume == 0 {
		takerBuyVolume, takerSellVolume = stored.TakerBuyVolume, stored.TakerSellVolume
		if takerBuyVolume == 0 && takerSellVolume == 0 {
			takerBuyVolume, takerSellVolume = incoming.TakerBuyVolume, incoming.TakerSellVolume
		}
	}

	return Candle{
		Timestamp:        incoming.Timestamp,
		Open:             FirstNonZero(incoming.Open, stored.Open),
		High:             math.Max(stored.High, incoming.High),
		Low:              MinPositive(stored.Low, incoming.Low),
		Close:            FirstNonZero(stored.Close, incoming.Close),
		Volume:           volumeMerged,
		CreatedAt:        FirstNonZeroInt(stored.CreatedAt, now),
		LastUpdate:       now,
		TakerBuyVolume:   takerBuyVolume,
		TakerSellVolume:  takerSellVolume,
		SideUnknownCount: MaxInt64(stored.SideUnknownCount, incoming.SideUnknownCount),
		LateTradeCount:   MaxInt64(stored.LateTradeCount, incoming.LateTradeCount),
		HasGap:           stored.HasGap || incoming.HasGap,
		AggregateMethod:  FirstNonEmpty(incoming.AggregateMethod, stored.AggregateMethod),
		Source:           FirstNonEmpty(incoming.Source, stored.Source),

		// The two clocks, for the same reason as the delta merge: without the horizon on the
		// bucket the NEXT live delta cannot be judged against the total just installed.
		FetchedAt:   MaxInt64(stored.FetchedAt, incoming.FetchedAt),
		RequestedAt: requestedAtMerged,
		LastTradeAt: MaxInt64(stored.LastTradeAt, incoming.LastTradeAt),
	}
}

// InsertCandleSorted inserts c into buf at its timestamp order and rebuilds the ring, keeping the
// newest Cap elements. It returns the logical insertion index and how many elements were dropped
// from the front, so the caller can distinguish an insertion that survived from one the trim
// discarded.
func InsertCandleSorted(buf *RingBuffer[Candle], c Candle, now int64) (insertAt, evictedPrefix int) {
	all := buf.Slice()
	insertAt = -1
	var newSlice []Candle
	for _, existing := range all {
		if insertAt < 0 && c.Timestamp < existing.Timestamp {
			if c.CreatedAt == 0 {
				c.CreatedAt = now
			}
			c.LastUpdate = now
			insertAt = len(newSlice)
			newSlice = append(newSlice, c)
		}
		newSlice = append(newSlice, existing)
	}
	if insertAt < 0 {
		if c.CreatedAt == 0 {
			c.CreatedAt = now
		}
		c.LastUpdate = now
		insertAt = len(newSlice)
		newSlice = append(newSlice, c)
	}

	// Rebuild buffer (keep only the last cap elements).
	capacity := buf.Cap()
	if len(newSlice) > capacity {
		evictedPrefix = len(newSlice) - capacity
		newSlice = newSlice[evictedPrefix:]
	}
	buf.Reset()
	for _, item := range newSlice {
		buf.Push(item)
	}
	return insertAt, evictedPrefix
}

// --- numeric helpers shared by every merge site ---

// FirstNonZero returns a if it is non-zero, else b.
func FirstNonZero(a, b float64) float64 {
	if a != 0 {
		return a
	}
	return b
}

// FirstNonZeroInt returns a if it is non-zero, else b.
func FirstNonZeroInt(a, b int64) int64 {
	if a != 0 {
		return a
	}
	return b
}

// MaxInt64 returns the larger of a and b. Used for MAX-merging integer counters when a REST
// snapshot (carrying the bucket TOTAL) merges into the accumulated live bucket.
func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// FirstNonEmpty returns a if it is non-empty, else b.
func FirstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// MinPositive returns the smaller positive value, treating zero as "not measured" rather than as
// a price that occurred.
func MinPositive(a, b float64) float64 {
	if a == 0 {
		return b
	}
	if b == 0 {
		return a
	}
	return math.Min(a, b)
}
