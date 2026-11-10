package candle

import (
	"cmp"
	"slices"
)

// KlineSettleGraceMs is how long after a bucket closes an exchange kline may still be
// incomplete: measured convergence is under 5s, and being early installs a wrong number
// permanently while being late costs one cycle.
const KlineSettleGraceMs = 10_000

// SeedFoldStats is what a fold observed, so reporting can happen after the caller releases the
// buffer lock.
type SeedFoldStats struct {
	Merged   int
	Inserted int
	// Exceeded counts buckets where the stored total was greater and no authority settled it.
	Exceeded int
	// Healed counts buckets an authoritative record corrected upward.
	Healed int
	// Lowered counts buckets an authoritative record moved downward; it is reported separately
	// because only the operator can tell an over-count from a short record.
	Lowered int
	// Unsorted records that the batch violated its documented oldest-first order.
	Unsorted bool
	// BufferLen is the depth the buffer ended at.
	BufferLen int
}

// MergeBatchIntoBuffer folds a seed/repair batch into a live buffer and reports what it saw.
//
// A single merge-join over sorted sequences is O(n+m); per-bucket merges would rebuild the ring
// for every insert. A seed batch carries bucket totals, never increments, so it MAX-merges
// unconditionally rather than dispatching on the persisted source, which could sum a total twice.
// MAX is a ratchet: an inflated value survives later repairs, but the measured failure is missing
// buckets, not inflated ones. See SeedFoldStats.Exceeded.
//
// The caller holds the buffer's write lock.
func MergeBatchIntoBuffer(buf *RingBuffer[Candle], batch []Candle, tfMs, now int64, allowUnknown ...bool) SeedFoldStats {
	var stats SeedFoldStats
	merge := func(stored, incoming Candle) Candle {
		if len(allowUnknown) > 0 && allowUnknown[0] && stored.Origin == OriginUnknown && stored.Source == string(CandleMergeSourceWSKline) && incoming.Source == string(CandleMergeSourceRESTTotal) && incoming.RequestedAt > 0 && incoming.RequestedAt <= incoming.FetchedAt && incoming.FetchedAt <= now && IsAuthoritativeClosedTotal(incoming, tfMs, now) {
			// This scope's feed is finer, so a legacy label cannot veto a settled venue record;
			// the old candle keeps no origin.
			stored.Source = ""
		}
		return UnionSeedInto(stored, incoming, tfMs, now)
	}
	if len(batch) == 0 {
		return stats
	}

	// The merge-join needs sorted input, which callers document rather than the type enforce.
	// Checking is O(m); a violation sorts a copy, since the caller also writes this batch to cold
	// and Redis.
	if !slices.IsSortedFunc(batch, byTimestamp) {
		stats.Unsorted = true
		batch = slices.Clone(batch)
		slices.SortFunc(batch, byTimestamp)
	}

	existing := buf.Slice() // oldest -> newest
	out := make([]Candle, 0, len(existing)+len(batch))

	// A batch bucket either folds onto the one just emitted (a duplicate in the batch) or lands as
	// a new entry, preserving the one-candle-per-bucket invariant.
	takeFromBatch := func(c Candle) {
		if n := len(out); n > 0 && out[n-1].Timestamp == c.Timestamp {
			out[n-1] = merge(out[n-1], c)
			return
		}
		if c.CreatedAt == 0 {
			c.CreatedAt = now
		}
		c.LastUpdate = now
		out = append(out, c)
		stats.Inserted++
	}

	i, j := 0, 0
	for i < len(existing) && j < len(batch) {
		switch {
		case existing[i].Timestamp < batch[j].Timestamp:
			out = append(out, existing[i])
			i++
		case existing[i].Timestamp > batch[j].Timestamp:
			takeFromBatch(batch[j])
			j++
		default:
			if IsAuthoritativeClosedTotal(batch[j], tfMs, now) {
				// Direction matters: the authoritative branch replaces in either direction, so
				// the path that can lower a value needs its own counter.
				switch {
				case existing[i].Volume > batch[j].Volume:
					stats.Lowered++
				case existing[i].Volume < batch[j].Volume:
					stats.Healed++
				}
			} else if existing[i].Volume > batch[j].Volume {
				stats.Exceeded++
			}
			out = append(out, merge(existing[i], batch[j]))
			stats.Merged++
			i, j = i+1, j+1
		}
	}
	for ; i < len(existing); i++ {
		out = append(out, existing[i])
	}
	for ; j < len(batch); j++ {
		takeFromBatch(batch[j])
	}

	// The union can exceed the ring: keep the NEWEST capacity buckets, which is the window every
	// consumer reads from.
	if capacity := buf.Cap(); len(out) > capacity {
		out = out[len(out)-capacity:]
	}

	buf.Reset()
	for k := range out {
		buf.Push(out[k])
	}
	stats.BufferLen = buf.Len()

	return stats
}

func byTimestamp(a, b Candle) int { return cmp.Compare(a.Timestamp, b.Timestamp) }

// UnionSeedInto merges one batch bucket into the stored one so the result carries every piece of
// information either side had. A seed must be MONOTONIC IN INFORMATION: it exists to add the
// buckets the stream missed, and it may not subtract detail from the buckets the stream got right.
//
// The union has no absolute level to reconcile, and that is the point. Adopting a batch's
// cumulative CVD level would move one bucket while its already-anchored successor stayed put;
// refusing would leave a level that disagreed with the chain around it. The per-bucket taker pair
// means the same thing regardless of what preceded it, so a union of two buckets is just a union
// of two measurements.
func UnionSeedInto(stored, incoming Candle, tfMs, now int64) Candle {
	// A seed does not overwrite a bucket the venue itself wrote.
	if VenueHasSpoken(stored, incoming) {
		return stored
	}
	// AUTHORITY SETTLES IT. Where the exchange's own record for a finished bucket is available,
	// there is nothing to merge: our value is a reconstruction OF that record, and a
	// reconstruction can be wrong in either direction.
	if IsAuthoritativeClosedTotal(incoming, tfMs, now) {
		healed := incoming
		// Local bookkeeping survives — it describes OUR handling of the bucket, not the market's
		// activity in it, so the exchange has nothing to say about it.
		healed.CreatedAt = FirstNonZeroInt(stored.CreatedAt, now)
		healed.LateTradeCount = stored.LateTradeCount
		healed.HasGap = stored.HasGap || incoming.HasGap
		// A second independent settled REST read can confirm a previous marked replacement. Its
		// request must start after the first response arrived; duplicate or overlapping requests
		// cannot clear the mark.
		if stored.HasGap && !incoming.HasGap &&
			stored.Source == string(CandleMergeSourceRESTTotal) && incoming.Source == string(CandleMergeSourceRESTTotal) &&
			IsAuthoritativeClosedTotal(stored, tfMs, now) && incoming.FetchedAt > stored.FetchedAt &&
			incoming.RequestedAt > stored.FetchedAt && incoming.RequestedAt <= incoming.FetchedAt &&
			incoming.FetchedAt <= now && SameExactSeedValues(stored, incoming) {
			healed.HasGap = false
		}

		// A BUCKET WHOSE MEASUREMENT WAS ERASED MUST SAY SO.
		//
		// This branch REPLACES, so when the authoritative record is smaller than what we had
		// assembled, a live measurement is discarded. That can be legitimate — an over-count
		// being settled — or it can be a record that arrived short. Nobody downstream can tell
		// the difference, and the bucket would close declaring itself intact; nothing revisits a
		// bucket that never said it was wrong.
		//
		// Raising HasGap here reuses the remedy that ALREADY exists — the continuity check
		// enqueues a targeted re-read for any closed bucket carrying the mark. HasGap is a bool
		// and onClose fires once per bucket, so this is at most ONE extra re-read per bucket.
		if stored.Volume > incoming.Volume {
			healed.HasGap = true
		}
		healed.LastUpdate = now
		return healed
	}

	m := MergeCandleValues(stored, incoming, true)
	m.LastUpdate = now
	return m
}

// SameExactSeedValues reports whether two candles carry bit-identical seed measurements, ignoring
// bookkeeping. Used to prove an independent settled REST read confirms an earlier one.
func SameExactSeedValues(a, b Candle) bool {
	return a.Timestamp == b.Timestamp && a.Open == b.Open && a.High == b.High && a.Low == b.Low && a.Close == b.Close &&
		a.Volume == b.Volume && a.TakerBuyVolume == b.TakerBuyVolume && a.TakerSellVolume == b.TakerSellVolume
}

// IsAuthoritativeClosedTotal reports whether a candle is the exchange's FINAL record for a
// bucket, and therefore settles any disagreement instead of competing in a MAX.
//
// Both halves of the test are load-bearing.
//
// DECLARED EXCHANGE TOTAL. Authority comes from provenance, never from the numbers. An unlabelled
// candle has unknown provenance — a warm boot replays our OWN persisted data — and letting it
// overwrite would be authority by accident. The `Source != ""` guard matters because IsTotalSource
// treats unlabelled as a total; without it, every replay would claim to be the exchange.
//
// THE WINDOW HAS ELAPSED. A total for a bucket that is still OPEN is a photograph taken mid-bar:
// complete as of the fetch and stale a second later. Replacing with it would discard every trade
// the live stream saw afterwards, on every seed. Only once the bucket can no longer receive
// trades is the exchange's kline final.
//
// IT CARRIES THE ORDER FLOW. The seeder degrades to plain FetchOHLCV under rate limits, so a
// "total" can arrive with volume but NO taker split. Settling with it would replace the volume
// while leaving our accumulated pair in place — and if the volume moves DOWN, the result is
// buy + sell > volume: a bar claiming more classified flow than it had trades.
//
// Authority is per-record, not per-field: a record that did not measure the order flow cannot
// settle the order flow, and settling only half a bucket produces an internally impossible one.
// Such a batch falls back to the plain union, which is conservative and consistent.
func IsAuthoritativeClosedTotal(c Candle, tfMs, now int64) bool {
	if tfMs <= 0 || c.Source == "" || !IsTotalSource(c.Source) {
		return false
	}
	if c.TakerBuyVolume+c.TakerSellVolume <= 0 {
		return false
	}
	// THE RECORD MUST HAVE SETTLED, NOT MERELY THE BUCKET CLOSED. See
	// docs/decisions/0011-settled-kline-authority.md. A kline fetched moments after its bucket
	// rolls is still filling in; replacing with it installs a SHORT record over a correct live
	// reconstruction while every discard counter stays at zero.
	//
	// An unstamped total (FetchedAt == 0) is NOT trusted to replace. It falls through to the
	// plain union, which takes a maximum and therefore cannot lower a bucket — the conservative
	// direction, and the one that fails safe if a producer is ever added without a stamp.
	if c.FetchedAt <= 0 || c.FetchedAt < c.Timestamp+tfMs+KlineSettleGraceMs {
		return false
	}
	return c.Timestamp+tfMs <= now
}

// MergedForBatch returns, for every bucket the batch carried, the value the ring holds after the
// fold — in the batch's own order, so the caller can hand it straight to the durable tiers in
// place of the raw input.
//
// A bucket the fold could not place (it fell outside the ring's capacity) comes back as the batch
// supplied it: the tiers should still receive it, and the ring simply has no opinion.
//
// The caller holds the buffer's write lock.
func MergedForBatch(buf *RingBuffer[Candle], batch []Candle) []Candle {
	if len(batch) == 0 {
		return nil
	}
	if buf == nil {
		return batch
	}
	stored := buf.Slice()
	byTs := make(map[int64]Candle, len(stored))
	for i := range stored {
		byTs[stored[i].Timestamp] = stored[i]
	}
	out := make([]Candle, len(batch))
	for i := range batch {
		if m, ok := byTs[batch[i].Timestamp]; ok {
			out[i] = m
			continue
		}
		out[i] = batch[i]
	}
	return out
}
