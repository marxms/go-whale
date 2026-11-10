package ingestion

// seenWindowSize is how far back, in trade ids, a late arrival is still recognized as new.
//
// It must exceed the worst reordering the transport can produce. Observed reordering is
// LOCAL — the sampled diagnostic caught gap=1, i.e. two adjacent ids swapped — so 8192 is
// three orders of magnitude of headroom. It also bounds memory: one bit per id, 1 KiB per
// symbol, ~500 KiB for the whole fleet.
//
// Anything older than the window is treated as a genuine replay and rejected. That is the
// deliberate trade: a trade delayed by more than 8192 ids of the same symbol would be
// dropped, but such a delay is not reordering, it is a stream that has fallen apart, and
// gap recovery is the mechanism for that case.
const seenWindowSize = 8192

// holeConfirmIDs is how far the id sequence must advance PAST a hole before the hole is
// called a trade that never arrived rather than one that is merely late.
//
// It exists because a forward jump and a reorder are IDENTICAL at the instant they happen:
// the Binance SDK dispatches every websocket message on its own goroutine, so the id after a
// gap routinely arrives before the id inside it. Reporting the jump immediately turns every
// reorder into a false alarm — measured on the real BNB tape from the incident with adjacent
// pairs swapped, per-batch reporting without a frontier fired 103 times on a tape that was
// COMPLETE.
//
// 8 is eight times the worst reordering this transport has ever produced (the sampled
// diagnostic caught gap=1, two adjacent ids swapped) and small enough that a hole settles
// within a batch or two — hundreds of milliseconds at the 50-200ms drain cadence — so the
// verdict still lands while the bucket that needs it is open.
const holeConfirmIDs = 8

// sequenceIsContiguous reports whether this venue numbers trades CONSECUTIVELY per symbol,
// which is the precondition for reading a hole in the ids as a lost message.
//
// It is a gate and not an assumption because the property is Binance's, not a law. Binance
// aggregate trades carry `a`, an unbroken per-symbol counter — verified against the exchange:
// XRPUSDT 1027 aggTrades over ids 767575152..767576178, span == count, zero holes, and the
// same for BNBUSDT (1458 ids) and seven other affected symbols.
//
// No other venue in this fleet documents that, and this handler also serves bingx spot and
// future (config.yaml). Counting "holes" in ids that were never meant to be dense
// would not merely produce a false alarm: it would mark every bucket for repair and turn the
// shared REST budget into a self-inflicted outage. A venue joins this list when someone has
// measured its tape, not when someone hopes.
func sequenceIsContiguous(exchange string) bool {
	return exchange == "binance"
}

// seenWindow answers "has this trade id already been aggregated?" for ONE symbol, and does
// so correctly when ids arrive OUT OF ORDER.
//
// # Why a monotonic watermark is not enough
//
// A check of the form `if id <= lastID { drop }`, justified by "Binance aggTrade 'a' field is
// always increasing", is true of the EXCHANGE and false of our ARRIVAL ORDER — and the
// difference silently costs volume.
//
// The Binance Go SDK dispatches every websocket message on a NEW GOROUTINE
// (common/websocket.go executeCallbacks: `for _, cb := range callbacks { go func(...) }`),
// so two consecutive trades for one symbol race each other into our buffer. When the later
// id won that race and a flush happened in between, the earlier id arrived after the
// watermark had passed it and was discarded as "already seen" — despite never having been
// seen. Measured in production: binance:future:REZ/USDT id=44822238 against watermark
// 44822239, a gap of ONE. Sparse, always a deficit, permanent — exactly the shape of the
// divergence against Binance klines.
//
// A watermark cannot be made correct under unordered delivery; that is what monotonic
// means. This window can: it remembers WHICH ids were taken, not merely the largest, so
// order stops being load-bearing.
//
// Not goroutine-safe by design — the caller already holds the handler lock while deciding,
// and adding a second lock here would nest locks on the ~50k trades/s path for no gain.
type seenWindow struct {
	highest int64
	init    bool
	bits    [seenWindowSize / 64]uint64

	// contiguous says the venue's ids may be read as a dense sequence, so a hole means a lost
	// message. Set once at construction from sequenceIsContiguous; false disables the
	// integrity verdict entirely and confirmMissing becomes a no-op.
	contiguous bool

	// checked is the newest id this window has already given a FINAL verdict on. Everything
	// above it is still inside the reordering frontier and has no verdict yet.
	//
	// It is what turns this bitset from a dedup structure into an INTEGRITY signal. Every drop
	// counter on the path can read zero — messages_in_total == trades_ingested_total to the
	// digit, trades_dropped_total 0, both discard reasons 0 — while symbols close a bucket
	// short against the exchange's own aggTrade tape. Every instrument is correct and none is
	// pointed at the question, because all of them measure what we did with the trades that
	// ARRIVED.
	checked int64
}

func (w *seenWindow) slot(id int64) (word int, mask uint64) {
	// Venue trade ids are non-negative; clamp a malformed id instead of letting the
	// conversion wrap it into an arbitrary slot.
	if id < 0 {
		id = 0
	}
	i := uint64(id) % seenWindowSize
	return int(i / 64), 1 << (i % 64)
}

func (w *seenWindow) isSet(id int64) bool {
	word, mask := w.slot(id)
	return w.bits[word]&mask != 0
}

func (w *seenWindow) set(id int64) {
	word, mask := w.slot(id)
	w.bits[word] |= mask
}

func (w *seenWindow) clear(id int64) {
	word, mask := w.slot(id)
	w.bits[word] &^= mask
}

// confirmMissing gives a FINAL verdict on every id that has fallen behind the reordering
// frontier since the last call, and reports how many of them never arrived.
//
// Call it ONCE PER BATCH, after the batch has been admitted — never per trade. Reordering is
// already resolved for everything it covers, which is exactly what keeps the count free of the
// false alarms a per-jump counter produces.
//
// Cost is O(ids the batch advanced) — amortized O(1) per trade, zero allocation — and it reads
// the bitset the dedup already maintains. In the normal case every bit is set and the loop
// finds nothing.
//
// The count is a LOWER BOUND by construction: an id delayed past the frontier is counted here
// and still admitted later, so a pathological reorder inflates the number but can never hide a
// loss. That asymmetry is deliberate — the failure this exists to catch was silence.
func (w *seenWindow) confirmMissing() int64 {
	if !w.init || !w.contiguous {
		return 0
	}

	var missing int64

	// The ring can only speak for the last seenWindowSize ids. If the sequence jumped further
	// than that — a disconnect, not a reorder — admit already wiped the bits, so the skipped
	// span is counted WITHOUT scanning it. This is what keeps the work bounded on the one path
	// that could otherwise iterate a hundred thousand ids on the trade goroutine.
	if oldestKnown := w.highest - seenWindowSize; w.checked < oldestKnown {
		missing += oldestKnown - w.checked
		w.checked = oldestKnown
	}

	return missing + w.adjudicate(w.highest-holeConfirmIDs)
}

// confirmTail gives a FINAL verdict on the ids still inside the reordering frontier, and it is
// the difference between a repair that arrives and one that arrives too late to matter.
//
// # THE TAIL OF EVERY BUCKET WAS UNJUDGED AT THE MOMENT IT CLOSED
//
// confirmMissing deliberately withholds judgement on the newest holeConfirmIDs ids, because a
// hole that young is far more likely to be reordering than loss — without that margin a complete
// tape produced 103 false positives. The margin is right, and it has a cost nobody had priced:
// the verdict on the LAST ids of a bucket lands only after the sequence has advanced 8 further,
// which is AFTER the bucket rolled.
//
// By then HasGap can no longer be raised on it: checkOpenTimeContinuity reads the flag off the
// candle as it CLOSES, so a bucket damaged in its final trades closes declaring itself intact,
// no targeted re-read is enqueued, and nothing ever revisits it. The measured shape matches
// exactly — the deficit sits at the end of the bucket, it is one-sided because the last trades
// of a burst usually are, and it appears in many symbols at the same instant because all 500
// cross their boundary on the same wall clock.
//
// A batch that CROSSES a bucket boundary is the one moment where waiting costs more than
// deciding: that batch is what rolls the bucket, so this is the last instant the mark can still
// reach it. The reorder margin is dropped for exactly that call. The asymmetry is the one this
// file already argues for elsewhere — marking a bucket that turns out to be complete costs one
// redundant REST re-read, while failing to mark the short one costs a permanent error nobody can
// see.
//
// In production this window is not hypothetical and not noisy: over 11h and 21,3M trades,
// trades_missing_total reached 1558 while candle_late_update_total stayed at zero on every
// reason — the ids genuinely never arrive, and nothing arrives late enough to be re-judged.
func (w *seenWindow) confirmTail() int64 {
	if !w.init || !w.contiguous {
		return 0
	}
	return w.adjudicate(w.highest)
}

// adjudicate counts unseen ids up to frontier and advances the verdict line to it. Shared so the
// two callers cannot drift apart on the one invariant that matters: an id is judged exactly
// once, whichever of them gets to it first.
func (w *seenWindow) adjudicate(frontier int64) int64 {
	var missing int64
	for x := w.checked + 1; x <= frontier; x++ {
		if !w.isSet(x) {
			missing++
		}
	}
	if frontier > w.checked {
		w.checked = frontier
	}
	return missing
}

// beyondWindow reports whether id is older than anything the ring can still speak for.
//
// It exists to SPLIT a rejection that was being reported under one name. admit returns false
// for two conditions that could not be more different: an id already aggregated (a genuine
// duplicate — harmless, the exchange or the reconnect sent it twice) and an id that fell off
// the back of the window (a trade that arrived so late its bucket is long gone — a REAL LOSS,
// never aggregated, invisible in the candle).
//
// Both were counted as `duplicate_id`, under a comment claiming the instrumentation had made
// "a lost trade distinguishable from a trade that never existed". It had not: the two share a
// label, so an operator reading a rising duplicate_id cannot tell a noisy reconnect from data
// being dropped on the floor. That is the same defect this activity fixed elsewhere and
// reintroduced here.
func (w *seenWindow) beyondWindow(id int64) bool {
	return w.init && id <= w.highest-seenWindowSize
}

// admit reports whether id is NEW and records it. It returns false for an id already taken
// and for one older than the window.
func (w *seenWindow) admit(id int64) bool {
	if !w.init {
		w.init = true
		w.highest = id
		// The verdict line starts at the FIRST id this stream ever delivered. Nothing below it
		// was ever expected — a boot or a resubscribe joins the exchange's sequence in the
		// middle — so nothing below it may be reported as lost.
		w.checked = id
		w.set(id)
		return true
	}

	switch {
	case id > w.highest:
		// The slots between the old highest and the new one wrap onto stale bits from a
		// previous lap of the ring; clear them so they are not mistaken for "seen".
		if id-w.highest >= seenWindowSize {
			w.bits = [seenWindowSize / 64]uint64{}
		} else {
			for x := w.highest + 1; x < id; x++ {
				w.clear(x)
			}
		}
		w.highest = id
		w.set(id)
		return true

	case id <= w.highest-seenWindowSize:
		// Older than anything the window can speak for. Rejecting is the safe answer: at
		// this distance it is a replay, not reordering.
		return false

	case w.isSet(id):
		return false // genuine duplicate

	default:
		// A late arrival still inside the window — the case the watermark silently lost.
		w.set(id)
		return true
	}
}
