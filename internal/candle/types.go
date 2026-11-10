package candle

import (
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/trade"
)

type (
	// Trade is the canonical trade value owned by package trade.
	Trade = trade.Trade
	// LastTrade is the canonical last-trade snapshot owned by package trade.
	LastTrade = trade.LastTrade
)

// Origin is process-local provenance; persisted legacy candles remain Unknown.
type Origin uint8

// Origin records whether a candle was produced natively or derived from another series.
const (
	OriginUnknown Origin = iota
	OriginNative
	OriginDerived
)

// Candle is the domain value. It deliberately carries NO serialization tags: the JSON
// names, order and omitempty rules live in the adapter DTOs that own each boundary — the
// HTTP API (httpapi.CandleDTO), the realtime envelope (realtime.CandleDTO) and the legacy
// Redis member (redisstore.candleLegacyWire). Each one maps explicitly to and from this
// type, so a persistence or wire concern cannot become a property of the domain by
// editing a struct tag.
type Candle struct {
	Origin     Origin
	Timestamp  int64
	Open       float64
	High       float64
	Low        float64
	Close      float64
	Volume     float64
	CreatedAt  int64
	LastUpdate int64

	// THE ONLY ORDER-FLOW VALUES THIS SERVICE MEASURES.
	//
	// The aggregator classifies each trade by its aggressor side and sums into one of these
	// two; a trade whose side cannot be determined lands in SideUnknownCount instead of being
	// pushed onto a side. Everything else people call "order flow" — per-bar delta, cumulative
	// delta, aggression ratio — is arithmetic on this pair, and arithmetic belongs to whoever
	// is asking, not to the process that did the measuring. Computing it here would put the
	// same formula in two places and let the results drift: an independent max() per field is
	// not max() of the difference, so a merged increment can disagree with the pair.
	//
	// NO omitempty IN ANY WIRE DTO, DELIBERATELY, and this is the load-bearing part.
	//
	// omitempty drops a zero, and zero is a real, frequent market state here: a bar where all
	// aggression sat on one side. Measured across the fleet, ~1% of 5m bars had taker-buy or
	// taker-sell at zero — precisely the bars carrying the strongest order-flow signal. There
	// is no second copy to recover them from, so the pair must always be on the wire, zeros
	// included.
	//
	// Absence therefore means "payload from before this contract", never "zero". Presence with
	// value 0 on BOTH sides means the bar had volume but no classified flow, which consumers
	// must treat as ABSTENTION — see the `buy + sell > 0` guard they all apply.
	TakerBuyVolume  float64
	TakerSellVolume float64
	Source          string
	AggregateMethod string

	// FetchedAt is the wall-clock instant (ms UTC) at which an exchange RESPONSE carrying this
	// candle was parsed. Zero for anything assembled from the live stream.
	//
	// It exists because authority was being decided from the wrong clock. A REST kline for a
	// just-closed bucket is still settling: measured against the aggTrade tape, it converges
	// only after ~5s. A repair fetched inside that window installed a value shorter than the
	// live stream had already assembled, and reported it as a heal. "The bucket closed" and
	// "the record settled" are different facts, and only this field can tell them apart.
	FetchedAt int64
	// RequestedAt is the wall-clock instant (ms UTC) at which the REQUEST that produced this
	// candle was ISSUED. Zero for anything assembled from the live stream.
	//
	// # WHY BOTH ENDS ARE NEEDED, AND WHY ONE IS NOT ENOUGH
	//
	// A total covers [bucket open, H] where H is the instant the exchange computed it. H is never
	// published, but it is BRACKETED: the request cannot have been answered before it was sent,
	// and cannot have been computed after we parsed the reply.
	//
	//	RequestedAt ≤ H ≤ FetchedAt
	//
	// Each end answers a different question, and using one for the other's job is wrong in a way
	// that is expensive rather than merely imprecise:
	//
	//	FetchedAt (upper)   "our live tail reaches PAST anything this total could contain"
	//	                    → the total must NOT supersede it. Used against annihilation.
	//	RequestedAt (lower) "these trades are OLDER than the request, so the total certainly
	//	                    counts them" → a delta carrying only those must NOT be added again.
	//	                    Used against double counting.
	//
	// Using FetchedAt as a lower bound instead would discard trades that happened between H and
	// the parse — trades the total never contained — and can cost a large fraction of the
	// volume on replay. The producers already know this instant: the seeder timed its own
	// snapshot lag with it, and gap recovery has it at the call site.
	RequestedAt int64
	// LastTradeAt is the exchange timestamp (ms UTC) of the NEWEST trade that contributed to
	// this candle. Zero for anything not assembled from the live tape.
	//
	// It is the counterpart FetchedAt was missing, and without it the open-bucket merge is blind
	// by construction. FetchedAt says how far an exchange TOTAL reaches; LastTradeAt says how far
	// our own LIVE accumulation reaches. Only the two together answer the question the merge
	// actually has to answer: does this total already contain what we measured, or not?
	//
	// Without it the merge decides by ARRIVAL ORDER and by MAGNITUDE — and both are wrong for the
	// same reason. A live batch that reaches the engine BEFORE the total but carries trades
	// from AFTER the snapshot instant has its measurement erased by max(); a batch that reaches
	// the engine AFTER the total while carrying trades from BEFORE it is summed on top and
	// counted twice. Same defect, opposite signs, and which one a symbol gets depends only on
	// when its buffer happens to drain.
	LastTradeAt int64
	// KlineEventAt is the venue's OWN event timestamp (ms UTC) on a pushed candle. Zero for
	// anything not sourced from a venue kline stream.
	//
	// It is the ordering key for the one producer that REPLACES rather than merges. A venue
	// candle is a running total, so two frames for the same bucket are two readings of the same
	// quantity and the newer one wins — but "newer" has to mean newer AT THE VENUE.
	//
	// Arrival order cannot say that: the SDK dispatches every websocket message on its own
	// goroutine, so two frames for one symbol race into our process and can swap. Magnitude
	// cannot say it either — that was the assumption behind max(), and it cannot distinguish a
	// reading that legitimately supersedes another from one that merely happens to be larger.
	// Only the venue's clock orders the venue's own readings.
	KlineEventAt int64
	// SideUnknownCount is volume this service saw but could NOT attribute to a side. The wire
	// DTOs KEEP omitempty on it on purpose, unlike the pair above: it is never persisted to the cold tier, so
	// every candle read back from storage has it at zero. Publishing that zero unconditionally
	// would make each historical bar assert "no unclassified trades" — fabricating a
	// measurement that was never taken, which is the failure this whole contract exists to
	// remove. Telemetry, not contract.
	SideUnknownCount int64
	LateTradeCount   int64
	HasGap           bool
}

// ParseTimeframeMs returns the interval duration in milliseconds and whether
// the interval is recognized.
func ParseTimeframeMs(tf string) (int64, bool) {
	interval, err := market.ParseTimeframe(tf)
	if err != nil {
		return 0, false
	}
	return interval.Milliseconds(), true
}

// ParseTimeframeToSeconds returns the interval in seconds, or zero when the
// interval is invalid. Configuration validation rejects invalid intervals
// before they reach this function; zero keeps an invalid value from silently
// becoming an hour-long bucket.
func ParseTimeframeToSeconds(tf string) int64 {
	interval, err := market.ParseTimeframe(tf)
	if err != nil {
		return 0
	}
	return interval.Seconds()
}
