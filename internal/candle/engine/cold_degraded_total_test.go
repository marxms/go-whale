package engine

import (
	"context"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// TestSeedCandlesBatch_ColdReceivesTheMergedViewNotTheRawBatch closes a hole in a rule the
// other two tiers enforce, WITHOUT the regression that the obvious fix carries.
//
// # THE PROBLEM
//
// A "total" can arrive carrying volume but NO taker split: the seeder degrades to plain
// FetchOHLCV under rate limits, and — more importantly — FetchKlinesWithCVD refuses any
// non-Binance client outright (adapters/outbound/venue/klines.go), so for BingX, which is
// enabled in production, EVERY seeded and gap-recovered candle looks like that.
//
// The cold table is DEDUP UPSERT and ILP has no NULL, so writing the raw batch replaces a
// measured pair with 0/0 and the loss is permanent and indistinguishable from a real zero.
//
// # WHY SKIPPING WAS THE WRONG FIX, AND HOW THIS ONE DIFFERS
//
// The first attempt withheld such rows from the cold tier. That was measured against the
// wrong premise — "the live on-close path already wrote the measured value, so skipping
// loses nothing". There are exactly two writers of the cold tier, the on-close path and this
// one, and neither covers a bucket that predates the subscription. For BingX it withheld
// EVERYTHING, permanently, with no third writer able to repair it: a skip trades a
// recoverable wrong value for an unrecoverable absent one.
//
// The engine already computes the right answer. SeedCandles folds the batch into the ring,
// and that union keeps the measured pair while taking the batch's volume where it is larger.
// Persisting THAT — instead of the raw batch — means a degraded record can neither erase
// order flow nor leave a hole, and it makes the three tiers agree by construction rather
// than by three separate rules that have to be kept in sync by hand.
func TestSeedCandlesBatch_ColdReceivesTheMergedViewNotTheRawBatch(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "ETH/USDT"
		tf       = "5m"
	)
	step := int64(5 * 60 * 1000)
	ts := time.Now().UnixMilli()/step*step - 20*step

	ce := NewCandleEngine(map[string]int{"default": 64})
	fake := &fakeColdAppender{}
	ts2 := &TieredStore{engine: ce, redis: nil, coldAppender: fake}

	// The live stream measured this bucket's order flow.
	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{{
		Timestamp: ts, Open: 100, High: 101, Low: 99, Close: 100,
		Volume: 500, TakerBuyVolume: 300, TakerSellVolume: 200,
	}})

	// A degraded total for the same bucket: real volume, no taker split at all.
	degraded := []candle.Candle{{
		Timestamp: ts, Open: 100, High: 102, Low: 98, Close: 101,
		Volume: 520, Source: string(redisstore.CandleMergeSourceRESTTotal),
	}}
	if err := ts2.appendClosedToCold(context.Background(), exchange, market, symbol, tf,
		ce.SeedCandles(exchange, market, symbol, tf, degraded)); err != nil {
		t.Fatalf("appendClosedToCold: %v", err)
	}

	if len(fake.got) != 1 {
		t.Fatalf("routed %d rows, want 1 — the bucket must still reach the durable tier. "+
			"Withholding it is not a safe degradation: for an exchange whose REST never "+
			"carries taker (BingX) it withholds everything, and no other writer can repair "+
			"an absent row", len(fake.got))
	}
	got := fake.got[0]
	if got.TakerBuyVolume != 300 || got.TakerSellVolume != 200 {
		t.Errorf("taker pair = %v/%v, want 300/200 — the cold tier received the RAW batch. "+
			"QuestDB dedups on the key, so a 0/0 row replaces the measured pair, and ILP has "+
			"no NULL to say 'not measured'", got.TakerBuyVolume, got.TakerSellVolume)
	}
	if got.Volume != 520 {
		t.Errorf("Volume = %v, want 520 — the union must still take the exchange's larger "+
			"volume; preserving order flow may not cost the volume repair", got.Volume)
	}
}

// TestSeedCandlesBatch_ColdStillGetsABucketTheRingNeverHad is the anti-hole guard. A bucket
// that exists ONLY in a degraded batch — the BingX case, and any bucket predating the
// subscription — must reach the durable tier. OHLCV with no order flow is a partial
// measurement; the consumer's abstention rule already handles it. An absent row is not
// partial, it is a gap, and nothing repairs it.
func TestSeedCandlesBatch_ColdStillGetsABucketTheRingNeverHad(t *testing.T) {
	t.Parallel()

	const (
		exchange = "bingx"
		market   = "future"
		symbol   = "BTC/USDT"
		tf       = "5m"
	)
	step := int64(5 * 60 * 1000)
	ts := time.Now().UnixMilli()/step*step - 30*step

	ce := NewCandleEngine(map[string]int{"default": 64})
	fake := &fakeColdAppender{}
	ts2 := &TieredStore{engine: ce, coldAppender: fake}

	batch := []candle.Candle{{
		Timestamp: ts, Open: 100, High: 101, Low: 99, Close: 100,
		Volume: 500, Source: string(redisstore.CandleMergeSourceRESTTotal),
	}}
	if err := ts2.appendClosedToCold(context.Background(), exchange, market, symbol, tf,
		ce.SeedCandles(exchange, market, symbol, tf, batch)); err != nil {
		t.Fatalf("appendClosedToCold: %v", err)
	}

	if len(fake.got) != 1 || fake.got[0].Volume != 500 {
		t.Fatalf("routed %+v — a bucket only the batch has MUST be persisted. BingX never "+
			"carries taker over REST (FetchKlinesWithCVD refuses non-Binance clients), so "+
			"withholding these withholds the entire exchange, permanently", fake.got)
	}
}

// TestSeedCandles_ReturnsWhatItStored pins the contract the persistence path depends on:
// the returned slice is the MERGED view of the batch's buckets, in batch order, not the
// input echoed back.
func TestSeedCandles_ReturnsWhatItStored(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "SOL/USDT"
		tf       = "5m"
	)
	step := int64(5 * 60 * 1000)
	base := time.Now().UnixMilli()/step*step - 40*step

	ce := NewCandleEngine(map[string]int{"default": 64})
	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{Timestamp: base, Close: 100, Volume: 500, TakerBuyVolume: 300, TakerSellVolume: 200},
	})

	out := ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{Timestamp: base, Close: 101, Volume: 600, Source: string(redisstore.CandleMergeSourceRESTTotal)},
		{Timestamp: base + step, Close: 102, Volume: 700, Source: string(redisstore.CandleMergeSourceRESTTotal)},
	})

	if len(out) != 2 {
		t.Fatalf("returned %d candles, want one per batch bucket", len(out))
	}
	if out[0].Timestamp != base || out[1].Timestamp != base+step {
		t.Fatalf("returned timestamps %d,%d — must follow the batch", out[0].Timestamp, out[1].Timestamp)
	}
	if out[0].TakerBuyVolume != 300 {
		t.Errorf("merged[0] taker buy = %v, want 300 carried from the ring — the return value "+
			"is what the tiers persist, so it must be the union and not the input",
			out[0].TakerBuyVolume)
	}
	if out[1].Volume != 700 {
		t.Errorf("merged[1] volume = %v, want 700 — a bucket the ring never had must come "+
			"back verbatim", out[1].Volume)
	}
}
