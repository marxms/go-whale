package engine

import (
	"testing"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// TestUpdateCandle_LateCorrectionLeavesRAM guards against a correction that the engine makes
// and then keeps to itself.
//
// A late trade arrives after the bucket has rolled; mergeHistorical merges it into the ring
// buffer and the buffer becomes correct. But a merge with no outlet — mutating the slot,
// incrementing the counter and returning — leaves QuestDB, the hot Redis copy, the realtime bus
// and the WS hub holding the PARTIAL value forever, because the periodic snapshot only ever
// publishes the newest candle per key. The engine and every durable copy of the same bucket
// disagree, and nothing reconciles them.
//
// # WHY THIS IS THE ONE WORTH FIXING
//
// The other divergences against the exchange are not ours to fix: an aggTrade carries ONE
// timestamp for a group of fills, so a group straddling a bucket boundary is split by the
// kline and not by us. Measured on Binance's own data, our value equals the sum of its
// aggTrade stream exactly while its kline differs — the pairs cancel to zero and no data is
// lost.
//
// This one is different: the correct value is ALREADY IN MEMORY. Publishing it costs no REST
// call, no scheduled sweep and no new goroutine — the trigger is mergeHistorical itself,
// which already runs and already detects the correction.
func TestUpdateCandle_LateCorrectionLeavesRAM(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "BTC/USDT"
		tf       = "5m"
		step     = int64(5 * 60 * 1000)
	)
	base := int64(1_700_000_000_000) / step * step
	ce := NewCandleEngine(map[string]int{"default": 16})

	var closed []candle.Candle
	ce.OnCandleClose(func(_ CompositeKey, c candle.Candle) { closed = append(closed, c) })

	var revised []candle.Candle
	ce.OnCandleRevised(func(_ CompositeKey, c candle.Candle) { revised = append(revised, c) })

	// Bucket N accumulates from the live stream.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: base, Open: 100, High: 101, Low: 99, Close: 100,
		Volume: 279.931, TakerBuyVolume: 187.849, TakerSellVolume: 92.082,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})

	// Bucket N+1 opens, which CLOSES N and publishes it — at the value it had right then.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: base + step, Open: 100, High: 100, Low: 100, Close: 100,
		Volume: 1, TakerBuyVolume: 1, TakerSellVolume: 0,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})

	if len(closed) != 1 {
		t.Fatalf("expected exactly 1 close for bucket N, got %d", len(closed))
	}
	if closed[0].Volume != 279.931 {
		t.Fatalf("the close published %v, want the value at roll time 279.931", closed[0].Volume)
	}

	// A late SELL for bucket N arrives after the roll — the shape production measured.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: base, Open: 100, High: 100, Low: 100, Close: 100,
		Volume: 0.052, TakerBuyVolume: 0, TakerSellVolume: 0.052,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})

	// The engine repairs itself — this already worked.
	got := candleAt(ce, exchange, market, symbol, tf, base)
	if got == nil {
		t.Fatal("bucket N vanished")
	}
	if diff := got.Volume - 279.983; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("engine volume = %v, want 279.983 — the late trade was not merged", got.Volume)
	}

	// AND THE REPAIR MUST LEAVE MEMORY. Without this the durable copies stay at 279.931
	// forever: the snapshot only writes the NEWEST bucket, and nothing else revisits a closed
	// one.
	if len(revised) != 1 {
		t.Fatalf("expected 1 revision to be published for the corrected bucket, got %d — the "+
			"engine fixed itself in RAM and told nobody, so QuestDB, Redis, the realtime bus "+
			"and the WS hub keep the partial value permanently", len(revised))
	}
	if revised[0].Timestamp != base {
		t.Errorf("revision timestamp = %d, want %d — the wrong bucket was published",
			revised[0].Timestamp, base)
	}
	if diff := revised[0].Volume - 279.983; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("revision volume = %v, want 279.983 — the published correction does not carry "+
			"the repaired value", revised[0].Volume)
	}
}

// TestUpdateCandle_NoRevisionWithoutACorrection keeps the new signal meaningful. A revision
// event means "a closed bucket changed"; emitting one per live tick would make the WS edge
// re-render constantly and turn a real correction into noise nobody looks at.
func TestUpdateCandle_NoRevisionWithoutACorrection(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "ETH/USDT"
		tf       = "5m"
		step     = int64(5 * 60 * 1000)
	)
	base := int64(1_700_000_000_000) / step * step
	ce := NewCandleEngine(map[string]int{"default": 16})

	var revised int
	ce.OnCandleRevised(func(_ CompositeKey, _ candle.Candle) { revised++ })

	// Ordinary live traffic into the OPEN bucket, then a roll. Neither corrects a closed
	// bucket, so neither may publish a revision.
	for range 3 {
		ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
			Timestamp: base, Close: 100, Volume: 10,
			TakerBuyVolume: 6, TakerSellVolume: 4,
			Source: string(redisstore.CandleMergeSourceWSDelta),
		})
	}
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: base + step, Close: 101, Volume: 5,
		TakerBuyVolume: 3, TakerSellVolume: 2,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})

	if revised != 0 {
		t.Errorf("revisions = %d, want 0 — a revision must mean a CLOSED bucket changed, not "+
			"that a live tick arrived", revised)
	}
}
