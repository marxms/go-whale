package app

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/inbound/realtime"
	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
)

func TestSeedRevisionFanoutDoesNotEnqueueColdTwice(t *testing.T) {
	p := questdb.NewPipeline(nil, 8, time.Hour)
	a := &App{qdbPipeline: p}
	key := engine.CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "XPL/USDT", Timeframe: "30m"}
	c := candle.Candle{Timestamp: 1, Close: 1}
	a.onSeedCandleRevised(key, c)
	if got := p.Stats().Pending; got != 0 {
		t.Fatalf("seed fanout enqueued cold independently: %d", got)
	}
	a.onCandleRevised(key, c)
	if got := p.Stats().Pending; got != 1 {
		t.Fatalf("ordinary revision lost its existing cold path: %d", got)
	}
}

// TestOnCandleClose_SurvivesAColdTierThatNeverCameUp is the guard for a coupling that made
// one dependency's failure silently disable four unrelated mechanisms.
//
// # THE COUPLING
//
// Registering both callbacks inside `if qdbErr == nil` means that when QuestDB fails to start the
// process keeps running — deliberately, on Redis alone — and logs exactly one consequence:
// "cold storage DISABLED". The same branch would also carry
//
//	tieredStore.WriteClosedToHot   the hot-tier close write (the -8.85% deficit)
//	checkOpenTimeContinuity        missing-bucket detection
//	realtimeProducer.PublishClose  the outbound event bus
//	hub.IngestClose                the chart WS edge
//	OnCandleRevised (entirely)     the late-correction path, all of it
//
// none of which touch QuestDB. A cold tier that is down is a durability problem; it must not
// become a silent correctness-and-liveness problem for every other consumer in the process.
// QuestDB has failed or been unreachable at boot in production, so this path has been taken.
//
// The test drives the sink directly with a nil pipeline, which is precisely the state a
// failed QuestDB leaves behind.
func TestOnCandleClose_SurvivesAColdTierThatNeverCameUp(t *testing.T) {
	t.Parallel()

	engineInst := engine.NewCandleEngine(map[string]int{"default": 16})
	hub := realtime.NewHub(engineInst, func() int64 { return 0 })

	a := &App{
		qdbPipeline: nil, // QuestDB failed to start — the whole point
		tieredStore: nil, // not wired in this fixture; the nil guard must hold
		hub:         hub,
	}

	key := engine.CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}
	closed := candle.Candle{
		Timestamp: 1_700_000_000_000, Close: 100, Volume: 10,
		TakerBuyVolume: 6, TakerSellVolume: 4,
	}

	a.onCandleClose(key, closed)

	// The continuity baseline is the observable proof that the sink ran end to end: it is
	// recorded by the continuity tracker, which the sink must reach regardless of the cold tier.
	got, ok := a.continuityTracker().LatestClosed(key)

	if !ok || got != closed.Timestamp {
		t.Errorf("continuity baseline = %d (present=%t), want %d — gap detection is wired "+
			"inside the close sink, and registering that sink inside the QuestDB branch would mean "+
			"a cold tier that never came up also stops the process noticing missing buckets",
			got, ok, closed.Timestamp)
	}
}

// TestOnCandleRevised_SurvivesAColdTierThatNeverCameUp covers the other half. Registering the
// late-correction path in the same branch would revert the engine to repairing itself in RAM and
// telling nobody — the exact defect that path exists to fix, reachable through an unrelated
// failure.
func TestOnCandleRevised_SurvivesAColdTierThatNeverCameUp(t *testing.T) {
	t.Parallel()

	engineInst := engine.NewCandleEngine(map[string]int{"default": 16})
	hub := realtime.NewHub(engineInst, func() int64 { return 0 })

	a := &App{
		qdbPipeline: nil,
		hub:         hub,
	}
	key := engine.CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}
	revised := candle.Candle{
		Timestamp: 1_700_000_000_000, Close: 100, Volume: 279.983,
		TakerBuyVolume: 187.849, TakerSellVolume: 92.134,
	}

	a.onCandleRevised(key, revised)

	// A revision must NOT touch the continuity baseline: that baseline tracks the newest
	// close, and dragging it backwards to an older bucket would make the next genuine close
	// look like a gap and trigger a pointless backfill.
	_, recorded := a.continuityTracker().LatestClosed(key)

	if recorded {
		t.Error("a revision recorded a continuity baseline — that baseline tracks the NEWEST " +
			"close, so feeding it an older bucket drags it backwards and makes the next real " +
			"close read as a gap")
	}
}

// TestOnCandleClose_NilSinksAreAllIndividuallySafe states the degradation contract field by
// field, so a future wiring change cannot quietly reintroduce a shared fate. Each sink is
// optional for its own reason, and none of them may take the others down.
func TestOnCandleClose_NilSinksAreAllIndividuallySafe(t *testing.T) {
	t.Parallel()

	a := &App{}
	key := engine.CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "ETH/USDT", Timeframe: "5m"}
	c := candle.Candle{Timestamp: 1_700_000_000_000, Close: 100, Volume: 1}

	// Everything nil: cold disabled, no tiered store, no realtime producer, no WS edge.
	// This must be a complete no-op rather than a panic on the trade-processing goroutine.
	a.onCandleClose(key, c)
	a.onCandleRevised(key, c)
}
