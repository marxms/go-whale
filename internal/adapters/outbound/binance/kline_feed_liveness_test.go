package binancesdk

import (
	"context"
	"testing"
	"time"
)

// A LIVE PARTITION MUST NOT VOUCH FOR A DEAD ONE.
//
// 500 symbols dial as 2 combined-stream connections — measured in production: "kline feed: 500
// symbols over 2 partition(s) at 5m". A single atomic on the feed, written by every symbol's
// handler on every partition, changes nothing the watchdog can see when one connection dies: the
// survivor refreshes the shared clock every few milliseconds while ~250 symbols stop producing
// candles on all eight timeframes.
//
// Ownership silences the tape for the whole scope, so those symbols have no second writer. They go
// stale, and a stale candle differs from Binance's exactly as much as a wrong one does. The
// watchdog existed to catch precisely this and could only ever fire when EVERY partition died at
// once — the one case that needs no watchdog.
//
// The feed is only as live as its quietest connection.
func TestFeedReportsTheQuietestPartitionNotTheNewest(t *testing.T) {
	now := time.Now().UnixMilli()

	f := NewKlineFeed(context.Background(), "binance", "future", "5m", nil)
	dead := &klinePartition{symbols: 250}
	dead.lastFrameAt.Store(now - 300_000) // silent for five minutes
	live := &klinePartition{symbols: 250}
	live.lastFrameAt.Store(now) // delivering right now

	// Live one LAST, so a "newest wins" implementation reports zero silence and passes nothing.
	f.mu.Lock()
	f.parts = []*klinePartition{dead, live}
	f.mu.Unlock()

	silent, have := f.SecondsSinceLastFrame()
	if !have {
		t.Fatal("feed reports no partitions")
	}
	if silent < 290 {
		t.Fatalf("silence = %.0fs; a live partition is vouching for a dead one carrying %d symbols "+
			"— the whole failure this exists to catch", silent, dead.symbols)
	}

	worst, stranded := f.QuietestPartition()
	if worst < 290 || stranded != 250 {
		t.Fatalf("QuietestPartition() = (%.0fs, %d symbols), want the dead partition (~300s, 250)", worst, stranded)
	}
}

// A partition that NEVER delivers is as fatal for its symbols as one that delivered and stopped.
// The old signal reported "never delivered" as a reason NOT to restart, so a connection that came
// up dead sat there forever. Clocks start at dial time, which collapses both into "silent since T".
func TestNeverDeliveredPartitionIsCountedAsSilent(t *testing.T) {
	f := NewKlineFeed(context.Background(), "binance", "future", "5m", nil)
	p := &klinePartition{symbols: 250}
	p.lastFrameAt.Store(time.Now().UnixMilli() - 120_000) // dialed two minutes ago, never spoke

	f.mu.Lock()
	f.parts = []*klinePartition{p}
	f.mu.Unlock()

	silent, have := f.SecondsSinceLastFrame()
	if !have || silent < 110 {
		t.Fatalf("a partition that never spoke reports %.0fs silence (have=%v); its 250 symbols have "+
			"had no candle producer since dial", silent, have)
	}
}

// No partitions at all is a different fact from a silent one — but NOT a reason to stop watching.
//
// The first version of this test asserted the opposite, with the comment "the watchdog uses it to
// stop", and pinned a REGRESSION as if it were the contract. Stop() clears the partition set, so
// this is exactly the state a FAILED REDIAL leaves behind; the watchdog read it as "nothing to
// report" and never retried, leaving all 500 symbols frozen on all eight timeframes for the life of
// the process. The flag reports a fact; only the caller decides what it means, and the caller now
// treats it as "restart".
func TestNoPartitionsReportsNotLive(t *testing.T) {
	f := NewKlineFeed(context.Background(), "binance", "future", "5m", nil)
	if _, have := f.SecondsSinceLastFrame(); have {
		t.Fatal("a feed with no partitions claims to have one")
	}

	// And a STOPPED feed must report the same, because that is the failed-redial state.
	p := &klinePartition{symbols: 250}
	p.lastFrameAt.Store(1)
	f.mu.Lock()
	f.parts = []*klinePartition{p}
	f.mu.Unlock()
	f.Stop()
	if _, have := f.SecondsSinceLastFrame(); have {
		t.Fatal("a stopped feed still claims a live partition")
	}
}
