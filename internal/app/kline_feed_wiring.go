package app

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/marxms/go-whale/internal/adapters/inbound/scheduler"
	binancesdk "github.com/marxms/go-whale/internal/adapters/outbound/binance"
	"github.com/marxms/go-whale/internal/adapters/outbound/binancews"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/netdiag"
	"github.com/marxms/go-whale/internal/recovery"
)

// klineFeed is the transport boundary consumed by ownership, discovery and recovery.
type klineFeed interface {
	Start([]string) error
	AddSymbols([]string) error
	Mute([]string)
	Stop()
	SecondsSinceLastFrame() (float64, bool)
	QuietestPartition() (float64, int)
	NeedsRestart() bool
}

type klineFeedFactory func(context.Context, string, string, string, func(string, candle.Candle)) klineFeed

type klineGapHeads map[engine.CompositeKey]int64

func (heads klineGapHeads) GetLatestTimestamp(_ context.Context, exchangeID, marketType, symbol, tf string) (int64, bool) {
	head, ok := heads[engine.CompositeKey{Exchange: exchangeID, MarketType: marketType, Symbol: symbol, Timeframe: tf}]
	return head, ok
}

// Reserve before replacement frames advance the engine. Partial failed attempts keep
// the earlier head; reconciling the current fleet also releases deranked keys.
func (a *App) reserveKlineGapHeads(ctx context.Context, exchangeID, marketType string, fleet []string) {
	heads := make(klineGapHeads, len(fleet))
	for _, symbol := range fleet {
		key := engine.CompositeKey{
			Exchange: exchangeID, MarketType: marketType,
			Symbol: market.NormalizeSymbol(symbol), Timeframe: recovery.FastestBootTimeframe,
		}
		head, ok := a.klineGapHeads[key]
		if a.candleEngine != nil {
			latest, have := a.candleEngine.GetLatestTimestamp(ctx, exchangeID, marketType, key.Symbol, key.Timeframe)
			if have && (!ok || latest < head) {
				head, ok = latest, true
			}
		}
		if ok {
			heads[key] = head
		}
	}
	a.klineGapHeads = heads
}

func newBinanceKlineFeed(log *slog.Logger) klineFeedFactory {
	return func(ctx context.Context, exchangeID, marketType, interval string, sink func(string, candle.Candle)) klineFeed {
		return binancesdk.NewKlineFeed(ctx, exchangeID, marketType, interval, sink, log)
	}
}

func (a *App) createKlineFeed(ctx context.Context, exchangeID, marketType string) klineFeed {
	factory := a.newKlineFeed
	if factory == nil {
		factory = newBinanceKlineFeed(a.log)
	}
	var ownership sync.Once
	onCandle := func(symbol string, c candle.Candle) {
		// Ownership moves on the FIRST delivered frame, not on a successful dial.
		// Declaring it before any candle arrives silenced the tape behind a feed
		// that might never speak, leaving the scope with no producer at all.
		ownership.Do(func() { a.declareVenueOwnership(exchangeID, marketType) })
		a.candleEngine.UpdateCandle(exchangeID, marketType, symbol, binancesdk.KlineFeedInterval, c)
		a.candleEngine.FoldUp(exchangeID, marketType, symbol, binancesdk.KlineFeedInterval, c.Timestamp)
	}
	feed := factory(ctx, exchangeID, marketType, binancesdk.KlineFeedInterval, onCandle)
	// The concrete feed may own a metrics port; test fakes need not. Wiring is explicit at
	// the composition root rather than a package-level default.
	if setter, ok := feed.(interface{ SetMetrics(binancesdk.Metrics) }); ok {
		setter.SetMetrics(a.binanceMetrics)
	}
	if setter, ok := feed.(interface{ SetNetdiag(*netdiag.Correlator) }); ok && a.netdiagCorrelator != nil {
		setter.SetNetdiag(a.netdiagCorrelator)
	}
	return feed
}

// startKlineFeed makes the venue the OWNER of this scope's candle.
//
// # WHY THIS IS NOT RECONCILIATION
//
// The feed does not merge with anything. It writes {open, high, low, close, volume, takerBuy,
// takerSell} and the tape stops writing them for the same scope, so no field has two producers.
// That is the whole design: two writers on one open bucket — a total landing on a live
// accumulation — is the topology that produced the measured defects, and no arbitration rule fixed
// it. An earlier attempt erased live measurement; another inflated buckets by 75%.
//
// It is also not a REST reconciliation: this is a push stream, there is no polling, no schedule
// and no repair job. The candle is still born from a live feed — from the feed that is
// authoritative for those fields.
//
// Only Binance takes this path today, because only Binance publishes a candle we can transport.
// BingX has no equivalent, so its adapter keeps aggregating the tape and the tape keeps writing
// its candle. The consumer never learns which, because the output contract is the same.
func (a *App) startKlineFeed(ctx context.Context, exchangeID, marketType string, symbols []string) {
	if exchangeID != market.Binance.String() || marketType != market.Future.String() || len(symbols) == 0 {
		return
	}
	a.klineMu.Lock()
	alreadyRunning := a.klineFeed != nil
	a.klineMu.Unlock()
	if a.candleEngine == nil || alreadyRunning {
		return
	}

	feed := a.createKlineFeed(ctx, exchangeID, marketType)
	if err := feed.Start(symbols); err != nil {
		feed.Stop()
		// NOT fatal, and deliberately so. Without the feed the tape is still the writer for this
		// scope — the system degrades to the tape's candle rather than to no candles at all. The
		// ownership hand-off below is therefore only performed on success: declaring it after a
		// failed start would leave the scope with NO writer.
		a.logger().Error("kline feed: start failed; the tape remains the candle writer for this scope",
			"exchange", exchangeID, "market_type", marketType, "symbols", len(symbols), "error", err)
		return
	}
	a.klineMu.Lock()
	if a.streamCtx != nil && a.streamCtx.Err() != nil {
		a.klineMu.Unlock()
		feed.Stop()
		return
	}
	a.klineFeed = feed
	a.klineScope = exchangeID + ":" + marketType
	a.klineSymbols = appendMissing(nil, symbols)
	a.candleEngine.RegisterDerivedScope(exchangeID, marketType, binancesdk.KlineFeedInterval)
	a.klineMu.Unlock()
	a.startKlineWatchdog(ctx, exchangeID, marketType)
	// Ownership is declared by the first delivered frame, not here. See onCandle.
}

// startKlineWatchdog restarts the feed when it goes silent.
//
// # WHY THIS IS NOT OPTIONAL
//
// Ownership silences the tape for every timeframe in the scope, so this feed is the SOLE producer
// of every candle for those symbols. A partition dying quietly costs ALL signals, on every
// timeframe, with nothing in the data to say so — the shape of the 40-minute staleness this design
// can produce.
//
// The aggTrade path carries exactly this, and for exactly this reason: the SDK's own reconnect is
// insufficient.
// declareVenueOwnership hands every timeframe of the scope to the venue's kline
// feed and retires the tape's admitted/applied pair. Every derived timeframe is
// included: the fold produces 15m through 1w from the 5m feed, so leaving any of
// them to the tape would restore the two-writer topology this design removed.
// noteKlineHealthy starts, or advances, the stability window that clears the
// reconnect failure count. A feed that delivered one frame and died must not
// reset the count on the same tick it failed.
func (a *App) noteKlineHealthy() {
	now := time.Now()
	if a.klineHealthySince.IsZero() {
		a.klineHealthySince = now
		return
	}
	if a.klineBackoffPolicy().Stable(now.Sub(a.klineHealthySince)) {
		a.klineFailures = 0
	}
}

func (a *App) klineBackoffPolicy() binancews.Backoff {
	if a.klineBackoff == (binancews.Backoff{}) {
		return binancews.DefaultBackoff()
	}
	return a.klineBackoff
}

func (a *App) declareVenueOwnership(exchangeID, marketType string) {
	if a.handler == nil || a.candleEngine == nil || a.tfReg == nil {
		return
	}
	for _, tf := range a.tfReg.All() {
		a.handler.MarkVenueOwnedCandles(exchangeID, marketType, tf)
	}
	a.handler.ForgetAdmittedVolume(exchangeID, marketType)
	a.candleEngine.ForgetAppliedDeltaVolume(exchangeID, marketType, a.tfReg.All())
	a.logger().Info("kline feed: venue now owns candles",
		"exchange", exchangeID,
		"market_type", marketType,
		"timeframe", binancesdk.KlineFeedInterval)
}

func (a *App) startKlineWatchdog(ctx context.Context, exchangeID, marketType string) {
	// THE TICKER BELONGS TO THE SCHEDULER. The watchdog still owns the question — "is this
	// partition silent?" — and the effect; the scheduler owns the clock and the cancellation.
	go scheduler.Every(ctx, 30*time.Second, func(ctx context.Context) bool {
		return a.restartKlineFeedIfSilent(ctx, exchangeID, marketType)
	})
}

// restartKlineFeedIfSilent performs one watchdog tick, and reports whether the watchdog should
// stop entirely (there is no feed left to watch).
//
// It is a method rather than the body of the select so the lock is released by `defer` on every
// path. A function boundary makes a missed unlock unrepresentable.
func (a *App) restartKlineFeedIfSilent(ctx context.Context, exchangeID, marketType string) (watchdogDone bool) {
	a.klineMu.Lock()
	defer a.klineMu.Unlock()

	if a.streamCtx != nil && a.streamCtx.Err() != nil {
		return true
	}
	feed := a.klineFeed
	if feed == nil {
		return true
	}
	// The QUIETEST partition, not the feed as a whole. 500 symbols dial as 2 connections, and a
	// single shared clock let the live one vouch for the dead one — the watchdog reported a healthy
	// feed while half the fleet went stale on all eight timeframes, with the tape silenced and no
	// second writer to cover for them.
	//
	// "Never delivered" is no longer a special case: a partition's clock starts at dial time, so a
	// connection that never speaks is silent from then, which is exactly as fatal for its symbols as
	// one that spoke and stopped.
	// A FEED WITH NO PARTITIONS IS DEAD, NOT QUIET, AND THIS IS WHERE A FAILED REDIAL LANDS.
	//
	// Stop() clears the partition set, so after a redial that failed, a.klineFeed points at a feed
	// reporting (0, false). Reading that as "nothing to report" would disarm the watchdog for the
	// life of the process: every later tick would return early, startKlineFeed would not re-run
	// either (alreadyRunning is checked against a non-nil pointer), and all 500 symbols would freeze
	// on all eight timeframes with the tape silenced behind them. So "no partitions" means RESTART.
	if fault, ok := feed.(interface{ ConfigFault(time.Time) bool }); ok && fault.ConfigFault(time.Now()) {
		// A started feed that never delivered a frame is a configuration error:
		// redialing the same route repeats the outage. The feed logged it once.
		return false
	}
	silent, havePartitions := feed.SecondsSinceLastFrame()
	restartNeeded := feed.NeedsRestart()
	if len(a.klineSymbols) == 0 && len(a.klinePendingAttach) == 0 && !restartNeeded {
		return false
	}
	if havePartitions && silent < binancesdk.KlineSilenceLimitSeconds && !restartNeeded && len(a.klinePendingAttach) == 0 {
		a.noteKlineHealthy()
		return false
	}
	now := time.Now()
	if now.Before(a.klineNextAttempt) {
		return false
	}
	a.klineHealthySince = time.Time{}
	//nolint:gosec // G404: reconnect jitter, not a cryptographic use.
	delay := a.klineBackoffPolicy().Delay(a.klineFailures, rand.Float64())
	a.klineFailures++
	a.klineNextAttempt = now.Add(delay)
	switch {
	case restartNeeded || len(a.klinePendingAttach) > 0:
		a.logger().Error("kline feed: transport recovery required; restarting despite recent frames",
			"exchange", exchangeID, "market_type", marketType, "pending_symbols", len(a.klinePendingAttach))
	case !havePartitions:
		a.logger().Error("kline feed: no live partition; the scope has no candle producer",
			"exchange", exchangeID, "market_type", marketType)
	default:
		_, strandedSymbols := feed.QuietestPartition()
		a.logger().Error("kline feed: a silent partition is the only producer of every timeframe; restarting",
			"symbols", strandedSymbols,
			"silent_seconds", silent,
			"exchange", exchangeID,
			"market_type", marketType)
	}
	feed.Stop()
	fleet := appendMissing(a.klineSymbols, a.klinePendingAttach)

	// Mark before Start can close the holed bucket with a frame from the next bucket.
	if a.candleEngine != nil {
		if marked := a.candleEngine.MarkOpenBucketGapped(exchangeID, marketType, binancesdk.KlineFeedInterval, fleet); marked > 0 {
			a.logger().Warn("kline feed: open buckets marked incomplete across the restart hole", "buckets", marked)
		}
	}
	a.reserveKlineGapHeads(ctx, exchangeID, marketType, fleet)

	fresh := a.createKlineFeed(ctx, exchangeID, marketType)
	// Restart from the LIVE fleet. Restarting from the boot list would silently drop every symbol
	// periodic discovery had attached since — and in a scope where ownership has silenced the tape,
	// dropping a symbol from the feed leaves it with no candle producer at all, permanently.
	if err := fresh.Start(fleet); err != nil {
		fresh.Stop()
		// The old feed is already stopped and the tape is still silenced, so this scope now has no
		// producer at all. Loud, and the next tick tries again.
		a.logger().Error("kline feed: restart failed; the scope has no candle producer until the next attempt",
			"exchange", exchangeID, "market_type", marketType, "error", err)
		return a.streamCtx != nil && a.streamCtx.Err() != nil
	}
	if a.streamCtx != nil && a.streamCtx.Err() != nil {
		fresh.Stop()
		return true
	}
	a.klineFeed = fresh
	a.klineSymbols = fleet
	a.klinePendingAttach = nil

	// THE HOLE THE RESTART JUST LEFT, HEALED THE WAY THE PROCESS RESTART ALREADY HEALS ITS OWN.
	//
	// Venue kline frames carry the bucket's running total, so the silence that triggered this
	// restart is only survivable while frames keep coming for the SAME bucket. It lasts at least
	// klineSilenceLimit on a 30s tick — 60 to 95 seconds — so it straddles a 5m boundary a good
	// fraction of the time, and when it does, the bucket that was open keeps whatever total arrived
	// before the socket died. Binance never resends a finished bucket. The short value then folds up
	// into all seven coarser timeframes.
	//
	// The process-restart path calls enqueueBootGap for exactly this, and this site creates the same
	// hole on purpose, several times a day.
	//
	// Marking comes FIRST: without it the returning REST total is vetoed by venueHasSpoken and the
	// repair is fetched, charged against the REST budget, and thrown away.
	//
	// This is the "extreme case" recovery is reserved for: not a schedule, not a sweep, but a named
	// hole this process knows it created.
	if a.gapRecovery != nil {
		recovery.EnqueueBootGap(ctx, a.gapRecovery, a.klineGapHeads, exchangeID, marketType, a.klineSymbols, time.Now())
	}
	a.klineGapHeads = nil
	return false
}

// extendKlineFeed brings symbols found by periodic discovery under the venue's ownership.
//
// Ownership is declared per SCOPE, so the moment it is on, the tape is silent for every symbol in
// that scope — including ones the feed has never heard of. A symbol discovered after boot would
// therefore have no producer at all until a restart, which is strictly worse than the two-writer
// topology this design replaced.
//
// The lock is held ACROSS the attach, deliberately. Releasing it to dial would reopen the window
// this closes: the watchdog could stop and replace the feed in between, and these symbols would be
// attached to a corpse — the very state of "no producer at all" the function exists to prevent.
// Both callers are slow-cadence background goroutines (a 30s ticker and the discovery cycle), and
// nothing on the ingestion hot path takes this lock.
func (a *App) extendKlineFeed(exchangeID, marketType string, symbols []string) {
	a.klineMu.Lock()
	defer a.klineMu.Unlock()

	if a.klineFeed == nil || len(symbols) == 0 {
		return
	}
	// The feed owns ONE scope. startKlineFeed enforces that with a literal; accepting the scope as
	// arguments and using them only in log messages would let a discovery cycle on any other
	// exchange or market type attach its symbols to the Binance-futures feed — and record them under
	// exchange="binance". Enabling discovery elsewhere is one line of YAML.
	if scope := exchangeID + ":" + marketType; scope != a.klineScope {
		a.logger().Error("kline feed: refusing to attach symbols from another scope",
			"symbols", len(symbols), "scope", scope, "owned_scope", a.klineScope)
		return
	}
	// Symbols stranded by a previous failed attach ride along with this batch. They are subscribed
	// and seeded but have no live producer, so every cycle that passes without retrying them is a
	// cycle they spend frozen.
	if len(a.klinePendingAttach) > 0 {
		symbols = appendMissing(a.klinePendingAttach, symbols)
		a.klinePendingAttach = nil
	}
	if err := a.klineFeed.AddSymbols(symbols); err != nil {
		// Loud, because these symbols are in a silenced scope: they have no candle producer at all
		// until this succeeds.
		// They are already in the manager's subscription set, so the next cycle classifies them as
		// neither new nor deranked and never revisits them; they are also not in klineSymbols, so a
		// watchdog restart does not re-dial them. Without this pending list they would have no live
		// producer for the life of the process.
		//
		// dial() aborts the whole batch on one connect failure, so this strands every symbol of the
		// cycle at once. Held for the next call to retry, which is what the log claims.
		a.klinePendingAttach = appendMissing(a.klinePendingAttach, symbols)
		a.logger().Error("kline feed: failed to attach newly discovered symbols; they have no live candle producer until retried",
			"symbols", len(symbols), "exchange", exchangeID, "market_type", marketType, "error", err)
		return
	}
	// The live fleet, so a watchdog restart brings these symbols back with it.
	a.klineSymbols = appendMissing(a.klineSymbols, symbols)
	a.logger().Warn("kline feed: attached newly discovered symbols",
		"symbols", len(symbols), "exchange", exchangeID, "market_type", marketType)
}

// dropFromKlineFeed makes a derank actually stick.
//
// Periodic discovery removes a symbol by unsubscribing it and purging its engine buffer and Redis
// keys. The venue feed heard none of that and keeps pushing the symbol's candle, and UpdateCandle
// recreates the buffer on arrival — so a deliberately removed symbol returns from the dead across
// all eight timeframes, minutes later.
//
// Two things are needed and neither is enough alone: MUTE stops the frames from the live socket
// (which cannot be unsubscribed without redialling the whole partition and dropping every other
// symbol on it), and dropping it from klineSymbols stops the watchdog re-dialing it on the next
// restart. Tracking the live fleet without this would make the restart path carry every deranked
// symbol forward, defeating the purpose.
func (a *App) dropFromKlineFeed(exchangeID, marketType string, symbols []string) {
	a.klineMu.Lock()
	defer a.klineMu.Unlock()

	if a.klineFeed == nil || len(symbols) == 0 {
		return
	}
	if scope := exchangeID + ":" + marketType; scope != a.klineScope {
		return
	}
	a.klineFeed.Mute(symbols)

	gone := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		gone[s] = struct{}{}
	}
	kept := a.klineSymbols[:0]
	for _, s := range a.klineSymbols {
		if _, dropped := gone[s]; !dropped {
			kept = append(kept, s)
		}
	}
	a.klineSymbols = kept
	keptPending := a.klinePendingAttach[:0]
	for _, s := range a.klinePendingAttach {
		if _, dropped := gone[s]; !dropped {
			keptPending = append(keptPending, s)
		}
	}
	a.klinePendingAttach = keptPending
	for _, s := range symbols {
		delete(a.klineGapHeads, engine.CompositeKey{
			Exchange: exchangeID, MarketType: marketType,
			Symbol: market.NormalizeSymbol(s), Timeframe: recovery.FastestBootTimeframe,
		})
	}
	a.logger().Warn("kline feed: muted deranked symbols",
		"symbols", len(symbols), "exchange", exchangeID, "market_type", marketType)
}

// appendMissing returns base plus every element of extra it does not already contain.
//
// Attaching the same symbol twice would open a second socket delivering the same frames and would
// double-count klineSymbolsAttached, so the retry list and the fresh batch are merged rather than
// concatenated — a symbol can appear in both when a cycle re-discovers what a previous cycle failed
// to attach.
func appendMissing(base, extra []string) []string {
	have := make(map[string]struct{}, len(base)+len(extra))
	out := make([]string, 0, len(base)+len(extra))
	for _, batch := range [][]string{base, extra} {
		for _, s := range batch {
			if _, dup := have[s]; dup {
				continue
			}
			have[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}
