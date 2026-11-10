package binancesdk

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/binancews"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/platform/netdiag"
)

// KlineFeed carries the venue's OWN candle for every subscribed symbol.
//
// # WHY THIS IS A SEPARATE CONNECTION
//
// The aggTrade path attaches handlers by walking `symbols[off:off+len(slice)]`, which is only
// correct while streams are 1:1 with symbols. Adding a second stream per symbol to that URL
// breaks the mapping, and that loop is the most delicate code in the service — it is what a
// fleet-wide partition drop runs through.
//
// So this dials its own sockets. The cost is a few more connections, well inside Binance's 1024
// streams per connection; the gain is that the path which is stable today is not touched at all,
// and a failure here cannot take ingestion down with it.
//
// # WHY ONE INTERVAL IS ENOUGH
//
// Every timeframe this service publishes (15m, 30m, 1h, 2h, 4h, 1d, 1w) is an exact multiple of
// 5m, and the fold is exact: volume and both taker legs SUM, high is a MAX, low is a MIN, open is
// the first and close is the last. A single @kline_5m per symbol is therefore a sufficient source
// for all eight, and the stream count follows the same rule as @aggTrade rather than multiplying
// by the number of timeframes.
// klineFaultRestartThreshold is how many consecutive transport errors escalate
// from a socket reconnect to a full feed restart. A single read timeout is not
// enough: the transport reconnects in place and a frame clears the count.
const klineFaultRestartThreshold = 3

// klineFirstFrameDeadline is how long a started feed may run without a single
// frame before it is treated as a configuration failure. A wrong route or
// stream class never delivers, and redialing it only repeats the outage.
const klineFirstFrameDeadline = 60 * time.Second

// KlineFeed owns one venue kline stream: it subscribes, decodes frames and reports
// per-partition liveness to the ingestion layer.
type KlineFeed struct {
	interval   string
	exchange   string
	marketType string
	onCandle   func(symbol string, c candle.Candle)

	// LIVENESS IS PER PARTITION, BECAUSE A FLEET-WIDE SIGNAL CANNOT SEE HALF THE FLEET DIE.
	//
	// A single atomic on the feed, written by EVERY symbol's handler across every partition, lets
	// one dying partition leave the other refreshing the shared clock every few milliseconds —
	// the watchdog sees a healthy feed forever while ~250 symbols stop producing candles on all
	// eight timeframes. 500 symbols dial as 2 partitions (measured in production: "500 symbols
	// over 2 partition(s)"). Ownership silences the tape for the scope, so there is no second
	// writer to cover for them: they simply go stale, and a stale candle differs from Binance's
	// just as surely as a wrong one.
	//
	// A watchdog fed by that shared clock can only ever fire when EVERY partition dies at once,
	// which is the one case that needs no watchdog to notice.
	//
	// So the clock lives on the partition, and the feed reports the WORST of them.

	// muted holds symbols whose frames must be discarded — see Mute.
	//
	// Copy-on-write behind an atomic pointer, because the read is on the hot path (every frame,
	// every symbol) and the write happens only when periodic discovery deranks something, which is
	// rare. Nil until the first derank, so the common case is one atomic load and nothing else.
	muted atomic.Pointer[map[string]struct{}]

	// Operations serialize membership and publication; Stop cancels before
	// waiting for opMu so an in-flight handshake cannot hold shutdown hostage.
	opMu       sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	membership map[string]*klinePartition
	// transportFaults counts consecutive transport errors; a delivered frame
	// resets it. terminal covers failures retrying cannot fix, such as a dial
	// that never attached the fleet.
	transportFaults atomic.Int32
	terminal        atomic.Bool
	startedAt       atomic.Int64
	framed          atomic.Bool
	configFaultOnce sync.Once

	mu      sync.Mutex
	parts   []*klinePartition
	metrics Metrics
	log     *slog.Logger
	// wsBasePath is resolved at construction; tests may override it.
	wsBasePath string
	// netdiag correlates this feed's disconnects with the trade transports.
	netdiag *netdiag.Correlator
}

// SetNetdiag injects the cross-venue disconnect correlator.
func (f *KlineFeed) SetNetdiag(correlator *netdiag.Correlator) { f.netdiag = correlator }

// SetMetrics injects the observability port. It is called by the composition root before
// Start; a zero value remains a safe no-op for tests.
func (f *KlineFeed) SetMetrics(m Metrics) {
	f.metrics = m
	m.setKlineRoute(f.exchange, f.marketType, routeFor(f.exchange, f.marketType, streamClassKline))
	m.initScope(f.exchange, f.marketType)
	if m.KlineInit != nil {
		m.KlineInit(f.exchange, f.marketType)
	}
}

// klinePartition is one combined-stream connection and the symbols riding on it.
//
// lastFrameAt starts at DIAL time rather than zero, which collapses two cases that would
// otherwise need separate handling: a partition that delivered and stopped, and one that never
// delivered at all. Both are "silent since T", and both are equally fatal for the symbols on
// them. It also removes the "never delivered, so do not restart yet" special case that would let
// a partition which failed to produce anything sit dead forever.
type klinePartition struct {
	client      *binancews.Connection
	lastFrameAt atomic.Int64 // ms UTC; initialized to dial time
	symbols     int
	members     []string
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	frames     atomic.Int64
	reconnects atomic.Int64
	restores   atomic.Int64
}

// PartitionStat is one connection's contribution to the feed, published so a
// silent partition is visible without reading the process's sockets.
type PartitionStat struct {
	ID           int
	Symbols      int
	Frames       int64
	LastFrameAge time.Duration
	Reconnects   int64
	Restores     int64
}

// Mute discards a symbol's frames and retains the socket while other symbols use it.
//
// # WHY MUTE INSTEAD OF UNSUBSCRIBE
//
// The SDK's combined-stream connection fixes its stream list at dial time, so dropping one symbol
// means redialling the partition — which would drop the frames of every other symbol on it, to
// stop delivering one. That trade is backwards.
//
// # WHY IT IS NEEDED AT ALL
//
// Periodic discovery deranks a symbol by unsubscribing it and purging its engine buffer and Redis
// keys. The venue feed never heard about any of that and keeps pushing its candle, and UpdateCandle
// recreates the buffer on arrival — so the symbol returns from the dead across all eight
// timeframes, minutes after being deliberately removed. Silencing the frame is what makes the
// derank actually stick until the next restart drops the stream for real.
func (f *KlineFeed) Mute(symbols []string) {
	if len(symbols) == 0 {
		return
	}
	f.opMu.Lock()
	defer f.opMu.Unlock()
	f.mu.Lock()

	next := map[string]struct{}{}
	if cur := f.muted.Load(); cur != nil {
		for s := range *cur {
			next[s] = struct{}{}
		}
	}
	for _, s := range symbols {
		next[s] = struct{}{}
	}
	f.muted.Store(&next)
	// Fully muted partitions have no remaining producer to protect. Retire
	// them instead of letting their silent clocks trigger repeated restarts.
	kept := make([]*klinePartition, 0, len(f.parts))
	var retired []*klinePartition
	for _, p := range f.parts {
		active := false
		for _, symbol := range p.members {
			if _, muted := next[symbol]; !muted {
				active = true
				break
			}
		}
		if active {
			kept = append(kept, p)
		} else {
			retired = append(retired, p)
			for _, symbol := range p.members {
				delete(f.membership, symbol)
			}
		}
	}
	f.parts = kept
	f.mu.Unlock()
	closeKlinePartitions(retired)
	f.reportAttached(0, 0)
}

// Unmute clears a symbol's mute. AddSymbols reconciles its transport membership.
//
// # WHY MUTE ALONE WAS A DEFECT
//
// Discovery is a liquidity-ranked top-500 refreshed every six hours, and the margin at the cut is
// around one percent of 24h volume — so symbols cross the boundary in BOTH directions, routinely.
// Mute shipped without this sibling, which made it monotone: a symbol deranked in one cycle and
// re-promoted in a later one was re-subscribed, re-seeded, counted in klineSymbolsAttached and
// logged as attached, while every frame for it was discarded by the handler's first statement.
//
// With ownership silencing the tape for the whole scope, that symbol had NO producer on any of the
// eight timeframes — the exact state AddSymbols exists to prevent, reached through the door added
// to fix a different instance of it. And the watchdog does not rescue it: a discovery cycle packs
// its symbols onto ONE partition, so a genuinely-new symbol on the same connection keeps that
// partition's clock fresh and the feed never looks silent.
func (f *KlineFeed) Unmute(symbols []string) {
	if len(symbols) == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	cur := f.muted.Load()
	if cur == nil {
		return
	}
	next := map[string]struct{}{}
	for s := range *cur {
		next[s] = struct{}{}
	}
	for _, s := range symbols {
		delete(next, s)
	}
	f.muted.Store(&next)
}

// isMuted reports whether this symbol's frames should be discarded. One atomic load, and nothing
// more in the overwhelmingly common case where nothing has ever been deranked.
func (f *KlineFeed) isMuted(symbol string) bool {
	m := f.muted.Load()
	if m == nil {
		return false
	}
	_, found := (*m)[symbol]
	return found
}

// NewKlineFeed builds a feed that emits one candle per received frame.
//
// onCandle runs inline on each partition's reader, so the sink must be safe for concurrent
// partitions. It is, because it is the engine, whose
// UpdateCandle takes the write lock and orders venue frames by the venue's own clock rather than
// by arrival.
func NewKlineFeed(parent context.Context, exchange, marketType, interval string, onCandle func(symbol string, c candle.Candle), logs ...*slog.Logger) *KlineFeed {
	if interval == "" {
		interval = "5m"
	}
	ctx, cancel := context.WithCancel(parent)
	feed := &KlineFeed{
		interval: interval, exchange: exchange, marketType: marketType, onCandle: onCandle,
		ctx: ctx, cancel: cancel, membership: make(map[string]*klinePartition), log: logger.OrDiscard(logs...),
		wsBasePath: defaultFuturesWSBasePath(),
		netdiag:    netdiag.New(),
	}
	if isTestnetWSBase(feed.wsBasePath) {
		feed.log.Warn("kline feed: Binance futures TESTNET configured; candle volume is NOT production data",
			"ws_base", feed.wsBasePath,
			"override", "GOWHALE_BINANCE_FUTURES_WS_URL")
	}
	return feed
}

// NeedsRestart reports a closed lifecycle or a latched transport failure.
// Stop is terminal for this instance; the owner must construct a fresh feed.
func (f *KlineFeed) NeedsRestart() bool {
	return f.terminal.Load() || f.transportFaults.Load() >= klineFaultRestartThreshold || f.ctx.Err() != nil
}

// noteFrame clears the consecutive-fault count: the transport proved it can
// deliver, so an earlier failure must not accumulate toward a restart.
func (f *KlineFeed) noteFrame() {
	f.transportFaults.Store(0)
	f.framed.Store(true)
}

// ConfigFault reports a feed that never delivered a frame within the first-frame
// deadline. The owner must alarm; reconnecting cannot fix a wrong route.
func (f *KlineFeed) ConfigFault(now time.Time) bool {
	started := f.startedAt.Load()
	if started == 0 || f.framed.Load() {
		return false
	}
	if now.Sub(time.UnixMilli(started)) < klineFirstFrameDeadline {
		return false
	}
	f.configFaultOnce.Do(func() {
		logger.OrDiscard(f.log).Error("kline feed: no frame within the first-frame deadline; check the route and stream class",
			"exchange", f.exchange,
			"market_type", f.marketType,
			"route", routeFor(f.exchange, f.marketType, streamClassKline),
			"interval", f.interval)
	})
	return true
}

// SecondsSinceLastFrame reports how long the WORST partition has been silent, and whether there
// is any partition to report on.
//
// The worst, not the newest. Averaging or taking the newest is what made a dead partition
// invisible: with 500 symbols on 2 connections, the survivor refreshes a shared clock every few
// milliseconds while the other half of the fleet goes stale. The feed is only as live as its
// quietest connection, because the symbols on that connection have no other producer.
func (f *KlineFeed) SecondsSinceLastFrame() (float64, bool) {
	f.mu.Lock()
	parts := f.parts
	f.mu.Unlock()
	if len(parts) == 0 {
		return 0, false
	}
	now := time.Now().UnixMilli()
	worst := float64(0)
	for _, p := range parts {
		if s := float64(now-p.lastFrameAt.Load()) / 1000; s > worst {
			worst = s
		}
	}
	return worst, true
}

// QuietestPartition reports the silence of the worst partition and how many symbols ride on it,
// so a restart can say what it is actually rescuing.
func (f *KlineFeed) QuietestPartition() (silentSeconds float64, symbols int) {
	f.mu.Lock()
	parts := f.parts
	f.mu.Unlock()
	now := time.Now().UnixMilli()
	for _, p := range parts {
		if s := float64(now-p.lastFrameAt.Load()) / 1000; s > silentSeconds {
			silentSeconds, symbols = s, p.symbols
		}
	}
	return silentSeconds, symbols
}

// PartitionStats reports one row per live partition: its frames, the age of its
// last frame, and how often it was replaced or restored.
func (f *KlineFeed) PartitionStats() []PartitionStat {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now().UnixMilli()
	out := make([]PartitionStat, 0, len(f.parts))
	for _, part := range f.parts {
		out = append(out, PartitionStat{
			ID:           part.client.ID,
			Symbols:      part.symbols,
			Frames:       part.frames.Load(),
			LastFrameAge: time.Duration(now-part.lastFrameAt.Load()) * time.Millisecond,
			Reconnects:   part.reconnects.Load(),
			Restores:     part.restores.Load(),
		})
	}
	return out
}

// klineStreams maps domain symbols to their kline stream names for this feed's interval.
func (f *KlineFeed) klineStreams(symbols []string) []string {
	out := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		out = append(out, strings.ToLower(strings.ReplaceAll(sym, "/", ""))+"@kline_"+f.interval)
	}
	return out
}

// Start dials the partitions and attaches a handler per symbol.
//
// Partitioning reuses the same budget the aggTrade path uses, because the constraint is a
// property of the connection rather than of the stream type.
func (f *KlineFeed) Start(symbols []string) error {
	f.opMu.Lock()
	defer f.opMu.Unlock()
	if err := f.ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	old := f.parts
	f.parts = nil
	f.mu.Unlock()
	clear(f.membership)
	closeKlinePartitions(old)
	// Old watchers have joined. Any event from this new attempt must remain
	// latched, including one arriving before the full fleet is published.
	f.transportFaults.Store(0)
	f.terminal.Store(false)
	f.startedAt.Store(time.Now().UnixMilli())
	f.framed.Store(false)
	f.configFaultOnce = sync.Once{}
	symbols = uniqueKlineSymbols(symbols)
	restarted := f.startedAt.Load() != 0
	parts, attached, err := f.dial(symbols)
	if err != nil {
		f.terminal.Store(true)
		f.reportAttached(0, 0)
		return err
	}
	if restarted {
		for _, part := range parts {
			part.reconnects.Add(1)
		}
	}
	f.registerMembership(parts)
	f.mu.Lock()
	f.parts = parts // Start REPLACES the partition set; AddSymbols appends to it.
	f.mu.Unlock()
	f.reportAttached(attached, len(symbols))
	return nil
}

// AddSymbols attaches handlers for symbols discovered after Start.
//
// Without it, periodic discovery creates a scope where ownership has silenced the tape for all
// eight timeframes and the venue feed has never heard of the symbol — so it has NO producer at
// all, permanently, until a restart. That is strictly worse than the two-writer topology this
// design replaced.
//
// New symbols go onto their OWN connection rather than into an existing partition: the SDK's
// combined-stream connection is established with its stream list at dial time, so growing one
// means redialling it, and redialling a live partition to add one symbol would drop the frames of
// every symbol already on it.
func (f *KlineFeed) AddSymbols(symbols []string) error {
	f.opMu.Lock()
	defer f.opMu.Unlock()
	if err := f.ctx.Err(); err != nil {
		return err
	}
	symbols = uniqueKlineSymbols(symbols)
	// Attaching a symbol and silencing it are contradictory instructions, and the mute is the
	// stale one: it was recorded when this symbol was deranked, and discovery has now decided it
	// belongs again. Clearing it here rather than at the caller keeps the pair inseparable — the
	// defect this fixes existed because Mute had a call site and Unmute had none.
	f.Unmute(symbols)
	missing := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		p, exists := f.membership[symbol]
		if !exists {
			missing = append(missing, symbol)
			continue
		}
		if p.ctx.Err() != nil || p.client == nil || !p.client.Healthy() {
			f.terminal.Store(true)
			return fmt.Errorf("kline symbol %s needs feed restart", symbol)
		}
	}
	parts, attached, err := f.dial(missing)
	if err != nil {
		f.terminal.Store(true)
		f.reportAttached(0, 0)
		return err
	}
	for _, part := range parts {
		part.restores.Store(int64(part.symbols))
	}
	f.registerMembership(parts)
	f.mu.Lock()
	f.parts = append(f.parts, parts...)
	f.mu.Unlock()
	f.reportAttached(attached, len(missing))
	return nil
}

func uniqueKlineSymbols(symbols []string) []string {
	seen := make(map[string]struct{}, len(symbols))
	out := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		symbol = strings.ToUpper(symbol)
		if _, exists := seen[symbol]; !exists {
			seen[symbol] = struct{}{}
			out = append(out, symbol)
		}
	}
	return out
}

// Caller holds opMu; only fully opened attempts publish membership.
func (f *KlineFeed) registerMembership(parts []*klinePartition) {
	for _, p := range parts {
		for _, symbol := range p.members {
			f.membership[symbol] = p
		}
	}
}

// dial opens the partitions for these symbols and attaches a handler to each, WITHOUT deciding
// what happens to the resulting connections — Start replaces, AddSymbols appends.
//
// Sharing one receiver is what lets membership and the mute set live on one feed. A second
// KlineFeed would carry its own liveness clock (papered over by a wrapping callback) and, once
// symbols could be deranked, its own mute set — so muting a symbol on the real feed would
// silently fail to reach any symbol discovery had added. Sharing one receiver removes the class
// rather than patching this instance of it.
func (f *KlineFeed) dial(symbols []string) ([]*klinePartition, int, error) {
	if len(symbols) == 0 {
		return nil, 0, nil
	}
	streams := f.klineStreams(symbols)
	parts := partitionStreams(streams)

	opened := make([]*klinePartition, 0, len(parts))
	attached := 0
	off := 0
	for i, slice := range parts {
		ctx, cancel := context.WithCancel(f.ctx)
		ws := binancews.NewConnection(i, f.wsBasePath,
			binancews.WithRoute(routeFor(f.exchange, f.marketType, streamClassKline)),
			binancews.WithReadIdle(readIdleFor(f.exchange, f.marketType)))
		part := symbols[off : off+len(slice)]
		off += len(part)

		conn := &klinePartition{client: ws, symbols: len(slice), ctx: ctx, cancel: cancel}
		handlers := make(map[string]binancews.Handler, len(part))
		for _, symbol := range part {
			symbol := symbol
			stream := strings.ToLower(strings.ReplaceAll(symbol, "/", "")) + "@kline_" + f.interval
			handlers[stream] = func(data []byte) {
				if conn.ctx.Err() != nil || f.isMuted(symbol) {
					return
				}
				var event klineEvent
				if err := json.Unmarshal(data, &event); err != nil {
					f.metrics.incKlineRejected(f.exchange, f.marketType)
					return
				}
				candle, ok := toVenueCandle(event)
				if !ok {
					f.metrics.incKlineRejected(f.exchange, f.marketType)
					return
				}
				f.metrics.incKlineFrame(f.exchange, f.marketType)
				f.metrics.incKlinePartitionFrame(f.exchange, f.marketType, i)
				f.noteFrame()
				conn.frames.Add(1)
				conn.lastFrameAt.Store(time.Now().UnixMilli())
				f.onCandle(symbol, candle)
			}
		}
		if err := connectMarketStream(ctx, f.ctx, ws, handlers); err != nil {
			cancel()
			closeKlinePartitions(opened)
			return nil, 0, fmt.Errorf("kline partition %d/%d (%d streams): %w", i+1, len(parts), len(slice), err)
		}
		conn.lastFrameAt.Store(time.Now().UnixMilli())
		opened = append(opened, conn)
		attached += len(part)
		conn.members = append(conn.members, part...)

		conn.wg.Add(1)
		go func(partitionID int) {
			defer conn.wg.Done()
			watchTransportError(conn.ctx, partitionID, ws, f.marketType, f.metrics, f.netdiag, func(string) { f.transportFaults.Add(1) }, f.log)
		}(i)
	}
	if err := f.ctx.Err(); err != nil {
		closeKlinePartitions(opened)
		return nil, 0, err
	}
	logger.OrDiscard(f.log).Warn("kline feed attached symbols",
		"symbols", attached, "partitions", len(parts), "timeframe", f.interval,
		"route", routeFor(f.exchange, f.marketType, streamClassKline),
		"stream_class", streamClassKline)
	return opened, attached, nil
}

// reportAttached publishes the gauge and says so out loud when a symbol ended up with no producer.
//
// Reconciliation reports current active membership, never the number of past dials.
func (f *KlineFeed) reportAttached(attached, requested int) {
	active := 0
	for symbol := range f.membership {
		if !f.isMuted(symbol) {
			active++
		}
	}
	f.metrics.setKlineAttached(f.exchange, f.marketType, float64(active))
	// A per-symbol attach failure inside a successful dial is not fatal, but it is not free either:
	// ownership silences the tape for the whole scope, so a symbol without a handler has NO producer
	// for any timeframe. Logged at WARN so it is visible at production log level.
	if attached < requested {
		logger.OrDiscard(f.log).Warn("kline feed: symbols without a candle producer",
			"attached", attached, "requested", requested, "missing", requested-attached)
	}
}

// Stop permanently cancels this feed, aborts any dial, and joins every partition.
func (f *KlineFeed) Stop() {
	f.cancel()
	f.opMu.Lock()
	defer f.opMu.Unlock()
	f.mu.Lock()
	parts := f.parts
	f.parts = nil
	f.mu.Unlock()
	clear(f.membership)
	closeKlinePartitions(parts)
	f.reportAttached(0, 0)
}

func closeKlinePartitions(parts []*klinePartition) {
	for _, p := range parts {
		if p.cancel != nil {
			p.cancel()
		}
	}
	for _, p := range parts {
		if p.client != nil {
			_ = p.client.Close()
		}
		p.wg.Wait()
	}
}

// toVenueCandle maps the venue's frame onto our contract.
//
// # WHAT IS TRANSPORTED AND WHAT IS DERIVED
//
// open, high, low, close, volume and taker-buy are the venue's own numbers and are copied. The
// sell leg is DERIVED as volume − takerBuy, and on the venue's side that identity is exact by
// construction: every trade has exactly one taker and the venue has no unclassified bucket. It is
// the same derivation the parity gate applies to the exchange side, for the same reason.
//
// KlineEventAt carries the venue's own event clock, which orders frames for the same bucket
// independently of partition replacement and arrival.
//
// A frame missing any field it should carry is DROPPED rather than defaulted. A zero volume that
// means "the venue did not say" is indistinguishable from one that means "nothing traded", and
// this whole activity exists because a fabricated number outlived every attempt to find it.
func toVenueCandle(event klineEvent) (candle.Candle, bool) {
	k := event.Kline
	if k == nil || k.OpenTime == nil || k.Open == nil || k.High == nil ||
		k.Low == nil || k.Close == nil || k.Volume == nil || k.TakerBuyBaseVol == nil {
		return candle.Candle{}, false
	}
	open, err1 := strconv.ParseFloat(*k.Open, 64)
	high, err2 := strconv.ParseFloat(*k.High, 64)
	low, err3 := strconv.ParseFloat(*k.Low, 64)
	cls, err4 := strconv.ParseFloat(*k.Close, 64)
	vol, err5 := strconv.ParseFloat(*k.Volume, 64)
	buy, err6 := strconv.ParseFloat(*k.TakerBuyBaseVol, 64)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil || err6 != nil {
		return candle.Candle{}, false
	}

	// The venue publishes only the buy leg; the remainder is the sell leg, exactly.
	sell := vol - buy
	if sell < 0 {
		// Never observed, and if it happens the frame is not internally consistent — dropping it
		// is the only answer that does not publish a number the venue did not state.
		return candle.Candle{}, false
	}

	eventAt := int64(0)
	if event.EventTime != nil {
		eventAt = *event.EventTime
	}
	return candle.NormalizeOHLC(candle.Candle{
		Timestamp:       *k.OpenTime,
		Open:            open,
		High:            high,
		Low:             low,
		Close:           cls,
		Volume:          vol,
		TakerBuyVolume:  buy,
		TakerSellVolume: sell,
		KlineEventAt:    eventAt,
		Source:          string(redisstore.CandleMergeSourceWSKline),
	}), true
}
