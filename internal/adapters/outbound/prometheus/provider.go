// Package prommetrics is the concrete Prometheus implementation of every
// capability-scoped metrics port in the process. It owns all collector definitions and
// registers them against an injected Registerer, so no domain package imports Prometheus
// and no collector lives on the default registry.
package prommetrics

import (
	"math"
	"strconv"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/marxms/go-whale/internal/adapters/inbound/realtime"
	binancesdk "github.com/marxms/go-whale/internal/adapters/outbound/binance"
	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/adapters/outbound/venue"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/derivatives/collector"
	"github.com/marxms/go-whale/internal/health"
	"github.com/marxms/go-whale/internal/ingestion"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/platform/netdiag"
	"github.com/marxms/go-whale/internal/recovery"
)

// Prometheus namespace, subsystem and label names. They are the metric ABI — dashboards
// and alerts resolve these exact strings — so every collector spells them from one place.
const (
	namespaceGowhale = "gowhale"

	subsystemStream      = "stream"
	subsystemStreaming   = "streaming"
	subsystemBinanceSDK  = "binancesdk"
	subsystemDerivatives = "derivatives"
	subsystemCoinalyze   = "coinalyze"

	labelExchange   = "exchange"
	labelMarketType = "market_type"
	labelReason     = "reason"
	labelSymbol     = "symbol"
	labelOutcome    = "outcome"
	labelTimeframe  = "timeframe"
	labelFeed       = "feed"
	labelMarket     = "market"
	labelCapability = "capability"
)

// Provider holds every registered collector and exposes the owning-package ports.
type Provider struct {
	streaming   streaming.Metrics
	binance     binancesdk.Metrics
	realtime    realtime.Metrics
	venue       venue.Metrics
	questdb     questdb.Metrics
	redis       redisstore.Metrics
	engine      engine.Metrics
	collector   collector.Metrics
	healthRedis health.RedisBudgetMetrics
	ingestion   ingestion.Metrics
	netdiag     netdiag.Metrics
	recovery    recovery.Metrics
	healthSink  health.GaugeSink

	coinalyzeCycleSeconds func(float64)
	coinalyzeCycleOverrun func()
	bootBucketSettle      func(exchange, marketType, timeframe string)
	gappedBucketRepair    func(exchange, marketType, timeframe string)

	collectors []prometheus.Collector

	// coldFlushAge is set explicitly by the composition root once the durable store
	// exists. Nil means "no cold tier", which the gauge publishes as NaN.
	coldFlushAge atomic.Value // stores func() float64
}

// SetColdFlushAgeSource wires the durable tier's last-commit clock into
// cold_last_flush_age_seconds. The GaugeFunc is already registered and reads this source at
// scrape time, so no collector is re-registered when the store is built.
func (p *Provider) SetColdFlushAgeSource(fn func() float64) {
	if fn == nil {
		return
	}
	p.coldFlushAge.Store(fn)
}

func (p *Provider) coldFlushAgeValue() float64 {
	fn, ok := p.coldFlushAge.Load().(func() float64)
	if !ok || fn == nil {
		return math.NaN()
	}
	return fn()
}

// New defines every collector and registers it against reg. A duplicate registration
// returns an error: constructing the set twice against one registry must fail.
func New(reg prometheus.Registerer) (*Provider, error) {
	p := &Provider{}

	// Streaming.
	streamLiveness := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemStream,
		Name: "liveness",
		Help: "1 if a healthy connected stream exists for the exchange:market_type scope, else 0",
	}, []string{labelExchange, labelMarketType})
	streamSymbolsTotal := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemStream,
		Name: "symbols_total",
		Help: "Number of monitored (non-TradFi) subscribed symbols per exchange:market_type scope",
	}, []string{labelExchange, labelMarketType})
	streamSymbolsStale := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemStream,
		Name: "symbols_stale",
		Help: "Number of monitored symbols with a stale most-recent persisted candle; TradFi markets follow venue hours and are excluded",
	}, []string{labelExchange, labelMarketType})
	startupFailures := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemStreaming,
		Name: "startup_failures_total",
		Help: "Total startup failures by exchange, market_type, and stage",
	}, []string{labelExchange, labelMarketType, "stage"})
	bootSeedConfirmed := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemStreaming,
		Name: "boot_seed_delivery_confirmed",
		Help: "Symbols whose live stream was confirmed delivering before the boot seed snapshot, per exchange:market_type",
	}, []string{labelExchange, labelMarketType})
	bootSeedBlind := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemStreaming,
		Name: "boot_seed_delivery_blind",
		Help: "Symbols seeded at boot without confirmed stream delivery (grace expired), per exchange:market_type",
	}, []string{labelExchange, labelMarketType})

	p.streaming = streaming.Metrics{
		StreamLiveness:            func(ex, mt string, live bool) { streamLiveness.WithLabelValues(ex, mt).Set(boolFloat(live)) },
		StreamSymbolsTotal:        func(ex, mt string, n float64) { streamSymbolsTotal.WithLabelValues(ex, mt).Set(n) },
		StreamSymbolsStale:        func(ex, mt string, n float64) { streamSymbolsStale.WithLabelValues(ex, mt).Set(n) },
		StartupFailures:           func(ex, mt, stage string) { startupFailures.WithLabelValues(ex, mt, stage).Inc() },
		BootSeedDeliveryConfirmed: func(ex, mt string, n float64) { bootSeedConfirmed.WithLabelValues(ex, mt).Set(n) },
		BootSeedDeliveryBlind:     func(ex, mt string, n float64) { bootSeedBlind.WithLabelValues(ex, mt).Set(n) },
	}

	// Health sink: liveness is the same series the health aggregation publishes.
	p.healthSink = health.GaugeSink{
		StreamLiveness: func(ex, mt string, live bool) { streamLiveness.WithLabelValues(ex, mt).Set(boolFloat(live)) },
	}

	// Binance.
	goroutinesTotal := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "goroutines_total",
		Help: "Total number of goroutines in the SDKClient by exchange and market type",
	}, []string{labelExchange, labelMarketType})
	reconnectAttempts := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "reconnect_attempts_total",
		Help: "Total number of reconnection attempts by exchange, market type, and reason",
	}, []string{labelExchange, labelMarketType, labelReason})
	reconnectFailures := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "reconnect_failures_total",
		Help: "Total number of failed reconnection attempts by exchange and market type",
	}, []string{labelExchange, labelMarketType})
	disconnectCause := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "disconnect_cause_total",
		Help: "WebSocket disconnects by classified cause (close_<code>/tcp_reset/eof/io_timeout/...), captured from the SDK ErrorChan.",
	}, []string{labelExchange, labelMarketType, "cause"})
	lastMessageAge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "last_message_age_seconds",
		Help: "Age of the last received message in seconds by exchange and market type",
	}, []string{labelExchange, labelMarketType})
	partitionStale := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "partition_stale_fraction",
		Help: "Fraction of a partition's symbols stale per the liveness ledger",
	}, []string{labelExchange, labelMarketType, "partition"})
	healthState := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "health_state",
		Help: "Current health state: 0=healthy, 1=stalled, 2=reconnecting, 3=disconnected",
	}, []string{labelExchange, labelMarketType})
	messagesIn := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "messages_in_total",
		Help: "Total number of messages received by exchange and market type",
	}, []string{labelExchange, labelMarketType})
	tradesDropped := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "trades_dropped_total",
		Help: "Trades LOST by exchange and market type (parse failure, callback panic, or an append refused by a full buffer). Excludes trades that were stored.",
	}, []string{labelExchange, labelMarketType})
	tradesIngested := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "trades_ingested_total",
		Help: "Trades parsed AND buffered by exchange and market type. The ingest numerator: messages_in_total counts frames, this counts trades that survived into a buffer.",
	}, []string{labelExchange, labelMarketType})
	reconnectSignalsDropped := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "reconnect_signals_dropped_total",
		Help: "Total number of reconnect signals dropped because a reconnect was already queued, by exchange and market type",
	}, []string{labelExchange, labelMarketType})
	resubscribeIncomplete := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "resubscribe_incomplete_total",
		Help: "Total symbols not restored after a reconnect (partial fleet restore), by exchange and market type",
	}, []string{labelExchange, labelMarketType})
	resubscribeUnverified := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "resubscribe_unverified_total",
		Help: "Symbols attached but not delivering data within the verification window after a reconnect",
	}, []string{labelExchange, labelMarketType})
	dynamicSubscribeDeferred := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "dynamic_subscribe_deferred_total",
		Help: "Dynamic subscribes deferred because the target partition was not live",
	}, []string{labelExchange, labelMarketType})
	starvationSuppressed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemBinanceSDK,
		Name: "starvation_suppressed_reconnect_total",
		Help: "Watchdog reconnects suppressed because all partitions stale coincided with process descheduling",
	}, []string{labelExchange, labelMarketType})
	klineFramesTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kline_feed_frames_total",
		Help: "Venue candle frames accepted from the stream, by scope. A FLAT series while the market is open is the signature of a dead partition.",
	}, []string{labelExchange, labelMarketType})
	klineFramesRejected := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kline_feed_frames_rejected_total",
		Help: "Frames dropped because a field the contract requires was absent or unparseable. Dropping is correct — a zero that means 'the venue did not say' is indistinguishable from one that means 'nothing traded' — but a rising series means the payload changed shape.",
	}, []string{labelExchange, labelMarketType})
	klineSymbolsAttached := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kline_feed_symbols_attached",
		Help: "Symbols with a live handler. Below the subscribed fleet size, the difference is symbols whose candles have NO producer, because ownership silences the tape for the whole scope.",
	}, []string{labelExchange, labelMarketType})
	klinePartitionFrames := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kline_feed_partition_frames_total",
		Help: "Candle frames accepted per connection. A partition whose series stays flat while its peers advance is the silent socket.",
	}, []string{labelExchange, labelMarketType, "partition"})
	klineRoute := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kline_feed_route",
		Help: "Stream-class route the feed dialled. An unrouted futures connection receives only public-class streams and zero klines.",
	}, []string{labelExchange, labelMarketType, "route"})

	p.binance = binancesdk.Metrics{
		GoroutinesTotal:        func(ex, mt string, n float64) { goroutinesTotal.WithLabelValues(ex, mt).Set(n) },
		ReconnectAttemptsTotal: func(ex, mt, reason string) { reconnectAttempts.WithLabelValues(ex, mt, reason).Inc() },
		ReconnectFailuresTotal: func(ex, mt string) { reconnectFailures.WithLabelValues(ex, mt).Inc() },
		DisconnectCauseTotal:   func(ex, mt, cause string) { disconnectCause.WithLabelValues(ex, mt, cause).Inc() },
		LastMessageAgeSeconds:  func(ex, mt string, age float64) { lastMessageAge.WithLabelValues(ex, mt).Set(age) },
		PartitionStaleFraction: func(ex, mt, partition string, fraction float64) {
			partitionStale.WithLabelValues(ex, mt, partition).Set(fraction)
		},
		PartitionStaleFractionDelete: func(ex, mt string) {
			partitionStale.DeletePartialMatch(prometheus.Labels{labelExchange: ex, labelMarketType: mt})
		},
		HealthState:                   func(ex, mt string, state float64) { healthState.WithLabelValues(ex, mt).Set(state) },
		MessagesInTotal:               func(ex, mt string) { messagesIn.WithLabelValues(ex, mt).Inc() },
		TradesDroppedTotal:            func(ex, mt string) { tradesDropped.WithLabelValues(ex, mt).Inc() },
		TradesIngestedTotal:           func(ex, mt string) { tradesIngested.WithLabelValues(ex, mt).Inc() },
		ReconnectSignalsDropped:       func(ex, mt string) { reconnectSignalsDropped.WithLabelValues(ex, mt).Inc() },
		ResubscribeIncompleteTotal:    func(ex, mt string, n float64) { resubscribeIncomplete.WithLabelValues(ex, mt).Add(n) },
		ResubscribeUnverifiedTotal:    func(ex, mt string, n float64) { resubscribeUnverified.WithLabelValues(ex, mt).Add(n) },
		DynamicSubscribeDeferredTotal: func(ex, mt string) { dynamicSubscribeDeferred.WithLabelValues(ex, mt).Inc() },
		StarvationSuppressedTotal:     func(ex, mt string) { starvationSuppressed.WithLabelValues(ex, mt).Inc() },
		KlineFramesTotal:              func(ex, mt string) { klineFramesTotal.WithLabelValues(ex, mt).Inc() },
		KlineFramesRejected:           func(ex, mt string) { klineFramesRejected.WithLabelValues(ex, mt).Inc() },
		KlineSymbolsAttached:          func(ex, mt string, n float64) { klineSymbolsAttached.WithLabelValues(ex, mt).Set(n) },
		KlinePartitionFrame: func(ex, mt string, partition int) {
			klinePartitionFrames.WithLabelValues(ex, mt, strconv.Itoa(partition)).Inc()
		},
		KlineRoute: func(ex, mt, route string) { klineRoute.WithLabelValues(ex, mt, route).Set(1) },
		KlineInit: func(ex, mt string) {
			klineFramesTotal.WithLabelValues(ex, mt)
			klineFramesRejected.WithLabelValues(ex, mt)
			klineSymbolsAttached.WithLabelValues(ex, mt).Set(0)
		},
		InitScope: func(ex, mt string) {
			messagesIn.WithLabelValues(ex, mt)
			tradesIngested.WithLabelValues(ex, mt)
			tradesDropped.WithLabelValues(ex, mt)
		},
	}

	// Realtime.
	realtimeDropped := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "realtime_publish_dropped_total",
		Help: "Realtime events discarded because the outbound queue was full, by channel. >0 means the bus could not keep up and subscribers will detect a sequence gap and rehydrate.",
	}, []string{"channel"})
	p.realtime = realtime.Metrics{
		PublishDropped: func(channel string) { realtimeDropped.WithLabelValues(channel).Inc() },
	}

	// Coverage gauges updated by the health aggregation.
	symbolsConfigured := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gowhale_symbols_configured",
		Help: "Symbols the configuration declares for this exchange/market_type.",
	}, []string{labelExchange, labelMarketType})
	symbolsEffective := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gowhale_symbols_effective",
		Help: "Symbols actually subscribed for this exchange/market_type. Below gowhale_symbols_configured means a silent coverage deficit.",
	}, []string{labelExchange, labelMarketType})
	p.healthSink.SymbolsConfigured = func(ex, mt string, n int) { symbolsConfigured.WithLabelValues(ex, mt).Set(float64(n)) }
	p.healthSink.SymbolsEffective = func(ex, mt string, n int) { symbolsEffective.WithLabelValues(ex, mt).Set(float64(n)) }

	// Venue.
	lsrShareSumViolation := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "long_short_share_sum_violation_total",
		Help: "Long/short rows rejected because longAccount + shortAccount did not sum to 1 within tolerance, or because one of the two was absent.",
	}, []string{labelExchange, labelSymbol, labelReason})
	capabilityAbsent := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale,
		Name:      "venue_capability_absent_total",
		Help:      "Requests refused because the venue scope declares the capability absent.",
	}, []string{labelExchange, labelMarketType, labelCapability})
	p.venue = venue.Metrics{
		LongShortShareSumViolation: func(ex, symbol, reason string) {
			lsrShareSumViolation.WithLabelValues(ex, symbol, reason).Inc()
		},
		CapabilityAbsent: func(ex, mt, capability string) {
			capabilityAbsent.WithLabelValues(ex, mt, capability).Inc()
		},
	}

	// QuestDB.
	derivativesColdDegraded := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: "derivatives_cold",
		Name: "degraded_rows_total",
		Help: "Durable derivatives rows written by a degraded cycle, by what the erosion guard " +
			"chose: preserved (the previous value was carried forward) or hole (the row was " +
			"dropped so the previous value would not be erased).",
	}, []string{labelOutcome})
	coldPipelineBlocked := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cold_pipeline_blocked", Help: "1 when an error retains unconfirmed cold DAY work (including uncertain ACK), 0 during normal apply waits.",
	})
	coldPipelinePendingRows := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cold_pipeline_pending_rows", Help: "Rows owned by the pipeline sweep until confirmed applied; excludes entries still in its bounded input channel.",
	})
	coldSeedNoopRows := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cold_seed_noop_rows_total", Help: "Seed/backfill rows whose complete physical cold write was already applied before this sweep; this is not a WAL acknowledgement for other rows.",
	})
	coldFlushAge := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cold_last_flush_age_seconds",
		Help: "Seconds since the cold tier (QuestDB) last COMMITTED a batch. Climbs when writes are failing or when no closed candles are being produced; NaN when no cold store is configured. This is the signal that was absent during the 13h silent cold-write outage.",
	}, p.coldFlushAgeValue)
	p.questdb = questdb.Metrics{
		DerivativesColdDegraded: func(outcome string, n float64) { derivativesColdDegraded.WithLabelValues(outcome).Add(n) },
		ColdPipelineBlocked:     func(v float64) { coldPipelineBlocked.Set(v) },
		ColdPipelinePendingRows: func(v float64) { coldPipelinePendingRows.Set(v) },
		ColdSeedNoopRows:        func(n float64) { coldSeedNoopRows.Add(n) },
	}

	// Redis derivatives.
	seriesNonFinite := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "series_non_finite_discarded_total",
		Help: "Numeric fields that are not representable (NaN/±Inf) replaced by ABSENCE at " +
			"snapshot write, per field. Non-zero means the feed delivered a value that is not " +
			"a number; before this sweep, a single one canceled the series write for that " +
			"symbol across every timeframe.",
	}, []string{"field"})
	p.redis = redisstore.Metrics{
		SeriesNonFiniteDiscarded: func(field string) { seriesNonFinite.WithLabelValues(field).Inc() },
	}

	// Candle engine.
	warmSeed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "candle_warm_seed_total",
		Help: "Warm-boot buffer seeding by outcome, one sample per (symbol, timeframe) — the outcomes sum to symbols x timeframes. outcome=questdb|redis (seeded from that tier), outcome=empty (tiers reachable, no history), outcome=unavailable (a tier errored — historical measurements were not loaded), outcome=budget_exhausted (the warm-up budget expired before this pair was attempted at all).",
	}, []string{labelExchange, labelMarketType, labelTimeframe, labelOutcome})
	deltaInsideHorizon := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "candle_delta_inside_horizon_total",
		Help: "Live deltas dropped from an OPEN bucket because their newest trade predates the RequestedAt of an exchange total already merged into that bucket — the total already counts them.",
	}, []string{labelExchange, labelMarketType, labelTimeframe})
	appliedDeltaVolume := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "engine_applied_delta_volume_total",
		Help: "Base-asset quantity from live deltas actually accumulated into a candle, by timeframe. Compare the single-timeframe series against stream_admitted_volume_total: equal means nothing is lost inside this process.",
	}, []string{labelExchange, labelMarketType, labelTimeframe})
	coldMissingBuckets := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cold_missing_buckets",
		Help: "Missing closed-candle buckets in the cold tier (QuestDB) BETWEEN each symbol's first and last timestamp in the lookback window, summed across symbols with data, per exchange/market_type/timeframe. >0 = a permanent historical hole. Says NOTHING about symbols with no rows at all — see cold_symbols_absent.",
	}, []string{labelExchange, labelMarketType, labelTimeframe})
	coldSymbolsWithGaps := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cold_symbols_with_gaps",
		Help: "Number of symbols with >=1 missing cold bucket, per exchange/market_type/timeframe.",
	}, []string{labelExchange, labelMarketType, labelTimeframe})
	coldSymbolsExpected := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cold_symbols_expected",
		Help: "Active (subscribed) symbols for this exchange/market_type — the denominator for cold_symbols_absent.",
	}, []string{labelExchange, labelMarketType, labelTimeframe})
	coldSymbolsAbsent := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cold_symbols_absent",
		Help: "Active symbols with NO cold rows at all in the lookback window, per exchange/market_type/timeframe. Invisible to cold_missing_buckets (which only measures holes between a symbol's own first and last row); a value approaching cold_symbols_expected means the cold tier stopped accepting writes.",
	}, []string{labelExchange, labelMarketType, labelTimeframe})
	durableRows := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "durable_rows",
		Help: "Rows in the durable table per timeframe.",
	}, []string{labelTimeframe})
	durableMissingBuckets := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "durable_missing_buckets",
		Help: "Buckets with NO row between the oldest and the newest of the timeframe. This is TABLE continuity, not per-symbol continuity.",
	}, []string{labelTimeframe})
	durableColumnCovered := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "durable_column_covered",
		Help: "1 when the declared column carries a non-null row AND the durable read reaches it, 0 otherwise. Coverage is written ∧ readable.",
	}, []string{labelTimeframe, "column"})
	durableTimeframeCovered := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "durable_timeframe_covered",
		Help: "1 when the declared coverage timeframe has at least one durable row, 0 otherwise.",
	}, []string{labelTimeframe})
	durableIncoherentFunding := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "durable_incoherent_funding_rows",
		Help: "Rows with funding_rate_8h under an n_step that does not authorize normalization. " +
			"This is GROUP coherence: per-column filling counts these rows as healthy.",
	}, []string{labelTimeframe})
	durableDimensionsMeasured := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "durable_dimensions_measured",
		Help: "How many of the required integrity dimensions the last pass actually measured.",
	})
	foldSeedRevisions := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "candle_seed_ancestor_revisions_unconfirmed_total",
		Help: "Closed ancestor revisions produced by seed repair; the existing callback does not confirm durable admission.",
	})
	lateUpdate := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "candle_late_update_total",
		Help: "Updates received for an already-rolled bucket, by merge branch. reason=additive (ws_delta accumulated), reason=max_total (REST/snapshot total MAX-merged), reason=inserted (bucket was absent and IS NOW IN the ring), reason=evicted (bucket was older than the whole ring and the trim discarded it — only gap recovery can fix it).",
	}, []string{labelExchange, labelMarketType, labelTimeframe, labelReason})
	seedMerge := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "candle_seed_merge_total",
		Help: "Buckets folded into a live buffer by a seed/repair batch, by outcome. outcome=merged (present in both, unioned), outcome=inserted (absent from the buffer — the live stream never delivered it), outcome=stored_exceeded (the union kept a stored total larger than the batch's, with no authority available to settle it), outcome=healed (an authoritative record moved a bucket UP — a missed-trade repair), outcome=lowered (an authoritative record moved a bucket DOWN — either an over-count being settled or a short record being installed; the only path that can reduce a stored value, and it had no counter at all until this one).",
	}, []string{labelExchange, labelMarketType, labelTimeframe, labelOutcome})
	seedRepairEvicted := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "candle_seed_ancestor_unadmitted_evicted_series_total",
		Help: "Series in which an ancestor repair left the resident ring before cold admission.",
	})
	seedRepairOutsideRetention := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "candle_seed_ancestor_outside_cold_retention_total",
		Help: "Seed ancestor repairs excluded by the existing cold retention boundary.",
	})
	coldAppendAborted := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cold_append_aborted_total",
		Help: "Seed/backfill cold-tier appends aborted mid-batch (ctx-cancel / pipeline shutdown), leaving post-abort candles unwritten to cold. The batch error is surfaced for retry.",
	})

	p.engine = engine.Metrics{
		WarmSeedTotal:                 func(ex, mt, tf, outcome string) { warmSeed.WithLabelValues(ex, mt, tf, outcome).Inc() },
		CandleDeltaInsideHorizonTotal: func(ex, mt, tf string) { deltaInsideHorizon.WithLabelValues(ex, mt, tf).Inc() },
		EngineAppliedDeltaVolumeTotal: func(ex, mt, tf string, volume float64) { appliedDeltaVolume.WithLabelValues(ex, mt, tf).Add(volume) },
		DeleteAppliedDeltaVolume:      func(ex, mt, tf string) { appliedDeltaVolume.DeleteLabelValues(ex, mt, tf) },
		PreregisterLateUpdate:         func(ex, mt, tf, reason string) { lateUpdate.WithLabelValues(ex, mt, tf, reason) },
		CandleLateUpdateTotal:         func(ex, mt, tf, reason string) { lateUpdate.WithLabelValues(ex, mt, tf, reason).Inc() },
		CandleSeedMergeTotal:          func(ex, mt, tf, outcome string, n float64) { seedMerge.WithLabelValues(ex, mt, tf, outcome).Add(n) },
		FoldSeedRevisionsUnconfirmed:  func() { foldSeedRevisions.Inc() },
		SeedRepairEvicted:             func() { seedRepairEvicted.Inc() },
		SeedRepairOutsideRetention:    func() { seedRepairOutsideRetention.Inc() },
		ColdAppendAborted:             func() { coldAppendAborted.Inc() },
		ColdMissingBuckets:            func(ex, mt, tf string, v float64) { coldMissingBuckets.WithLabelValues(ex, mt, tf).Set(v) },
		ColdSymbolsWithGaps:           func(ex, mt, tf string, v float64) { coldSymbolsWithGaps.WithLabelValues(ex, mt, tf).Set(v) },
		ColdSymbolsExpected:           func(ex, mt, tf string, v float64) { coldSymbolsExpected.WithLabelValues(ex, mt, tf).Set(v) },
		ColdSymbolsAbsent:             func(ex, mt, tf string, v float64) { coldSymbolsAbsent.WithLabelValues(ex, mt, tf).Set(v) },
		DurableRows:                   func(tf string, v float64) { durableRows.WithLabelValues(tf).Set(v) },
		DurableMissingBuckets:         func(tf string, v float64) { durableMissingBuckets.WithLabelValues(tf).Set(v) },
		DurableColumnCovered:          func(tf, column string, v float64) { durableColumnCovered.WithLabelValues(tf, column).Set(v) },
		DurableTimeframeCovered:       func(tf string, v float64) { durableTimeframeCovered.WithLabelValues(tf).Set(v) },
		DurableIncoherentFunding:      func(tf string, v float64) { durableIncoherentFunding.WithLabelValues(tf).Set(v) },
		DurableDimensionsMeasured:     func(v float64) { durableDimensionsMeasured.Set(v) },
	}

	// Derivatives collector.
	feedAvailable := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "feed_available",
		Help: "Whether a source serves a timeframe for a feed: 1 available, 0 unavailable. A cell never measured is absent rather than 0.",
	}, []string{"source", labelFeed, labelTimeframe})
	feedRequest := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "feed_request_total",
		Help: "Bootstrap history requests by outcome: points, empty, empty_undeclared, unsupported, error. Distinguishes a period the provider does not serve from a legitimate collection of zero items.",
	}, []string{labelExchange, labelFeed, labelTimeframe, labelOutcome})
	collectedPoints := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "collected_points",
		Help: "Points the bootstrap assembled for a (symbol, feed, timeframe) — the depth actually reached, not the depth requested.",
	}, []string{labelExchange, labelFeed, labelSymbol, labelTimeframe})
	collectedSpan := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "collected_span_seconds",
		Help: "Seconds between the oldest and the newest point the bootstrap assembled for a (symbol, feed, timeframe).",
	}, []string{labelExchange, labelFeed, labelSymbol, labelTimeframe})
	historyPages := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "history_pages_total",
		Help: "REST pages issued by the backward-walking history pagination, per feed and timeframe.",
	}, []string{labelExchange, labelFeed, labelTimeframe})
	fundingIntervalComparison := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "funding_interval_comparison_total",
		Help: "Comparisons between the MEASURED settlement spacing and the interval the exchange DECLARES: agree, diverge, no_measurement, no_metadata.",
	}, []string{labelExchange, "result"})
	fundingIntervalMeasurement := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "funding_interval_measurement_total",
		Help: "Outcome of each attempt to MEASURE the settlement spacing: measured, fetch_error, empty_history, too_few_settlements, no_known_cadence.",
	}, []string{labelExchange, labelOutcome})
	fundingProfileEntries := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "funding_profile_registry_entries",
		Help: "How many entries the funding profile registry has NOW.",
	})
	fundingIntervalDivergence := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemDerivatives,
		Name: "funding_interval_divergence_total",
		Help: "Observations where the measured settlement spacing disagrees with the declared fundingIntervalHours. The measured value wins; this counts how often the metadata was wrong.",
	}, []string{labelExchange, labelSymbol, "measured_hours", "declared_hours"})

	coinalyzeCallsSpent := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemCoinalyze,
		Name: "calls_spent_total",
		Help: "Coinalyze API calls this process debited from its local budget, charged per symbol and per attempt.",
	})
	coinalyzeRateRefusals := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemCoinalyze,
		Name: "rate_refusals_total",
		Help: "HTTP 429 responses received from Coinalyze.",
	})
	coinalyzeExternalQuotaRefusals := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemCoinalyze,
		Name: "external_quota_refusals_total",
		Help: "HTTP 429 responses that arrived while this process was emitting under its own per-minute ceiling. They cannot be explained by local emission: the API key is shared with another consumer.",
	})

	p.collector = collector.Metrics{
		FeedAvailable: func(source, feed, tf string, available bool) {
			feedAvailable.WithLabelValues(source, feed, tf).Set(boolFloat(available))
		},
		FeedRequestTotal: func(ex, feed, tf, outcome string) { feedRequest.WithLabelValues(ex, feed, tf, outcome).Inc() },
		CollectedPoints: func(ex, feed, symbol, tf string, points float64) {
			collectedPoints.WithLabelValues(ex, feed, symbol, tf).Set(points)
		},
		CollectedSpanSeconds: func(ex, feed, symbol, tf string, seconds float64) {
			collectedSpan.WithLabelValues(ex, feed, symbol, tf).Set(seconds)
		},
		HistoryPagesTotal:               func(ex, feed, tf string) { historyPages.WithLabelValues(ex, feed, tf).Inc() },
		FundingIntervalComparisonTotal:  func(ex, result string) { fundingIntervalComparison.WithLabelValues(ex, result).Inc() },
		FundingIntervalMeasurementTotal: func(ex, outcome string) { fundingIntervalMeasurement.WithLabelValues(ex, outcome).Inc() },
		FundingProfileRegistryEntries:   func(n float64) { fundingProfileEntries.Set(n) },
		FundingIntervalDivergenceTotal: func(ex, symbol, measured, declared string) {
			fundingIntervalDivergence.WithLabelValues(ex, symbol, measured, declared).Inc()
		},
		CoinalyzeCallsSpentTotal:            func(n float64) { coinalyzeCallsSpent.Add(n) },
		CoinalyzeRateRefusalsTotal:          func(n float64) { coinalyzeRateRefusals.Add(n) },
		CoinalyzeExternalQuotaRefusalsTotal: func(n float64) { coinalyzeExternalQuotaRefusals.Add(n) },
	}

	// Health: Redis budget.
	redisOccupancy := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "redis_budget_occupancy_ratio",
		Help: "Redis occupancy as a fraction of a ceiling. ceiling=maxmemory is what Redis evicts against; ceiling=container is what kills the process.",
	}, []string{"ceiling"})
	redisKeysWithoutTTL := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redis_budget_keys_without_ttl",
		Help: "Resident keys with no TTL. Under a volatile-* policy these never enter the eviction queue.",
	})
	redisKeysTotal := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redis_budget_keys_total",
		Help: "Resident keys, from INFO keyspace.",
	})
	redisEvictedKeys := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redis_budget_evicted_keys",
		Help: "Cumulative evicted_keys as reported by INFO stats. Alert on its increase, not its value.",
	})
	redisAlert := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redis_budget_alert",
		Help: "1 while occupancy crossed the declared ratio on either ceiling, or evicted_keys grew between samples.",
	})
	p.healthRedis = health.RedisBudgetMetrics{
		Occupancy:      func(ceiling string, ratio float64) { redisOccupancy.WithLabelValues(ceiling).Set(ratio) },
		KeysWithoutTTL: func(n float64) { redisKeysWithoutTTL.Set(n) },
		KeysTotal:      func(n float64) { redisKeysTotal.Set(n) },
		EvictedKeys:    func(n float64) { redisEvictedKeys.Set(n) },
		Alert:          func(v float64) { redisAlert.Set(v) },
	}

	// Ingestion.
	tradesDiscarded := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "trades_discarded_total",
		Help: "Inbound trades dropped before candle aggregation, by reason. reason=duplicate_id (the id was already aggregated, or is older than the whole seen-window), reason=stale_timestamp (older than the per-symbol watermark by more than 60s), reason=unparseable (the payload carried no usable timestamp/price/amount).",
	}, []string{labelExchange, labelMarketType, labelReason})
	tradesMissing := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "trades_missing_total",
		Help: "Aggregate trades the exchange issued (consecutive ids) that never reached this process. Non-zero means the live tape was incomplete and the affected buckets are under-counted until an authoritative REST record settles them.",
	}, []string{labelExchange, labelMarketType})
	tradesMissingTail := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "trades_missing_tail_total",
		Help: "Subset of trades_missing_total whose verdict was forced by a bucket boundary rather than by the reordering frontier. These are losses in a bucket's FINAL trades: before the boundary verdict existed, their verdict landed after the bucket had already closed, so the bar closed declaring itself intact and was never repaired.",
	}, []string{labelExchange, labelMarketType})
	admittedVolume := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "stream_admitted_volume_total",
		Help: "Sum of the base-asset quantity of every trade admitted into the aggregator. Compare against engine_applied_delta_volume_total for the same scope: equal means the shortfall is upstream of this process, greater means it is ours.",
	}, []string{labelExchange, labelMarketType})

	p.ingestion = ingestion.Metrics{
		TradesDiscarded:   func(ex, mt, reason string) { tradesDiscarded.WithLabelValues(ex, mt, reason).Inc() },
		TradesMissing:     func(ex, mt string, n float64) { tradesMissing.WithLabelValues(ex, mt).Add(n) },
		TradesMissingTail: func(ex, mt string, n float64) { tradesMissingTail.WithLabelValues(ex, mt).Add(n) },
		AdmittedVolume:    func(ex, mt string, volume float64) { admittedVolume.WithLabelValues(ex, mt).Add(volume) },
		PreregisterDiscarded: func(ex, mt, reason string) {
			tradesDiscarded.WithLabelValues(ex, mt, reason)
		},
		PreregisterMissing:  func(ex, mt string) { tradesMissing.WithLabelValues(ex, mt) },
		PreregisterAdmitted: func(ex, mt string) { admittedVolume.WithLabelValues(ex, mt) },
		ForgetAdmitted:      func(ex, mt string) { admittedVolume.DeleteLabelValues(ex, mt) },
	}

	// Netdiag.
	reconnectScope := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: "netdiag",
		Name: "reconnect_scope_total",
		Help: "Reconnects classified by blast radius: local_network (>=2 exchanges dropped within the correlation window) vs exchange_specific.",
	}, []string{labelExchange, "scope"})
	p.netdiag = netdiag.Metrics{
		ReconnectScope: func(ex, scope string) { reconnectScope.WithLabelValues(ex, scope).Inc() },
	}

	// Recovery.
	validationTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gap_recovery_validation_total",
		Help: "Total number of gap-recovery validation attempts.",
	}, []string{labelExchange, labelMarketType, "result"})
	validationFailed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gap_recovery_validation_failed",
		Help: "Gap-recovery VALIDATIONS that detected remaining gaps — one per failed validation, independent of hole size. For the size, see gap_recovery_missing_buckets.",
	}, []string{labelExchange, labelMarketType})
	missingBuckets := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gap_recovery_missing_buckets",
		Help:    "Size, in BUCKETS, of the hole still open when a gap-recovery validation fails. Distinct from gap_recovery_validation_failed, which counts failures without their size.",
		Buckets: []float64{1, 4, 16, 64, 256, 1024, 4096},
	}, []string{labelExchange, labelMarketType})
	periodicBackstop := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gowhale_periodic_backstop_enqueue_total",
		Help: "Staleness-gated periodic backstop enqueues with no preceding disconnect for the scope. ~0 in steady state proves the event-driven recovery path is sufficient.",
	}, []string{labelExchange, labelMarketType})
	noProgress := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gaprecovery_no_progress_total",
		Help: "Symbols suspended after K consecutive gap-recovery cycles with no freshness advance.",
	}, []string{labelExchange, labelMarket})
	suspendedSymbols := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gaprecovery_suspended_symbols",
		Help: "Number of symbols currently suspended by the gap-recovery no-progress breaker.",
	}, []string{labelExchange, labelMarket})
	restBudgetDeferred := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gaprecovery_rest_budget_deferred_total",
		Help: "Gap-recovery jobs deferred because the aggregate REST-call budget was exhausted.",
	}, []string{labelExchange, labelMarket})
	targetedRepair := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gap_recovery_targeted_repair_total",
		Help: "Gap-recovery jobs whose explicit [since, until] window was honored because it ends at or before the ring head — an interior repair, not a catch-up. Without this the worker re-derives [latest, now] and a request for one older bucket becomes a no-op that reports success.",
	}, []string{labelExchange, labelMarketType, labelTimeframe})
	openBucketSnapshotLag := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespaceGowhale, Subsystem: "seed",
		Name:    "open_bucket_snapshot_lag_ms",
		Help:    "Milliseconds between issuing the seed klines request and applying its open bucket — the irreducible uncovered window",
		Buckets: []float64{10, 25, 50, 100, 200, 400, 800, 1600, 3200},
	}, []string{labelExchange, labelMarketType, labelTimeframe})

	p.recovery = recovery.Metrics{
		ValidationTotal:         func(ex, mt, result string) { validationTotal.WithLabelValues(ex, mt, result).Inc() },
		ValidationFailed:        func(ex, mt string) { validationFailed.WithLabelValues(ex, mt).Inc() },
		MissingBuckets:          func(ex, mt string, missing int) { missingBuckets.WithLabelValues(ex, mt).Observe(float64(missing)) },
		PeriodicBackstopEnqueue: func(ex, mt string) { periodicBackstop.WithLabelValues(ex, mt).Inc() },
		NoProgressTotal:         func(ex, mt string) { noProgress.WithLabelValues(ex, mt).Inc() },
		SuspendedSymbols:        func(ex, mt string, n float64) { suspendedSymbols.WithLabelValues(ex, mt).Set(n) },
		RESTBudgetDeferredTotal: func(ex, mt string) { restBudgetDeferred.WithLabelValues(ex, mt).Inc() },
		TargetedRepairTotal:     func(ex, mt, tf string) { targetedRepair.WithLabelValues(ex, mt, tf).Inc() },
		OpenBucketSnapshotLagMs: func(ex, mt, tf string, lagMs float64) {
			openBucketSnapshotLag.WithLabelValues(ex, mt, tf).Observe(lagMs)
		},
	}

	// App-owned metrics: exposed as granular functions so this adapter does not import app.
	coinalyzeCycleSeconds := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemCoinalyze,
		Name:    "cycle_duration_seconds",
		Help:    "Duration of one Coinalyze collection cycle. The cycle runs synchronously inside the scheduling ticker's select, so a cycle longer than the interval makes the runtime drop ticks instead of queueing them.",
		Buckets: prometheus.ExponentialBuckets(30, 2, 10),
	})
	coinalyzeCycleOverrun := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespaceGowhale, Subsystem: subsystemCoinalyze,
		Name: "cycle_overrun_total",
		Help: "Coinalyze collection cycles that outlasted their configured interval. Each one costs at least one dropped scheduling tick.",
	})
	bootBucketSettle := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "candle_boot_bucket_settle_total",
		Help: "Boot buckets enqueued for settlement against the exchange's final record. The bucket open when the process starts is assembled from a REST snapshot MAX-merged with a partial live stream, which can destroy the last trade of the bucket and therefore its close price; once closed, the exchange's record settles it. Exactly one per key per boot.",
	}, []string{labelExchange, labelMarketType, labelTimeframe})
	gappedBucketRepair := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "candle_gapped_bucket_repair_total",
		Help: "Closed buckets enqueued for an authoritative re-read because aggregate trades the exchange issued never reached this process. Pairs with trades_missing_total: that counts lost trades, this counts the bars they made wrong.",
	}, []string{labelExchange, labelMarketType, labelTimeframe})
	p.coinalyzeCycleSeconds = func(seconds float64) { coinalyzeCycleSeconds.Observe(seconds) }
	p.coinalyzeCycleOverrun = func() { coinalyzeCycleOverrun.Inc() }
	p.bootBucketSettle = func(ex, mt, tf string) { bootBucketSettle.WithLabelValues(ex, mt, tf).Inc() }
	p.gappedBucketRepair = func(ex, mt, tf string) { gappedBucketRepair.WithLabelValues(ex, mt, tf).Inc() }

	// Process-level collectors: go_* and process_* must remain.
	all := []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		newGCCPUFractionCollector(),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "go_gc_headroom_ratio",
			Help: "Go runtime memory mapped and not released, divided by GOMEMLIMIT. Instantaneous, not cumulative. Approaching 1.0 means the GC pacer can no longer satisfy its target and will collect continuously. NaN when no memory limit is configured.",
		}, func() float64 {
			ratio, configured := health.GCHeadroomRatio()
			if !configured {
				return math.NaN()
			}
			return ratio
		}),
		// ── Streaming ────────────────────────────────────────────────────
		streamLiveness, streamSymbolsTotal, streamSymbolsStale,
		startupFailures, bootSeedConfirmed, bootSeedBlind,
		// ── Binance ──────────────────────────────────────────────────────
		goroutinesTotal, reconnectAttempts, reconnectFailures, disconnectCause,
		lastMessageAge, partitionStale, healthState, messagesIn, tradesDropped,
		tradesIngested, reconnectSignalsDropped, resubscribeIncomplete,
		resubscribeUnverified, dynamicSubscribeDeferred, starvationSuppressed,
		klineFramesTotal, klineFramesRejected, klineSymbolsAttached, klinePartitionFrames, klineRoute,
		// ── Realtime ─────────────────────────────────────────────────────
		realtimeDropped,
		// ── Coverage ──────────────────────────────────────────────────────
		symbolsConfigured, symbolsEffective,
		// ── Derivatives adapters ──────────────────────────────────────────
		lsrShareSumViolation, capabilityAbsent, derivativesColdDegraded, coldPipelineBlocked,
		coldPipelinePendingRows, coldSeedNoopRows, coldFlushAge, seriesNonFinite,
		// ── Candle engine ─────────────────────────────────────────────────
		warmSeed, deltaInsideHorizon, appliedDeltaVolume, coldMissingBuckets,
		coldSymbolsWithGaps, coldSymbolsExpected, coldSymbolsAbsent, durableRows,
		durableMissingBuckets, durableColumnCovered, durableTimeframeCovered,
		durableIncoherentFunding, durableDimensionsMeasured, foldSeedRevisions,
		lateUpdate, seedMerge, seedRepairEvicted, seedRepairOutsideRetention,
		coldAppendAborted,
		// ── Derivatives collector ─────────────────────────────────────────
		feedAvailable, feedRequest, collectedPoints, collectedSpan, historyPages,
		fundingIntervalComparison, fundingIntervalMeasurement, fundingProfileEntries,
		fundingIntervalDivergence, coinalyzeCallsSpent, coinalyzeRateRefusals,
		coinalyzeExternalQuotaRefusals,
		// ── Health Redis budget ───────────────────────────────────────────
		redisOccupancy, redisKeysWithoutTTL, redisKeysTotal, redisEvictedKeys, redisAlert,
		// ── Ingestion ─────────────────────────────────────────────────────
		tradesDiscarded, tradesMissing, tradesMissingTail, admittedVolume,
		// ── Netdiag ───────────────────────────────────────────────────────
		reconnectScope,
		// ── Recovery ──────────────────────────────────────────────────────
		validationTotal, validationFailed, missingBuckets, periodicBackstop,
		noProgress, suspendedSymbols, restBudgetDeferred, targetedRepair,
		openBucketSnapshotLag,
		// ── App ───────────────────────────────────────────────────────────
		coinalyzeCycleSeconds, coinalyzeCycleOverrun, bootBucketSettle, gappedBucketRepair,
	}

	for _, c := range all {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	p.collectors = all
	return p, nil
}

func boolFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// Accessors return the owning-package ports.

// Streaming returns the streaming client metrics port.
func (p *Provider) Streaming() streaming.Metrics { return p.streaming }

// Binance returns the Binance adapter metrics port.
func (p *Provider) Binance() binancesdk.Metrics { return p.binance }

// Realtime returns the realtime edge metrics port.
func (p *Provider) Realtime() realtime.Metrics { return p.realtime }

// Venue returns the venue adapter metrics port.
func (p *Provider) Venue() venue.Metrics { return p.venue }

// QuestDB returns the QuestDB adapter metrics port.
func (p *Provider) QuestDB() questdb.Metrics { return p.questdb }

// Redis returns the Redis adapter metrics port.
func (p *Provider) Redis() redisstore.Metrics { return p.redis }

// Engine returns the candle engine metrics port.
func (p *Provider) Engine() engine.Metrics { return p.engine }

// Collector returns the derivatives collector metrics port.
func (p *Provider) Collector() collector.Metrics { return p.collector }

// HealthRedis returns the Redis health-probe metrics port.
func (p *Provider) HealthRedis() health.RedisBudgetMetrics { return p.healthRedis }

// Ingestion returns the ingestion metrics port.
func (p *Provider) Ingestion() ingestion.Metrics { return p.ingestion }

// Netdiag returns the network diagnostics metrics port.
func (p *Provider) Netdiag() netdiag.Metrics { return p.netdiag }

// Recovery returns the gap-recovery metrics port.
func (p *Provider) Recovery() recovery.Metrics { return p.recovery }

// HealthSink returns the health gauge sink.
func (p *Provider) HealthSink() health.GaugeSink { return p.healthSink }

// CoinalyzeCycleSeconds returns the Coinalyze cycle duration observer.
func (p *Provider) CoinalyzeCycleSeconds() func(float64) { return p.coinalyzeCycleSeconds }

// CoinalyzeCycleOverrun returns the Coinalyze cycle overrun counter.
func (p *Provider) CoinalyzeCycleOverrun() func() { return p.coinalyzeCycleOverrun }

// BootBucketSettle returns the boot settle observer.
func (p *Provider) BootBucketSettle() func(exchange, marketType, timeframe string) {
	return p.bootBucketSettle
}

// GappedBucketRepair returns the callback that records a gapped-bucket repair.
func (p *Provider) GappedBucketRepair() func(exchange, marketType, timeframe string) {
	return p.gappedBucketRepair
}
