package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// MarketDataHandler handles derivatives HTTP endpoints.
// Response shapes are compatible with the analyzer controller for seamless frontend migration.
type MarketDataHandler struct {
	// The three derivatives capabilities are separate ports: the snapshot/series routes
	// read through Reader, the normalized reading needs BaselineReader, and the price axis
	// needs PriceReader. A fake implements only the ones its route exercises.
	reader    derivatives.Reader
	baselines derivatives.BaselineReader
	prices    derivatives.PriceReader
	// derivWindow serves the window that goes past the ZSET horizon. NIL when the durable
	// tier did not come up, in which case the ZSET answers alone.
	derivWindow *derivatives.DerivativesWindowReader
	// durableBudget is the durable read's deadline. A field, not a direct constant, so a
	// test can exercise degradation in milliseconds instead of seconds — what is asserted
	// is the BEHAVIOR at the timeout, not the duration.
	durableBudget time.Duration
	// durableTimeframes are the buckets the durable tier ACTUALLY holds.
	//
	// Querying a timeframe the collector does not write is opening partitions to prove
	// there is nothing there — and MEASURED on the LAN, that is exactly what the batch route
	// did: 30 symbols × `30m` per dashboard probe, against a
	// table holding only `1h`, `4h` and `1d`. The cost showed up as durable reads timing
	// out in cascade.
	//
	// Empty means "I do not know", and then the decision falls back to reach alone.
	durableTimeframes map[string]struct{}
	defaultExchange   string
	defaultMarketType string
	tfReg             *candle.TimeframeRegistry

	// nowMs is the clock the reading decides bucket closure against — see scoredPair.
	// It is a field so a test can place itself inside or after a bar instead of racing
	// the wall clock; production leaves it nil and gets time.Now. Same idiom as
	// DerivativesPoller.now and fundingProfileCache.nowMs.
	nowMs func() int64
	log   *slog.Logger
}

// NewMarketDataHandler builds the derivatives measurement handler over its read ports.
func NewMarketDataHandler(
	reader derivatives.Reader,
	baselines derivatives.BaselineReader,
	prices derivatives.PriceReader,
	defaultExchange string,
	defaultMarketType string,
	tfReg *candle.TimeframeRegistry,
	logs ...*slog.Logger,
) *MarketDataHandler {
	return &MarketDataHandler{
		reader:            reader,
		baselines:         baselines,
		prices:            prices,
		defaultExchange:   defaultExchange,
		defaultMarketType: defaultMarketType,
		tfReg:             tfReg,
		log:               logger.OrDiscard(logs...),
	}
}

// WithDerivativesWindow wires the reader that crosses the ZSET cap.
//
// SEPARATE FROM THE CONSTRUCTOR on purpose: the durable tier is optional — without
// QuestDB the ZSET answers alone, and a mandatory parameter would force every caller to
// decide about a dependency it may not have. Called once, at assembly time.
func (h *MarketDataHandler) WithDerivativesWindow(reader *derivatives.DerivativesWindowReader, timeframes ...string) *MarketDataHandler {
	h.derivWindow = reader
	if len(timeframes) > 0 {
		h.durableTimeframes = make(map[string]struct{}, len(timeframes))
		for _, tf := range timeframes {
			h.durableTimeframes[tf] = struct{}{}
		}
	}
	if h.durableBudget <= 0 {
		h.durableBudget = derivativesDurableReadBudget
	}
	return h
}

// --- Derivatives endpoints ---

// normalizedReading scores the newest bar of the series against the symbol's own
// baseline.
//
// A baseline that has not been computed yet still produces a reading — one carrying
// the state and the reason. The row is never dropped: an immature or degenerate
// baseline is a seal, and removing the row would make "not scorable yet" look like
// "nothing to see here".
func (h *MarketDataHandler) normalizedReading(ctx context.Context, exchange, marketType, symbol, timeframe string,
	snapshots []*derivatives.DerivativesSnapshot,
) NormalizedReading {
	tfMs := candle.ParseTimeframeToSeconds(timeframe) * 1000
	nowMs := h.now()

	// Read the common ruler before the independent price axis can consume the
	// remaining budget. A stalled candle read must not erase a healthy OI score.
	baseline, err := h.baselines.GetBaseline(ctx, exchange, marketType, symbol, timeframe)
	if err != nil {
		// Logged and not returned: the series itself is intact and worth serving, and
		// the reading that goes out in its place already declares that it has no ruler.
		cause := normalizedFailureReason(ctx, err, "baseline")
		logger.OrDiscard(h.log).Warn("derivatives: baseline read failed",
			"symbol", symbol, "timeframe", timeframe, "failure", cause)

		// The reason is REWRITTEN because the nil-baseline path names the wrong
		// missing thing here: it says nobody has computed the ruler, when the ruler
		// may well exist and we simply could not read it. Those two call for opposite
		// responses — wait, versus go look at the store — and this envelope exists to
		// stop a consumer from having to guess which.
		//
		// NO REFERENCE, and that is the right call: without a ruler no score comes out, so
		// there is no score to refuse — and the reason for the three axes is already
		// declared just below.
		reading := normalizedFromSeries(nil, snapshots, tfMs, nowMs, priceIncrement{}, 0)
		reading.OpenInterest.Reason = baselineUnreadable
		reading.LongShortRatio.Reason = baselineUnreadable
		reading.Price.Reason = baselineUnreadable
		reading.UnavailableReason = cause
		return reading
	}
	return h.normalizedReadingAgainst(ctx, exchange, marketType, symbol, timeframe, snapshots, baseline)
}

func (h *MarketDataHandler) normalizedReadingAgainst(ctx context.Context, exchange, marketType, symbol, timeframe string, snapshots []*derivatives.DerivativesSnapshot, baseline *derivatives.DerivativesBaseline) NormalizedReading {
	tfMs := candle.ParseTimeframeToSeconds(timeframe) * 1000
	nowMs := h.now()
	// Price and derivatives are scored on the same closed bar — and on the same SERIES.
	//
	// The bar must be resolved over the SAME series the axes score. Resolving it over the
	// RAW series makes the two disagree whenever the filter discards the newest point — a
	// stitched point, for example, which is the NORMAL outcome of the deep window. MEASURED
	// by fixture: `priceIncrementBar` one bar ahead of the axes' timestamp, that is, a price
	// `z` from one instant beside an OI `z` from another.
	price, priceErr := h.priceIncrement(ctx, exchange, marketType, symbol, timeframe,
		scoredSeriesBar(snapshots, tfMs, nowMs))
	reading := normalizedFromSeries(baseline, snapshots, tfMs, nowMs, price,
		h.warmReferenceBar(ctx, exchange, marketType, symbol, timeframe, nowMs))
	if priceErr != nil {
		reading.UnavailableReason = normalizedFailureReason(ctx, priceErr, "price")
	}
	return reading
}

// warmBarClock is the ONLY use the reading makes of the warm tier: the INSTANT of its
// newest closed bar. A local interface plus a type assertion, the same pattern as
// `canonicalCoarseStore` — the port in `internal/adapters/outbound/redis/interfaces.go`
// does not gain a method for a read that only the clock consumes, and a store that does
// not implement it degrades to today's behavior instead of failing to compile.
type warmBarClock interface {
	NewestClosedSeriesBar(ctx context.Context, exchange, marketType, symbol, timeframe string, nowMs int64) (int64, bool, error)
}

// warmReferenceBar resolves the reference against which the score is compared.
//
// ZERO MEANS NO REFERENCE, AND WITHOUT A REFERENCE THE SEAL IS INERT: an empty ZSET, a
// store that does not implement the clock or a failed read NEVER refuse a score for lack
// of a clock. Refusing when the reference is absent would trade an unavailability of the
// warm tier for an entire fleet with no reading — the opposite of what this envelope
// exists to do.
func (h *MarketDataHandler) warmReferenceBar(ctx context.Context, exchange, marketType, symbol, timeframe string, nowMs int64) int64 {
	clock, ok := h.reader.(warmBarClock)
	if !ok {
		return 0
	}
	bar, present, err := clock.NewestClosedSeriesBar(ctx, exchange, marketType, symbol, timeframe, nowMs)
	if err != nil {
		// A failing clock does not erase the reading: it goes on with the score its own
		// series supports, and the log is the record that the seal could not be applied.
		logger.OrDiscard(h.log).Warn("derivatives: warm-series clock failed",
			"symbol", symbol, "timeframe", timeframe, responseKeyError, err)
		return 0
	}
	if !present {
		return 0
	}
	return bar
}

// now reads the handler's time source.
func (h *MarketDataHandler) now() int64 {
	if h.nowMs != nil {
		return h.nowMs()
	}
	return time.Now().UnixMilli()
}

// baselineUnreadable names a store that would not answer, as distinct from a ruler
// that was never built.
const baselineUnreadable = "the baseline for this (symbol, timeframe) could not be read"

// priceIncrement resolves r(t) for one bar, turning a store failure into a NAMED
// absence rather than into a zero or into a dropped axis.
func (h *MarketDataHandler) priceIncrement(ctx context.Context, exchange, marketType, symbol, timeframe string,
	barTs int64,
) (priceIncrement, error) {
	value, reason, err := h.prices.PriceIncrement(ctx, exchange, marketType, symbol, timeframe, barTs)
	if err != nil {
		logger.OrDiscard(h.log).Warn("derivatives: price increment failed",
			"symbol", symbol, "timeframe", timeframe, "failure", normalizedFailureReason(ctx, err, "price"))
		return priceIncrement{missing: "the candle series for this (symbol, timeframe) could not be read"}, err
	}
	if value == nil && reason == "" {
		// Defensive, and it must never fire: an absent value with no reason is exactly
		// the silent hole this envelope exists to prevent, so it is named rather than
		// left blank.
		reason = "no price increment was produced and the store gave no reason"
	}
	return priceIncrement{value: value, missing: reason}, nil
}

const (
	normalizedBatchReadBudget = 3 * time.Second
	derivativesBatchWorkers   = 8
)

func normalizedFailureReason(ctx context.Context, err error, stage string) string {
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return "canceled"
	}
	deadline, hasDeadline := ctx.Deadline()
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) ||
		(hasDeadline && !time.Now().Before(deadline)) {
		return "deadline_exceeded"
	}
	return stage + "_unavailable"
}

type batchDerivativesRead struct {
	symbol    string
	snapshots []*derivatives.DerivativesSnapshot
	reading   NormalizedReading
}

func (h *MarketDataHandler) readBatchSymbol(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) batchDerivativesRead {
	result := batchDerivativesRead{symbol: symbol}
	var baseline *derivatives.DerivativesBaseline
	var cached bool
	err := ctx.Err()
	if err == nil {
		result.snapshots, baseline, cached, err = h.derivativesReadingSeries(ctx, exchange, marketType, symbol, timeframe, limit)
	}
	if err != nil {
		cause := normalizedFailureReason(ctx, err, "series")
		axis := NormalizedAxis{State: "immature", Reason: cause}
		result.reading = NormalizedReading{UnavailableReason: cause, OpenInterest: axis, LongShortRatio: axis, Price: axis}
		return result
	}
	if len(result.snapshots) != 0 {
		result.snapshots = declareProvenance(result.snapshots, derivatives.SourceVenue)
		result.reading = h.readingForSeries(ctx, exchange, marketType, symbol, timeframe, result.snapshots, baseline, cached)
	} else if cached {
		axis := NormalizedAxis{State: "immature", NMin: baseline.NMin, Reason: "canonical history has no observations"}
		result.reading = NormalizedReading{UnavailableReason: "series_unavailable", OpenInterest: axis, LongShortRatio: axis, Price: axis}
	}
	return result
}

// ── Batch API ──────────────────────────────────────────────────────────

// GetBatchDerivativesSeries returns derivatives mini-series for multiple symbols.
// GET /api/batch/derivatives-series?exchange=binance&marketType=future&timeframe=30m&limit=30&symbols=BTC/USDT,ETH/USDT.
func (h *MarketDataHandler) GetBatchDerivativesSeries(w http.ResponseWriter, r *http.Request) {
	exchange := r.URL.Query().Get("exchange")
	marketType := r.URL.Query().Get("marketType")
	timeframe := r.URL.Query().Get("timeframe")
	symbolsRaw := r.URL.Query().Get("symbols")

	if exchange == "" {
		exchange = h.defaultExchange
	}
	if marketType == "" {
		marketType = h.defaultMarketType
	}
	if timeframe == "" {
		timeframe = h.tfReg.Default()
	}

	limit := parseLimit(r, 30)

	if symbolsRaw == "" {
		writeJSON(h.log, w, http.StatusBadRequest, map[string]string{responseKeyError: "symbols query parameter is required"})
		return
	}

	var symbols []string
	for _, s := range strings.Split(symbolsRaw, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			symbols = append(symbols, s)
		}
	}

	data := make(map[string][]BatchDerivativesSeriesEntry, len(symbols))
	// COVERAGE IS PER SYMBOL because the limit is per symbol: a batch of thirty can have
	// twenty-nine complete and one truncated, and a single marker for the whole batch would
	// make the exception disappear into the rule.
	coverage := make(map[string]SeriesCoverage, len(symbols))
	funding := make(map[string]FundingDerivation, len(symbols))
	normalized := make(map[string]NormalizedReading, len(symbols))
	started := time.Now()
	ctx := r.Context()
	if limit <= 3 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, normalizedBatchReadBudget)
		defer cancel()
	}
	jobs := make(chan string, len(symbols))
	results := make(chan batchDerivativesRead, len(symbols))
	for _, symbol := range symbols {
		jobs <- symbol
	}
	close(jobs)
	var workers sync.WaitGroup
	for i := 0; i < min(derivativesBatchWorkers, len(symbols)); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for symbol := range jobs {
				results <- h.readBatchSymbol(ctx, exchange, marketType, symbol, timeframe, limit)
			}
		}()
	}
	workers.Wait()
	close(results)
	failures := make(map[string]int)

	for result := range results {
		symbol, snapshots := result.symbol, result.snapshots
		// THROUGH THE SAME WINDOW AS THE SINGULAR ROUTE. A direct
		// `h.derivStore.GetSnapshotSeries` here was MEASURED live: 1,200 buckets requested,
		// 200 served, envelope without a coverage marker. It is the dashboard route — the
		// long history reached the chart and did not reach the panel, through the same
		// feature.
		//
		// The per-symbol cost is the same as the singular route's: a window that fits in the
		// ZSET does not touch the database, and one that does not fit was already paying the
		// query anyway.
		if cause := result.reading.UnavailableReason; cause != "" {
			normalized[symbol] = result.reading
			failures[cause]++
		}
		if len(snapshots) == 0 {
			continue
		}

		// No post-processing here either — see GetDerivativesSeries for what was
		// removed and what it costs.
		entries := make([]BatchDerivativesSeriesEntry, 0, len(snapshots))
		for _, snap := range snapshots {
			if snap == nil {
				continue
			}
			entry := BatchDerivativesSeriesEntry{
				BatchDerivativesSeriesPoint: BatchDerivativesSeriesPoint{
					Timestamp:      snap.Timestamp,
					OpenInterest:   snap.OpenInterest,
					OIChangePct:    snap.OIChangePct,
					FundingRate:    snap.FundingRate,
					LongShortRatio: snap.LongShortRatio,
				},
				OpenInterestBase: snap.OpenInterestBase,
				NStep:            snap.NStep,
				FundingAtAnchor:  snap.FundingAtAnchor,
				Source:           snap.Source,
			}
			// No interval, no normalized value. Serving an 8h-equivalent whose N
			// came from nowhere would be a cross-asset comparison built on a guess.
			if hasDerivedInterval(snap.NStep) {
				entry.FundingRate8h = snap.FundingRate8h
			}
			entries = append(entries, entry)
		}
		data[symbol] = entries
		cov := newSeriesCoverage(limit, len(entries), nil)
		// Count the emitted entries: fundingRate8h may be withheld even when the
		// stored snapshot carries a value without interval provenance.
		for _, entry := range entries {
			if entry.OpenInterestBase != nil {
				cov.Fields["openInterestBase"]++
			}
			if entry.FundingRate != nil {
				cov.Fields["fundingRate"]++
			}
			if entry.FundingRate8h != nil {
				cov.Fields["fundingRate8h"]++
			}
			if entry.LongShortRatio != nil {
				cov.Fields["longShortRatio"]++
			}
		}
		coverage[symbol] = cov
		funding[symbol] = deriveFunding(snapshots)
		// Same reading the single-symbol route serves, from the same helper, so the
		// two routes cannot drift into disagreeing about the same (symbol, timeframe).
		// One extra Redis GET per symbol — the ruler is per pair and there is no
		// fleet-wide ruler to read instead.
		normalized[symbol] = result.reading
	}
	for cause, count := range failures {
		logger.OrDiscard(h.log).Error("derivatives batch: symbols failed",
			"exchange", exchange,
			"market_type", marketType,
			"timeframe", timeframe,
			"symbols", len(symbols),
			"elapsed", time.Since(started),
			"failure", cause,
			"affected", count)
	}

	writeJSON(h.log, w, http.StatusOK, BatchDerivativesSeriesEnvelope{
		Timeframe: timeframe,
		Data:      data,
		// applies to BOTH read routes, and this is the dashboard one. It served
		// 2,000 of 3,000 requested with no marker at all — the same silent truncation the
		// singular route already declared, at the address more people read.
		Coverage:    coverage,
		Funding:     funding,
		Normalized:  normalized,
		Measurement: h.measurementDeclaration(timeframe),
	})
}

// durableHolds says whether it is worth querying the durable tier for this timeframe.
//
// Without a declaration it answers YES: the decision falls back to reach alone, which is
// the previous behavior. With a declaration, a timeframe outside it never reaches the
// database.
func (h *MarketDataHandler) durableHolds(timeframe string) bool {
	if len(h.durableTimeframes) == 0 {
		return true
	}
	_, ok := h.durableTimeframes[timeframe]
	return ok
}

// ServerWriteTimeout is the response write deadline, and the production server USES it from here.
//
// EXPORTED, and not duplicated: the case that guarded the relationship between this value
// and the durable read budget compared the budget against a COPY of the number, hand-typed
// in the test with a comment pointing at `lifecycle.go`. Lowering the real `WriteTimeout`
// reproduced the `i/o timeout` measured on the LAN and nothing complained — the assertion
// guarded the copy, not the value.
//
// Fifteen seconds: see `derivativesDurableReadBudget` for the breakdown.
const ServerWriteTimeout = 15 * time.Second

// derivativesDurableReadBudget is how much the durable read may spend before degrading.
//
// Ten seconds against the fifteen of `ServerWriteTimeout`: the slack is what is left to
// serialize and write the degradation response. See `derivativesSeries`.
const derivativesDurableReadBudget = 10 * time.Second

func (h *MarketDataHandler) measurementDeclaration(timeframe string) MeasurementDeclaration {
	declaration := newMeasurementDeclaration(timeframe)
	if store, ok := h.reader.(interface{ HasCanonicalCoarse(string) bool }); ok && store.HasCanonicalCoarse(timeframe) {
		// THE RESIDUAL LAG THE SEAL DOES NOT SEE.
		//
		// The reading is the tail captured at the ruler's last recompute, and it can sit up
		// to ONE bar behind the cold series until the next one.
		//
		// THE FIRST DRAFT OF THIS SENTENCE SAID THE OPPOSITE OF THE CODE. It claimed that
		// this residual lag "is not detected" — but the seal refuses exactly
		// `reading.Timestamp < referenceBar`, and a bar behind the reference IS smaller
		// than it: with a clock, this lag is detected, and it is detected precisely by it.
		// What is not detected is the case where there is NO CLOCK — the warm tier's
		// reference is optional by design (empty ZSET, store without the method, failed
		// read all leave the gate inert), and then there is nothing to compare against.
		// Declaring this is what keeps the surface from reading "scored on the newest bar"
		// where the sentence was not measured.
		declaration.Limitations = append(declaration.Limitations, "the reading is the tail captured at the last ruler recompute, and the warm series is consulted only as a clock: when no warm reference exists the gate cannot compare, so a tail sitting one bar behind the cold series goes undetected")
		if timeframe == "1w" {
			declaration.DurableAbsences = nil
			declaration.Limitations = append(declaration.Limitations, "Coinalyze weekly values are derived from daily observations: closed weeks use Sunday and are labeled Monday 00:00 UTC; an absent Sunday remains absent")
		}
	}
	return declaration
}

type canonicalCoarseStore interface {
	HasCanonicalCoarse(string) bool
	GetCanonicalReading(context.Context, string, string, string, string) (*derivatives.DerivativesBaseline, error)
}

func (h *MarketDataHandler) derivativesReadingSeries(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]*derivatives.DerivativesSnapshot, *derivatives.DerivativesBaseline, bool, error) {
	if store, ok := h.reader.(canonicalCoarseStore); ok && store.HasCanonicalCoarse(timeframe) && limit > 0 && limit <= 3 {
		baseline, err := store.GetCanonicalReading(ctx, exchange, marketType, symbol, timeframe)
		if err != nil {
			return nil, nil, true, err
		}
		if baseline == nil || baseline.Source != derivatives.SourceCoinalyze {
			return nil, nil, true, fmt.Errorf("canonical coarse ruler unavailable")
		}
		tail := baseline.SeriesTail
		if len(tail) > limit {
			tail = tail[:limit]
		}
		return tail, baseline, true, nil
	}
	points, err := h.derivativesSeries(ctx, exchange, marketType, symbol, timeframe, limit)
	return points, nil, false, err
}

func (h *MarketDataHandler) readingForSeries(ctx context.Context, exchange, marketType, symbol, timeframe string, snapshots []*derivatives.DerivativesSnapshot, baseline *derivatives.DerivativesBaseline, cached bool) NormalizedReading {
	if cached {
		if baseline != nil {
			snapshots = baseline.SeriesTail
		}
		return h.normalizedReadingAgainst(ctx, exchange, marketType, symbol, timeframe, snapshots, baseline)
	}
	return h.normalizedReading(ctx, exchange, marketType, symbol, timeframe, snapshots)
}

// derivativesSeries serves the requested window, crossing the ZSET cap when needed.
//
// THE DECISION IS BY REACH, not by the tier's existence: if what was requested fits in the
// warm horizon, the ZSET answers alone and the database is not touched — a recent read
// cannot pay the price of a durable query.
func (h *MarketDataHandler) derivativesSeries(
	ctx context.Context,
	exchange, marketType, symbol, timeframe string,
	limit int,
) ([]*derivatives.DerivativesSnapshot, error) {
	if store, ok := h.reader.(interface{ HasCanonicalCoarse(string) bool }); ok && store.HasCanonicalCoarse(timeframe) {
		return h.reader.GetSnapshotSeries(ctx, exchange, marketType, symbol, timeframe, limit)
	}
	if h.derivWindow == nil || !h.durableHolds(timeframe) {
		return h.reader.GetSnapshotSeries(ctx, exchange, marketType, symbol, timeframe, limit)
	}
	tfMs, ok := candle.ParseTimeframeMs(timeframe)
	if !ok || tfMs <= 0 {
		return h.reader.GetSnapshotSeries(ctx, exchange, marketType, symbol, timeframe, limit)
	}

	// ITS OWN DEADLINE, SHORTER THAN THE RESPONSE WRITE'S.
	//
	// MEASURED on the LAN: the FIRST deep read after a restart took 24.4 s —
	// the cost of opening the daily partitions of 5.5 years — and the client received ZERO
	// bytes with the connection dropped. The server's `WriteTimeout` is 15 s and counts from
	// the start of the request, so it expired in the MIDDLE of writing the body:
	//
	//	http: encoding response body failed after status 200 was sent:... i/o timeout
	//
	// The following calls took 0.13 s, so it is warm-up, not load. But "once per restart" is
	// exactly when someone opens the chart after a deploy.
	//
	// The deadline here is SHORTER than the write's on purpose: expiring it falls into the
	// degradation branch just below — serve the ZSET, with a log — instead of consuming the
	// entire write budget and returning nothing. Serving less history is an answer; a
	// dropped connection is not.
	orcamento := h.durableBudget
	if orcamento <= 0 {
		orcamento = derivativesDurableReadBudget
	}
	deadline, cancel := context.WithTimeout(ctx, orcamento)
	defer cancel()

	to := time.Now().UTC()
	from := to.Add(-time.Duration(int64(limit)*tfMs) * time.Millisecond)
	series, err := h.derivWindow.Series(deadline, derivatives.DerivativesScope{
		// THE SOURCE IS A TERM OF THE DURABLE QUERY, NOT ORNAMENT. `QueryDerivativesRange`
		// filters by `source` in its first parameter; a scope with an empty source matches
		// zero rows and returns success — the deep window would come back empty with no
		// error at all, which is the silent truncation this route exists to abolish.
		//
		// The source is Coinalyze because it is the ONLY one that writes the durable tier:
		// the live loop writes only the ZSET, and that is why the recent part comes from the
		// warm tier and the deep part comes from it. Each point carries its own provenance, so
		// the seam between the two shows up for the reader instead of being erased here.
		Source:   derivatives.SourceCoinalyze,
		Exchange: exchange, MarketType: marketType, Symbol: symbol, Timeframe: timeframe,
	}, from, to)
	if err != nil {
		// THE DURABLE TIER FAILING MUST NOT ERASE THE RECENT WINDOW. A query that does not
		// return is a reason to serve less history, never to serve nothing.
		logger.OrDiscard(h.log).Warn("derivatives: durable window failed; serving the ZSET only",
			"symbol", symbol, "timeframe", timeframe, responseKeyError, err)
		return h.reader.GetSnapshotSeries(ctx, exchange, marketType, symbol, timeframe, limit)
	}
	return descending(series), nil
}

// descending returns the series from NEWEST to oldest, which is this route's contract.
//
// # WHY IT EXISTS, AND WHAT HAPPENED WITHOUT IT
//
// `GetSnapshotSeries` always returned descending — it is in the port (`internal/derivatives/ports.go`)
// — and `normalizedFromSeries` is documented as requiring that order: `scoredPair` walks the
// slice from index 0 and takes the first CLOSED bucket as "the newest bar".
//
// The window reader returns ASCENDING, for an equally good reason: a time series in
// increasing order is what every chart consumes. Two internally coherent pieces with
// opposite contracts — and wiring them together inverted the scored bar without anything
// complaining.
//
// MEASURED live, in the `1d` response with 2,000 points: `normalized.timestamp` came out as
// `1614729600000`, that is, **2021-03-03**. The decision axis was being computed over a bar
// from five years ago, with `state: "mature"` and not a single line of error. With 30 recent
// points the coincidence does not show; a deep window is what exposes it.
//
// The inversion happens HERE, at the boundary, and not in the reader: the reader has
// consumers of its own that depend on the ascending order, and it is this route that has a
// public contract to honor.
func descending(series []*derivatives.DerivativesSnapshot) []*derivatives.DerivativesSnapshot {
	out := make([]*derivatives.DerivativesSnapshot, len(series))
	for i, snapshot := range series {
		out[len(series)-1-i] = snapshot
	}
	return out
}
