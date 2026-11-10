package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// defaultSeedLimit is the depth with which a series is born when nothing declares one.
// OVERRIDABLE DEFAULT: candle_engine.buffer_sizes replaces it on every production call.
const defaultSeedLimit = 200

type seedWindow struct {
	historical []candle.Candle
	current    *candle.Candle

	// requestedAt is when the request left, captured after the shared budget released it, so it
	// measures the round trip rather than queueing. See openBucketSnapshotLagMs.
	requestedAt time.Time

	// fetchedAt is when the response was parsed; the authority guard needs this later instant,
	// since the request can precede the bucket's close.
	fetchedAt time.Time
}

func fetchSeedWindow(ctx context.Context, client HistorySource, symbol string, timeframe string, historicalNeeded bool, seedLimit int, budget *exchange.RestBudget, log *slog.Logger) (*seedWindow, error) {
	log = logger.OrDiscard(log)
	tfSeconds := candle.ParseTimeframeToSeconds(timeframe)
	if tfSeconds == 0 {
		return nil, fmt.Errorf("invalid timeframe: %s", timeframe)
	}

	// currentBucket is calculated after the fetch: a boundary crossing during the call would
	// misclassify and drop the new bucket's candle.

	limit := int64(1)
	if historicalNeeded {
		limit = int64(seedLimit + 1)
	}

	// Reserve the weight before spending it: the seed shares one per-IP pool with gap recovery,
	// the derivatives poller and discovery, and pacing each in isolation produced 4,602 HTTP
	// 429s on a 500-symbol deploy. The weight comes from the actual limit, including the +1.
	if budget != nil {
		if err := budget.AcquireKlines(ctx, int(limit)); err != nil {
			return nil, fmt.Errorf("rest budget: %w", err)
		}
	}

	// Taken after the budget released us: it measures the exchange round trip, not time queued
	// behind the rate limiter.
	requestedAt := time.Now()

	var fetched []candle.Candle
	var ferr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				ferr = fmt.Errorf("panic during seed fetch: %v", r)
			}
		}()

		// Prefer CVD-complete klines; fall back to unified OHLCV for other exchanges or on
		// failure, losing only CVD on the seeded depth.
		if cvd, ok, cvdErr := client.FetchKlinesWithCVD(ctx, symbol, timeframe, int(limit)); ok {
			fetched = cvd
			return
		} else if cvdErr != nil {
			// Degrade to OHLCV-only below, but make it observable.
			log.Debug("seed: CVD path degraded to OHLCV",
				"symbol", symbol, "timeframe", timeframe, "error", cvdErr)
		}

		if err := ctx.Err(); err != nil {
			ferr = err
			return
		}
		ohlcvs, err := client.FetchOHLCV(ctx, symbol, timeframe, 0, limit)
		if err != nil {
			ferr = err
			return
		}
		fetched = append(fetched, ohlcvs...)
	}()
	if ferr != nil {
		return nil, ferr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(fetched) == 0 {
		return nil, nil
	}

	// Classify after the fetch against the actual current time; Monday-aligned for 1w.
	currentBucket := candle.BucketTimestamp(time.Now().UnixMilli(), timeframe)

	// Captured after the response is in hand and before classification, so it measures the
	// exchange's write horizon rather than our processing.
	fetchedAt := time.Now()

	window := &seedWindow{historical: make([]candle.Candle, 0, len(fetched)), requestedAt: requestedAt, fetchedAt: fetchedAt}
	for _, c := range fetched {
		if c.Timestamp == currentBucket {
			cc := c
			window.current = &cc
			continue
		}
		if historicalNeeded && c.Timestamp < currentBucket {
			window.historical = append(window.historical, c)
		}
	}

	return window, nil
}

// MultiExchangeStore is the narrow write surface the seeder needs.
type MultiExchangeStore interface {
	AddStreamingCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, candle candle.Candle) error
	SeedCandlesBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, candles []candle.Candle) error
}

// SeedResult contains the outcome of a seeding operation.
type SeedResult struct {
	Succeeded []string // timeframes that seeded successfully
	Failed    []string // timeframes that failed (fetch or store error)
	Empty     []string // timeframes with empty response (not an error)
	AllFailed bool     // true if ALL timeframes failed (critical)

	// Transient reports an exchange refusal, timeout or unavailable protective history: a
	// condition about infrastructure, not about the symbol. Callers use AllFailed to roll back
	// a subscription, and that verdict is wrong when the exchange is rate-limiting us — then
	// every symbol fails and the rollback deletes the fleet. AllFailed+Transient keeps the
	// subscription and registers the gap-recovery retry window.
	Transient bool

	PartialError error // aggregated error if some timeframes failed
}

// isTransientFetchErr reports whether a seed failure is about the exchange refusing us rather
// than about the symbol: rate limits (429/-1003), the escalated 418 ban, timeouts and resets.
// None says anything about symbol validity, so none may justify a rollback.
func isTransientFetchErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"429", "418", "-1003", "too many requests", "ddosprotection",
		"rate limit", "timeout", "connection reset", "eof", "temporarily",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

// applySeedWindow routes one fetched seed window into the store in three ordered steps: fold
// the historical batch into the ring, apply the open bucket, then persist to cold and Redis.
//
// Step 3 cannot precede step 2: persisting blocks while live deltas keep accumulating into the
// open bucket, and the REST total would then absorb deltas that never reached it. Step 1
// cannot follow step 2: applying the open bucket rolls the ring's newest bucket, and after a
// warm boot that is a stale Redis record whose close would look like a gap.
//
// Both sides are declared REST totals, so the engine can settle against them instead of taking
// a MAX; on the open bucket the declaration is also what stops the total being accumulated on
// top of the live partial. AddStreamingCandle rather than SeedCurrentBucketCandle is deliberate:
// the seeder populates normal data, and only GapRecoveryManager sets the gap-recovered flag.
func applySeedWindow(ctx context.Context, metrics Metrics, store MultiExchangeStore,
	exchangeID, marketTypeStr, internalSymbol, tf string, window *seedWindow, logs ...*slog.Logger,
) error {
	log := logger.OrDiscard(logs...)
	// A zero time.Time yields a large negative UnixMilli, so normalize it here: an unstamped
	// window declares itself unstamped and the guard falls back to the union, which cannot lower
	// a bucket.
	var stamp int64
	if !window.fetchedAt.IsZero() {
		stamp = window.fetchedAt.UnixMilli()
	}

	// Normalize and validate before stamping provenance: a candle the validator still refuses
	// never reaches the store, and one that only violated its own bounds is repaired rather
	// than dropping the bucket. Both persistence paths share this gate.
	historical, rejected := prepareCandles(DefaultCandlesValidator{}, window.historical, internalSymbol, tf)
	for _, r := range rejected {
		metrics.incRejectedCandle(exchangeID, marketTypeStr, tf, r.Reason)
		log.Warn("seed: candle rejected at the write boundary",
			"exchange", exchangeID,
			"market_type", marketTypeStr,
			"symbol", internalSymbol,
			"timeframe", tf,
			"timestamp", r.Timestamp,
			"reason", r.Reason)
	}
	for i := range historical {
		// Stamped where the response is labeled, not where it is merged: an unstamped total is
		// refused authority and fails safe to the union.
		historical[i].Source = string(candle.CandleMergeSourceRESTTotal)
		historical[i].FetchedAt = stamp
		historical[i].RequestedAt = 0
		if !window.requestedAt.IsZero() {
			historical[i].RequestedAt = window.requestedAt.UnixMilli()
		}
	}

	// candle.SeedSplitter, not a locally declared interface: a local one can silently fail to
	// assert on the store and turn the three-step order into dead code.
	splitter, canSplit := store.(candle.SeedSplitter)
	var merged []candle.Candle
	if canSplit && len(historical) > 0 {
		merged = splitter.FoldSeedBatch(exchangeID, marketTypeStr, internalSymbol, tf, historical)
		// A wrapper may forward the capability without being able to split. Nil means "I
		// could not fold", so fall back rather than persist an empty batch.
		canSplit = merged != nil
	}

	if window.current != nil {
		currentCandles, currentRejected := prepareCandles(DefaultCandlesValidator{}, []candle.Candle{*window.current}, internalSymbol, tf)
		for _, r := range currentRejected {
			metrics.incRejectedCandle(exchangeID, marketTypeStr, tf, r.Reason)
			log.Warn("seed: current candle rejected at the write boundary",
				"exchange", exchangeID,
				"market_type", marketTypeStr,
				"symbol", internalSymbol,
				"timeframe", tf,
				"timestamp", r.Timestamp,
				"reason", r.Reason)
		}
		if len(currentCandles) == 1 {
			current := currentCandles[0]
			current.Source = string(candle.CandleMergeSourceRESTTotal)
			current.FetchedAt = stamp
			// The lower end of the bracket: trades older than the request cannot have arrived after
			// it was answered, which proves a later delta is already inside this total. Without it
			// the merge cannot refuse to count a batch twice. See candle.Candle.RequestedAt.
			if !window.requestedAt.IsZero() {
				current.RequestedAt = window.requestedAt.UnixMilli()
			}
			// Recorded at the apply, not the write return: the uncovered window ends when the total
			// is handed to the store. A zero requestedAt is skipped rather than reported as a
			// multi-decade lag.
			if !window.requestedAt.IsZero() {
				metrics.observeOpenBucketSnapshotLag(exchangeID, marketTypeStr, tf,
					float64(time.Since(window.requestedAt).Milliseconds()))
			}
			if err := store.AddStreamingCandle(ctx, exchangeID, marketTypeStr, internalSymbol, tf, current); err != nil {
				// Not critical on its own: the open bucket keeps accumulating from the live
				// stream and the next seed pass settles it once it closes.
				log.Debug("seed: current bucket write warning",
					"exchange", exchangeID,
					"market_type", marketTypeStr,
					"symbol", internalSymbol,
					"timeframe", tf,
					"error", err)
			}
		}
	}

	if len(historical) == 0 {
		return nil
	}
	if canSplit {
		// merged for the cold tier, raw for Redis — see TieredStore.PersistSeedBatch for why
		// handing the union to Redis would forge the exchange's authority.
		return splitter.PersistSeedBatch(ctx, exchangeID, marketTypeStr, internalSymbol, tf, merged, historical)
	}
	return store.SeedCandlesBatch(ctx, exchangeID, marketTypeStr, internalSymbol, tf, historical)
}

// SeedAllTimeframesMulti seeds all supported timeframes for a symbol; seedDepthMap provides
// per-timeframe depths with "default" as fallback. Callers inspect AllFailed to decide rollback.
func SeedAllTimeframesMulti(ctx context.Context, metrics Metrics, store MultiExchangeStore, client HistorySource, budget *exchange.RestBudget,
	exchangeID string, marketType market.MarketType, valid map[string]struct{}, symbol string, seedDepthMap map[string]int, tfReg *candle.TimeframeRegistry, logs ...*slog.Logger,
) error {
	result := SeedAllTimeframesMultiWithResult(ctx, metrics, store, client, budget, exchangeID, marketType, valid, symbol, seedDepthMap, tfReg, logs...)
	return result.PartialError
}

// SeedAllTimeframesMultiWithResult is the detailed version that returns SeedResult.
func SeedAllTimeframesMultiWithResult(ctx context.Context, metrics Metrics, store MultiExchangeStore, client HistorySource, budget *exchange.RestBudget,
	exchangeID string, marketType market.MarketType, valid map[string]struct{}, symbol string, seedDepthMap map[string]int, tfReg *candle.TimeframeRegistry, logs ...*slog.Logger,
) *SeedResult {
	log := logger.OrDiscard(logs...)
	result := &SeedResult{
		Succeeded: make([]string, 0, len(tfReg.All())),
		Failed:    make([]string, 0),
		Empty:     make([]string, 0),
	}

	internalSymbol := market.NormalizeSymbol(symbol)
	marketTypeStr := string(marketType)

	if _, ok := valid[internalSymbol]; !ok {
		return result // Empty result for invalid symbols
	}

	var failedTFs []string

	for _, tf := range tfReg.All() {
		if err := ctx.Err(); err != nil {
			result.Failed = append(result.Failed, tf)
			failedTFs = append(failedTFs, tf)
			continue
		}
		seedLimit := defaultSeedLimit
		if v, ok := seedDepthMap[tf]; ok && v > 0 {
			seedLimit = v
		} else if v, ok := seedDepthMap["default"]; ok && v > 0 {
			seedLimit = v
		}

		window, err := fetchSeedWindow(ctx, client, internalSymbol, tf, true, seedLimit, budget, log)
		if err != nil {
			log.Error("seed fetch failed",
				"exchange", exchangeID,
				"market_type", marketType,
				"symbol", internalSymbol,
				"timeframe", tf,
				"error", err)
			result.Failed = append(result.Failed, tf)
			failedTFs = append(failedTFs, tf)
			// Record that the exchange refused US, so the caller does not read a rate
			// limit as "this symbol is unusable" and unsubscribe a valid symbol.
			//
			// A refusal also brakes the WHOLE process, not just this symbol: the limit is
			// per-IP, so continuing at full pace on the other 499 symbols is what escalates
			// a 429 warning into a 418 ban. Penalizing the shared budget is the only backoff
			// that reaches every consumer at once.
			if isTransientFetchErr(err) {
				result.Transient = true
				if budget != nil {
					budget.PenalizeForRefusal()
				}
			}
			continue
		}

		if window == nil {
			log.Debug("seed: empty response",
				"exchange", exchangeID, "market_type", marketType, "symbol", internalSymbol, "timeframe", tf)
			result.Empty = append(result.Empty, tf)
			continue
		}

		if err := applySeedWindow(ctx, metrics, store, exchangeID, marketTypeStr, internalSymbol, tf, window, log); err != nil {
			log.Error("historical seed failed",
				"exchange", exchangeID,
				"market_type", marketType,
				"symbol", internalSymbol,
				"timeframe", tf,
				"error", err)
			result.Failed = append(result.Failed, tf)
			failedTFs = append(failedTFs, tf)
			if errors.Is(err, redisstore.ErrSeedHistoryUnavailable) || errors.Is(err, redisstore.ErrSeedColdAdmissionUnavailable) {
				// QuestDB availability is not a verdict about the symbol and is not
				// an exchange rate limit. Reuse KEEP/reseed without penalizing REST.
				result.Transient = true
			}
			continue
		}

		result.Succeeded = append(result.Succeeded, tf)
		log.Debug("seed: timeframe seeded",
			"exchange", exchangeID,
			"market_type", marketTypeStr,
			"symbol", internalSymbol,
			"timeframe", tf,
			"historical", len(window.historical),
			"current", window.current != nil)
	}

	// Determine overall result
	totalTFs := len(tfReg.All())
	result.AllFailed = len(result.Succeeded) == 0 && len(result.Failed) > 0

	if len(failedTFs) > 0 {
		result.PartialError = fmt.Errorf("seed failed for %d/%d timeframes [%v]", len(failedTFs), totalTFs, failedTFs)
	}

	return result
}
