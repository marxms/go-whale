package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"

	"golang.org/x/sync/semaphore"
)

const (
	warmOutcomeEmpty       = "empty"
	warmOutcomeUnavailable = "unavailable"
	warmOutcomeBudget      = "budget_exhausted"
)

// warmOutcome classifies one warm-seed attempt. It is a separate, pure function because the
// distinction it encodes — "no data" versus "could not read" — is the one the boot path was
// missing, and a rule that matters that much should be stated once and pinned by a test
// rather than living inline in a goroutine nothing can exercise.
//
// A tier error loses to a tier that DELIVERED: if QuestDB failed but Redis served the
// candles, the buffer is correctly warmed and reporting "unavailable" would be a false alarm.
func warmOutcome(source string, coldErr, redisErr error) string {
	if source != "" {
		return source
	}
	if coldErr != nil || redisErr != nil {
		return warmOutcomeUnavailable
	}
	return warmOutcomeEmpty
}

// firstNonNilErr returns the first non-nil error, so the boot summary can carry ONE concrete
// cause instead of a bare count that tells an operator nothing about what to go and fix.
func firstNonNilErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// WarmCandleEngine seeds CandleEngine buffers from QuestDB (primary) or Redis (fallback) on startup.
// ctx is the process lifetime context; the warm-up applies its own 60s ceiling on top of it.
func WarmCandleEngine(ctx context.Context, eng *CandleEngine, coldStore candle.ColdStore, redis *redisstore.RedisStore, cfg config.Config) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	symbols, err := redis.GetAllActiveSymbols(ctx)
	if err != nil {
		logger.OrDiscard(eng.log).Warn("candle engine warm cache: failed to get active symbols", "error", err)
		return
	}

	timeframes := cfg.CandleEngine.Timeframes
	// SQL embeds the time bounds as literals. One snapshot keeps the query text reusable
	// across symbols in pgx's statement cache while retaining timestamp partition pruning.
	warmTo := time.Now()

	var (
		seeded          int64
		coldSeeded      int64
		redisCount      int64
		unavailable     int64
		budgetExhausted int64
	)
	// One representative failure for the summary. sync.Once gives the write-once semantics,
	// and wg.Wait() below establishes the happens-before that makes the read safe.
	var (
		firstFailure sync.Once
		sampleErr    error
	)

	sem := semaphore.NewWeighted(16)
	var wg sync.WaitGroup

	for _, sym := range symbols {
		for _, tf := range timeframes {
			wg.Add(1)
			go func(s redisstore.SymbolInfo, t string) {
				defer wg.Done()
				// Wait for a slot under the WARM-UP's OWN budget (the 60s ctx above), not a
				// per-task 5s deadline. ~4000 (symbol x timeframe) goroutines are spawned at once
				// against 16 slots, so most of them queue. A fixed per-task deadline makes queued
				// work self-destruct in a burst even on a healthy backend, leaving a large share of
				// the engine's buffers UNSEEDED. The parent ctx already bounds the whole warm-up,
				// and the semaphore weight is unchanged, so using it adds no extra concurrency
				// against QuestDB.
				if err := sem.Acquire(ctx, 1); err != nil {
					// Reached only when the warm-up budget itself is exhausted; the remaining pairs
					// warm from live closes and the gap-recovery backfill.
					//
					// RECORDED, not just logged: this branch fires when the boot is already
					// degraded, and the outcome set must close against symbols x timeframes.
					eng.metrics.incWarmSeed(s.Exchange, s.MarketType, t, warmOutcomeBudget)
					atomic.AddInt64(&budgetExhausted, 1)
					logger.OrDiscard(eng.log).Warn("warm candle engine: budget exhausted before seeding",
						"exchange", s.Exchange,
						"market_type", s.MarketType,
						"symbol", s.Symbol,
						"timeframe", t,
						"error", err)
					return
				}
				defer sem.Release(1)

				// Fallback for the SINGLE SOURCE OF TRUTH, not a literal of its own: see declared_depth.go.
				seedDepth := config.DefaultResidentDepth
				if v, ok := cfg.CandleEngine.BufferSizes[t]; ok && v > 0 {
					seedDepth = v
				} else if v, ok := cfg.CandleEngine.BufferSizes["default"]; ok && v > 0 {
					seedDepth = v
				}

				var seedCandles []candle.Candle
				var source string

				// A TIER THAT FAILED IS NOT A TIER THAT WAS EMPTY.
				//
				// "QuestDB is unreachable" and "this symbol genuinely has no history yet" are
				// operationally opposite. A cold-tier outage once spent thirteen hours wedged;
				// every warm read failed, every buffer came up EMPTY, and the REST seeder filled
				// 800 buckets per key from REST without measured taker history. The only trace was
				// a summary saying 0 buffers seeded.
				//
				// This does NOT change what happens next: an unreachable tier still falls through
				// to the seeder, because refusing to boot on a cold-tier outage would be a worse
				// failure. It changes only whether an operator can tell the two apart.
				var coldErr, redisErr error

				if coldStore != nil {
					to := warmTo
					from := to.Add(-candle.SeedWindow(candle.Timeframe(t), seedDepth))
					cold, qErr := coldStore.QueryRange(ctx, s.Exchange, s.MarketType, s.Symbol, t, from, to, seedDepth)
					switch {
					case qErr != nil:
						coldErr = qErr
					case len(cold) > 0:
						seedCandles = cold
						source = "questdb"
					}
				}

				// Fallback to Redis
				if len(seedCandles) == 0 {
					rCandles, rErr := redis.GetCandlesDesc(ctx, s.Exchange, s.MarketType, s.Symbol, t, seedDepth)
					switch {
					case rErr != nil:
						redisErr = rErr
					case len(rCandles) > 0:
						seedCandles = rCandles
						source = "redis"
					}
				}

				outcome := warmOutcome(source, coldErr, redisErr)
				eng.metrics.incWarmSeed(s.Exchange, s.MarketType, t, outcome)

				if len(seedCandles) == 0 {
					if outcome == warmOutcomeUnavailable {
						atomic.AddInt64(&unavailable, 1)
						// One sample, not 4,000 log lines: the summary reports the count and
						// this explains it. A per-key warning here would be the same cry-wolf
						// flood that makes a real signal unreadable.
						firstFailure.Do(func() {
							sampleErr = firstNonNilErr(coldErr, redisErr)
						})
					}
					return
				}

				// Both QueryRange and GetCandlesDesc return newest-first; SeedCandles expects oldest-first.
				for i, j := 0, len(seedCandles)-1; i < j; i, j = i+1, j-1 {
					seedCandles[i], seedCandles[j] = seedCandles[j], seedCandles[i]
				}

				eng.LoadStoredCandles(s.Exchange, s.MarketType, s.Symbol, t, seedCandles)
				atomic.AddInt64(&seeded, 1)
				if source == "questdb" {
					atomic.AddInt64(&coldSeeded, 1)
				} else {
					atomic.AddInt64(&redisCount, 1)
				}
			}(sym, tf)
		}
	}

	wg.Wait()
	logger.OrDiscard(eng.log).Info("candle engine warm cache complete",
		"symbols", len(symbols),
		"buffers_seeded", seeded,
		"questdb", coldSeeded,
		"redis", redisCount)

	// A warm boot that could not READ is not the same event as one that found nothing, and
	// the line above cannot tell them apart — it reports 0 seeded for both. That ambiguity is
	// what let a cold-tier outage boot the whole fleet onto empty buffers with nothing in the log
	// pointing at the cause.
	//
	// Reported as a count plus ONE sample error: the count says how wide the damage is, the
	// sample says where to go and look.
	if unavailable > 0 {
		logger.OrDiscard(eng.log).Warn("candle engine warm cache: buffers could not be read; they boot empty without historical taker measurements",
			"unavailable", unavailable,
			"buffers", int64(len(symbols)*len(timeframes)),
			"sample_error", sampleErr)
	}

	// A third condition, distinct from both of the above: pairs that were never ATTEMPTED.
	// Same missing history (an empty buffer), different cause — the host could not warm the fleet
	// inside the budget — and therefore a different fix (capacity or budget, not the cold tier).
	// Without a total, this was visible only as N scattered per-pair warnings.
	if budgetExhausted > 0 {
		logger.OrDiscard(eng.log).Warn("candle engine warm cache: buffers were never attempted; they boot empty without historical taker measurements",
			"never_attempted", budgetExhausted,
			"buffers", int64(len(symbols)*len(timeframes)))
	}
}
