package questdb

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ============================================================================
// MIGRATION OF WHAT EXISTS ONLY IN REDIS — candles 1d and 1w
// ============================================================================
//
// Retention is global and reapplied on every boot, and the read path has no REST leg, so for
// 1d and 1w Redis is the only copy of candles older than the durable window. Without this
// migration, an eviction policy that touches everything would drop exactly those keys first.
//
// The boundary is read as MIN(timestamp) of the durable tier per (symbol, timeframe), never a
// literal date, and a symbol with zero durable rows migrates its entire series. The migration
// reads Redis and writes QuestDB; it deletes nothing.

// migrationHotStore is the hot read the migration needs.
type migrationHotStore interface {
	ScanCandleKeys(ctx context.Context, timeframe string) ([]redisstore.CandleKeyRef, error)
	GetCandlesByScoreRange(ctx context.Context, exchange, marketType, symbol, timeframe string,
		minScore, maxScore int64) ([]candle.Candle, error)
}

// migrationColdStore is the durable tier: where the boundary is read and where candles are written.
type migrationColdStore interface {
	GetOldestTimestamp(ctx context.Context, exchange, marketType, symbol, timeframe string) (time.Time, error)
	WriteBatches(ctx context.Context, groups map[candle.SeriesKey][]candle.Candle) error
}

// SoleCopyFlushThreshold is how many candles accumulate before a commit. It is exported
// because it is also the work-ceiling unit per boot pass. Committing per row measured 1.0 row
// per commit, 3.91 TB written to store 3.6 GB; a point-by-point migration would reintroduce
// that amplification.
const SoleCopyFlushThreshold = 20_000

// SoleCopyMigrationOptions parameterizes the pass.
type SoleCopyMigrationOptions struct {
	// Timeframes are the cells to migrate. Empty is an error, not "all": the cell list is the
	// caller's declaration.
	Timeframes []string
	// Apply false is the zero-value default; in dry mode the pass counts and writes no row.
	Apply bool
	// FlushEvery lets tests tighten the batch. Zero falls back to the default.
	FlushEvery int
	// MaxCandles is the work ceiling of this pass in candles; zero means no ceiling. Boot
	// declares one because a crashlooping binary runs this once per restart. At the ceiling the
	// pass flushes, marks Truncated and returns; resumption re-reads the boundary.
	MaxCandles int
}

// SoleCopyCell is the result of one cell (timeframe).
type SoleCopyCell struct {
	Timeframe string
	// Keys is how many candle keys the keyspace had for this cell.
	Keys int
	// KeysWithSoleCopy is how many had a member older than the durable boundary.
	KeysWithSoleCopy int
	// Candles is how many candles were sole copies.
	Candles int
	// KeysWithoutColdRows is how many keys had no durable row at all, so the whole series is
	// a sole copy.
	KeysWithoutColdRows int
}

// SoleCopyMigrationReport is what the pass returns to be cited.
type SoleCopyMigrationReport struct {
	Applied bool
	Cells   []SoleCopyCell
	// Commits counts confirmed days (or projections in dry mode); one per candle would bring
	// the write amplification back.
	Commits int
	// Truncated says the pass stopped at the ceiling, not at the boundary.
	Truncated bool
}

// Candles sums the candles of all cells.
func (r SoleCopyMigrationReport) Candles() int {
	var total int
	for _, c := range r.Cells {
		total += c.Candles
	}
	return total
}

// SoleCopyMigrator flushes to the durable tier the candles that exist only in Redis.
type SoleCopyMigrator struct {
	hot  migrationHotStore
	cold migrationColdStore
	log  *slog.Logger
}

// NewSoleCopyMigrator assembles the migration.
func NewSoleCopyMigrator(hot migrationHotStore, cold migrationColdStore, logs ...*slog.Logger) *SoleCopyMigrator {
	return &SoleCopyMigrator{hot: hot, cold: cold, log: logger.OrDiscard(logs...)}
}

// Run walks the declared cells and migrates whatever is a sole copy.
func (m *SoleCopyMigrator) Run(ctx context.Context, opts SoleCopyMigrationOptions) (SoleCopyMigrationReport, error) {
	report := SoleCopyMigrationReport{Applied: opts.Apply}
	if len(opts.Timeframes) == 0 {
		return report, fmt.Errorf("migration: no timeframe declared")
	}
	flushEvery := opts.FlushEvery
	if flushEvery <= 0 {
		flushEvery = SoleCopyFlushThreshold
	}

	pendentes := make(map[candle.SeriesKey][]candle.Candle)
	var pendentesN int

	flush := func() error {
		if pendentesN == 0 {
			return nil
		}
		var entries []candleEntry
		for key, cs := range pendentes {
			for _, c := range cs {
				entries = append(entries, candleEntry{
					Exchange:   key.Scope.Exchange.String(),
					MarketType: key.Scope.MarketType.String(),
					Symbol:     key.Scope.Symbol.String(),
					Timeframe:  key.Timeframe.String(),
					Candle:     c,
				})
			}
		}
		days := appendDayWrites(nil, entries)
		// MIN(timestamp) resumes safely only when missing DAYs arrive newest first.
		for left, right := 0, len(days)-1; left < right; left, right = left+1, right-1 {
			days[left], days[right] = days[right], days[left]
		}
		if opts.Apply {
			plan := &coldDayPlan{days: days}
			phase, err := applyColdPlan(ctx, m.cold, plan)
			report.Commits += plan.next
			if err != nil {
				return fmt.Errorf("migration: phase=%s after %d commit(s): %w", phase, report.Commits, err)
			}
		} else {
			report.Commits += len(days)
		}

		pendentes = make(map[candle.SeriesKey][]candle.Candle)
		pendentesN = 0
		return nil
	}

	// ABORTING PUBLISHES THE PARTIAL RESULT. Every exit from here down goes through
	// `publicar`, and the current timeframe cell is appended to the report BEFORE the symbol
	// loop — not after it. See the `publicar` comment below.
	publicar := func(err error) (SoleCopyMigrationReport, error) {
		for _, c := range report.Cells {
			logger.OrDiscard(m.log).Info("sole-copy migration cell",
				"mode", soleCopyMode(opts.Apply),
				"timeframe", c.Timeframe,
				"keys", c.Keys,
				"keys_with_sole_copy", c.KeysWithSoleCopy,
				"keys_without_cold_rows", c.KeysWithoutColdRows,
				"candles", c.Candles)
		}
		return report, err
	}

	var migradas int
	var truncada bool

	for _, tf := range opts.Timeframes {
		// The cell enters the report now and is mutated by index, so every early return still
		// reports the series already migrated instead of showing zero.
		report.Cells = append(report.Cells, SoleCopyCell{Timeframe: tf})
		celula := len(report.Cells) - 1

		refs, err := m.hot.ScanCandleKeys(ctx, tf)
		if err != nil {
			return publicar(fmt.Errorf("migration: scanning keys for %s: %w", tf, err))
		}
		// Deterministic order: the pass is cited by number, and a report that changes shape
		// between identical executions is not evidence.
		sort.Slice(refs, func(i, j int) bool { return refs[i].Symbol < refs[j].Symbol })
		report.Cells[celula].Keys = len(refs)

		for _, ref := range refs {
			if err := ctx.Err(); err != nil {
				return publicar(err)
			}

			oldest, err := m.cold.GetOldestTimestamp(ctx, ref.Exchange, ref.MarketType, ref.Symbol, tf)
			if err != nil {
				return publicar(fmt.Errorf("migration: durable boundary for %s %s: %w", ref.Symbol, tf, err))
			}

			// Without a durable row the whole series is a sole copy; MaxInt64 is the literal
			// translation of "there is no boundary".
			maxScore := int64(math.MaxInt64)
			if oldest.IsZero() {
				report.Cells[celula].KeysWithoutColdRows++
			} else {
				maxScore = oldest.UnixMilli() - 1
			}

			velas, err := m.hot.GetCandlesByScoreRange(ctx, ref.Exchange, ref.MarketType, ref.Symbol, tf,
				math.MinInt64, maxScore)
			if err != nil {
				return publicar(fmt.Errorf("migration: reading %s %s below %d: %w", ref.Symbol, tf, maxScore, err))
			}
			if len(velas) == 0 {
				continue
			}

			report.Cells[celula].KeysWithSoleCopy++
			report.Cells[celula].Candles += len(velas)

			key, keyErr := candle.SeriesKeyOf(ref.Exchange, ref.MarketType, ref.Symbol, tf)
			if keyErr != nil {
				return publicar(fmt.Errorf("migration: durable identity for %s %s: %w", ref.Symbol, tf, keyErr))
			}
			pendentes[key] = append(pendentes[key], velas...)
			pendentesN += len(velas)
			migradas += len(velas)

			if pendentesN >= flushEvery {
				if err := flush(); err != nil {
					return publicar(err)
				}
			}

			// The ceiling is checked after the whole symbol: resumption is a re-read of the
			// boundary, which is exact only at a symbol edge.
			if opts.MaxCandles > 0 && migradas >= opts.MaxCandles {
				truncada = true
				break
			}
		}

		if truncada {
			break
		}
	}

	report.Truncated = truncada

	if err := flush(); err != nil {
		return publicar(err)
	}
	return publicar(nil)
}

func soleCopyMode(apply bool) string {
	if apply {
		return "APPLYING"
	}
	return "dry-run"
}
