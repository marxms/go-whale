package questdb

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// The derivatives durable tier, and the failure that must not take collection down.
//
// # WHY IN A BATCH, AND NEVER ROW BY ROW
//
// `flushBatches` committed PER SERIES, and a series closes one bucket at a time — that is,
// one commit per ROW. Measured across four independent production windows: 1.0000 row per
// commit, against the client's default of 75,000. The derivatives table has exactly the same
// shape (DAY + DEDUP + out-of-order timestamps), so it inherits the same pathology if written
// point by point.
//
// # WHY A DURABLE FAILURE FAILS THE CYCLE
//
// QuestDB is the only tier that still receives this collection; keeping its failure
// swallowed would leave the cycle with NO FAILURE POINT AT ALL. That is why a durable failure
// is RETURNED: the caller aggregates errors per symbol and does NOT mark the symbol as
// seeded, so the next cycle asks for the deep window again.

// derivativesTableName is the durable tier table.
//
// A constant, not configuration: this table is born with the DDL in this package, and a
// configurable name would be a knob nobody turns and that only lets the two ends disagree.
const derivativesTableName = "derivatives"

// derivativesHoleRate is the HOLE RATE of a batch: what percentage of its rows became a hole
// rather than erode data.
func derivativesHoleRate(holes, rows int) float64 {
	if rows <= 0 {
		return 0
	}
	return 100 * float64(holes) / float64(rows)
}

// WriteDerivativesBatch writes the whole slice in ONE ILP transaction.
//
// A CONNECTION error aborts and is retried on a fresh connection, exactly like the candles
// path. Retrying is safe because the table has DEDUP UPSERT KEYS.
func (q *Store) WriteDerivativesBatch(ctx context.Context, points []derivatives.DerivativesColdPoint) error {
	if len(points) == 0 {
		return nil
	}

	// BEFORE THE ILP LOCK, and before the first new column exists: the erosion protection
	// queries the table, and a query holding the write mutex would pin the single writer for
	// the duration of a database round trip. See derivatives_erosion.go.
	lote := len(points)
	points, outcome := preserveOmittedColumns(ctx, q.priorsOf(), points, q.log)
	// COUNT WHENEVER THERE WAS A CHOICE. The log says what happened in this batch; the
	// counter is what sums across batches and becomes an alert — and it sums BOTH halves of
	// the partition.
	if outcome.degraded() {
		q.recordErosionOutcome(outcome)
	}

	// THE ALARM BELONGS TO THE HOLE, AND ONLY TO IT — the discriminating partition governs
	// the firing.
	if outcome.holes > 0 {
		logger.OrDiscard(q.log).Warn("questdb derivatives: cycle with a hole; rows preserved",
			"preserved", outcome.preserved,
			"holes", outcome.holes,
			"rows", lote,
			"hole_rate", derivativesHoleRate(outcome.holes, lote))
	}
	if len(points) == 0 {
		return nil
	}

	err := q.writeDerivativesBatchOnce(ctx, points)
	if err != nil && isConnectionError(err) {
		logger.OrDiscard(q.log).Warn("questdb derivatives: connection error; reconnecting", "error", err)
		if reconnErr := q.reconnectILP(ctx); reconnErr != nil {
			q.errCount.Add(1)
			return fmt.Errorf("questdb reconnect failed after derivatives flush error: %w", reconnErr)
		}
		err = q.writeDerivativesBatchOnce(ctx, points)
	}
	return err
}

// admissibleDerivativesRows separates what may be written from what must be refused, and
// records the refusal.
func admissibleDerivativesRows(rows []derivatives.DerivativesColdPoint, logs ...*slog.Logger) ([]derivatives.DerivativesColdPoint, int) {
	log := logger.OrDiscard(logs...)
	out := make([]derivatives.DerivativesColdPoint, 0, len(rows))
	var refused int
	for _, p := range rows {
		if err := p.Declared(); err != nil {
			// Refusal, not substitution. A point without a unit written with a blank unit is
			// indistinguishable, on read, from a measured point — and it is the read that
			// decides.
			refused++
			log.Error("questdb derivatives: row refused", "error", err)
			continue
		}
		if p.Empty() {
			// NO LOG: an empty row is routine; a row without a declaration is a defect.
			refused++
			continue
		}
		out = append(out, p)
	}
	return out, refused
}

func (q *Store) writeDerivativesBatchOnce(ctx context.Context, points []derivatives.DerivativesColdPoint) error {
	// THE SAME LOCK AS THE CANDLES PATH. This is the SECOND writer attached to the same
	// `LineSender`, and it is what broke the single-writer invariant. See `ilpMu`.
	q.ilpMu.Lock()
	defer q.ilpMu.Unlock()

	rows := sortedDerivativesRows(points)

	gravaveis, rowErrors := admissibleDerivativesRows(rows, q.log)

	for _, p := range gravaveis {
		line := q.ilpSender.
			Table(derivativesTableName).
			Symbol("source", p.Source).
			Symbol("exchange", p.Exchange).
			Symbol("market_type", p.MarketType).
			Symbol("symbol", p.Symbol).
			Symbol("timeframe", p.Timeframe).
			Symbol("denominated_in", p.DenominatedIn).
			Symbol("label_convention", p.LabelConvention)

		// THE RUNG GOES OUT HERE, IN THE SYMBOL BLOCK, AND NOT NEXT TO THE FUNDING IT
		// DESCRIBES.
		//
		// The line protocol requires ALL symbols before any value column. OMISSION IS
		// DECIDED BEFORE THE FLOATS BLOCK: `p.NStep` only goes out when the row carries the
		// funding it describes.
		if p.FundingRate != nil && p.NStep != "" {
			line = line.Symbol("n_step", string(p.NStep))
		}

		// AN ABSENT COLUMN IS OMITTED, NEVER ZEROED. ILP has no NULL, and writing 0 for a
		// funding that did not return would invent a reading.
		if p.OpenInterestBase != nil {
			line = line.Float64Column("open_interest_base", *p.OpenInterestBase)
		}
		if p.FundingRate != nil {
			line = line.Float64Column("funding_rate", *p.FundingRate)
		}
		if p.FundingRate8h != nil {
			line = line.Float64Column("funding_rate_8h", *p.FundingRate8h)
		}
		if p.FundingIntervalHours != nil {
			line = line.Float64Column("funding_interval_hours", *p.FundingIntervalHours)
		}
		if p.LongShortRatio != nil {
			line = line.Float64Column("long_short_ratio", *p.LongShortRatio)
		}

		// THE SIX ADDITIVE COLUMNS. The same rule: absent is OMITTED, never zeroed — and here
		// the rule has teeth, because `fundingAtAnchor` is a predicate.
		if p.OpenInterest != nil {
			line = line.Float64Column("open_interest", *p.OpenInterest)
		}
		if p.NextFundingRate != nil {
			line = line.Float64Column("next_funding_rate", *p.NextFundingRate)
		}
		if p.InterestRate != nil {
			line = line.Float64Column("interest_rate", *p.InterestRate)
		}
		if p.FundingAtAnchor != nil {
			line = line.BoolColumn("funding_at_anchor", *p.FundingAtAnchor)
		}
		if p.LSLongPct != nil {
			line = line.Float64Column("ls_long_pct", *p.LSLongPct)
		}
		if p.LSShortPct != nil {
			line = line.Float64Column("ls_short_pct", *p.LSShortPct)
		}

		if err := line.At(ctx, p.Timestamp); err != nil {
			q.errCount.Add(1)
			if isConnectionError(err) {
				return fmt.Errorf("questdb ilp derivatives row ts=%d: %w", p.Timestamp.UnixMilli(), err)
			}
			rowErrors++
			logger.OrDiscard(q.log).Warn("questdb derivatives: skipping row",
				"timestamp", p.Timestamp.UnixMilli(), "error", err)
			continue
		}
	}

	// ONE Flush for the whole batch. Over ILP-over-HTTP the request is the transaction, so
	// this is one WAL commit for the entire sweep instead of one per row.
	if err := q.ilpSender.Flush(ctx); err != nil {
		q.errCount.Add(1)
		return fmt.Errorf("questdb derivatives flush: %w", err)
	}

	if written := int64(len(rows) - rowErrors); written > 0 {
		q.rowsWritten.Add(written)
	}
	q.mu.Lock()
	q.lastFlush = time.Now()
	q.mu.Unlock()

	return nil
}

// sortedDerivativesRows returns the rows in ASCENDING timestamp order.
func sortedDerivativesRows(points []derivatives.DerivativesColdPoint) []derivatives.DerivativesColdPoint {
	rows := make([]derivatives.DerivativesColdPoint, len(points))
	copy(rows, points)
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Timestamp.Before(rows[j].Timestamp) })
	return rows
}
