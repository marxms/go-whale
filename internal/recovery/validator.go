package recovery

import (
	"context"
	"log/slog"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// RedisValidator implements Validator using a StorageWriter.
type RedisValidator struct {
	writer StorageWriter
	log    *slog.Logger
}

// NewRedisValidator creates a new validator backed by the given writer.
func NewRedisValidator(w StorageWriter, logs ...*slog.Logger) *RedisValidator {
	return &RedisValidator{writer: w, log: logger.OrDiscard(logs...)}
}

// bucketsBetween returns how many whole buckets of tfMs lie in [fromMs, toMs) — the SIZE of
// a hole whose first absent bucket is fromMs and whose next present bucket is toMs. Pure, so
// the arithmetic that turns a discontinuity into a magnitude is unit-testable on its own.
// Non-positive spans and a non-positive timeframe yield 0 rather than a negative count.
func bucketsBetween(fromMs, toMs, tfMs int64) int {
	if tfMs <= 0 || toMs <= fromMs {
		return 0
	}
	return int((toMs - fromMs) / tfMs)
}

// Validate checks whether a backfill is complete by querying stored candles
// in the gap range [gapStart, now], verifying interior continuity AND that the series
// reaches the previous closed bucket. The open bucket may legitimately be absent; the
// bucket before it may not, because the venue never resends a finished bucket and a series
// that simply stops is the shape a silent feed leaves behind.
//
// THE SECOND RETURN IS A BUCKET COUNT, AND IT USED NOT TO BE.
//
// It was incremented once per DISCONTINUITY: `missing++` fired when a timestamp was not the
// previous one plus one timeframe, no matter how far past it had jumped. A four-hour hole on
// 1m and a one-bucket blip both reported "1 missing candle(s)" — in those exact words, in the
// log, and to the caller. A magnitude that cannot distinguish 1 from 240 cannot size a
// retry, cannot rank two failing symbols, and cannot be alerted on.
//
// The head hole had the same defect in a sharper form: when the first stored candle is not
// gapStart, everything between them is absent and the function returned 0 — zero for the
// largest hole it is able to detect.
//
// Both now report BUCKETS, which is what the sentence "missing candle(s)" already claimed.
func (v *RedisValidator) Validate(ctx context.Context, exchangeID, marketType, symbol, tf string, gapStart, currentBucket int64) (bool, int) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tfSeconds := candle.ParseTimeframeToSeconds(tf)
	if tfSeconds == 0 {
		logger.OrDiscard(v.log).Warn("gap recovery validation: invalid timeframe", "timeframe", tf)
		return false, 0
	}
	tfMs := int64(tfSeconds) * 1000
	previousClosed := currentBucket - tfMs

	members, err := v.writer.GetCandlesByScoreRange(tctx, exchangeID, marketType, symbol, tf, gapStart, time.Now().Add(time.Second).UnixMilli())
	if err != nil {
		logger.OrDiscard(v.log).Warn("gap recovery validation: candle query failed",
			"exchange", exchangeID, "market_type", marketType, "symbol", symbol, "timeframe", tf, "error", err)
		return false, 0
	}
	if len(members) == 0 {
		logger.OrDiscard(v.log).Warn("gap recovery validation: no candles found for gap range",
			"exchange", exchangeID, "market_type", marketType, "symbol", symbol, "timeframe", tf, "gap_start", gapStart)
		return false, 0
	}

	missing := 0
	first := members[0]
	if first.Timestamp != gapStart {
		// The whole prefix [gapStart, first) is absent. Report its SIZE.
		missing = bucketsBetween(gapStart, first.Timestamp, tfMs)
		logger.OrDiscard(v.log).Warn("gap recovery validation: first candle mismatch; buckets missing at the head",
			"exchange", exchangeID,
			"market_type", marketType,
			"symbol", symbol,
			"timeframe", tf,
			"expected", gapStart,
			"actual", first.Timestamp,
			"missing", missing)
	}

	var prevTs int64
	for _, c := range members {
		if prevTs > 0 {
			expected := prevTs + tfMs
			if c.Timestamp == prevTs {
				continue
			}
			if c.Timestamp != expected {
				// COUNT THE BUCKETS IN THE HOLE, NOT THE HOLE.
				holeSize := bucketsBetween(expected, c.Timestamp, tfMs)
				missing += holeSize
				logger.OrDiscard(v.log).Debug("gap recovery validation: missing buckets in sequence",
					"missing", holeSize,
					"exchange", exchangeID,
					"market_type", marketType,
					"symbol", symbol,
					"timeframe", tf,
					"expected", expected,
					"actual", c.Timestamp)
			}
		}
		prevTs = c.Timestamp
	}

	// THE TAIL IS A HOLE TOO. Interior continuity can pass a series that simply stops
	// short of the present: the missing buckets are after the newest one, not between two
	// stored ones. Requiring the previous closed bucket (never the open one, which may
	// legitimately not exist yet) is what turns a silent feed into a validation failure
	// instead of a clean bill of health.
	newest := members[len(members)-1].Timestamp
	if tail := bucketsBetween(newest, previousClosed, tfMs); tail > 0 {
		missing += tail
		logger.OrDiscard(v.log).Warn("gap recovery validation: series stops before the previous closed bucket",
			"exchange", exchangeID,
			"market_type", marketType,
			"symbol", symbol,
			"timeframe", tf,
			"newest", newest,
			"previous_closed", previousClosed,
			"missing", tail)
	}

	if missing > 0 {
		logger.OrDiscard(v.log).Warn("gap recovery validation: missing buckets in sequence",
			"missing", missing, "exchange", exchangeID, "market_type", marketType, "symbol", symbol, "timeframe", tf)
		return false, missing
	}

	logger.OrDiscard(v.log).Info("gap recovery validated: gap closed",
		"exchange", exchangeID,
		"market_type", marketType,
		"symbol", symbol,
		"timeframe", tf,
		"gap_start", gapStart,
		"candles", len(members))
	return true, 0
}
