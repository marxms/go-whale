package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"

	"github.com/redis/go-redis/v9"
)

// snapshotTTL is the TTL for the real-time derivatives snapshot keys, overwritten every poll
// cycle. It is NOT RETENTION: it is a safety net against a deranked symbol. Series retention is
// the ZSET cap, which has no TTL.
const snapshotTTL = 900 * time.Second

// DerivativesRedisStore implements the derivatives consumer ports using Redis: a per-timeframe
// ZSET capped at maxSeriesSize, plus one latest snapshot key with a TTL safety net. Key patterns
// are declared in derivatives_keys.go.
type DerivativesRedisStore struct {
	client *redis.Client
	// maxSeriesSize is the configured cap for timeframes without an override; the effective cap
	// is per timeframe. See seriesCapFor.
	maxSeriesSize int64
	// declaredCaps carries normalized per-timeframe overrides; nil means none were named.
	declaredCaps map[string]int64
	tfReg        *candle.TimeframeRegistry
	coarseSeries CoarseSeriesReader

	// baselineLastAt throttles baseline recomputation per (symbol, timeframe).
	// Guarded by baselineMu; lazily created so a zero-value store is still usable.
	baselineMu     sync.Mutex
	baselineLastAt map[string]int64

	// metrics is the injected observability port. Zero value = safe no-op.
	metrics Metrics

	log *slog.Logger
}

// SetMetrics injects the observability port.
func (s *DerivativesRedisStore) SetMetrics(m Metrics) { s.metrics = m }

// NewDerivativesRedisStore builds the hot derivatives store from the declared caps.
func NewDerivativesRedisStore(client *redis.Client, cfg config.DerivativesConfig, tfReg *candle.TimeframeRegistry, logs ...*slog.Logger) *DerivativesRedisStore {
	log := logger.OrDiscard(logs...)
	declared := make(map[string]int64, len(cfg.SeriesCapByTimeframe))
	for tf := range cfg.SeriesCapByTimeframe {
		// Resolved through the config accessor rather than read out of the map, so the
		// poller and the store cannot disagree about what the operator declared.
		declared[normalizeTimeframe(tf)] = int64(cfg.DeclaredSeriesCap(tf))
	}

	s := &DerivativesRedisStore{
		client:         client,
		maxSeriesSize:  int64(cfg.MaxSeriesSize),
		declaredCaps:   declared,
		tfReg:          tfReg,
		baselineLastAt: make(map[string]int64),
		log:            log,
	}
	s.logSeriesHorizons()
	return s
}

// CoarseSeriesReader returns canonical newest-first observations. Install once before consumers start.
type CoarseSeriesReader func(context.Context, string, string, string, string, int) ([]*derivatives.DerivativesSnapshot, error)

// SetCoarseSeriesReader installs the canonical newest-first reader for daily and weekly series.
func (s *DerivativesRedisStore) SetCoarseSeriesReader(reader CoarseSeriesReader) {
	s.coarseSeries = reader
}

// HasCanonicalCoarse reports whether timeframe has a canonical coarse reader.
func (s *DerivativesRedisStore) HasCanonicalCoarse(timeframe string) bool {
	return s.coarseSeries != nil && (timeframe == "1d" || timeframe == "1w")
}

// GetCanonicalReading reads the ruler and its scoring tail in one Redis GET.
func (s *DerivativesRedisStore) GetCanonicalReading(ctx context.Context, exchange, marketType, symbol, timeframe string) (*DerivativesBaseline, error) {
	if !s.HasCanonicalCoarse(timeframe) {
		return nil, fmt.Errorf("coarse derivatives: noncanonical timeframe %s", timeframe)
	}
	baseline, err := s.GetBaseline(ctx, exchange, marketType, symbol, timeframe)
	if err != nil {
		return nil, err
	}
	if baseline == nil {
		return nil, fmt.Errorf("coarse derivatives: canonical baseline unavailable for %s %s", symbol, timeframe)
	}
	return baseline, nil
}

// normalizeTimeframe mirrors the config's key normalization so a lookup here answers
// the same question the YAML key asked.
func normalizeTimeframe(timeframe string) string {
	return strings.ToLower(strings.TrimSpace(timeframe))
}

// seriesCapFor returns the ZSET cap for one timeframe. The cap IS the retention mechanism
// (there is no TTL), so it must be able to mature the baseline: the floor is
// baselineFetchCount(N) = N+3, since N increments span N+1 observations and two points are
// dropped by the trailing rule. A per-timeframe entry replaces the global cap in both
// directions; only the baseline floor survives the override.
func (s *DerivativesRedisStore) seriesCapFor(timeframe string) int64 {
	floor := baselineFetchCount(derivativesBaselineWindowN(timeframe))

	configured := s.maxSeriesSize
	if declared, ok := s.declaredCaps[normalizeTimeframe(timeframe)]; ok {
		configured = declared
	}

	if configured > floor {
		return configured
	}
	return floor
}

// SeriesHorizonMs returns the time span the capped ZSET can cover for a timeframe.
// The cap is a member count; the horizon is what that count means in wall time, and
// it is the number an operator actually reasons about.
func (s *DerivativesRedisStore) SeriesHorizonMs(timeframe string) int64 {
	return s.seriesCapFor(timeframe) * candle.ParseTimeframeToSeconds(timeframe) * 1000
}

// logSeriesHorizons declares cap and horizon per timeframe once at construction.
// The resulting retention is a decision, and an undeclared decision is one nobody
// can notice going wrong.
func (s *DerivativesRedisStore) logSeriesHorizons() {
	if s.tfReg == nil {
		return
	}
	for _, tf := range s.tfReg.All() {
		logger.OrDiscard(s.log).Info("derivatives: series cap",
			"timeframe", tf,
			"members", s.seriesCapFor(tf),
			"horizon_hours", float64(s.SeriesHorizonMs(tf))/3600000.0,
			"baseline_window", derivativesBaselineWindowN(tf))
	}
}

// bucketAlignMsWithConvention places a point in its bucket, deriving the bucket from the
// feed's declared label convention instead of assuming one. The internal anchor is the start of
// the interval: a point naming the instant an interval closed belongs to the prior bucket, and
// adding a bucket to a start-labeled point would write the observation in the future. An
// undeclared convention is refused at ingestion, where the feed has a name for the log.
func bucketAlignMsWithConvention(
	timestampMs int64,
	timeframe string,
	convention derivatives.LabelConvention,
) int64 {
	return derivatives.BucketAlignMs(timestampMs, timeframe, convention)
}

// --- Interface implementation ---

// dropNonFiniteFloats replaces with absence every numeric field the serializer cannot
// represent, and returns the JSON names of what it discarded. The field list comes from the
// schema by reflection: a hand-written list would age, and json.Marshal aborts on the first
// occurrence, cancelling the whole symbol. Absence, never zero, which would assert an
// unmeasured value.
func dropNonFiniteFloats(snap *derivatives.DerivativesSnapshot) []string {
	if snap == nil {
		return nil
	}

	rv := reflect.ValueOf(snap).Elem()
	tipo := rv.Type()
	tipoFloat := reflect.TypeOf((*float64)(nil))

	var dropped []string
	for i := 0; i < tipo.NumField(); i++ {
		f := tipo.Field(i)
		if f.Type != tipoFloat {
			continue
		}
		fv := rv.Field(i)
		if fv.IsNil() {
			continue
		}
		v := fv.Interface().(*float64)
		if v == nil || (!math.IsNaN(*v) && !math.IsInf(*v, 0)) {
			continue
		}
		fv.Set(reflect.Zero(f.Type))
		dropped = append(dropped, derivativesJSONFieldName(f))
	}
	return dropped
}

// derivativesJSONFieldName is the name by which the field is known OUTSIDE this package —
// the same one in the API response and the same one `DerivativesDurableCoverage` uses as a key.
func derivativesJSONFieldName(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	if i := strings.IndexByte(tag, ','); i >= 0 {
		return tag[:i]
	}
	if tag == "" {
		return f.Name
	}
	return tag
}

// SaveSnapshot stores the real-time snapshot and appends to every per-timeframe series ZSET.
//
// The error contract is deliberate: a destination's refusal skips that write and never fails
// the poll, because failing would kill the siblings and repeat the whole cycle under retryOp.
// Omission is visible through SaveSnapshotForCycle, which adds the list of destinations that
// missed the snapshot. The finiteness sweep stays before retryOp so the field arrives absent at
// both destinations and the discard is counted once per snapshot; the caller's snapshot is not
// touched.
func (s *DerivativesRedisStore) SaveSnapshot(ctx context.Context, snapshot *derivatives.DerivativesSnapshot) error {
	_, err := s.SaveSnapshotForCycle(ctx, snapshot)
	return err
}

// SaveSnapshotForCycle writes like SaveSnapshot and returns, besides the contract error, the
// destinations that did not receive the snapshot. It exists because nil does not distinguish
// "wrote" from "did not error". The list is the result of the last attempt.
func (s *DerivativesRedisStore) SaveSnapshotForCycle(ctx context.Context, snapshot *derivatives.DerivativesSnapshot) ([]string, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("derivatives: nil snapshot")
	}

	sanitized := sanitizeSeriesSnapshot(s.metrics, s.log, snapshot)

	var notWritten []string
	err := retryOp(s.log, "derivatives:save", 2, func() error {
		var attemptErr error
		notWritten, attemptErr = s.saveSnapshotOnce(ctx, sanitized)
		return attemptErr
	})
	return notWritten, err
}

// sanitizeSeriesSnapshot replaces with absence every numeric field the serializer cannot
// represent, accounts for the discard per field and returns the copy to write. Both the live and
// backfill paths use it, so the discard is counted once, from one copy of the logic.
func sanitizeSeriesSnapshot(metrics Metrics, log *slog.Logger, snapshot *derivatives.DerivativesSnapshot) *derivatives.DerivativesSnapshot {
	log = logger.OrDiscard(log)
	sanitized := copySnapshot(snapshot)
	dropped := dropNonFiniteFloats(sanitized)
	if len(dropped) == 0 {
		return sanitized
	}
	for _, field := range dropped {
		metrics.incSeriesNonFiniteDiscarded(field)
	}
	log.Warn("derivatives: snapshot fields discarded as non-finite; series preserved",
		"exchange", sanitized.Exchange,
		"market_type", sanitized.MarketType,
		"symbol", sanitized.Symbol,
		"dropped_fields", len(dropped),
		"fields", dropped)
	return sanitized
}

// saveSnapshotOnce writes a snapshot to all destinations and returns those that did not receive
// it. Only marshal and the latest-key Set are errors for retryOp; a refused destination goes in
// the list.
func (s *DerivativesRedisStore) saveSnapshotOnce(ctx context.Context, snapshot *derivatives.DerivativesSnapshot) ([]string, error) {
	exchange := snapshot.Exchange
	mt := snapshot.MarketType
	sym := snapshot.Symbol

	// 1. Real-time snapshot key — field-level merge, like the series. Overwriting the whole key
	// erased fields a partial poll did not carry.
	//
	// A failing pre-merge read skips this key's write rather than returning an error: the key
	// has a TTL and the next poll heals it, while an error would abort the series fan-out and
	// baseline steps and be repeated by retryOp. notWritten accumulates the destinations that
	// missed the snapshot; it never becomes an error.
	var notWritten []string

	latestKey := derivLatestScopedKey(exchange, mt, sym)
	existing, readErr := s.GetLatestSnapshot(ctx, exchange, mt, sym)
	if readErr != nil {
		logger.OrDiscard(s.log).Warn("derivatives: snapshot write aborted; pre-merge read failed",
			"key", latestKey, "error", readErr)
		notWritten = append(notWritten, latestKey)
	} else {
		toWrite := snapshot
		if existing != nil {
			toWrite = mergeDerivativesSnapshots(existing, snapshot)
		}

		data, err := json.Marshal(toWrite)
		if err != nil {
			return notWritten, fmt.Errorf("derivatives: marshal snapshot: %w", err)
		}

		tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
		err = s.client.Set(tctx, latestKey, data, snapshotTTL).Err()
		cancel()
		if err != nil {
			return notWritten, fmt.Errorf("derivatives: set latest: %w", err)
		}
	}

	// 2. Append to ALL per-timeframe series ZSETs (batched: 2 round-trips instead of 14)
	timeframes := s.timeframes()
	if err := s.upsertSeriesBatch(ctx, snapshot, timeframes); err != nil {
		logger.OrDiscard(s.log).Warn("derivatives: batch series upsert failed",
			"symbol", sym, "exchange", exchange, "error", err)
		// It enters the list, not the error: the counter must distinguish "the venue went
		// down" from "we did not write".
		for _, tf := range timeframes {
			notWritten = append(notWritten, derivSeriesZsetKey(exchange, mt, sym, tf))
		}
	}

	// 3. Recompute at most one baseline that is past its refresh interval.
	s.refreshDueBaseline(ctx, snapshot, timeframes)

	return notWritten, nil
}

// timeframes returns the configured timeframes, or nothing when no registry was
// injected — a nil registry is a wiring mistake, not a reason to guess a default set
// and write series nobody asked for.
func (s *DerivativesRedisStore) timeframes() []string {
	if s.tfReg == nil {
		return nil
	}
	return s.tfReg.All()
}

// SaveSeriesOnly writes a historical snapshot to one timeframe's series ZSET without updating
// the latest key. Three bootstrap paths reach it, and all can land on a bucket a previous
// process filled from the live poll. The merge is field by field: what the backfill does not
// carry survives, and where both sides carry a field the last writer by arrival order wins, so
// in steady state the live sample survives. The funding group moves whole or not at all. If
// aggregate-over-sample is ever wanted it needs a provenance marker and a merge that reads it.
func (s *DerivativesRedisStore) SaveSeriesOnly(ctx context.Context, snapshot *derivatives.DerivativesSnapshot, timeframe string) error {
	if snapshot == nil {
		return fmt.Errorf("derivatives: nil snapshot")
	}

	// The backfill goes through the same sanitization as the live path; writing through the
	// codec guard would discard silently.
	return s.upsertSeries(ctx, sanitizeSeriesSnapshot(s.metrics, s.log, snapshot), timeframe)
}

// GetLatestSnapshot returns the newest snapshot of one scope, or nil when absent.
func (s *DerivativesRedisStore) GetLatestSnapshot(ctx context.Context, exchange, marketType, symbol string) (*derivatives.DerivativesSnapshot, error) {
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	data, err := s.client.Get(tctx, derivLatestScopedKey(exchange, marketType, symbol)).Bytes()
	cancel()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("derivatives: get latest: %w", err)
	}

	var snapshot derivatives.DerivativesSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("derivatives: unmarshal latest: %w", err)
	}
	return &snapshot, nil
}

// ServedSymbols returns the set of symbols this scope serves — the fleet the read answers for,
// not the list the collector carries. The two diverge because the collector's list is resolved
// once at construction while discovery reorders the served fleet. It is reached by type
// assertion from the cycle counter.
func (s *DerivativesRedisStore) ServedSymbols(ctx context.Context, exchange, marketType string) (map[string]struct{}, error) {
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	defer cancel()

	members, err := s.client.SMembers(tctx, activeSymbolsKeyV2(exchange, marketType)).Result()
	if err != nil {
		// An error is not an empty fleet: a registry that did not answer supports no absence.
		return nil, fmt.Errorf("derivatives: served fleet read %s/%s: %w", exchange, marketType, err)
	}

	out := make(map[string]struct{}, len(members))
	for _, m := range members {
		out[m] = struct{}{}
	}
	return out, nil
}

// GetSnapshotSeries reads directly from the per-timeframe ZSET.
// Data is stored in ZSET members (JSON snapshots). No separate keys or aggregation needed.
func (s *DerivativesRedisStore) GetSnapshotSeries(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]*derivatives.DerivativesSnapshot, error) {
	if s.HasCanonicalCoarse(timeframe) {
		if limit > 0 && limit <= 3 {
			baseline, err := s.GetCanonicalReading(ctx, exchange, marketType, symbol, timeframe)
			if err != nil {
				return nil, err
			}
			tail := baseline.SeriesTail
			if len(tail) > limit {
				tail = tail[:limit]
			}
			return tail, nil
		}
		return s.coarseSeries(ctx, exchange, marketType, symbol, timeframe, limit)
	}
	key := derivSeriesZsetKey(exchange, marketType, symbol, timeframe)

	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	// ZREVRANGE returns members in descending score order (newest first). The `timestamp`
	// stays INSIDE the member: it is the member's identity in the ZSET, not an echo of the
	// score — see the encoding block in derivatives_series_codec.go.
	var members []string
	var err error
	if limit > 0 {
		members, err = s.client.ZRevRange(tctx, key, 0, int64(limit-1)).Result()
	} else {
		members, err = s.client.ZRevRange(tctx, key, 0, -1).Result()
	}
	cancel()
	if err != nil {
		return nil, fmt.Errorf("derivatives: zrevrange series: %w", err)
	}

	if len(members) == 0 {
		return nil, nil
	}

	ident := derivativesSeriesIdentity{Exchange: exchange, MarketType: marketType, Symbol: symbol}
	snapshots := make([]*derivatives.DerivativesSnapshot, 0, len(members))
	for _, m := range members {
		snap, decErr := decodeDerivativesSeriesMember(m, ident)
		if decErr != nil {
			if errors.Is(decErr, ErrDerivativesSeriesUnknownVersion) {
				// A short series is the failure mode nobody detects. See the failure-rule
				// block in derivatives_series_codec.go.
				logger.OrDiscard(s.log).Error("derivatives: series written by a version this binary cannot read",
					"key", key, "error", decErr)
				return nil, fmt.Errorf("derivatives: series %s: %w", key, decErr)
			}
			logger.OrDiscard(s.log).Warn("derivatives: series entry unmarshal failed", "error", decErr)
			continue
		}
		snapshots = append(snapshots, snap)
	}

	return snapshots, nil
}

// warmSeriesClosedBarScan is how many members at the top of the ZSET are inspected to find the
// newest closed bar. Two suffice (at most one stored bucket is open); the third is slack.
const warmSeriesClosedBarScan = 3

// NewestClosedSeriesBar reports the label of the newest closed bar the hot tier stores for a
// (symbol, timeframe), and whether it stores any.
//
// It is a clock, not a source: it reads only the member's score, the same bucket instant
// upsertSeriesInto writes, and decodes no member, so no venue value leaves here. nowMs is the
// caller's clock, the same one BucketClosed uses. An empty ZSET and a timeframe with no bar
// width both return (0, false, nil).
func (s *DerivativesRedisStore) NewestClosedSeriesBar(ctx context.Context, exchange, marketType, symbol, timeframe string, nowMs int64) (int64, bool, error) {
	tfMs, ok := candle.ParseTimeframeMs(timeframe)
	if !ok || tfMs <= 0 {
		return 0, false, nil
	}
	key := derivSeriesZsetKey(exchange, marketType, symbol, timeframe)

	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	members, err := s.client.ZRevRangeWithScores(tctx, key, 0, warmSeriesClosedBarScan-1).Result()
	cancel()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, false, fmt.Errorf("derivatives: newest closed bar for %s: %w", key, err)
	}

	for _, member := range members {
		label := int64(member.Score)
		if BucketClosed(label, tfMs, nowMs) {
			return label, true, nil
		}
	}
	return 0, false, nil
}

// --- Per-timeframe ZSET management (identical to candle ZSET pattern) ---

// upsertSeries performs a field-level merge-on-write into the per-timeframe ZSET: bucket-align
// the timestamp, read the existing member, remove the score, write the merged JSON and cap by
// rank. The cap is the retention mechanism; there is no TTL.
func (s *DerivativesRedisStore) upsertSeries(ctx context.Context, snapshot *derivatives.DerivativesSnapshot, timeframe string) error {
	// The write applies the interval-start floor. The per-endpoint declaration lives in
	// derivativesLabelDeclarations and governs admission, not this layer's alignment: a
	// divergent declaration does not move this ZSET, while in the durable tier it does move the
	// recorded instant. Binding the declaration to this write would be a data migration.
	key := derivSeriesZsetKey(snapshot.Exchange, snapshot.MarketType, snapshot.Symbol, timeframe)
	return s.upsertSeriesInto(ctx, key, derivatives.LabelIntervalStart, snapshot, timeframe)
}

func (s *DerivativesRedisStore) upsertSeriesInto(
	ctx context.Context,
	key string,
	label derivatives.LabelConvention,
	snapshot *derivatives.DerivativesSnapshot,
	timeframe string,
) error {
	bucketTs := bucketAlignMsWithConvention(snapshot.Timestamp, timeframe, label)

	// Read existing entry at this bucket timestamp for field-level merge
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	existing, err := s.client.ZRangeByScore(tctx, key, &redis.ZRangeBy{
		Min:   fmt.Sprintf("%d", bucketTs),
		Max:   fmt.Sprintf("%d", bucketTs),
		Count: 1,
	}).Result()
	cancel()
	if err != nil && !errors.Is(err, redis.Nil) {
		// A failing pre-merge read aborts the upsert of this bucket: without the left side of
		// the merge there is no safe write, and writing the new snapshot alone would erase
		// fields it does not carry. Repeating the cycle is cheap and idempotent.
		logger.OrDiscard(s.log).Warn("derivatives: bucket upsert aborted; pre-merge read failed",
			"key", key, "bucket", bucketTs, "error", err)
		return fmt.Errorf("derivatives: pre-merge read %s bucket %d: %w", key, bucketTs, err)
	}

	ident := derivativesSeriesIdentity{
		Exchange:   snapshot.Exchange,
		MarketType: snapshot.MarketType,
		Symbol:     snapshot.Symbol,
	}

	var merged *derivatives.DerivativesSnapshot
	if len(existing) > 0 {
		// Parse existing and merge fields (latest non-nil wins)
		current, decErr := decodeDerivativesSeriesMember(existing[0], ident)
		switch {
		case decErr == nil:
			merged = mergeDerivativesSnapshots(current, snapshot)
		case errors.Is(decErr, ErrDerivativesSeriesUnknownVersion):
			// Same class as a failing pre-merge read: continuing would overwrite a member
			// written by a newer version, and that loss does not come back.
			logger.OrDiscard(s.log).Error("derivatives: bucket upsert aborted; member version is unknown",
				"key", key, "bucket", bucketTs, "error", decErr)
			return fmt.Errorf("derivatives: upsert %s bucket %d: %w", key, bucketTs, decErr)
		default:
			merged = copySnapshot(snapshot)
		}
	} else {
		merged = copySnapshot(snapshot)
	}

	// Override timestamp to bucket-aligned value
	merged.Timestamp = bucketTs
	dropPollToPollOIChange(merged)

	data, err := encodeDerivativesSeriesMember(merged)
	if err != nil {
		return fmt.Errorf("derivatives: encode merged: %w", err)
	}

	pipe := s.client.Pipeline()

	// Remove any existing entry at this bucket score (dedup)
	pipe.ZRemRangeByScore(ctx, key, fmt.Sprintf("%d", bucketTs), fmt.Sprintf("%d", bucketTs))

	// Add merged entry
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(bucketTs), Member: data})

	// Cap at the timeframe's own cap — remove oldest (lowest scores). NO TTL.
	pipe.ZRemRangeByRank(ctx, key, 0, -(s.seriesCapFor(timeframe) + 1))

	tctx2, cancel2 := ensureTimeout(ctx, redisDefaultOpTimeout)
	_, err = pipe.Exec(tctx2)
	cancel2()
	if err != nil {
		return fmt.Errorf("derivatives: upsert pipeline: %w", err)
	}

	return nil
}

// upsertSeriesBatch performs a batched read-merge-write across all timeframes in two pipelines:
// one read, in-memory merge, one write. It replaces 14 sequential round trips with 2.
func (s *DerivativesRedisStore) upsertSeriesBatch(ctx context.Context, snapshot *derivatives.DerivativesSnapshot, timeframes []string) error {
	type tfEntry struct {
		key      string
		bucketTs int64
		cap      int64
	}

	entries := make([]tfEntry, len(timeframes))
	for i, tf := range timeframes {
		entries[i] = tfEntry{
			key: derivSeriesZsetKey(snapshot.Exchange, snapshot.MarketType, snapshot.Symbol, tf),
			// By the convention, like everything else: alignment must not have a second home.
			bucketTs: bucketAlignMsWithConvention(snapshot.Timestamp, tf, derivatives.LabelIntervalStart),
			cap:      s.seriesCapFor(tf),
		}
	}

	// ---- READ pipeline: fetch existing entries for all timeframes ----
	readPipe := s.client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(entries))
	for i, e := range entries {
		cmds[i] = readPipe.ZRangeByScore(ctx, e.key, &redis.ZRangeBy{
			Min:   fmt.Sprintf("%d", e.bucketTs),
			Max:   fmt.Sprintf("%d", e.bucketTs),
			Count: 1,
		})
	}

	rctx, rcancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	_, execErr := readPipe.Exec(rctx)
	rcancel()

	// A failing pre-merge read refuses the bucket here too, per bucket: each timeframe is its
	// own key, and siblings whose read answered are still written with the full merge. The
	// Exec error is that of the first failed command, so if no command carries it the pipeline
	// never ran and the refusal applies to the whole batch.
	batchReadFailed := execErr != nil && !errors.Is(execErr, redis.Nil)
	if batchReadFailed {
		for _, cmd := range cmds {
			if cmdErr := cmd.Err(); cmdErr != nil && !errors.Is(cmdErr, redis.Nil) {
				batchReadFailed = false
				break
			}
		}
	}

	// ---- Merge in-memory ----
	type mergedEntry struct {
		key      string
		bucketTs int64
		cap      int64
		data     string
	}
	merged := make([]mergedEntry, 0, len(entries))

	var refused int
	var refusedCause error

	ident := derivativesSeriesIdentity{
		Exchange:   snapshot.Exchange,
		MarketType: snapshot.MarketType,
		Symbol:     snapshot.Symbol,
	}

	for i, e := range entries {
		existing, cmdErr := cmds[i].Result()

		cause := cmdErr
		if cause != nil && errors.Is(cause, redis.Nil) {
			cause = nil
		}
		if cause == nil && batchReadFailed {
			cause = execErr
		}
		if cause != nil {
			// Without the left side of the merge there is no safe write to make. Repeating
			// the cycle is cheap — `ZREM`+`ZADD` at the same score is idempotent — while
			// writing the partial is a loss no later cycle recovers.
			logger.OrDiscard(s.log).Warn("derivatives: bucket upsert aborted; pre-merge read failed",
				"key", e.key, "bucket", e.bucketTs, "error", cause)
			refused++
			if refusedCause == nil {
				refusedCause = cause
			}
			continue
		}

		var result *derivatives.DerivativesSnapshot
		if len(existing) > 0 {
			current, decErr := decodeDerivativesSeriesMember(existing[0], ident)
			switch {
			case decErr == nil:
				result = mergeDerivativesSnapshots(current, snapshot)
			case errors.Is(decErr, ErrDerivativesSeriesUnknownVersion):
				// Same per-bucket refusal as a failing pre-merge read: this bucket leaves
				// the batch, the healthy siblings continue. See upsertSeriesInto.
				logger.OrDiscard(s.log).Error("derivatives: bucket upsert aborted; member version is unknown",
					"key", e.key, "bucket", e.bucketTs, "error", decErr)
				refused++
				if refusedCause == nil {
					refusedCause = decErr
				}
				continue
			default:
				result = copySnapshot(snapshot)
			}
		} else {
			result = copySnapshot(snapshot)
		}

		result.Timestamp = e.bucketTs
		dropPollToPollOIChange(result)

		data, err := encodeDerivativesSeriesMember(result)
		if err != nil {
			logger.OrDiscard(s.log).Warn("derivatives: encode merged snapshot failed",
				"timeframe", timeframes[i], "error", err)
			continue
		}

		merged = append(merged, mergedEntry{key: e.key, bucketTs: e.bucketTs, cap: e.cap, data: data})
	}

	// The caller must know the batch came back incomplete; otherwise the refusal stays only in
	// this file's log.
	var refusal error
	if refused > 0 {
		refusal = fmt.Errorf("derivatives: %d bucket(s) of %s refused — pre-merge read failed: %w",
			refused, snapshot.Symbol, refusedCause)
	}

	if len(merged) == 0 {
		return refusal
	}

	// ---- WRITE pipeline: remove old + add merged + cap for all timeframes ----
	writePipe := s.client.Pipeline()
	for _, m := range merged {
		scoreStr := fmt.Sprintf("%d", m.bucketTs)
		writePipe.ZRemRangeByScore(ctx, m.key, scoreStr, scoreStr)
		writePipe.ZAdd(ctx, m.key, redis.Z{Score: float64(m.bucketTs), Member: m.data})
		writePipe.ZRemRangeByRank(ctx, m.key, 0, -(m.cap + 1))
	}

	wctx, wcancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	_, err := writePipe.Exec(wctx)
	wcancel()
	if err != nil {
		return fmt.Errorf("derivatives: batch write pipeline: %w", err)
	}

	return refusal
}

// mergeDerivativesSnapshots performs a field-level merge: new non-nil values overwrite existing,
// which combines the separate OI-only and funding-only bootstrap entries. Every optional field
// must appear here; a forgotten field is written by the poller and then dropped on the next
// merge, which looks like an intermittent feed.
func mergeDerivativesSnapshots(existing, incoming *derivatives.DerivativesSnapshot) *derivatives.DerivativesSnapshot {
	result := *existing
	// Keep identity from incoming
	result.Exchange = incoming.Exchange
	result.MarketType = incoming.MarketType
	result.Symbol = incoming.Symbol

	// Clocks only move forward: an older late read may contribute a field but must not
	// backdate the record.
	if incoming.Timestamp > result.Timestamp {
		result.Timestamp = incoming.Timestamp
	}
	if incoming.ObservedAt > result.ObservedAt {
		result.ObservedAt = incoming.ObservedAt
	}

	// --- open interest ---
	// The reading and the instant it was taken move together; splitting them pairs a
	// preserved reading with an advanced clock.
	if incoming.OpenInterestBase != nil {
		result.OpenInterestBase = incoming.OpenInterestBase
		result.OpenInterestObservedAtMs = incoming.OpenInterestObservedAtMs
	}
	if incoming.OpenInterest != nil {
		result.OpenInterest = incoming.OpenInterest
	}
	if incoming.OIChangePct != nil {
		result.OIChangePct = incoming.OIChangePct
	}
	if incoming.OIChangePctWindowMs != nil {
		result.OIChangePctWindowMs = incoming.OIChangePctWindowMs
	}

	// --- funding: one group, merged atomically ---
	// Rate, 8h-equivalent, interval, rung, interest rate and anchor flag are one observed rate
	// plus its interval resolution. Merging them field by field breaks the identity
	// FundingRate8h == FundingRate x 8/Interval (measured: 9.7% of live series points). The
	// trigger is the rate: a read without one leaves the previous group intact.
	if incoming.FundingRate != nil {
		result.FundingRate = incoming.FundingRate
		result.FundingRate8h = incoming.FundingRate8h
		result.FundingIntervalHours = incoming.FundingIntervalHours
		result.NStep = incoming.NStep
		result.InterestRate = incoming.InterestRate
		result.FundingAtAnchor = incoming.FundingAtAnchor
	}
	// NextFundingRate stays field by field: it is a forecast, not a term of the identity.
	if incoming.NextFundingRate != nil {
		result.NextFundingRate = incoming.NextFundingRate
	}

	// --- long / short ---
	if incoming.LongShortRatio != nil {
		result.LongShortRatio = incoming.LongShortRatio
	}
	if incoming.LSLongPct != nil {
		result.LSLongPct = incoming.LSLongPct
	}
	if incoming.LSShortPct != nil {
		result.LSShortPct = incoming.LSShortPct
	}

	// The field-by-field rule preserves what the incoming read did not carry, but the derived
	// funding fields are products of an interval: when the ladder reports none, preserving the
	// previous ones republishes an 8h-equivalent under a declaration saying it does not exist.
	if result.NStep == derivatives.NUnavailable {
		result.FundingRate8h = nil
		result.FundingIntervalHours = nil
		result.FundingAtAnchor = nil
	}
	return &result
}

// dropPollToPollOIChange removes the legacy open-interest delta from a series member.
// OIChangePct is a difference between consecutive polls, so writing it into every timeframe
// gave a 1d bucket a five-minute change under a daily label. The per-bucket increment already
// exists as the normalized axis, so no replacement is added. The latest key keeps the field,
// where "since the previous poll" is true; this function is scoped to the series because the
// defect is the window label. Nil-ing it also erases values already stamped into a bucket.
func dropPollToPollOIChange(member *derivatives.DerivativesSnapshot) {
	member.OIChangePct = nil
	// The window goes with it: a poll-to-poll span in a 1d bucket is the same category error.
	member.OIChangePctWindowMs = nil
	// The observation stamp goes too: its only reader is the next poll's window computation on
	// the latest record, and a second, finer instant in a bucket would confuse "when the bar
	// is" with "when the reading was".
	member.OpenInterestObservedAtMs = nil
}

// copySnapshot creates a shallow copy of a DerivativesSnapshot.
func copySnapshot(s *derivatives.DerivativesSnapshot) *derivatives.DerivativesSnapshot {
	copied := *s
	return &copied
}

// ============================================================================
// DATA LIFECYCLE CLEANUP
// ============================================================================
//
// Three sweeps delete derivatives keys; two live here, and pruner.PruneRedis is the third.
// All derive patterns and positional parsing from derivatives_keys.go.

// scanDerivativesKeys walks one glob with SCAN and hands each page to fn. A scan error stops
// the walk, since cleanup is best-effort.
func (s *DerivativesRedisStore) scanDerivativesKeys(ctx context.Context, pattern string, count int64, fn func(keys []string)) {
	var cursor uint64
	for {
		tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
		keys, nextCursor, err := s.client.Scan(tctx, cursor, pattern, count).Result()
		cancel()
		if err != nil {
			return
		}
		if len(keys) > 0 {
			fn(keys)
		}
		cursor = nextCursor
		if cursor == 0 {
			return
		}
	}
}

// removeFunc is DEL or UNLINK. The two cleanup families differ only in this.
type removeFunc func(ctx context.Context, keys ...string) *redis.IntCmd

// removeSymbolKeys deletes every derivatives key of one symbol using remove.
func (s *DerivativesRedisStore) removeSymbolKeys(ctx context.Context, exchange, marketType, symbol string, remove removeFunc) int64 {
	var totalRemoved int64

	removeKeys := func(keys ...string) {
		tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
		n, err := remove(tctx, keys...).Result()
		cancel()
		if err == nil {
			totalRemoved += n
		}
	}

	// The latest keys are addressed directly — no scan needed.
	removeKeys(derivSymbolDirectKeys(exchange, marketType, symbol)...)

	for _, pattern := range derivSymbolKeyPatterns(exchange, marketType, symbol) {
		s.scanDerivativesKeys(ctx, pattern, 100, func(keys []string) {
			removeKeys(keys...)
		})
	}

	return totalRemoved
}

// staleDerivativesSymbols discovers which symbols hold derivatives keys in this
// scope but are absent from activeSymbols.
func (s *DerivativesRedisStore) staleDerivativesSymbols(ctx context.Context, exchange, marketType string, activeSymbols map[string]struct{}) map[string]struct{} {
	stale := make(map[string]struct{})

	for _, pattern := range derivScopeKeyPatterns(exchange, marketType) {
		s.scanDerivativesKeys(ctx, pattern, 200, func(keys []string) {
			for _, key := range keys {
				sym := SymbolFromDerivativesKey(key)
				if sym == "" {
					// An unrecognized shape is left alone: guessing a symbol would delete live
					// data.
					continue
				}
				if _, active := activeSymbols[sym]; !active {
					stale[sym] = struct{}{}
				}
			}
		})
	}

	return stale
}

// CleanupSymbolDerivatives deletes all derivatives keys for a specific symbol
// (latest snapshot, every timeframe series, every baseline) using DEL.
func (s *DerivativesRedisStore) CleanupSymbolDerivatives(ctx context.Context, exchange, marketType, symbol string) int64 {
	return s.removeSymbolKeys(ctx, exchange, marketType, symbol, s.client.Del)
}

// PurgeStaleDerivatives removes all derivatives keys for symbols NOT in the activeSymbols set.
// Called on startup after discovery to clean stale derivatives data from previous runs.
func (s *DerivativesRedisStore) PurgeStaleDerivatives(ctx context.Context, exchange, marketType string, activeSymbols map[string]struct{}) int64 {
	staleSymbols := s.staleDerivativesSymbols(ctx, exchange, marketType, activeSymbols)
	if len(staleSymbols) == 0 {
		return 0
	}

	logger.OrDiscard(s.log).Info("purge stale derivatives: stale symbols found",
		"exchange", exchange, "market_type", marketType, "symbols", len(staleSymbols))

	var totalDeleted int64
	for sym := range staleSymbols {
		totalDeleted += s.CleanupSymbolDerivatives(ctx, exchange, marketType, sym)
	}

	logger.OrDiscard(s.log).Info("purge stale derivatives: keys deleted",
		"exchange", exchange, "market_type", marketType, "deleted_keys", totalDeleted)
	return totalDeleted
}

// ============================================================================
// UNLINK-BASED CLEANUP (asynchronous, non-blocking memory release)
// ============================================================================

// UnlinkSymbolDerivatives is identical to CleanupSymbolDerivatives but uses
// Redis UNLINK instead of DEL. Memory is reclaimed asynchronously by Redis.
func (s *DerivativesRedisStore) UnlinkSymbolDerivatives(ctx context.Context, exchange, marketType, symbol string) int64 {
	return s.removeSymbolKeys(ctx, exchange, marketType, symbol, s.client.Unlink)
}

// UnlinkStaleDerivatives is identical to PurgeStaleDerivatives but uses
// Redis UNLINK instead of DEL for both snapshot and series keys.
func (s *DerivativesRedisStore) UnlinkStaleDerivatives(ctx context.Context, exchange, marketType string, activeSymbols map[string]struct{}) int64 {
	staleSymbols := s.staleDerivativesSymbols(ctx, exchange, marketType, activeSymbols)
	if len(staleSymbols) == 0 {
		return 0
	}

	logger.OrDiscard(s.log).Info("unlink stale derivatives: stale symbols found",
		"exchange", exchange, "market_type", marketType, "symbols", len(staleSymbols))

	var totalUnlinked int64
	for sym := range staleSymbols {
		totalUnlinked += s.UnlinkSymbolDerivatives(ctx, exchange, marketType, sym)
	}

	logger.OrDiscard(s.log).Info("unlink stale derivatives: keys unlinked",
		"exchange", exchange, "market_type", marketType, "unlinked_keys", totalUnlinked)
	return totalUnlinked
}
