package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// THE READ FAILURE MUST BE SURGICAL FOR THE CASE TO DISCRIMINATE.
//
// Taking the whole server down (miniredis `SetError`) would make the write fail too, and
// then the bucket would survive even with the defect in place: the test would pass while
// asserting Redis behavior, not the upsert's. The hook below takes down ONLY the
// `ZRANGEBYSCORE` — the pre-merge read — and leaves `ZREM`/`ZADD` working. That asymmetry is
// exactly what separates "aborted" from "wrote the partial over it".
//
// `armed` exists because go-redis does not remove hooks: the same client must read again so
// the next cycle proves the bucket recomposes.
type failPreMergeRead struct {
	armed *atomic.Bool
	err   error
}

func (h failPreMergeRead) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h failPreMergeRead) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.armed.Load() && strings.EqualFold(cmd.Name(), "zrangebyscore") {
			cmd.SetErr(h.err)
			return h.err
		}
		return next(ctx, cmd)
	}
}

func (h failPreMergeRead) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// failPipelinedReadOfKey takes down the pre-merge read of the BATCHED path, and only that of
// ONE key.
//
// Two reasons not to reuse the hook above. First, go-redis does not pass pipelined commands
// through `ProcessHook` — `upsertSeriesBatch` reads through a pipeline, so the hook above
// simply does not reach this path. Second, taking down ALL `ZRANGEBYSCORE`s in the batch
// would not discriminate the granularity ("abort the upsert of THAT bucket"): with all of
// them down, refusing the whole batch and refusing bucket by bucket produce the same
// observable result. By taking down one timeframe's read, the healthy sibling must still be
// written WITH the merge — and that is what separates the refusal from over-correction.
//
// The WRITE pipeline (`ZREM`/`ZADD`/`ZREMRANGEBYRANK`) passes intact: that asymmetry is what
// separates "aborted" from "wrote the partial over it".
type failPipelinedReadOfKey struct {
	armed     *atomic.Bool
	keySubstr string
	err       error
}

func (h failPipelinedReadOfKey) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h failPipelinedReadOfKey) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (h failPipelinedReadOfKey) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !h.armed.Load() {
			return next(ctx, cmds)
		}

		var doomed, healthy []redis.Cmder
		for _, cmd := range cmds {
			args := cmd.Args()
			key := ""
			if len(args) > 1 {
				key = fmt.Sprint(args[1])
			}
			if strings.EqualFold(cmd.Name(), "zrangebyscore") && strings.Contains(key, h.keySubstr) {
				doomed = append(doomed, cmd)
				continue
			}
			healthy = append(healthy, cmd)
		}
		if len(doomed) == 0 {
			return next(ctx, cmds)
		}

		for _, cmd := range doomed {
			cmd.SetErr(h.err)
		}
		if len(healthy) > 0 {
			if err := next(ctx, healthy); err != nil {
				return err
			}
		}
		return h.err
	}
}

// THE SAME REFUSAL COVERS THE LIVE series path, bucket by bucket.
//
// `upsertSeriesBatch` is the path the live poll traverses (`SaveSnapshot` fans out to every
// timeframe) and it carried the whole class already refused on the sibling path: read
// failure -> `logger.Debug` -> `copySnapshot(snapshot)` -> the `ZREM`+`ZADD` pipe writes the
// NEW snapshot alone over the existing member. A funding-only write erased the bucket's open
// interest on ALL timeframes at once.
//
// The case takes down ONE timeframe's read and leaves the other healthy, in the SAME batch.
// Three things are asserted at once: the refused bucket preserves what it had, the healthy
// sibling is written with the full merge (the refusal did not become a batch abort), and the
// caller receives the failure instead of a silent success.
func TestUpsertSeriesBatch_PreMergeReadFailureAbortsOnlyTheBucketThatFailed(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1h", "1d")

	armed := &atomic.Bool{}
	readDown := errors.New("batch read unavailable")
	client.AddHook(failPipelinedReadOfKey{armed: armed, keySubstr: ":1h", err: readDown})

	// EXTERNAL LITERALS: 2026-08-20T13:00:00Z in ms since epoch. The full hour was chosen so
	// the two buckets do NOT coincide — 1h falls at 13:00 and 1d at 00:00 — otherwise the
	// score in the log would not distinguish which one was refused.
	const observedMs int64 = 1_787_230_800_000
	const bucket1hMs int64 = 1_787_230_800_000
	const bucket1dMs int64 = 1_787_184_000_000
	const openInterestBase = 107025.033
	const fundingRate = 0.0000942

	if got := time.Date(2026, 8, 20, 13, 0, 0, 0, time.UTC).UnixMilli(); got != observedMs {
		t.Fatalf("the observation literal left the calendar: %d != %d", got, observedMs)
	}
	if got := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC).UnixMilli(); got != bucket1dMs {
		t.Fatalf("the 1d bucket literal left the calendar: %d != %d", got, bucket1dMs)
	}

	// Cycle 1: the live poll fills both buckets with open interest, healthy read.
	withOI := &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: observedMs, OpenInterestBase: fptr(openInterestBase),
	}
	if err := store.SaveSnapshot(ctx, withOI); err != nil {
		t.Fatalf("SaveSnapshot (cycle 1): %v", err)
	}

	// Cycle 2: funding-only poll with the 1h pre-merge read down.
	fundingOnly := &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: observedMs, FundingRate: fptr(fundingRate),
	}
	armed.Store(true)
	log, sink := logger.NewCapture(logger.LevelDebug)
	store.log = log
	err := store.SaveSnapshot(ctx, fundingOnly)
	armed.Store(false)
	if err != nil {
		t.Fatalf("SaveSnapshot (cycle 2) — the snapshot key does not depend on the series: %v", err)
	}

	// The WARN names the refused bucket: series key AND score.
	key1h := derivSeriesZsetKey("binance", "future", "BTCUSDT", "1h")
	level, found := sink.LevelOf("key=" + key1h)
	if !found {
		t.Fatalf("no log line names bucket %s at score %d; got: %v", key1h, bucket1hMs, sink.Records())
	}
	if level != logger.LevelWarn {
		t.Fatalf("the abort came out at %s, the criterion requires WARN — at process level (Error) a Debug goes nowhere", level)
	}

	// The refusal reaches the caller: `saveSnapshotOnce` only records this line if
	// `upsertSeriesBatch` returned an error. Returning nil, the incomplete batch would pass
	// as complete.
	if _, propagated := sink.LevelOf("batch series upsert failed"); !propagated {
		t.Fatalf("the refusal did not propagate to the batch caller; got: %v", sink.Records())
	}

	// The refused bucket remains whole, and the partial did NOT enter.
	series1h, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1h", 10)
	if err != nil {
		t.Fatalf("GetSnapshotSeries 1h: %v", err)
	}
	if len(series1h) != 1 {
		t.Fatalf("%d point(s) in bucket 1h, wanted 1", len(series1h))
	}
	if series1h[0].Timestamp != bucket1hMs {
		t.Fatalf("bucket 1h = %d, wanted %d", series1h[0].Timestamp, bucket1hMs)
	}
	if series1h[0].OpenInterestBase == nil || *series1h[0].OpenInterestBase != openInterestBase {
		t.Fatalf("openInterestBase of 1h = %v, wanted %v — the funding-only write erased the bucket's OI",
			series1h[0].OpenInterestBase, openInterestBase)
	}
	if series1h[0].FundingRate != nil {
		t.Fatalf("fundingRate of 1h = %v — the aborted upsert wrote the partial anyway", *series1h[0].FundingRate)
	}

	// THE HEALTHY SIBLING OF THE SAME BATCH: the refusal belongs to the bucket, not the
	// batch. The 1d read, merged and wrote BOTH fields.
	series1d, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1d", 10)
	if err != nil {
		t.Fatalf("GetSnapshotSeries 1d: %v", err)
	}
	if len(series1d) != 1 {
		t.Fatalf("%d point(s) in bucket 1d, wanted 1", len(series1d))
	}
	if series1d[0].Timestamp != bucket1dMs {
		t.Fatalf("bucket 1d = %d, wanted %d", series1d[0].Timestamp, bucket1dMs)
	}
	if series1d[0].OpenInterestBase == nil || *series1d[0].OpenInterestBase != openInterestBase {
		t.Fatalf("openInterestBase of 1d = %v, wanted %v", series1d[0].OpenInterestBase, openInterestBase)
	}
	if series1d[0].FundingRate == nil || *series1d[0].FundingRate != fundingRate {
		t.Fatalf("fundingRate of 1d = %v, wanted %v — refusing one bucket took down the whole batch",
			series1d[0].FundingRate, fundingRate)
	}
}

// failWholePipelineWithoutStamping returns a pipeline error WITHOUT stamping it on any
// command.
//
// It is what go-redis does when the pipeline never runs: `withConn` failing to get a
// connection returns the error directly, without going through `setCmdsErr`. The effect on
// the caller is the worst possible — each command's `Result()` returns `(nil, nil)`, that is,
// empty WITHOUT error, which is byte for byte what a nonexistent bucket returns. Only the
// `Exec` error distinguishes the two.
type failWholePipelineWithoutStamping struct {
	armed *atomic.Bool
	err   error
}

func (h failWholePipelineWithoutStamping) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h failWholePipelineWithoutStamping) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h failWholePipelineWithoutStamping) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !h.armed.Load() {
			return next(ctx, cmds)
		}
		for _, cmd := range cmds {
			if strings.EqualFold(cmd.Name(), "zrangebyscore") {
				return h.err
			}
		}
		return next(ctx, cmds)
	}
}

// THE BATCH WHOSE READ PIPELINE NEVER RAN is also refused, entirely.
//
// This is the case a per-command test does not reach and the code most easily gets wrong:
// the pipeline fails before running, no command gets a stamped error, and `Result()` returns
// `(nil, nil)` from all of them. Empty without error is exactly what a new bucket returns —
// so a refusal that looked ONLY at the per-command error would conclude "there is nothing
// here" and write the partial snapshot over ALL buckets of the symbol at once.
func TestUpsertSeriesBatch_UnstampedPipelineFailureRefusesEveryBucket(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1h", "1d")

	armed := &atomic.Bool{}
	pipelineDown := errors.New("connection unavailable for the pipeline")
	client.AddHook(failWholePipelineWithoutStamping{armed: armed, err: pipelineDown})

	// EXTERNAL LITERALS: same observation as the sibling case, 2026-08-20T13:00:00Z in ms.
	const observedMs int64 = 1_787_230_800_000
	const openInterestBase = 107025.033
	const fundingRate = 0.0000942

	if got := time.Date(2026, 8, 20, 13, 0, 0, 0, time.UTC).UnixMilli(); got != observedMs {
		t.Fatalf("the observation literal left the calendar: %d != %d", got, observedMs)
	}

	withOI := &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: observedMs, OpenInterestBase: fptr(openInterestBase),
	}
	if err := store.SaveSnapshot(ctx, withOI); err != nil {
		t.Fatalf("SaveSnapshot (cycle 1): %v", err)
	}

	fundingOnly := &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: observedMs, FundingRate: fptr(fundingRate),
	}
	armed.Store(true)
	log, sink := logger.NewCapture(logger.LevelDebug)
	store.log = log
	if err := store.SaveSnapshot(ctx, fundingOnly); err != nil {
		t.Fatalf("SaveSnapshot (cycle 2): %v", err)
	}
	armed.Store(false)

	// BOTH refused buckets, each named in its own WARN.
	for _, tf := range []string{"1h", "1d"} {
		key := derivSeriesZsetKey("binance", "future", "BTCUSDT", tf)
		level, found := sink.LevelOf("key=" + key)
		if !found {
			t.Fatalf("no log line names bucket %s; got: %v", key, sink.Records())
		}
		if level != logger.LevelWarn {
			t.Fatalf("the abort of %s came out at %s, the criterion requires WARN", tf, level)
		}

		series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", tf, 10)
		if err != nil {
			t.Fatalf("GetSnapshotSeries %s: %v", tf, err)
		}
		if len(series) != 1 {
			t.Fatalf("%d point(s) in bucket %s, wanted 1", len(series), tf)
		}
		if series[0].OpenInterestBase == nil || *series[0].OpenInterestBase != openInterestBase {
			t.Fatalf("openInterestBase of %s = %v, wanted %v — empty-without-error passed as a new bucket and the partial entered",
				tf, series[0].OpenInterestBase, openInterestBase)
		}
		if series[0].FundingRate != nil {
			t.Fatalf("fundingRate of %s = %v — the aborted upsert wrote the partial anyway", tf, *series[0].FundingRate)
		}
	}
}

// failLatestKeyRead takes down ONLY the `GET` of the `latest` key, and only that one.
//
// The pre-merge read of this path is a standalone `GET`, not a `ZRANGEBYSCORE` nor a
// pipelined command — the two hooks above do not reach it. Scoping by the KEY, and not by
// the command name, is what keeps the asymmetry that makes the case discriminate: the `SET`
// just below and the series fan-out of the same cycle keep working, so "skipped the write"
// and "wrote the partial over it" produce different observables.
type failLatestKeyRead struct {
	armed *atomic.Bool
	key   string
	err   error
}

func (h failLatestKeyRead) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h failLatestKeyRead) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.armed.Load() && strings.EqualFold(cmd.Name(), "get") {
			args := cmd.Args()
			if len(args) > 1 && fmt.Sprint(args[1]) == h.key {
				cmd.SetErr(h.err)
				return h.err
			}
		}
		return next(ctx, cmd)
	}
}

func (h failLatestKeyRead) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// THE SAME REFUSAL COVERS the `latest` snapshot key.
//
// Third site of the class and the only one outside the `copySnapshot` predicate of the
// sweep: `saveSnapshotOnce` reads the key for the merge, and the read failure became
// `logger.Debug` with `toWrite` continuing to be the NEW snapshot alone — the following
// `SET` wrote the WHOLE key with the partial. A funding-only poll erased the
// `openInterestBase` five consumers read directly, and `SaveSnapshot` returned `nil`.
//
// The case asserts both halves of the declared refusal: the key preserves what it had (the
// partial did NOT enter) and the CYCLE CONTINUES — the series of the same poll is written
// with the full merge. The second half is what separates this refusal from an error
// refusal: returning an error, `retryOp` would repeat the cycle, the persistent failure
// would take down both attempts and the cycle's series fan-out would never happen.
func TestSaveSnapshot_PreMergeReadFailureSkipsTheLatestKeyInsteadOfErasingIt(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1h")

	latestKey := derivLatestScopedKey("binance", "future", "BTCUSDT")
	armed := &atomic.Bool{}
	readDown := errors.New("latest key read unavailable")
	client.AddHook(failLatestKeyRead{armed: armed, key: latestKey, err: readDown})

	// EXTERNAL LITERALS: 2026-08-20T13:00:00Z in ms since epoch; the two values come from
	// real BTCUSDT reads, as in the sibling cases.
	const observedMs int64 = 1_787_230_800_000
	const bucket1hMs int64 = 1_787_230_800_000
	const openInterestBase = 107025.033
	const fundingRate = 0.0000942

	if got := time.Date(2026, 8, 20, 13, 0, 0, 0, time.UTC).UnixMilli(); got != observedMs {
		t.Fatalf("the observation literal left the calendar: %d != %d", got, observedMs)
	}

	// Cycle 1: the live poll writes open interest to the key, healthy read.
	withOI := &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: observedMs, OpenInterestBase: fptr(openInterestBase),
	}
	if err := store.SaveSnapshot(ctx, withOI); err != nil {
		t.Fatalf("SaveSnapshot (cycle 1): %v", err)
	}

	// Cycle 2: funding-only poll with the key's pre-merge read down.
	fundingOnly := &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: observedMs, FundingRate: fptr(fundingRate),
	}
	armed.Store(true)
	log, sink := logger.NewCapture(logger.LevelDebug)
	store.log = log
	err := store.SaveSnapshot(ctx, fundingOnly)
	armed.Store(false)

	// The `SaveSnapshot` error contract does not change: the refusal is to skip this key's
	// write, not to fail the poll.
	if err != nil {
		t.Fatalf("SaveSnapshot (cycle 2) returned an error: %v — refusing this key is not a cycle failure", err)
	}

	// The WARN names the refused KEY, otherwise the operator sees "failed" without knowing
	// where.
	level, found := sink.LevelOf(latestKey)
	if !found {
		t.Fatalf("no log line names the key %s; got: %v", latestKey, sink.Records())
	}
	if level != logger.LevelWarn {
		t.Fatalf("the abort came out at %s, the criterion requires WARN — at process level (Error) a Debug goes nowhere", level)
	}

	// The key remains whole, and the partial did NOT enter.
	latest, err := store.GetLatestSnapshot(ctx, "binance", "future", "BTCUSDT")
	if err != nil {
		t.Fatalf("GetLatestSnapshot: %v", err)
	}
	if latest == nil {
		t.Fatal("the latest key disappeared — the refusal erased what it was supposed to preserve")
	}
	if latest.OpenInterestBase == nil || *latest.OpenInterestBase != openInterestBase {
		t.Fatalf("openInterestBase = %v, wanted %v — the funding-only write erased the latest key's OI",
			latest.OpenInterestBase, openInterestBase)
	}
	if latest.FundingRate != nil {
		t.Fatalf("fundingRate = %v — the aborted write wrote the partial anyway", *latest.FundingRate)
	}

	// THE CYCLE CONTINUES: the series fan-out of the SAME poll does not depend on this read
	// and was written with the full merge. Returning an error instead of skipping would kill
	// this step on both `retryOp` attempts.
	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1h", 10)
	if err != nil {
		t.Fatalf("GetSnapshotSeries: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("%d point(s) in bucket 1h, wanted 1 — refusing the latest key took down the series fan-out", len(series))
	}
	if series[0].Timestamp != bucket1hMs {
		t.Fatalf("bucket 1h = %d, wanted %d", series[0].Timestamp, bucket1hMs)
	}
	if series[0].OpenInterestBase == nil || *series[0].OpenInterestBase != openInterestBase {
		t.Fatalf("openInterestBase of the series = %v, wanted %v", series[0].OpenInterestBase, openInterestBase)
	}
	if series[0].FundingRate == nil || *series[0].FundingRate != fundingRate {
		t.Fatalf("fundingRate of the series = %v, wanted %v — the cycle stopped at the latest key refusal",
			series[0].FundingRate, fundingRate)
	}

	// Same class: the next cycle, with the read back, completes the merge without loss — the
	// key now carries BOTH fields.
	if err := store.SaveSnapshot(ctx, fundingOnly); err != nil {
		t.Fatalf("SaveSnapshot (cycle 3): %v", err)
	}
	latest, err = store.GetLatestSnapshot(ctx, "binance", "future", "BTCUSDT")
	if err != nil {
		t.Fatalf("GetLatestSnapshot (cycle 3): %v", err)
	}
	if latest.OpenInterestBase == nil || *latest.OpenInterestBase != openInterestBase {
		t.Fatalf("openInterestBase = %v after reprocessing, wanted %v", latest.OpenInterestBase, openInterestBase)
	}
	if latest.FundingRate == nil || *latest.FundingRate != fundingRate {
		t.Fatalf("fundingRate = %v after reprocessing, wanted %v", latest.FundingRate, fundingRate)
	}
}

// A FAILING PRE-MERGE READ ABORTS the bucket upsert.
//
// The path measured the opposite: `ZRangeByScore` failing became `logger.Debug` and
// execution continued with `merged = copySnapshot(snapshot)`, that is, the NEW snapshot
// alone. The `ZREM`+`ZADD` pipe just below writes that partial over the existing member, and
// every field the new snapshot does not carry disappears. A funding-only write erased the
// bucket's open interest, and nothing in the system could show it: the read answers, with
// one field less.
//
// The criterion is the inverse — abort, with a WARN-level log naming the bucket, without
// writing a partial snapshot — and the next cycle reprocesses supported by the DEDUP
// (`ZREM`+`ZADD` at the same score), which is what makes repetition cheap instead of risky.
func TestUpsertSeriesInto_PreMergeReadFailureAbortsInsteadOfErasingTheBucket(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1d")

	armed := &atomic.Bool{}
	readDown := errors.New("read unavailable")
	client.AddHook(failPreMergeRead{armed: armed, err: readDown})

	// EXTERNAL LITERALS: the bucket comes from the calendar (2026-08-20T00:00Z in ms since
	// epoch), not from `bucketAlignMs`; the two values come from real BTCUSDT reads.
	const bucketMs int64 = 1_787_184_000_000
	const openInterestBase = 107025.033
	const fundingRate = 0.0000942

	if got := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC).UnixMilli(); got != bucketMs {
		t.Fatalf("the bucket literal left the calendar: %d != %d", got, bucketMs)
	}

	// Cycle 1: the bucket receives open interest, with a healthy read.
	withOI := &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: bucketMs, OpenInterestBase: fptr(openInterestBase),
	}
	if err := store.SaveSeriesOnly(ctx, withOI, "1d"); err != nil {
		t.Fatalf("SaveSeriesOnly (cycle 1): %v", err)
	}

	// Cycle 2: funding-only write with the pre-merge read down.
	fundingOnly := &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: bucketMs, FundingRate: fptr(fundingRate),
	}
	armed.Store(true)
	log, sink := logger.NewCapture(logger.LevelDebug)
	store.log = log
	err := store.SaveSeriesOnly(ctx, fundingOnly, "1d")
	armed.Store(false)

	if err == nil {
		t.Fatal("the upsert returned success with the pre-merge read failing — the caller cannot reprocess what it does not know failed")
	}
	if !errors.Is(err, readDown) {
		t.Fatalf("the returned error does not wrap the read failure: %v", err)
	}

	// The WARN names the bucket: series key AND score, otherwise the operator sees "failed"
	// without knowing where.
	key := derivSeriesZsetKey("binance", "future", "BTCUSDT", "1d")
	level, found := sink.LevelOf(key)
	if !found {
		t.Fatalf("no log line names the bucket %s; got: %v", key, sink.Records())
	}
	if level != logger.LevelWarn {
		t.Fatalf("the abort came out at %s, the criterion requires WARN — at process level (Error) a Debug goes nowhere", level)
	}
	if _, namesScore := sink.LevelOf("1787184000000"); !namesScore {
		t.Fatalf("the log does not name the bucket score; got: %v", sink.Records())
	}

	// The member that existed remains whole, and the partial did NOT enter.
	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1d", 10)
	if err != nil {
		t.Fatalf("GetSnapshotSeries: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("%d point(s) in the bucket, wanted 1", len(series))
	}
	if series[0].OpenInterestBase == nil || *series[0].OpenInterestBase != openInterestBase {
		t.Fatalf("openInterestBase = %v, wanted %v — the funding-only write erased the bucket's OI",
			series[0].OpenInterestBase, openInterestBase)
	}
	if series[0].FundingRate != nil {
		t.Fatalf("fundingRate = %v — the aborted upsert wrote the partial anyway", *series[0].FundingRate)
	}

	// The next cycle, with the read back, completes the merge without loss.
	if err := store.SaveSeriesOnly(ctx, fundingOnly, "1d"); err != nil {
		t.Fatalf("SaveSeriesOnly (cycle 3): %v", err)
	}
	series, err = store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1d", 10)
	if err != nil {
		t.Fatalf("GetSnapshotSeries (cycle 3): %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("%d point(s) in the bucket after reprocessing, wanted 1 — the DEDUP did not coalesce", len(series))
	}
	if series[0].OpenInterestBase == nil || *series[0].OpenInterestBase != openInterestBase {
		t.Fatalf("openInterestBase = %v after reprocessing, wanted %v", series[0].OpenInterestBase, openInterestBase)
	}
	if series[0].FundingRate == nil || *series[0].FundingRate != fundingRate {
		t.Fatalf("fundingRate = %v after reprocessing, wanted %v", series[0].FundingRate, fundingRate)
	}
}
