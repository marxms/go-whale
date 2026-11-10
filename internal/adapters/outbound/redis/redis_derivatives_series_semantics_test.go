package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"
)

// seriesMemberFields decodes the RAW member stored in a timeframe ZSET.
//
// The decoded struct would hide the question being asked: a field that is absent and
// a field that is present-and-null both arrive as a nil pointer, and 7.2 is about the
// key not being on the wire at all.
func seriesMemberFields(t *testing.T, client *redis.Client, exchange, marketType, symbol, timeframe string) map[string]any {
	t.Helper()

	key := derivSeriesZsetKey(exchange, marketType, symbol, timeframe)
	members, err := client.ZRevRange(context.Background(), key, 0, 0).Result()
	if err != nil {
		t.Fatalf("zrevrange %s: %v", key, err)
	}
	if len(members) == 0 {
		t.Fatalf("timeframe %s holds no member at all", timeframe)
	}

	// Decoded by the PRODUCTION decoder, not by a second copy of the format: the member is
	// positional, and reimplementing the parsing here would make these cases assert against an
	// encoding the writer does not emit. The map is rebuilt by `json.Marshal`, so "absent field"
	// still means exactly what it meant — the distinction every case below measures.
	ident := derivativesSeriesIdentity{Exchange: exchange, MarketType: marketType, Symbol: symbol}
	snap, err := decodeDerivativesSeriesMember(members[0], ident)
	if err != nil {
		t.Fatalf("decode %s member: %v", timeframe, err)
	}

	remarshalled, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal %s member: %v", timeframe, err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(remarshalled, &decoded); err != nil {
		t.Fatalf("unmarshal %s member: %v", timeframe, err)
	}
	return decoded
}

func latestFields(t *testing.T, client *redis.Client, exchange, marketType, symbol string) map[string]any {
	t.Helper()

	raw, err := client.Get(context.Background(), derivLatestScopedKey(exchange, marketType, symbol)).Bytes()
	if err != nil {
		t.Fatalf("get latest: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal latest: %v", err)
	}
	return decoded
}

// TestSaveSnapshot_SeriesCarriesNoPollToPollOIChange.
//
// The poller measures oiChangePct between two CONSECUTIVE POLLS. The fan-out then
// wrote that one number into every timeframe ZSET, so the 1d member and the 5m member
// of the same instant carried the identical figure under keys whose only meaning is
// "the reading for this window". The number is well-formed and wrong about its window,
// which is why no consumer ever complained.
func TestSaveSnapshot_SeriesCarriesNoPollToPollOIChange(t *testing.T) {
	ctx := context.Background()
	timeframes := []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"}
	store, client := newTestDerivStore(t, timeframes...)

	snap := &derivatives.DerivativesSnapshot{
		Exchange:            "binance",
		MarketType:          "future",
		Symbol:              "BTCUSDT",
		Timestamp:           1_700_000_000_000,
		OpenInterestBase:    fptr(84_231.5),
		OIChangePct:         fptr(0.37),
		OIChangePctWindowMs: i64ptr(9 * 60 * 1000),
		FundingRate:         fptr(0.0001),
	}
	if err := store.SaveSnapshot(ctx, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	for _, tf := range timeframes {
		member := seriesMemberFields(t, client, "binance", "future", "BTCUSDT", tf)

		if value, present := member["oiChangePct"]; present {
			t.Errorf("the %s series member carries oiChangePct = %v — a poll-to-poll delta "+
				"stamped into a %s bucket claims a window it was never measured over", tf, value, tf)
		}
		// The window is the same category error one field over. Stripping the percentage
		// and keeping its span would leave a member announcing "9 min" next to nothing —
		// a horizon describing a number that is no longer there.
		if value, present := member["oiChangePctWindowMs"]; present {
			t.Errorf("the %s series member carries oiChangePctWindowMs = %v — the span of a "+
				"poll-to-poll pair has no meaning inside a %s bucket, and the two fields "+
				"travel together or not at all", tf, value, tf)
		}
		// The member is not merely emptied: the readings that DO belong to a bucket
		// have to survive the strip, or 7.2 would be deleting the series instead of
		// deleting one mislabelled column from it.
		if _, present := member["openInterestBase"]; !present {
			t.Errorf("the %s member lost openInterestBase — the strip took the decision quantity with it", tf)
		}
		if _, present := member["fundingRate"]; !present {
			t.Errorf("the %s member lost fundingRate", tf)
		}
	}

	// The `latest` key KEEPS it. That record is a single instant with no window claim
	// attached, so "the change since the previous poll" is a true statement about it,
	// and the screener reads it from there. Stripping it everywhere would be a
	// different task with a different consumer to migrate.
	latest := latestFields(t, client, "binance", "future", "BTCUSDT")
	if got, present := latest["oiChangePct"]; !present || got != 0.37 {
		t.Errorf("latest oiChangePct = %v (present=%v), want 0.37 — 7.2 removes the field from "+
			"the SERIES, not from the snapshot", got, present)
	}
	// And the window survives WITH it. On the latest key the pair is a true statement:
	// "this much change, over this measured span, ending at this instant". Keeping the
	// percentage while dropping its window here would recreate the bare number that the
	// requirement "a percentage of open-interest change SHALL NOT be rendered without
	// the window it covers" forbids — one layer below the renderer, where it is harder
	// to see.
	if got, present := latest["oiChangePctWindowMs"]; !present || got != 540000.0 {
		t.Errorf("latest oiChangePctWindowMs = %v (present=%v), want 540000 — the span has to "+
			"reach the consumer that reads the percentage, or the percentage arrives bare",
			got, present)
	}
}

// TestUpsertSeries_ErasesAPollToPollOIChangeWrittenByAnEarlierBuild covers the state of a
// deployment whose buckets already hold the field. Stripping only new writes would leave the
// already-written values to age out with the cap, which on a 1d ZSET is the better part of a
// year of stale, mislabelled numbers.
func TestUpsertSeries_ErasesAPollToPollOIChangeWrittenByAnEarlierBuild(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1d")

	const bucketTs = 1_700_006_400_000 // already aligned to a daily bucket
	key := derivSeriesZsetKey("binance", "future", "BTCUSDT", "1d")

	// A member in the legacy JSON form, exactly as older writers left it.
	legacy := `{"exchange":"binance","marketType":"future","symbol":"BTCUSDT",` +
		fmt.Sprintf(`"timestamp":%d,"openInterestBase":80000,"oiChangePct":1.25}`, bucketTs)
	if err := client.ZAdd(ctx, key, redis.Z{Score: float64(bucketTs), Member: legacy}).Err(); err != nil {
		t.Fatalf("seed legacy member: %v", err)
	}

	// A backfill write that carries no oiChangePct of its own. Under a field-level
	// merge the previous value would be PRESERVED, which is precisely how a stale
	// mislabelled number outlives the code that produced it.
	fresh := &derivatives.DerivativesSnapshot{
		Exchange:         "binance",
		MarketType:       "future",
		Symbol:           "BTCUSDT",
		Timestamp:        bucketTs,
		OpenInterestBase: fptr(81_000),
	}
	if err := store.SaveSeriesOnly(ctx, fresh, "1d"); err != nil {
		t.Fatalf("SaveSeriesOnly: %v", err)
	}

	member := seriesMemberFields(t, client, "binance", "future", "BTCUSDT", "1d")
	if value, present := member["oiChangePct"]; present {
		t.Errorf("oiChangePct = %v survived the rewrite — the strip only applies to new fields, "+
			"so the legacy value keeps being republished", value)
	}
	if got := member["openInterestBase"]; got != 81_000.0 {
		t.Errorf("openInterestBase = %v, want 81000 — the rewrite did not land", got)
	}
}

// TestMerge_AFailedOIFetchDoesNotShortenTheNextWindow is the regression an adversarial
// review found in the window field ON THE DAY IT SHIPPED, and it is worth the length
// because the defect is invisible from either half alone.
//
// THE SEQUENCE. Poll N succeeds at t0. Poll N+1 at t1 has a transient
// `FetchOpenInterest` failure — routine across a fan-out of five hundred symbols — so
// it carries funding but no open interest. Poll N+2 succeeds at t2.
//
// Both halves of the mechanism are correct on their own and were separately tested:
// `mergeDerivativesSnapshots` advances `Timestamp` to the newer record (a late field
// must not backdate the record it lands in) and preserves open-interest fields the
// incoming poll did not carry (a funding failure must not take open interest down with
// it). Composed, they store a reading from t0 under a timestamp of t1 — and a window
// computed from timestamps then reports `t2 − t1` for a measurement that actually
// spans `t2 − t0`.
//
// The number the trader sees is a percentage over a span SHORTER than the one it
// covers, which makes the move look faster than it was. That is the same category of
// claim — a horizon nobody measured — that the window field was introduced to abolish.
//
// What this test pins is the pairing: the stamp survives exactly as long as the reading
// it belongs to, and never a moment longer.
func TestMerge_AFailedOIFetchDoesNotShortenTheNextWindow(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "5m")

	const t0 = int64(1_700_000_000_000)
	const t1 = t0 + 300_000 // 5 min later — the poll whose OI fetch fails
	const t2 = t1 + 300_000 // 5 min after that — recovery

	// Poll N: a complete reading.
	if err := store.SaveSnapshot(ctx, &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp:                t0,
		OpenInterestBase:         fptr(80_000),
		OpenInterestObservedAtMs: i64ptr(t0),
		FundingRate:              fptr(0.0001),
	}); err != nil {
		t.Fatalf("poll N: %v", err)
	}

	// Poll N+1: funding only. This is the shape a failed OI fetch produces — the error
	// is logged and the cycle continues, so the snapshot is saved without the pair.
	if err := store.SaveSnapshot(ctx, &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp:   t1,
		FundingRate: fptr(0.00012),
	}); err != nil {
		t.Fatalf("poll N+1: %v", err)
	}

	stored, err := store.GetLatestSnapshot(ctx, "binance", "future", "BTCUSDT")
	if err != nil {
		t.Fatalf("GetLatestSnapshot: %v", err)
	}

	// The preserved reading is still the one from t0 — that half was always right.
	if stored.OpenInterestBase == nil || *stored.OpenInterestBase != 80_000 {
		t.Fatalf("openInterestBase = %v, want the preserved 80000", stored.OpenInterestBase)
	}
	// And the clock did advance — also right on its own.
	if stored.Timestamp != t1 {
		t.Fatalf("timestamp = %d, want %d", stored.Timestamp, t1)
	}

	// THE ASSERTION THE DEFECT LIVED UNDER. The stamp must still say t0, because that
	// is when the reading standing in this record was taken. If it followed the clock
	// to t1, the next poll would measure 300s over a 600s span.
	if stored.OpenInterestObservedAtMs == nil || *stored.OpenInterestObservedAtMs != t0 {
		t.Fatalf("openInterestObservedAtMs = %v, want %d — the stamp followed the CLOCK "+
			"instead of the READING, so the next window will be half its true span",
			stored.OpenInterestObservedAtMs, t0)
	}

	// Poll N+2 recovers. The window it publishes must cover BOTH intervals.
	window := t2 - *stored.OpenInterestObservedAtMs
	if window != 600_000 {
		t.Errorf("window = %dms, want 600000 — the measurement spans two poll intervals "+
			"because the middle one contributed no reading", window)
	}
	if window == 300_000 {
		t.Errorf("window came out as ONE interval: this is the defect exactly, and the " +
			"panel would print a percentage over half the time it really covers")
	}
}

// TestSaveSeriesOnly_TheObservationStampLeavesTheSeriesWithItsPair keeps the stamp from
// leaking into timeframe members, where a second and finer instant next to the bucket's
// own timestamp would blur "when the bar is" with "when the reading was".
func TestSaveSeriesOnly_TheObservationStampLeavesTheSeriesWithItsPair(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "5m")

	if err := store.SaveSnapshot(ctx, &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp:                1_700_000_137_000, // mid-bucket, as a live poll lands
		OpenInterestBase:         fptr(84_231.5),
		OpenInterestObservedAtMs: i64ptr(1_700_000_137_000),
		OIChangePct:              fptr(0.37),
		OIChangePctWindowMs:      i64ptr(300_000),
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	member := seriesMemberFields(t, client, "binance", "future", "BTCUSDT", "5m")
	if value, present := member["openInterestObservedAtMs"]; present {
		t.Errorf("the 5m member carries openInterestObservedAtMs = %v — it belongs to the "+
			"poll pair on the latest record, and the member already has the bucket's own "+
			"timestamp", value)
	}
	// The reading itself must survive: this strip removes a companion field, not data.
	if _, present := member["openInterestBase"]; !present {
		t.Errorf("the strip took openInterestBase with it")
	}

	latest := latestFields(t, client, "binance", "future", "BTCUSDT")
	if got, present := latest["openInterestObservedAtMs"]; !present || got != 1.700000137e+12 {
		t.Errorf("latest openInterestObservedAtMs = %v (present=%v) — it has to survive "+
			"HERE, because the next poll reads it to measure its window", got, present)
	}
}

// TestSaveSeriesOnly_LastWriterWinsPerFieldRegardlessOfOrigin: what a
// re-seed does to a bucket that already holds data, decided and asserted rather than
// left to be inferred from the pipeline.
//
// The rule has two halves: the incoming write REPLACES the fields it carries and
// PRESERVES the ones it does not. What decides the first half is ARRIVAL ORDER, not
// where the write came from — `mergeDerivativesSnapshots` is the same code path for
// the bootstrap and for the live poll, and `derivatives.DerivativesSnapshot` carries no
// origin marker for it to read.
//
// The rule grants no authority to provenance: "the venue's own aggregate outranks a mid-bucket
// sample" is a claim the system has nothing to support. A single ordering is compatible with
// BOTH explanations, which is why the mirror below also runs: reverse the arrival order and the
// LIVE sample wins. Two cases that disagree on the winner and agree on the rule are what make
// the test discriminate between "arrival order" and "provenance" instead of merely illustrating
// one of them.
func TestSaveSeriesOnly_LastWriterWinsPerFieldRegardlessOfOrigin(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "1h")

	const bucketTs = 1_700_002_800_000

	live := &derivatives.DerivativesSnapshot{
		Exchange:         "binance",
		MarketType:       "future",
		Symbol:           "BTCUSDT",
		Timestamp:        bucketTs + 137_000, // mid-bucket, as a live poll lands
		OpenInterestBase: fptr(80_000),
		FundingRate:      fptr(0.0002),
		LongShortRatio:   fptr(1.4),
	}
	if err := store.SaveSeriesOnly(ctx, live, "1h"); err != nil {
		t.Fatalf("seed live sample: %v", err)
	}

	// An OI-only backfill page for the same bucket: it disagrees about open interest
	// and says nothing about funding or long/short.
	backfill := &derivatives.DerivativesSnapshot{
		Exchange:         "binance",
		MarketType:       "future",
		Symbol:           "BTCUSDT",
		Timestamp:        bucketTs,
		OpenInterestBase: fptr(81_500),
	}
	if err := store.SaveSeriesOnly(ctx, backfill, "1h"); err != nil {
		t.Fatalf("backfill: %v", err)
	}

	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1h", 10)
	if err != nil {
		t.Fatalf("GetSnapshotSeries: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("series holds %d members, want 1 — the backfill created a second member "+
			"instead of landing in the bucket it belongs to", len(series))
	}
	member := series[0]

	if member.OpenInterestBase == nil || *member.OpenInterestBase != 81_500 {
		t.Errorf("openInterestBase = %v, want 81500 — the backfill was the LAST write, and where "+
			"both carry the field the last writer wins. Not because it is a backfill: see the "+
			"mirror below, where the live poll arrives last and wins the same way",
			member.OpenInterestBase)
	}
	if member.FundingRate == nil || *member.FundingRate != 0.0002 {
		t.Errorf("fundingRate = %v, want 0.0002 preserved — an OI-only page must not erase a "+
			"field it says nothing about", member.FundingRate)
	}
	if member.LongShortRatio == nil || *member.LongShortRatio != 1.4 {
		t.Errorf("longShortRatio = %v, want 1.4 preserved", member.LongShortRatio)
	}
	if member.Timestamp != bucketTs {
		t.Errorf("timestamp = %d, want the bucket %d", member.Timestamp, bucketTs)
	}
}

// TestSaveSeriesOnly_TheLiveSampleWinsWhenItArrivesLast is the mirror of the test above,
// and the pair is what carries the invariant.
//
// Same two writes, same bucket, ORDER REVERSED: the backfill lands first and the live poll
// lands second. If the rule were "the venue's aggregate outranks a mid-bucket sample", the
// backfill's 81 500 would survive here too. It does not: the live 80 000 wins, because it
// arrived last.
//
// One ordering alone cannot tell the two explanations apart. Two orderings that
// disagree on the WINNER while agreeing on the RULE can, and that is the only reason
// this second case exists.
func TestSaveSeriesOnly_TheLiveSampleWinsWhenItArrivesLast(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "1h")

	const bucketTs = 1_700_002_800_000

	backfill := &derivatives.DerivativesSnapshot{
		Exchange:         "binance",
		MarketType:       "future",
		Symbol:           "BTCUSDT",
		Timestamp:        bucketTs,
		OpenInterestBase: fptr(81_500),
	}
	if err := store.SaveSeriesOnly(ctx, backfill, "1h"); err != nil {
		t.Fatalf("backfill: %v", err)
	}

	live := &derivatives.DerivativesSnapshot{
		Exchange:         "binance",
		MarketType:       "future",
		Symbol:           "BTCUSDT",
		Timestamp:        bucketTs + 137_000, // mid-bucket, as a live poll lands
		OpenInterestBase: fptr(80_000),
		FundingRate:      fptr(0.0002),
	}
	if err := store.SaveSeriesOnly(ctx, live, "1h"); err != nil {
		t.Fatalf("live sample: %v", err)
	}

	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1h", 10)
	if err != nil {
		t.Fatalf("GetSnapshotSeries: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("series holds %d members, want 1 — both writes belong to the same bucket", len(series))
	}
	member := series[0]

	if member.OpenInterestBase == nil || *member.OpenInterestBase != 80_000 {
		t.Errorf("openInterestBase = %v, want 80000 — the LIVE sample arrived last and must win. "+
			"Seeing 81500 here would mean the backfill is being treated as authoritative, which "+
			"is the invariant the system refuses to build and the spec forbids declaring",
			member.OpenInterestBase)
	}
	if member.FundingRate == nil || *member.FundingRate != 0.0002 {
		t.Errorf("fundingRate = %v, want 0.0002 — carried only by the live write", member.FundingRate)
	}
	if member.Timestamp != bucketTs {
		t.Errorf("timestamp = %d, want the bucket %d", member.Timestamp, bucketTs)
	}
}

// TestSeriesCapFor_PerTimeframeOverride: the cap is configurable PER TIMEFRAME, in both
// directions, with only the baseline floor surviving the override.
func TestSeriesCapFor_PerTimeframeOverride(t *testing.T) {
	tests := []struct {
		name      string
		cfg       config.DerivativesConfig
		timeframe string
		wantCap   int64
	}{
		{
			name:      "with no override the global value still governs",
			cfg:       config.DerivativesConfig{MaxSeriesSize: 800},
			timeframe: "1d",
			wantCap:   800,
		},
		{
			// The case the single knob could not express. `1d` members are 288x the
			// wall time of `5m` members, so an operator wanting a shallow daily series
			// under a deep intraday one had no way to say it: the global value was a
			// FLOOR under every timeframe at once.
			name: "a per-timeframe value BELOW the global one is honored",
			cfg: config.DerivativesConfig{
				MaxSeriesSize:        800,
				SeriesCapByTimeframe: map[string]int{"1d": 120},
			},
			timeframe: "1d",
			wantCap:   120,
		},
		{
			name: "a per-timeframe value above the global one is honored",
			cfg: config.DerivativesConfig{
				MaxSeriesSize:        800,
				SeriesCapByTimeframe: map[string]int{"5m": 4000},
			},
			timeframe: "5m",
			wantCap:   4000,
		},
		{
			name: "a timeframe nobody named keeps the global value",
			cfg: config.DerivativesConfig{
				MaxSeriesSize:        800,
				SeriesCapByTimeframe: map[string]int{"1d": 120},
			},
			timeframe: "4h",
			wantCap:   800,
		},
		{
			// The one guarantee an operator cannot configure away. Below its own
			// baseline window a timeframe could never leave the immature state,
			// whatever the poller collected — the ruler would be starved by config.
			name: "an override below the baseline window is raised to it",
			cfg: config.DerivativesConfig{
				MaxSeriesSize:        800,
				SeriesCapByTimeframe: map[string]int{"5m": 10},
			},
			timeframe: "5m",
			wantCap:   291, // baselineFetchCount(288)
		},
		{
			name: "an override below the baseline window of the daily series is raised to it",
			cfg: config.DerivativesConfig{
				MaxSeriesSize:        800,
				SeriesCapByTimeframe: map[string]int{"1d": 4},
			},
			timeframe: "1d",
			wantCap:   33, // baselineFetchCount(30)
		},
		{
			name: "the key is matched on case and padding, not byte for byte",
			cfg: config.DerivativesConfig{
				MaxSeriesSize:        800,
				SeriesCapByTimeframe: map[string]int{" 1D ": 120},
			},
			timeframe: "1d",
			wantCap:   120,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: m.Addr()})
			t.Cleanup(func() { _ = client.Close() })

			store := NewDerivativesRedisStore(client, tt.cfg, candle.NewTimeframeRegistry([]string{tt.timeframe}))
			if got := store.seriesCapFor(tt.timeframe); got != tt.wantCap {
				t.Fatalf("seriesCapFor(%s) = %d, want %d", tt.timeframe, got, tt.wantCap)
			}

			// The horizon is the number an operator actually reasons about, and it is
			// derived from the cap — so it has to move with the override too.
			wantHorizon := tt.wantCap * candle.ParseTimeframeToSeconds(tt.timeframe) * 1000
			if got := store.SeriesHorizonMs(tt.timeframe); got != wantHorizon {
				t.Errorf("SeriesHorizonMs(%s) = %d, want %d — the declared horizon did not follow the cap",
					tt.timeframe, got, wantHorizon)
			}
		})
	}
}

// TestUpsertSeries_PerTimeframeCapIsEnforcedOnTheZSet closes the loop: the configured
// number has to reach the ZREMRANGEBYRANK, not merely be resolvable.
//
// It asserts the shape the single knob made impossible — a DEEPER intraday series next
// to a SHALLOWER daily one, in the same store.
func TestUpsertSeries_PerTimeframeCapIsEnforcedOnTheZSet(t *testing.T) {
	ctx := context.Background()

	m := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: m.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cfg := config.DerivativesConfig{
		MaxSeriesSize: 200,
		// 5m keeps its baseline floor of 291; 1d is deliberately set well below it and
		// lands on its own floor of 33. Both differ from the global 200 and from each
		// other, which no single value could produce.
		SeriesCapByTimeframe: map[string]int{"5m": 340, "1d": 5},
	}
	store := NewDerivativesRedisStore(client, cfg, candle.NewTimeframeRegistry([]string{"5m", "1d"}))

	for _, tc := range []struct {
		timeframe string
		periodMs  int64
		wantCard  int64
	}{
		{"5m", 5 * 60 * 1000, 340},
		{"1d", 24 * 60 * 60 * 1000, 33},
	} {
		start := bucketAlignMs(1_700_000_000_000, tc.timeframe)
		// Comfortably more points than the cap, so the trim is what decides the count.
		for i := int64(0); i < tc.wantCard+40; i++ {
			oi := 1000 + float64(i)
			snap := &derivatives.DerivativesSnapshot{
				Exchange:         "binance",
				MarketType:       "future",
				Symbol:           "BTCUSDT",
				Timestamp:        start + i*tc.periodMs,
				OpenInterestBase: &oi,
			}
			if err := store.SaveSeriesOnly(ctx, snap, tc.timeframe); err != nil {
				t.Fatalf("SaveSeriesOnly %s[%d]: %v", tc.timeframe, i, err)
			}
		}

		card, err := client.ZCard(ctx, derivSeriesZsetKey("binance", "future", "BTCUSDT", tc.timeframe)).Result()
		if err != nil {
			t.Fatalf("zcard %s: %v", tc.timeframe, err)
		}
		if card != tc.wantCard {
			t.Errorf("%s holds %d members, want %d — the per-timeframe cap did not reach the trim",
				tc.timeframe, card, tc.wantCard)
		}
	}
}
