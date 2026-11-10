package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/retention"

	"github.com/alicebob/miniredis/v2"
)

type failedSeedAppender struct{}

func (failedSeedAppender) EnqueueClosedBlocking(ctx context.Context, _, _, _, _ string, _ candle.Candle) error {
	return ctx.Err()
}

type retrySeedAppender struct {
	fail       bool
	timeframes []string
}

func (a *retrySeedAppender) EnqueueClosedBlocking(_ context.Context, _, _, _, timeframe string, _ candle.Candle) error {
	if a.fail {
		return errors.New("cold queue full")
	}
	a.timeframes = append(a.timeframes, timeframe)
	return nil
}

func TestFoldKnownParentDoesNotProveMissingChildPredatesListing(t *testing.T) {
	const ex, market, symbol = "binance", "future", "XPL/USDT"
	ce := NewCandleEngine(nil)
	parentTS := candle.BucketTimestamp(time.Now().Add(-3*time.Hour).UnixMilli(), "30m")
	parent := candle.Candle{Timestamp: parentTS, Open: 10, High: 12, Low: 9, Close: 11, Volume: 100, Source: string(redisstore.CandleMergeSourceRESTTotal)}
	ce.UpdateCandle(ex, market, symbol, "30m", parent)
	ce.UpdateCandle(ex, market, symbol, "15m", candle.Candle{Timestamp: parentTS + 15*60*1000, Open: 11, High: 12, Low: 10, Close: 11, Volume: 20, Source: string(redisstore.CandleMergeSourceWSKline)})
	ce.FoldUp(ex, market, symbol, "15m", parentTS+15*60*1000)
	got := ce.GetCandleByTimestamp(ex, market, symbol, "30m", parentTS)
	if got == nil || got.Volume != parent.Volume || got.Source != parent.Source {
		t.Fatalf("missing child overwrote an established parent: got=%+v", got)
	}
}

func TestSeedRepairsExistingAncestorsWithoutNewWS(t *testing.T) {
	const ex, market, symbol = "binance", "future", "XPL/USDT"
	ce := NewCandleEngine(nil)
	ce.RegisterDerivedScope(ex, market, "5m")
	base := candle.BucketTimestamp(time.Now().Add(-4*time.Hour).UnixMilli(), "1h")
	makeCandle := func(ts int64, volume, buy, sell float64, source string) candle.Candle {
		return candle.Candle{Timestamp: ts, Open: 10, High: 12, Low: 9, Close: 11, Volume: volume, TakerBuyVolume: buy, TakerSellVolume: sell, Source: source}
	}
	tail := makeCandle(base+45*60*1000, 8795166, 3795891, 4999275, string(redisstore.CandleMergeSourceWSKline))
	tail.Open, tail.High, tail.Low, tail.Close = .07667, .07683, .07560, .07575
	prior := makeCandle(base, 44508732, 21691804, 22816928, string(redisstore.CandleMergeSourceRESTTotal))
	prior.Open, prior.High, prior.Low, prior.Close = .07365, .07646, .07316, .07599
	old30 := makeCandle(base+30*60*1000, tail.Volume, tail.TakerBuyVolume, tail.TakerSellVolume, string(redisstore.CandleMergeSourceWSKline))
	old1h := old30
	old1h.Timestamp = base
	ce.LoadStoredCandles(ex, market, symbol, "30m", []candle.Candle{prior, old30})
	ce.LoadStoredCandles(ex, market, symbol, "1h", []candle.Candle{old1h})
	ce.UpdateCandle(ex, market, symbol, "15m", tail)
	ce.FoldUp(ex, market, symbol, "15m", tail.Timestamp)
	if got := ce.GetCandleByTimestamp(ex, market, symbol, "30m", old30.Timestamp); got.Volume != tail.Volume {
		t.Fatalf("missing child changed parent: %+v", got)
	}
	var revised []CompositeKey
	ce.OnCandleRevised(func(k CompositeKey, _ candle.Candle) { revised = append(revised, k) })
	early := makeCandle(base+30*60*1000, 14991394, 7708858, 7282536, string(redisstore.CandleMergeSourceRESTTotal))
	early.Open, early.High, early.Low, early.Close = .07598, .077, .07562, .07669
	early.FetchedAt = time.Now().UnixMilli()
	ce.SeedCandles(ex, market, symbol, "15m", []candle.Candle{early})
	got30 := ce.GetCandleByTimestamp(ex, market, symbol, "30m", old30.Timestamp)
	got1h := ce.GetCandleByTimestamp(ex, market, symbol, "1h", base)
	if got30.Volume != 23786560 || got1h.Volume != 68295292 || got1h.TakerBuyVolume != 33196553 || got1h.TakerSellVolume != 35098739 {
		t.Fatalf("ancestor totals: 30m=%+v 1h=%+v", got30, got1h)
	}
	if got30.Open != .07598 || got30.High != .077 || got30.Low != .07560 || got30.Close != .07575 || got30.TakerBuyVolume != 11504749 || got30.TakerSellVolume != 12281811 || got1h.Open != .07365 || got1h.High != .077 || got1h.Low != .07316 || got1h.Close != .07575 {
		t.Fatalf("XPL captured prices/taker: 30m=%+v 1h=%+v", got30, got1h)
	}
	if got30.Source != string(redisstore.CandleMergeSourceRESTTotal) || got1h.Source != string(redisstore.CandleMergeSourceRESTTotal) {
		t.Fatalf("REST-derived repair was mislabeled as independent live evidence: 30m=%s 1h=%s", got30.Source, got1h.Source)
	}
	if len(revised) != 2 || revised[0].Timeframe != "30m" || revised[1].Timeframe != "1h" {
		t.Fatalf("expected one revision per repaired parent: %v", revised)
	}
	ce.SeedCandles(ex, market, symbol, "15m", []candle.Candle{early})
	if len(revised) != 2 {
		t.Fatalf("unchanged seed emitted duplicate revisions: %v", revised)
	}
}

func TestFoldEqualLiveChildrenRefreshProvenanceWithoutRevision(t *testing.T) {
	const ex, market, symbol = "binance", "future", "XPL/USDT"
	ce := NewCandleEngine(nil)
	ts := candle.BucketTimestamp(time.Now().Add(-4*time.Hour).UnixMilli(), "30m")
	parent := candle.Candle{Timestamp: ts, Open: 1, High: 1, Low: 1, Close: 1, Volume: 20, TakerBuyVolume: 12, TakerSellVolume: 8, Source: string(redisstore.CandleMergeSourceRESTTotal)}
	ce.LoadStoredCandles(ex, market, symbol, "30m", []candle.Candle{parent})
	for i := range 2 {
		child := parent
		child.Timestamp += int64(i) * 15 * 60 * 1000
		child.Volume, child.TakerBuyVolume, child.TakerSellVolume = 10, 6, 4
		child.Source = string(redisstore.CandleMergeSourceWSKline)
		ce.UpdateCandle(ex, market, symbol, "15m", child)
	}
	revised := 0
	ce.OnCandleRevised(func(CompositeKey, candle.Candle) { revised++ })
	ce.FoldUp(ex, market, symbol, "15m", ts)
	got := ce.GetCandleByTimestamp(ex, market, symbol, "30m", ts)
	if got.Source != string(redisstore.CandleMergeSourceWSKline) || got.Origin != candle.OriginDerived || revised != 0 {
		t.Fatalf("equal live reconstruction: candle=%+v revisions=%d", got, revised)
	}
}

func TestFoldDoesNotPromoteRestoredUnknownChildrenToLiveEvidence(t *testing.T) {
	ce := NewCandleEngine(nil)
	ts := candle.BucketTimestamp(time.Now().Add(-4*time.Hour).UnixMilli(), "30m")
	children := make([]candle.Candle, 2)
	for i := range children {
		children[i] = candle.Candle{Timestamp: ts + int64(i)*15*60*1000, Open: 1, High: 1, Low: 1, Close: 1, Volume: 10, Source: string(redisstore.CandleMergeSourceWSKline)}
	}
	ce.LoadStoredCandles("binance", "future", "XPL/USDT", "15m", children)
	ce.FoldUp("binance", "future", "XPL/USDT", "15m", ts)
	got := ce.GetCandleByTimestamp("binance", "future", "XPL/USDT", "30m", ts)
	if got == nil || got.Source != "" || got.Origin != candle.OriginDerived {
		t.Fatalf("restored unknown child forged live evidence: %+v", got)
	}
}

func TestSeedPreservesNativeAndRepairsOnlyScopedLegacyCoarser(t *testing.T) {
	const ex, market, symbol = "binance", "future", "XPL/USDT"
	ce := NewCandleEngine(nil)
	ce.RegisterDerivedScope(ex, market, "5m")
	ts := candle.BucketTimestamp(time.Now().Add(-4*time.Hour).UnixMilli(), "1h")
	ws := candle.Candle{Timestamp: ts, Open: 1, High: 1, Low: 1, Close: 1, Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4, Source: string(redisstore.CandleMergeSourceWSKline)}
	rest := ws
	rest.Source, rest.FetchedAt, rest.Volume = string(redisstore.CandleMergeSourceRESTTotal), time.Now().UnixMilli(), 20
	rest.RequestedAt = rest.FetchedAt - 1
	rest.TakerBuyVolume, rest.TakerSellVolume = 12, 8
	ce.LoadStoredCandles(ex, market, symbol, "5m", []candle.Candle{ws})
	ce.LoadStoredCandles(ex, market, symbol, "15m", []candle.Candle{ws})
	ce.UpdateCandle(ex, market, symbol, "30m", ws)
	ce.SeedCandles(ex, market, symbol, "5m", []candle.Candle{rest})
	ce.SeedCandles(ex, market, symbol, "15m", []candle.Candle{rest})
	ce.SeedCandles(ex, market, symbol, "30m", []candle.Candle{rest})
	for tf, want := range map[string]float64{"5m": 10, "15m": 20, "30m": 10} {
		if got := ce.GetCandleByTimestamp(ex, market, symbol, tf, ts); got == nil || got.Volume != want {
			t.Fatalf("%s precedence: got=%+v want volume=%v", tf, got, want)
		}
	}
	second := ws
	second.Timestamp += 15 * 60 * 1000
	ce.UpdateCandle(ex, market, symbol, "15m", second)
	ce.FoldUp(ex, market, symbol, "15m", ts)
	if got := ce.GetCandleByTimestamp(ex, market, symbol, "30m", ts); got.Volume != 10 || got.Origin != candle.OriginNative {
		t.Fatalf("fold overwrote native parent: %+v", got)
	}
	// Origin is process-local and must not survive a serialize/deserialize boundary: a
	// restored candle without proof is Unknown. The wire struct is test-local; the adapters
	// that own the production contract pin the same rule in their own tests.
	restored := wireRoundTrip(t, []candle.Candle{*ce.GetCandleByTimestamp(ex, market, symbol, "30m", ts)})
	if restored[0].Origin != candle.OriginUnknown {
		t.Fatalf("native origin must not survive snapshot without proof: restored=%+v", restored[0])
	}
}

func TestSeedRebuildsBeforeCancelledColdAppend(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := config.CandleEngineConfig{BufferSizes: map[string]int{"default": 800}}
	redis, err := redisstore.NewRedisStore(t.Context(), config.Config{RedisAddr: mr.Addr(), RedisPoolSize: 1, RedisDialTimeoutS: 1, RedisReadTimeoutS: 1, RedisWriteTimeoutS: 1, RedisPoolTimeoutS: 1, CandleEngine: cfg}, 800, candle.NewTimeframeRegistry([]string{"15m", "30m"}))
	if err != nil {
		t.Fatal(err)
	}
	ce := NewCandleEngine(nil)
	ce.RegisterDerivedScope("binance", "future", "5m")
	ts := NewTieredStore(ce, redis, cfg)
	defer func() { _ = ts.Close() }()
	ts.SetColdAppender(failedSeedAppender{}, retention.Policy{})
	base := candle.BucketTimestamp(time.Now().Add(-4*time.Hour).UnixMilli(), "30m")
	old := candle.Candle{Timestamp: base, Open: 1, High: 1, Low: 1, Close: 1, Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4, Source: string(redisstore.CandleMergeSourceWSKline)}
	ce.LoadStoredCandles("binance", "future", "XPL/USDT", "30m", []candle.Candle{old})
	tail := old
	tail.Timestamp += 15 * 60 * 1000
	ce.UpdateCandle("binance", "future", "XPL/USDT", "15m", tail)
	var revisions int
	ce.OnCandleRevised(func(CompositeKey, candle.Candle) { revisions++ })
	early := old
	early.Source, early.FetchedAt = string(redisstore.CandleMergeSourceRESTTotal), time.Now().UnixMilli()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = ts.SeedCandlesBatch(ctx, "binance", "future", "XPL/USDT", "15m", []candle.Candle{early})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, redisstore.ErrSeedColdAdmissionUnavailable) {
		t.Fatalf("cold failure not returned: %v", err)
	}
	got := ce.GetCandleByTimestamp("binance", "future", "XPL/USDT", "30m", base)
	if got == nil || got.Volume != 20 || revisions != 1 {
		t.Fatalf("RAM repair/revision lost on cold failure: candle=%+v revisions=%d", got, revisions)
	}
}

func TestSeedRepairAdmissionRetriesWithoutDuplicateNotification(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := config.CandleEngineConfig{BufferSizes: map[string]int{"default": 8}}
	redis, err := redisstore.NewRedisStore(t.Context(), config.Config{RedisAddr: mr.Addr(), RedisPoolSize: 1, RedisDialTimeoutS: 1, RedisReadTimeoutS: 1, RedisWriteTimeoutS: 1, RedisPoolTimeoutS: 1, CandleEngine: cfg}, 8, candle.NewTimeframeRegistry([]string{"15m", "30m"}))
	if err != nil {
		t.Fatal(err)
	}
	ce := NewCandleEngine(nil)
	ce.RegisterDerivedScope("binance", "future", "5m")
	ts := NewTieredStore(ce, redis, cfg)
	defer func() { _ = ts.Close() }()
	cold := &retrySeedAppender{fail: true}
	ts.SetColdAppender(cold, retention.Policy{})
	base := candle.BucketTimestamp(time.Now().Add(-4*time.Hour).UnixMilli(), "30m")
	old := candle.Candle{Timestamp: base, Open: 1, High: 1, Low: 1, Close: 1, Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4, Source: string(redisstore.CandleMergeSourceWSKline)}
	ce.LoadStoredCandles("binance", "future", "XPL/USDT", "30m", []candle.Candle{old})
	tail := old
	tail.Timestamp += 15 * 60 * 1000
	ce.UpdateCandle("binance", "future", "XPL/USDT", "15m", tail)
	var notified int
	ce.OnSeedCandleRevised(func(CompositeKey, candle.Candle) { notified++ })
	early := old
	early.Source, early.FetchedAt = string(redisstore.CandleMergeSourceRESTTotal), time.Now().UnixMilli()
	batch := []candle.Candle{early}
	if err := ts.SeedCandlesBatch(context.Background(), "binance", "future", "XPL/USDT", "15m", batch); !errors.Is(err, redisstore.ErrSeedColdAdmissionUnavailable) {
		t.Fatalf("full cold queue must be classified: %v", err)
	}
	if hot, err := redis.GetCandlesDesc(context.Background(), "binance", "future", "XPL/USDT", "15m", 10); err != nil || len(hot) != 1 || hot[0].Volume != early.Volume {
		t.Fatalf("cold failure suppressed independent hot seed: hot=%v err=%v", hot, err)
	}
	if notified != 1 {
		t.Fatalf("first repair notifications=%d", notified)
	}
	cold.fail = false
	if err := ts.SeedCandlesBatch(context.Background(), "binance", "future", "XPL/USDT", "15m", batch); err != nil {
		t.Fatalf("identical retry did not admit retained parent: %v", err)
	}
	if notified != 1 || len(cold.timeframes) != 2 || cold.timeframes[0] != "30m" || cold.timeframes[1] != "15m" {
		t.Fatalf("unexpected retry: notifications=%d admitted=%v", notified, cold.timeframes)
	}
	if repairs, err := ce.pendingSeedRepairs("binance", "future", "XPL/USDT", "15m", batch); err != nil || len(repairs) != 0 {
		t.Fatalf("confirmed admission remained pending: %v %v", repairs, err)
	}
}

func TestLaterMatchingSettledRESTConfirmsGapAndRebuildsParent(t *testing.T) {
	const ex, market, symbol = "binance", "future", "XPL/USDT"
	ce := NewCandleEngine(nil)
	ce.RegisterDerivedScope(ex, market, "5m")
	base := candle.BucketTimestamp(time.Now().Add(-4*time.Hour).UnixMilli(), "30m")
	child := candle.Candle{Timestamp: base, Open: 1, High: 1, Low: 1, Close: 1, Volume: 20, TakerBuyVolume: 12, TakerSellVolume: 8, Source: string(redisstore.CandleMergeSourceRESTTotal), FetchedAt: time.Now().Add(-time.Minute).UnixMilli()}
	tail := child
	tail.Timestamp += 15 * 60 * 1000
	tail.Volume, tail.TakerBuyVolume, tail.TakerSellVolume = 10, 6, 4
	tail.Source = string(redisstore.CandleMergeSourceWSKline)
	parent := child
	parent.Volume, parent.TakerBuyVolume, parent.TakerSellVolume = 30, 18, 12
	ce.LoadStoredCandles(ex, market, symbol, "15m", []candle.Candle{child})
	ce.LoadStoredCandles(ex, market, symbol, "30m", []candle.Candle{parent})
	ce.UpdateCandle(ex, market, symbol, "15m", tail)
	var revised int
	ce.OnCandleRevised(func(CompositeKey, candle.Candle) { revised++ })
	var targeted []int64
	ce.OnSeedGap(func(key CompositeKey, ts int64) {
		if key != (CompositeKey{ex, market, symbol, "15m"}) {
			t.Errorf("targeted wrong identity: %+v", key)
		}
		targeted = append(targeted, ts)
	})
	first := child
	first.Volume, first.TakerBuyVolume, first.TakerSellVolume = 10, 6, 4
	first.FetchedAt = time.Now().Add(-time.Second).UnixMilli()
	ce.SeedCandles(ex, market, symbol, "15m", []candle.Candle{first})
	if got := ce.GetCandleByTimestamp(ex, market, symbol, "15m", base); !got.HasGap {
		t.Fatal("first downward correction lost its gap marker")
	}
	if len(targeted) != 1 || targeted[0] != base {
		t.Fatalf("first closed gap should request one targeted repair: %v", targeted)
	}
	if got := ce.GetCandleByTimestamp(ex, market, symbol, "30m", base); got.Volume != 30 || revised != 0 {
		t.Fatalf("unconfirmed child changed parent: %+v revisions=%d", got, revised)
	}
	ce.SeedCandles(ex, market, symbol, "15m", []candle.Candle{first})
	if got := ce.GetCandleByTimestamp(ex, market, symbol, "15m", base); !got.HasGap || revised != 0 {
		t.Fatalf("same response was treated as independent confirmation: child=%+v revisions=%d", got, revised)
	}
	overlap := first
	overlap.RequestedAt, overlap.FetchedAt = first.FetchedAt-1, first.FetchedAt+1
	ce.SeedCandles(ex, market, symbol, "15m", []candle.Candle{overlap})
	if got := ce.GetCandleByTimestamp(ex, market, symbol, "15m", base); !got.HasGap || len(targeted) != 1 {
		t.Fatalf("overlapping response cleared or rescheduled gap: child=%+v targets=%v", got, targeted)
	}
	confirmed := first
	confirmed.RequestedAt = overlap.FetchedAt + 1
	confirmed.FetchedAt = overlap.FetchedAt + 2
	ce.SeedCandles(ex, market, symbol, "15m", []candle.Candle{confirmed})
	if got := ce.GetCandleByTimestamp(ex, market, symbol, "15m", base); got.HasGap {
		t.Fatalf("later identical settled REST did not confirm child: %+v", got)
	}
	if got := ce.GetCandleByTimestamp(ex, market, symbol, "30m", base); got.Volume != 20 || revised != 1 {
		t.Fatalf("confirmed child did not revise ancestor: %+v revisions=%d", got, revised)
	}
	if len(targeted) != 1 {
		t.Fatalf("repeated or confirmed seed rescheduled the gap: %v", targeted)
	}
}

func TestFoldSkipsOnlyIntervalsProvenBeforeListing(t *testing.T) {
	const ex, market, symbol = "binance", "future", "NEW/USDT"
	ce := NewCandleEngine(nil)
	base := candle.BucketTimestamp(time.Now().Add(-4*time.Hour).UnixMilli(), "1h")
	parent := candle.Candle{Timestamp: base, Open: 1, High: 1, Low: 1, Close: 1, Volume: 99, Source: string(redisstore.CandleMergeSourceRESTTotal)}
	ce.LoadStoredCandles(ex, market, symbol, "1h", []candle.Candle{parent})
	ce.SetListingFloors(ex, market, []string{symbol}, map[string]int64{symbol: base + 35*60*1000})
	ce.FoldUp(ex, market, symbol, "30m", base+30*60*1000)
	if got := ce.GetCandleByTimestamp(ex, market, symbol, "1h", base); got.Volume != 99 {
		t.Fatalf("missing interval containing listing was skipped: %+v", got)
	}
	child := parent
	child.Timestamp, child.Volume, child.Source = base+30*60*1000, 10, string(redisstore.CandleMergeSourceWSKline)
	ce.UpdateCandle(ex, market, symbol, "30m", child)
	ce.FoldUp(ex, market, symbol, "30m", child.Timestamp)
	if got := ce.GetCandleByTimestamp(ex, market, symbol, "1h", base); got.Volume != 10 {
		t.Fatalf("proven prelisting interval blocked the fold: %+v", got)
	}
	ce.SetListingFloors(ex, market, []string{symbol}, map[string]int64{symbol: time.Now().Add(time.Hour).UnixMilli()})
	if len(ce.listingFloors) != 0 {
		t.Fatal("future listing time was retained as proof")
	}
}

func TestConfirmedChildClearsEqualParentGapAndPropagates(t *testing.T) {
	const ex, market, symbol = "binance", "future", "XPL/USDT"
	ce := NewCandleEngine(nil)
	ce.RegisterDerivedScope(ex, market, "5m")
	base := candle.BucketTimestamp(time.Now().Add(-4*time.Hour).UnixMilli(), "1h")
	first := candle.Candle{Timestamp: base + 30*60*1000, Open: 1, High: 1, Low: 1, Close: 1, Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4, Source: string(redisstore.CandleMergeSourceRESTTotal), FetchedAt: time.Now().Add(-time.Second).UnixMilli(), HasGap: true}
	tail := first
	tail.Timestamp += 15 * 60 * 1000
	tail.HasGap = false
	ce.LoadStoredCandles(ex, market, symbol, "15m", []candle.Candle{first, tail})
	prior := first
	prior.Timestamp, prior.HasGap = base, false
	parent := first
	parent.Volume, parent.TakerBuyVolume, parent.TakerSellVolume = 20, 12, 8
	ce.LoadStoredCandles(ex, market, symbol, "30m", []candle.Candle{prior, parent})
	hour := parent
	hour.Timestamp, hour.Volume, hour.TakerBuyVolume, hour.TakerSellVolume = base, 40, 24, 16
	ce.LoadStoredCandles(ex, market, symbol, "1h", []candle.Candle{hour})
	var revised []CompositeKey
	var closes int
	ce.OnCandleRevised(func(k CompositeKey, _ candle.Candle) { revised = append(revised, k) })
	ce.OnCandleClose(func(CompositeKey, candle.Candle) { closes++ })
	confirm := first
	confirm.HasGap = false
	confirm.RequestedAt, confirm.FetchedAt = first.FetchedAt+1, first.FetchedAt+2
	ce.SeedCandles(ex, market, symbol, "15m", []candle.Candle{confirm})
	if got := ce.GetCandleByTimestamp(ex, market, symbol, "30m", parent.Timestamp); got.HasGap {
		t.Fatalf("equal complete parent retained gap: %+v", got)
	}
	if got := ce.GetCandleByTimestamp(ex, market, symbol, "1h", base); got.Volume != 30 {
		t.Fatalf("gap blocked propagation to hour: %+v", got)
	}
	if len(revised) != 2 || revised[0].Timeframe != "30m" || revised[1].Timeframe != "1h" || closes != 0 {
		t.Fatalf("gap transition must emit one revision per changed parent, no close: revisions=%v closes=%d", revised, closes)
	}
	ce.SeedCandles(ex, market, symbol, "15m", []candle.Candle{confirm})
	if len(revised) != 2 || closes != 0 {
		t.Fatalf("duplicate confirmation emitted events: revisions=%v closes=%d", revised, closes)
	}
}

func TestEvictedUnadmittedSeedParentIsReported(t *testing.T) {
	ce := NewCandleEngine(map[string]int{"default": 1})
	base := candle.BucketTimestamp(time.Now().Add(-4*time.Hour).UnixMilli(), "30m")
	key := CompositeKey{"binance", "future", "XPL/USDT", "30m"}
	parent := candle.Candle{Timestamp: base, Open: 1, High: 1, Low: 1, Close: 1, Volume: 10}
	ce.LoadStoredCandles(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, []candle.Candle{parent})
	ce.mu.Lock()
	ce.markSeedRepairLocked(key, base)
	ce.mu.Unlock()
	newer := parent
	newer.Timestamp += 30 * 60 * 1000
	ce.UpdateCandle(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, newer)
	if _, err := ce.pendingSeedRepairs(key.Exchange, key.MarketType, key.Symbol, "15m", []candle.Candle{{Timestamp: base}}); err == nil {
		t.Fatal("evicted unadmitted repair was reported as success")
	}
	if pending, evicted := ce.SeedRepairStatus(); pending != 0 || evicted != 1 {
		t.Fatalf("eviction status: pending=%d evicted=%d", pending, evicted)
	}
}

func TestSeedRepairOutsideRetentionIsExcludedWithoutColdWrite(t *testing.T) {
	ce := NewCandleEngine(nil)
	base := candle.BucketTimestamp(time.Now().Add(-10*24*time.Hour).UnixMilli(), "30m")
	key := CompositeKey{"binance", "future", "XPL/USDT", "30m"}
	parent := candle.Candle{Timestamp: base, Open: 1, High: 1, Low: 1, Close: 1, Volume: 10}
	ce.LoadStoredCandles(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, []candle.Candle{parent})
	ce.mu.Lock()
	ce.markSeedRepairLocked(key, base)
	ce.mu.Unlock()
	cold := &retrySeedAppender{}
	ts := &TieredStore{engine: ce, coldAppender: cold, coldBoundary: retention.Policy{RetentionDays: 1}}
	if err := ts.admitSeedRepairs(context.Background(), key.Exchange, key.MarketType, key.Symbol, "15m", []candle.Candle{{Timestamp: base}}); err != nil {
		t.Fatal(err)
	}
	if len(cold.timeframes) != 0 {
		t.Fatalf("expired partition was rewritten: %v", cold.timeframes)
	}
	if pending, evicted := ce.SeedRepairStatus(); pending != 0 || evicted != 0 {
		t.Fatalf("retention exclusion left pending=%d evicted=%d", pending, evicted)
	}
}

func TestSeedRepairAdmissionDoesNotClearConcurrentChange(t *testing.T) {
	ce := NewCandleEngine(nil)
	base := candle.BucketTimestamp(time.Now().Add(-4*time.Hour).UnixMilli(), "30m")
	key := CompositeKey{"binance", "future", "XPL/USDT", "30m"}
	parent := candle.Candle{Timestamp: base, Open: 1, High: 1, Low: 1, Close: 1, Volume: 10}
	ce.LoadStoredCandles(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, []candle.Candle{parent})
	ce.mu.Lock()
	ce.markSeedRepairLocked(key, base)
	ce.mu.Unlock()
	batch := []candle.Candle{{Timestamp: base}}
	repairs, err := ce.pendingSeedRepairs(key.Exchange, key.MarketType, key.Symbol, "15m", batch)
	if err != nil || len(repairs) != 1 {
		t.Fatalf("repair not retained: %v err=%v", repairs, err)
	}
	newer := parent
	newer.Volume, newer.Source = 11, string(redisstore.CandleMergeSourceWSKline)
	ce.UpdateCandle(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, newer)
	if err := ce.finishSeedRepair(repairs[0]); err == nil {
		t.Fatal("admission of stale snapshot cleared a newer value")
	}
	if pending, _ := ce.SeedRepairStatus(); pending != 1 {
		t.Fatalf("changed repair no longer pending: %d", pending)
	}
}
