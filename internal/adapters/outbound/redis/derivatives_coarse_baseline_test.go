package redisstore

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
)

func coarseStorePoints(tf string, count int) []*derivatives.DerivativesSnapshot {
	step := candle.ParseTimeframeToSeconds(tf) * 1000
	current := candle.BucketTimestamp(time.Now().UnixMilli(), tf)
	out := make([]*derivatives.DerivativesSnapshot, count)
	for i := 0; i < count; i++ {
		v := 1000 * math.Exp(float64(i)*0.001+math.Sin(float64(i))*0.01)
		ls := 1.2 * math.Exp(math.Cos(float64(i))*0.02)
		out[i] = &derivatives.DerivativesSnapshot{Timestamp: current - int64(i)*step, Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT", Source: derivatives.SourceCoinalyze, OpenInterestBase: &v, LongShortRatio: &ls}
	}
	return out
}

func TestCoarseBaselineUsesDurableWindowAndAtomicTail(t *testing.T) {
	for _, tf := range []string{"1d", "1w"} {
		t.Run(tf, func(t *testing.T) {
			store, client := newTestDerivStore(t, tf)
			ctx := context.Background()
			calls := 0
			points := coarseStorePoints(tf, 291)
			for i, p := range points[:3] {
				p.FundingRate = fptr(float64(i) * .0001)
				p.FundingRate8h = fptr(float64(i) * .0002)
				p.FundingIntervalHours = fptr(4)
				p.NStep = derivatives.NFromMeasuredSpacing
			}
			store.SetCoarseSeriesReader(func(_ context.Context, _, _, _, timeframe string, limit int) ([]*derivatives.DerivativesSnapshot, error) {
				calls++
				if limit != 291 || timeframe != tf {
					t.Fatalf("query limit=%d tf=%s", limit, timeframe)
				}
				return points, nil
			})
			if !store.HasCanonicalCoarse(tf) || store.HasCanonicalCoarse("4h") {
				t.Fatal("canonical scope changed")
			}
			step := candle.ParseTimeframeToSeconds(tf) * 1000
			closes := alternatingCloses(100, 291)
			seedCandles(t, client, "binance", "future", "BTCUSDT", tf, points[len(points)-1].Timestamp, step, closes)
			baseline, err := store.RecomputeBaseline(ctx, "binance", "future", "BTCUSDT", tf)
			if err != nil {
				t.Fatal(err)
			}
			if baseline.Price == nil || baseline.Price.N != 288 || baseline.PriceState != derivatives.BaselineMature {
				t.Fatalf("actual candle ruler missing: %+v", baseline)
			}
			if baseline.Source != derivatives.SourceCoinalyze || baseline.WindowN != 288 || baseline.NMin != 30 || baseline.OpenInterest.N != 288 || baseline.LongShortRatio.N != 288 || baseline.OpenInterestState != derivatives.BaselineMature || len(baseline.SeriesTail) != 3 {
				t.Fatalf("baseline=%+v", baseline)
			}
			if baseline.OpenInterest.SpanMs != 287*candle.ParseTimeframeToSeconds(tf)*1000 {
				t.Fatal("scored bar entered ruler")
			}
			persisted, err := store.GetBaseline(ctx, "binance", "future", "BTCUSDT", tf)
			if err != nil || persisted.Source != derivatives.SourceCoinalyze {
				t.Fatalf("persisted=%+v err=%v", persisted, err)
			}
			for _, limit := range []int{1, 2, 3} {
				got, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", tf, limit)
				if err != nil || calls != 1 || !reflect.DeepEqual(got, points[:limit]) {
					t.Fatalf("complete tail limit=%d got=%+v calls=%d err=%v", limit, got, calls, err)
				}
			}
			failure := errors.New("source unavailable")
			store.SetCoarseSeriesReader(func(context.Context, string, string, string, string, int) ([]*derivatives.DerivativesSnapshot, error) {
				return nil, failure
			})
			if _, err := store.RecomputeBaseline(ctx, "binance", "future", "BTCUSDT", tf); !errors.Is(err, failure) {
				t.Fatalf("source error lost: %v", err)
			}
			after, _ := store.GetBaseline(ctx, "binance", "future", "BTCUSDT", tf)
			if after.UpdatedAt != baseline.UpdatedAt || after.OpenInterest.Median != baseline.OpenInterest.Median {
				t.Fatal("failed read overwrote ruler")
			}
		})
	}
}

func TestCoarseBaselineRejectsOldNativeCache(t *testing.T) {
	store, client := newTestDerivStore(t, "1w")
	ctx := context.Background()
	client.Set(ctx, derivBaselineKey("binance", "future", "BTCUSDT", "1w"), `{"timeframe":"1w","updatedAt":100,"windowN":4}`, 0)
	store.SetCoarseSeriesReader(func(context.Context, string, string, string, string, int) ([]*derivatives.DerivativesSnapshot, error) {
		t.Fatal("hot read consulted cold")
		return nil, nil
	})
	b, err := store.GetBaseline(ctx, "binance", "future", "BTCUSDT", "1w")
	if err != nil || b != nil {
		t.Fatalf("native ruler leaked: %+v %v", b, err)
	}
	if _, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1w", 3); err == nil {
		t.Fatal("missing canonical ruler silently fell back")
	}
}

func TestCoarseBaselineExcludesByActualScoredBar(t *testing.T) {
	store, _ := newTestDerivStore(t, "1d")
	ctx := context.Background()
	points := coarseStorePoints("1d", 291)[1:] // no open point in cold source
	points[30].OpenInterestBase = nil
	store.SetCoarseSeriesReader(func(context.Context, string, string, string, string, int) ([]*derivatives.DerivativesSnapshot, error) {
		return points, nil
	})
	b, err := store.RecomputeBaseline(ctx, "binance", "future", "BTCUSDT", "1d")
	if err != nil {
		t.Fatal(err)
	}
	points[0].OpenInterestBase = fptr(1e20)
	after, err := store.RecomputeBaseline(ctx, "binance", "future", "BTCUSDT", "1d")
	if err != nil {
		t.Fatal(err)
	}
	if after.OpenInterest.Median != b.OpenInterest.Median || after.OpenInterest.MAD != b.OpenInterest.MAD {
		t.Fatal("scored observation changed ruler")
	}
	if b.OpenInterest.N != 286 || b.LongShortRatio.N != 288 || b.OpenInterest.SpanMs != 287*24*3600*1000 {
		t.Fatalf("wrong exclusion or bridged hole: %+v", b)
	}
}

func TestCoarseBaselineImmatureAndDegenerateStayUnscorable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		count    int
		constant bool
		state    derivatives.BaselineState
	}{{"young", 20, false, derivatives.BaselineImmature}, {"constant", 100, true, derivatives.BaselineDegenerate}} {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := newTestDerivStore(t, "1w")
			points := coarseStorePoints("1w", tc.count)
			if tc.constant {
				for _, p := range points {
					p.OpenInterestBase = fptr(100)
				}
			}
			store.SetCoarseSeriesReader(func(context.Context, string, string, string, string, int) ([]*derivatives.DerivativesSnapshot, error) {
				return points, nil
			})
			b, err := store.RecomputeBaseline(context.Background(), "binance", "future", "BTCUSDT", "1w")
			if err != nil {
				t.Fatal(err)
			}
			if b.NMin != 30 || b.OpenInterestState != tc.state {
				t.Fatalf("state=%s min=%d", b.OpenInterestState, b.NMin)
			}
			if _, state, ok := b.OpenInterest.Z(.1, b.NMin); ok || state != tc.state {
				t.Fatalf("unscorable ruler emitted: %s %t", state, ok)
			}
		})
	}
}
