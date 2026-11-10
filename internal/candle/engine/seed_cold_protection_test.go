package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/retention"
)

type seedProtectionCold struct {
	*scoreRangeColdStore
	err      error
	reads    int
	limit    int
	deadline time.Time
}

func (c *seedProtectionCold) QueryRange(ctx context.Context, ex, market, symbol, tf string, from, to time.Time, limit int) ([]candle.Candle, error) {
	c.reads++
	c.limit = limit
	c.deadline, _ = ctx.Deadline()
	if c.err != nil {
		return nil, c.err
	}
	return c.scoreRangeColdStore.QueryRange(ctx, ex, market, symbol, tf, from, to, limit)
}

func TestProtectSeedTaker_PreservesReadBudgetAndCallerDeadline(t *testing.T) {
	for _, callerBudget := range []time.Duration{time.Minute, 5 * time.Second} {
		rig := newScoreRangeRig(t.Context(), t, 20)
		cold := &seedProtectionCold{scoreRangeColdStore: &scoreRangeColdStore{}}
		rig.tiered.SetColdStore(cold)
		rig.tiered.SetColdAppender(&fakeColdAppender{}, retention.Policy{})
		ctx, cancel := context.WithTimeout(context.Background(), callerBudget)
		before := time.Now()
		raw := srSeries(before.UnixMilli()-3_000_000, 1, 300_000)
		_, err := rig.tiered.protectSeedTaker(ctx, srExchange, srMarketType, srSymbol, srTimeframe, raw)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		want := min(callerBudget, 30*time.Second)
		if cold.deadline.IsZero() || cold.deadline.Before(before.Add(want-time.Second)) || cold.deadline.After(before.Add(want+time.Second)) {
			t.Fatalf("caller budget %v: unexpected deadline %v", callerBudget, cold.deadline)
		}
	}
}

func TestPersistSeedBatch_ColdTakerSurvivesOutsideRingAndSparseBatch(t *testing.T) {
	ctx := context.Background()
	step := int64(300_000)
	base := time.Now().UnixMilli()/step*step - 30*step
	for _, cap := range []int{2, 20} {
		t.Run(fmt.Sprintf("cap-%d", cap), func(t *testing.T) {
			rig := newScoreRangeRig(ctx, t, cap)
			stored := srSeries(base+9*step, 10, step)
			for i := range stored {
				stored[i].Volume = 10
				stored[i].TakerBuyVolume = float64(i + 1)
				stored[i].TakerSellVolume = 10 - float64(i+1)
			}
			cold := &seedProtectionCold{scoreRangeColdStore: &scoreRangeColdStore{rows: stored}}
			appender := &fakeColdAppender{}
			rig.tiered.SetColdStore(cold)
			rig.tiered.SetColdAppender(appender, retention.Policy{})
			// Only the endpoints are requested; DESC LIMIT 2 would lose the old endpoint.
			raw := []candle.Candle{stored[0], stored[9]}
			for i := range raw {
				raw[i].TakerBuyVolume = 0
				raw[i].TakerSellVolume = 0
				raw[i].Source = "rest_total"
			}
			if err := rig.redis.SeedCandlesBatch(ctx, srExchange, srMarketType, srSymbol, srTimeframe, []candle.Candle{stored[0], stored[9]}); err != nil {
				t.Fatal(err)
			}
			// Occupy the small ring with newer records, leaving both endpoints outside it.
			rig.tiered.engine.SeedCandles(srExchange, srMarketType, srSymbol, srTimeframe, srSeries(base+20*step, 2, step))
			merged := rig.tiered.FoldSeedBatch(srExchange, srMarketType, srSymbol, srTimeframe, raw)
			before := append([]candle.Candle(nil), merged...)
			if err := rig.tiered.PersistSeedBatch(ctx, srExchange, srMarketType, srSymbol, srTimeframe, merged, raw); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, merged) {
				t.Fatal("persist mutated its caller's merged slice")
			}
			if len(appender.got) != 2 {
				t.Fatalf("unexpected cold rows: %+v", appender.got)
			}
			for i, want := range []candle.Candle{stored[0], stored[9]} {
				got := appender.got[i]
				if got.Timestamp != want.Timestamp || got.Open != want.Open || got.High != want.High || got.Low != want.Low || got.Close != want.Close || got.Volume != want.Volume || got.TakerBuyVolume != want.TakerBuyVolume || got.TakerSellVolume != want.TakerSellVolume {
					t.Fatalf("cold measurement erased: got %+v want %+v", got, want)
				}
				hot, err := rig.redis.GetCandleByTimestamp(ctx, srExchange, srMarketType, srSymbol, srTimeframe, want.Timestamp)
				if err != nil || hot == nil || hot.TakerBuyVolume != want.TakerBuyVolume || hot.TakerSellVolume != want.TakerSellVolume {
					t.Fatalf("hot taker erased: %+v %v", hot, err)
				}
				resident := rig.tiered.engine.GetCandleByTimestamp(srExchange, srMarketType, srSymbol, srTimeframe, want.Timestamp)
				if cap == 20 && (resident == nil || resident.TakerBuyVolume != want.TakerBuyVolume || resident.TakerSellVolume != want.TakerSellVolume) {
					t.Fatalf("resident taker erased: %+v", resident)
				}
			}
			if cold.reads != 1 || cold.limit < 10 {
				t.Fatalf("cold scope reads=%d limit=%d, want one full interval", cold.reads, cold.limit)
			}
		})
	}
}

func TestPersistSeedBatch_ProtectiveReadFailureWritesNeitherTier(t *testing.T) {
	rig := newScoreRangeRig(t.Context(), t, 20)
	broken := errors.New("cold unavailable")
	cold := &seedProtectionCold{scoreRangeColdStore: &scoreRangeColdStore{}, err: broken}
	appender := &fakeColdAppender{}
	rig.tiered.SetColdStore(cold)
	rig.tiered.SetColdAppender(appender, retention.Policy{})
	raw := srSeries(time.Now().UnixMilli()-3_000_000, 1, 300_000)
	err := rig.tiered.SeedCandlesBatch(context.Background(), srExchange, srMarketType, srSymbol, srTimeframe, raw)
	if !errors.Is(err, broken) {
		t.Fatalf("want retryable cold failure, got %v", err)
	}
	if len(appender.got) != 0 {
		t.Fatal("cold received degraded replacement after failed read")
	}
	hot, err := rig.redis.GetCandlesDesc(context.Background(), srExchange, srMarketType, srSymbol, srTimeframe, 20)
	if err != nil || len(hot) != 0 {
		t.Fatalf("Redis was written: %+v %v", hot, err)
	}
}

func TestPersistSeedBatch_ConditionalReadsAndConfirmedEmpty(t *testing.T) {
	for _, name := range []string{"measured", "one-sided", "resident", "zero-volume", "empty-confirmed"} {
		t.Run(name, func(t *testing.T) {
			rig := newScoreRangeRig(t.Context(), t, 20)
			cold := &seedProtectionCold{scoreRangeColdStore: &scoreRangeColdStore{}}
			appender := &fakeColdAppender{}
			rig.tiered.SetColdStore(cold)
			rig.tiered.SetColdAppender(appender, retention.Policy{})
			raw := srSeries(time.Now().UnixMilli()-3_000_000, 1, 300_000)
			switch name {
			case "measured":
				raw[0].TakerBuyVolume = .4
				raw[0].TakerSellVolume = .6
			case "one-sided":
				raw[0].TakerBuyVolume = 1
			case "resident":
				stored := raw[0]
				stored.TakerBuyVolume = .4
				stored.TakerSellVolume = .6
				rig.tiered.engine.SeedCandles(srExchange, srMarketType, srSymbol, srTimeframe, []candle.Candle{stored})
			case "zero-volume":
				raw[0].Volume = 0
			}
			wantRaw := raw[0]
			wantMerged := wantRaw
			if name == "resident" {
				wantMerged.TakerBuyVolume = .4
				wantMerged.TakerSellVolume = .6
			}
			if err := rig.tiered.SeedCandlesBatch(context.Background(), srExchange, srMarketType, srSymbol, srTimeframe, raw); err != nil {
				t.Fatal(err)
			}
			wantReads := 0
			if name == "empty-confirmed" {
				wantReads = 1
			}
			if cold.reads != wantReads || len(appender.got) != 1 {
				t.Fatalf("reads=%d writes=%d want %d/1", cold.reads, len(appender.got), wantReads)
			}
			if !reflect.DeepEqual(raw[0], wantRaw) {
				t.Fatal("persist mutated raw input")
			}
			assertSeedFinancialPayload(t, "cold", &appender.got[0], wantMerged)
			hot, err := rig.redis.GetCandleByTimestamp(context.Background(), srExchange, srMarketType, srSymbol, srTimeframe, raw[0].Timestamp)
			if err != nil {
				t.Fatal(err)
			}
			// Redis receives raw input; resident measurements stay authoritative only
			// in the ring/cold merge, never acquire an exchange provenance in Redis.
			assertSeedFinancialPayload(t, "Redis", hot, wantRaw)
			resident := rig.tiered.engine.GetCandleByTimestamp(srExchange, srMarketType, srSymbol, srTimeframe, raw[0].Timestamp)
			assertSeedFinancialPayload(t, "ring", resident, wantMerged)
		})
	}
}

func assertSeedFinancialPayload(t *testing.T, tier string, got *candle.Candle, want candle.Candle) {
	t.Helper()
	if got == nil || got.Timestamp != want.Timestamp || got.Open != want.Open || got.High != want.High || got.Low != want.Low || got.Close != want.Close || got.Volume != want.Volume || got.TakerBuyVolume != want.TakerBuyVolume || got.TakerSellVolume != want.TakerSellVolume {
		t.Fatalf("%s financial payload changed: got %+v want %+v", tier, got, want)
	}
}
