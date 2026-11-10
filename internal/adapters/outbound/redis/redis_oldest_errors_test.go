package redisstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

type oldestPipelineFailure struct {
	err      error
	nilFirst bool
}

func (h oldestPipelineFailure) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (h oldestPipelineFailure) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h oldestPipelineFailure) ProcessPipelineHook(_ redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(_ context.Context, cmds []redis.Cmder) error {
		if h.nilFirst {
			cmds[0].SetErr(redis.Nil)
			cmds[len(cmds)-1].SetErr(h.err)
			return redis.Nil
		}
		return h.err
	}
}

func TestOldestCandleTimestampsWrongTypeRejectsPartial(t *testing.T) {
	s, c := newTestRedisStore(t)
	defer func() { _ = c.Close() }()
	ctx := context.Background()
	if err := c.ZAdd(ctx, "candles:binance:future:BTC/USDT:1d", redis.Z{Score: 123, Member: "old"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "candles:binance:future:ETH/USDT:1d", "wrong type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	got, err := s.OldestCandleTimestamps(ctx, "1d")
	if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") || got != nil {
		t.Fatalf("partial measurement accepted: got=%v err=%v", got, err)
	}
}

func TestOldestCandleTimestampsPipelineFailures(t *testing.T) {
	for _, nilFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "transport", true: "nil_masks_later_failure"}[nilFirst], func(t *testing.T) {
			s, c := newTestRedisStore(t)
			defer func() { _ = c.Close() }()
			ctx := context.Background()
			for _, symbol := range []string{"BTC/USDT", "ETH/USDT"} {
				if err := c.ZAdd(ctx, candlesKeyV2("binance", "future", symbol, "1d"), redis.Z{Score: 123, Member: "old"}).Err(); err != nil {
					t.Fatal(err)
				}
			}
			cause := errors.New("injected frontier read failure")
			c.AddHook(oldestPipelineFailure{err: cause, nilFirst: nilFirst})
			got, err := s.OldestCandleTimestamps(ctx, "1d")
			if !errors.Is(err, cause) || got != nil {
				t.Fatalf("partial measurement accepted: got=%v err=%v", got, err)
			}
		})
	}
}

func TestOldestCandleTimestampsCompleteAndEmpty(t *testing.T) {
	s, c := newTestRedisStore(t)
	defer func() { _ = c.Close() }()
	ctx := context.Background()
	got, err := s.OldestCandleTimestamps(ctx, "1d")
	if err != nil || len(got) != 0 {
		t.Fatalf("confirmed empty rejected: %v %v", got, err)
	}
	key := candlesKeyV2("binance", "future", "BTC/USDT", "1d")
	if err := c.ZAdd(ctx, key, redis.Z{Score: 789, Member: "new"}, redis.Z{Score: 123, Member: "old"}).Err(); err != nil {
		t.Fatal(err)
	}
	got, err = s.OldestCandleTimestamps(ctx, "1d")
	ref := CandleKeyRef{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "1d"}
	if err != nil || len(got) != 1 || got[ref] != 123 {
		t.Fatalf("oldest boundary changed: %v %v", got, err)
	}
}
