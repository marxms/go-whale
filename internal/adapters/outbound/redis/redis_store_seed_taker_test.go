package redisstore

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// closed5mBucketTs returns an aligned 5m bucket timestamp old enough that
// SeedCandlesBatch classifies it as a CLOSED bucket (now - ts >= timeframeMs).
func closed5mBucketTs() int64 {
	const fiveMinMs = int64(300_000)
	return (time.Now().UnixMilli() - 600_000) / fiveMinMs * fiveMinMs
}

// TestSeedCandlesBatch_ClosedBucketPropagatesRESTTaker is the RED guard for the
// SeedCandlesBatch closed-bucket merge dropping order-flow. A merge that copies only OHLCV
// from the incoming REST kline silently DISCARDS the taker/CVD aggregates that
// parseSDKKlineItem derived — so a gap-recovered closed bucket stays CVD-void in Redis even
// when the REST kline carried full fidelity.
func TestSeedCandlesBatch_ClosedBucketPropagatesRESTTaker(t *testing.T) {
	store, _ := newTestRedisStore(t)
	ctx := context.Background()
	ex, mt, sym, tf := "binance", "future", "BTC/USDT", "5m"
	ts := closed5mBucketTs()

	// 1) The existing stored candle is CVD-void (taker=0) — the legacy stored state.
	cvdVoid := candle.Candle{Timestamp: ts, Open: 100, High: 110, Low: 95, Close: 105, Volume: 50}
	if err := store.SeedCandlesBatch(ctx, ex, mt, sym, tf, []candle.Candle{cvdVoid}); err != nil {
		t.Fatalf("seed void: %v", err)
	}

	// 2) A richer REST kline for the SAME closed bucket carries the definitive taker totals.
	rich := candle.Candle{
		Timestamp: ts, Open: 100, High: 112, Low: 94, Close: 106, Volume: 50,
		TakerBuyVolume: 30, TakerSellVolume: 20,
	}
	if err := store.SeedCandlesBatch(ctx, ex, mt, sym, tf, []candle.Candle{rich}); err != nil {
		t.Fatalf("seed rich: %v", err)
	}

	got, err := store.GetCandlesDesc(ctx, ex, mt, sym, tf, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 candle, got %d", len(got))
	}
	c := got[0]
	if c.TakerBuyVolume != 30 || c.TakerSellVolume != 20 {
		t.Fatalf("REST taker must propagate on closed-bucket merge: buy=%v sell=%v (want 30/20, bug => 0/0)",
			c.TakerBuyVolume, c.TakerSellVolume)
	}
}

// TestSeedCandlesBatch_ClosedBucketPoorRESTKeepsLiveTaker is the control / no-regression
// guard: an OHLCV-only REST kline (taker=0) over an existing taker-rich row MUST NOT zero
// the live order-flow — the `> 0` gate protects it. OHLCV (volume MAX) still updates.
func TestSeedCandlesBatch_ClosedBucketPoorRESTKeepsLiveTaker(t *testing.T) {
	store, _ := newTestRedisStore(t)
	ctx := context.Background()
	ex, mt, sym, tf := "binance", "future", "ETH/USDT", "5m"
	ts := closed5mBucketTs()

	rich := candle.Candle{
		Timestamp: ts, Open: 100, High: 110, Low: 95, Close: 105, Volume: 50,
		TakerBuyVolume: 30, TakerSellVolume: 20,
	}
	if err := store.SeedCandlesBatch(ctx, ex, mt, sym, tf, []candle.Candle{rich}); err != nil {
		t.Fatalf("seed rich: %v", err)
	}
	poor := candle.Candle{Timestamp: ts, Open: 100, High: 111, Low: 94, Close: 106, Volume: 51}
	if err := store.SeedCandlesBatch(ctx, ex, mt, sym, tf, []candle.Candle{poor}); err != nil {
		t.Fatalf("seed poor: %v", err)
	}

	got, err := store.GetCandlesDesc(ctx, ex, mt, sym, tf, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	c := got[0]
	if c.Volume != 51 {
		t.Fatalf("volume MAX should update to 51, got %v", c.Volume)
	}
}
