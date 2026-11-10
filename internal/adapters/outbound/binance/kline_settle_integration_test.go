//go:build integration

// Gate K1 — does a CLOSED kline from the websocket stream ever get REVISED?
//
//	KSETTLE_SYMBOLS="btcusdt,ethusdt,solusdt" KSETTLE_INTERVALS="1m,5m" KSETTLE_MINUTES=12 \
//	  go test -tags=integration -count=1 -timeout 30m -v -run KlineSettle ./internal/adapters/outbound/binance/
//
// It subscribes to Binance and reads. It touches nothing of ours — no engine, no store, no
// handler — because the question is about the EXCHANGE's behavior, not about our code, and
// routing it through our pipeline would let our own defects contaminate the answer.
package binancesdk

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/binancews"
)

// TestKlineSettle_AClosedBucketIsNeverRevised is the PRECONDITION on giving the kline stream
// authority over OHLCV.
//
// # WHY THIS HAS TO BE MEASURED BEFORE ANYTHING IS BUILT
//
// The plan is to stop COMPUTING volume and start TRANSPORTING it: the kline owns
// {open, high, low, close, volume, takerBuy} and the aggTrade tape owns order flow. One source
// per quantity, no merge, no reconciliation — which is what makes it simpler than what it
// replaces rather than more complex.
//
// That only holds if a closed kline is FINAL. This project has already measured the REST kline
// NOT being final: a bucket that had just closed kept converging for ~5 s (BNB -1,68% at +1,59s;
// DOGE -1,30% at +1,14s; SOL still -0,32% at +3,04s; zero from +5s on). A repair that fetched at
// a 2 s delay installed a value SHORTER than the one the live stream had already assembled
// correctly, and reported it as a heal.
//
// If the STREAM behaves the same way, adopting `x:true` naively swaps one defect for another —
// and it would be the fourth time in this activity that something was declared finished early.
//
// # WHAT COUNTS AS A REVISION
//
// A second frame for the same (symbol, interval, openTime) carrying x=true and a DIFFERENT
// volume or taker-buy volume. Repeats with identical values are not revisions: the exchange is
// entitled to resend, and treating a resend as a revision would fabricate the very finding this
// test exists to look for.
func TestKlineSettle_AClosedBucketIsNeverRevised(t *testing.T) {
	symbols := splitEnv("KSETTLE_SYMBOLS", "btcusdt,ethusdt,solusdt,dogeusdt,1000shibusdt")
	intervals := splitEnv("KSETTLE_INTERVALS", "1m,5m")
	minutes, err := strconv.Atoi(envOrDefault("KSETTLE_MINUTES", "12"))
	if err != nil || minutes <= 0 {
		t.Fatalf("KSETTLE_MINUTES must be a positive integer, got %q", os.Getenv("KSETTLE_MINUTES"))
	}

	type key struct {
		symbol, interval string
		openTime         int64
	}
	type closed struct {
		volume, takerBuy string
		at               time.Time
	}

	var (
		mu        sync.Mutex
		seen      = map[key]closed{}
		closes    int
		repeats   int
		revisions []string
	)

	conn := binancews.NewConnection(0, defaultFuturesWSBasePath(),
		binancews.WithRoute(routeFor("binance", "future", streamClassKline)),
		binancews.WithReadIdle(readIdleFor("binance", "future")))
	handlers := make(map[string]binancews.Handler)
	for _, sym := range symbols {
		for _, iv := range intervals {
			sym, iv := sym, iv
			stream := sym + "@kline_" + iv
			handlers[stream] = func(data []byte) {
				var event klineEvent
				if err := json.Unmarshal(data, &event); err != nil || event.Kline == nil || event.Kline.OpenTime == nil {
					return
				}
				k := event.Kline
				if k.Closed == nil || !*k.Closed {
					return // still open; only FINAL frames are in scope
				}
				kk := key{sym, iv, *k.OpenTime}
				cur := closed{volume: derefStr(k.Volume), takerBuy: derefStr(k.TakerBuyBaseVol), at: time.Now()}

				mu.Lock()
				defer mu.Unlock()
				prev, ok := seen[kk]
				if !ok {
					seen[kk] = cur
					closes++
					return
				}
				// A RESEND IS NOT A REVISION. Identical values mean the exchange repeated
				// itself, which costs nothing and must not be reported as instability.
				if prev.volume == cur.volume && prev.takerBuy == cur.takerBuy {
					repeats++
					return
				}
				revisions = append(revisions, strings.Join([]string{
					sym, iv, time.UnixMilli(kk.openTime).UTC().Format(time.RFC3339),
					"vol " + prev.volume + " -> " + cur.volume,
					"takerBuy " + prev.takerBuy + " -> " + cur.takerBuy,
					"after " + cur.at.Sub(prev.at).String(),
				}, " | "))
				seen[kk] = cur
			}
		}
	}
	if err := conn.Connect(context.Background(), handlers); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	t.Logf("listening for %d minutes on %d symbols x %d intervals", minutes, len(symbols), len(intervals))
	time.Sleep(time.Duration(minutes) * time.Minute)

	mu.Lock()
	defer mu.Unlock()

	// AN EMPTY SAMPLE IS A HARNESS FAILURE, NOT A PASS. This is the exact shape that made gate
	// A6 publish a fabricated floor: a fetch that found nothing reported perfect agreement.
	if closes == 0 {
		t.Fatal("zero closed klines observed — the subscription produced no data, so this run " +
			"proves NOTHING about settling. It is a harness failure, not evidence of stability")
	}

	t.Logf("VERDICT: %d closed buckets observed, %d identical resends, %d REVISIONS", closes, repeats, len(revisions))
	for _, r := range revisions {
		t.Logf("  REVISION: %s", r)
	}
	if len(revisions) > 0 {
		t.Errorf("a closed kline was REVISED %d time(s). The stream is not final on x:true, so it "+
			"cannot be given authority over OHLCV without a settling rule — the same trap the REST "+
			"kline set when a 2s-delayed repair installed a value shorter than the live stream had", len(revisions))
	}
}

func splitEnv(k, def string) []string {
	v := envOrDefault(k, def)
	out := []string{}
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func envOrDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func deref(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
