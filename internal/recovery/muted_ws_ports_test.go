package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/market"
)

// venueProfile is the port shape a venue presents, not an exchange branch in recovery.
// The Binance-like profile serves CVD-complete klines; the BingX-like one only serves
// OHLCV. Both feed the same backstop, manager and filler.
type venueProfile struct {
	exchange    string
	marketType  market.MarketType
	supportsCVD bool
	takerBuy    float64
}

// mutedVenue is a HistorySource whose last persisted candle predates the mute; it answers
// any window up to the current open bucket, exactly as a venue does during a 30-minute
// WebSocket silence.
type mutedVenue struct {
	supportsCVD bool
	takerBuy    float64
}

func (v *mutedVenue) FetchOHLCV(_ context.Context, _ string, tf string, sinceMs, limit int64) ([]candle.Candle, error) {
	step := candle.ParseTimeframeToSeconds(tf) * 1000
	if step <= 0 {
		return nil, nil
	}
	current := candle.BucketTimestamp(time.Now().UnixMilli(), tf)
	start := candle.BucketTimestamp(sinceMs, tf)
	if start <= 0 {
		start = current - 6*step
	}
	var out []candle.Candle
	for at := start; at <= current && int64(len(out)) < limit; at += step {
		out = append(out, candle.Candle{
			Timestamp:       at,
			Open:            100,
			High:            101,
			Low:             99,
			Close:           100.5,
			Volume:          10,
			TakerBuyVolume:  v.takerBuy,
			TakerSellVolume: 10 - v.takerBuy,
		})
	}
	return out, nil
}

func (v *mutedVenue) FetchKlinesWithCVD(ctx context.Context, symbol, tf string, limit int) ([]candle.Candle, bool, error) {
	if !v.supportsCVD {
		return nil, false, nil
	}
	cs, err := v.FetchOHLCV(ctx, symbol, tf, 0, int64(limit))
	return cs, true, err
}

func (v *mutedVenue) Close() []error { return nil }

// cvdAwareFiller is the Binance-like profile's filler: it uses the rich kline and keeps
// the taker pair, so the profile actually exercises the CVD-capable path instead of
// pretending the two venues differ only in name.
type cvdAwareFiller struct{}

func (f cvdAwareFiller) Name() string { return "test-cvd" }

func (f cvdAwareFiller) Fill(ctx context.Context, client HistorySource, req FillRequest) (FillResult, error) {
	candles, ok, err := client.FetchKlinesWithCVD(ctx, req.Symbol, req.Timeframe, req.FetchLimit)
	if err != nil || !ok {
		return NewBasicOHLCVFiller().Fill(ctx, client, req)
	}
	var result FillResult
	for _, c := range candles {
		if c.Timestamp < req.GapStart || (req.UntilMs > 0 && c.Timestamp > req.UntilMs) {
			continue
		}
		if c.Timestamp == req.CurrentBucket {
			current := c
			result.Current = &current
			continue
		}
		result.Historical = append(result.Historical, c)
	}
	result.TakerVolumesAuthoritative = true
	return result, nil
}

type singleClientPool struct{ client HistorySource }

func (p singleClientPool) Get(string, string) (HistorySource, error) { return p.client, nil }
func (p singleClientPool) CloseAll()                                 {}

// persistedStoreProvider reads the watermark the consumer reads: the store, never an
// engine buffer.
type persistedStoreProvider struct{ store StorageWriter }

func (p persistedStoreProvider) GetLatestTimestamp(ctx context.Context, exchange, marketType, symbol, tf string) (int64, bool) {
	cs, err := p.store.GetCandlesByScoreRange(ctx, exchange, marketType, symbol, tf, 0, time.Now().Add(time.Minute).UnixMilli())
	if err != nil || len(cs) == 0 {
		return 0, false
	}
	return cs[len(cs)-1].Timestamp, true
}

// runMutedWSRecovery runs the same 30-minute mute scenario at the ports for one profile:
// the backstop sees the stale persisted head, enqueues a [now-30m, now] window, REST fills
// every missing bucket including the open one, validation passes, and the next tick finds
// nothing to do. No exchange-specific branch exists in internal/recovery.
func runMutedWSRecovery(t *testing.T, profile venueProfile) {
	t.Helper()
	ctx := context.Background()
	store, _ := newValidatorTestStore(t)
	writer := DefaultStorageWriter{Store: store}

	const symbol = "BTC/USDT"
	const tf = "5m"
	step := int64(candle.ParseTimeframeToSeconds(tf)) * 1000
	now := time.Now()
	current := candle.BucketTimestamp(now.UnixMilli(), tf)
	lastPersisted := candle.BucketTimestamp(now.Add(-30*time.Minute).UnixMilli(), tf)

	// The series the WS had written before it went silent: it stops 30 minutes ago.
	var beforeMute []candle.Candle
	for at := lastPersisted - 3*step; at <= lastPersisted; at += step {
		beforeMute = append(beforeMute, candle.Candle{
			Timestamp: at, Open: 100, High: 101, Low: 99, Close: 100.5, Volume: 10,
		})
	}
	if err := writer.SeedCandlesBatch(ctx, profile.exchange, "future", symbol, tf, beforeMute); err != nil {
		t.Fatalf("seed before the mute: %v", err)
	}

	client := &mutedVenue{supportsCVD: profile.supportsCVD, takerBuy: profile.takerBuy}
	m := NewManager(writer, 1, candle.NewTimeframeRegistry([]string{tf}))
	t.Cleanup(m.Stop)
	m.clientPool = singleClientPool{client: client}
	if profile.supportsCVD {
		m.SetFillerForExchange(profile.exchange, cvdAwareFiller{})
	} else {
		m.SetFiller(NewBasicOHLCVFiller())
	}
	provider := persistedStoreProvider{store: writer}
	m.SetTimestampProvider(provider)
	// The test drives the retry pipeline synchronously when needed.
	m.retry.(*MemoryRetryScheduler).pending.Store(true)

	backstop := NewBackstop(m, provider,
		[]Scope{{ExchangeID: profile.exchange, MarketType: profile.marketType}},
		[]string{tf}, Metrics{})
	subs := []string{profile.exchange + ":future:" + symbol}

	if got := backstop.ReconcileOnce(ctx, subs, now, 30*time.Minute, nil); got != 1 {
		t.Fatalf("backstop reconciled %d scopes, want the stale one", got)
	}
	job, ok := <-m.queue.Read()
	if !ok {
		t.Fatal("backstop did not enqueue the muted symbol")
	}
	job.Delay = 0
	m.process(ctx, 0, job)

	// Every bucket up to and including the open one was written, so the series has a
	// producer again instead of relying on an unbounded recovery loop.
	atCurrent, err := writer.GetCandlesByScoreRange(ctx, profile.exchange, "future", symbol, tf, current, current)
	if err != nil || len(atCurrent) != 1 || atCurrent[0].Timestamp != current {
		t.Fatalf("current bucket %d not persisted: %+v err=%v", current, atCurrent, err)
	}

	if profile.supportsCVD {
		if got := atCurrent[0].TakerBuyVolume; got != profile.takerBuy {
			t.Fatalf("taker buy = %v, want %v: the CVD-capable profile lost the measured pair", got, profile.takerBuy)
		}
	}

	ok, missing := m.validator.Validate(ctx, profile.exchange, "future", symbol, tf,
		lastPersisted, candle.BucketTimestamp(time.Now().UnixMilli(), tf))
	if !ok || missing != 0 {
		t.Fatalf("recovered series still incomplete: ok=%v missing=%d", ok, missing)
	}

	// No missing-bucket loop: nothing was scheduled and the next tick has nothing to do.
	if len(m.retry.(*MemoryRetryScheduler).queue) != 0 {
		t.Fatalf("recovery scheduled a retry after a complete fill: %+v", m.retry.(*MemoryRetryScheduler).queue)
	}
	m.retry.(*MemoryRetryScheduler).processReadyOnce(time.Now().Add(RetryMaxDelay + time.Second))
	if m.queue.Len() != 0 {
		t.Fatalf("recovery re-entered the queue: %d jobs", m.queue.Len())
	}
	if got := backstop.ReconcileOnce(ctx, subs, time.Now(), 30*time.Minute, nil); got != 0 {
		t.Fatalf("next backstop tick reconciled %d scopes, want 0 (the series is current)", got)
	}
}

func TestMutedWSRecoversBinanceProfile(t *testing.T) {
	runMutedWSRecovery(t, venueProfile{
		exchange:    "binance",
		marketType:  market.Future,
		supportsCVD: true,
		takerBuy:    4,
	})
}

func TestMutedWSRecoversBingXProfile(t *testing.T) {
	runMutedWSRecovery(t, venueProfile{
		exchange:    "bingx",
		marketType:  market.Future,
		supportsCVD: false,
	})
}
