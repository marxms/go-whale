package app

// lifecycle_e2e_test.go proves the concurrent lifecycle end to end.
//
// The cases below drive the real composition root and, wherever the suite already
// supports it, the real adapters: miniredis for the hot tier and the Pub/Sub transport,
// the real candle engine and tiered store, the real recovery manager and the real
// distribution producer. Only the venue transport (kline feed, history source,
// publisher) is doubled, because contacting an exchange or a shared server from a unit
// test is not allowed.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/adapters/inbound/httpapi"
	"github.com/marxms/go-whale/internal/adapters/inbound/realtime"
	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/ingestion"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/recovery"
	"github.com/marxms/go-whale/internal/retention"
	"github.com/marxms/go-whale/internal/trade"
)

// ─── fixtures ────────────────────────────────────────────────────────────────

// lifecycleConfig is the smallest graph the lifecycle needs: a live hot tier and the
// candle engine on. The durable tier stays off unless the case turns it on, so no case
// can reach QuestDB by accident.
func lifecycleConfig(t *testing.T, mutate ...func(*config.Config)) (config.Config, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	cfg := config.Config{
		Port:                  "0",
		RedisAddr:             mr.Addr(),
		RedisPoolSize:         10,
		RedisDialTimeoutS:     1,
		RedisReadTimeoutS:     1,
		RedisWriteTimeoutS:    1,
		RedisPoolTimeoutS:     1,
		RedisMaxRetries:       0,
		RealtimeChannelPrefix: "lifecycle",
		CandleEngine: config.CandleEngineConfig{
			Enabled:            true,
			Timeframes:         []string{"5m", "15m"},
			BufferSizes:        map[string]int{"default": 64},
			RedisWriteThrough:  true,
			SnapshotIntervalMS: int(time.Hour / time.Millisecond),
		},
	}
	for _, m := range mutate {
		m(&cfg)
	}
	return cfg, mr
}

func newLifecycleApp(t *testing.T, cfg config.Config, opts ...appOption) *App {
	t.Helper()
	app, err := NewApp(cfg, candle.NewTimeframeRegistry(cfg.CandleEngine.Timeframes), nil, opts...)
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	if app == nil {
		t.Fatal("NewApp returned (nil, nil)")
	}
	t.Cleanup(func() { _ = app.Shutdown(context.Background()) })
	return app
}

// freeTCPPort reserves a port, releases it, and returns it as a string. Run builds its
// own http.Server from cfg.Port, so the test needs a port that is free at that instant.
func freeTCPPort(t *testing.T) string {
	t.Helper()
	var listenCfg net.ListenConfig
	ln, err := listenCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return strconv.Itoa(port)
}

func TestRunFailsWhenHTTPPortIsOccupied(t *testing.T) {
	var listenCfg net.ListenConfig
	ln, err := listenCfg.Listen(t.Context(), "tcp", ":0")
	if err != nil {
		t.Fatalf("occupy port: %v", err)
	}
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	defer func() { _ = ln.Close() }()

	cfg, _ := lifecycleConfig(t, func(c *config.Config) { c.Port = port })
	app := newLifecycleApp(t, cfg)
	app.marginBrackets = nil // the real catalog would call Binance; this case is not about it

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := app.Run(ctx); err == nil {
		t.Fatal("Run returned nil with the HTTP port occupied; a process serving no API must not exit 0")
	} else if !strings.Contains(err.Error(), "http server") {
		t.Fatalf("Run error = %v, want it to name the HTTP server failure", err)
	}
}

// lifecycleHTTPClient disables keep-alives: the default transport's idle connections
// keep a read goroutine alive past the test, which is a test artifact and not an app
// leak.
func lifecycleHTTPClient() *http.Client {
	return &http.Client{
		Timeout:   2 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
}

func waitForHTTPStatus(t *testing.T, client *http.Client, url string, want int) []byte {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastCode int
	var lastErr error
	for time.Now().Before(deadline) {
		req, reqErr := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		if reqErr != nil {
			lastErr = reqErr
			time.Sleep(10 * time.Millisecond)
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(10 * time.Millisecond)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			time.Sleep(10 * time.Millisecond)
			continue
		}
		lastCode = resp.StatusCode
		if resp.StatusCode == want {
			return body
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("GET %s never returned %d (last status %d, last error %v)", url, want, lastCode, lastErr)
	return nil
}

// ─── fake manager: readiness without an exchange ─────────────────────────────

type lifecycleNoopHandler struct{}

func (lifecycleNoopHandler) HandleTrades(string, any) error { return nil }
func (lifecycleNoopHandler) HandleTradesWithContext(context.Context, string, string, string, any) error {
	return nil
}

// lifecycleSource is a connected streaming source with no transport. It makes the real
// HybridManager report a real connection, which is what /ready reads.
type lifecycleSource struct {
	connected atomic.Bool
}

func (s *lifecycleSource) Subscribe(context.Context, string) error { return nil }
func (s *lifecycleSource) Unsubscribe(string) error                { return nil }
func (s *lifecycleSource) GetTrades(context.Context, string) ([]trade.Trade, error) {
	return nil, nil
}
func (s *lifecycleSource) HealthCheck() bool                 { return s.connected.Load() }
func (s *lifecycleSource) Type() streaming.ClientType        { return streaming.ClientTypeNative }
func (s *lifecycleSource) Close() error                      { return nil }
func (s *lifecycleSource) GetBufferFillRatio(string) float64 { return 0 }
func (s *lifecycleSource) ConnectionStats() streaming.ConnectionStats {
	connected := s.connected.Load()
	return streaming.ConnectionStats{
		ExchangeID: "binance",
		MarketType: "future",
		Connected:  connected,
		Healthy:    connected,
		Symbols:    1,
	}
}

// installLifecycleManager swaps the real manager for one whose source is controlled by
// the test, then rebuilds the HTTP health collector so it reads that manager. The
// manager itself, its scheduler and its workers are the production ones.
func installLifecycleManager(t *testing.T, app *App, source *lifecycleSource) {
	t.Helper()
	source.connected.Store(true)
	manager := ingestion.NewHybridManager(lifecycleNoopHandler{}, 1, map[string]streaming.ClientFactory{
		"binance": func(streaming.ClientConfig) (streaming.StreamingClient, error) {
			return source, nil
		},
	})
	if err := manager.Subscribe(context.Background(), "BTC/USDT", "binance", market.Future); err != nil {
		t.Fatalf("subscribe fake source: %v", err)
	}
	t.Cleanup(manager.Stop)

	app.manager = manager
	provider, err := app.buildObservability()
	if err != nil {
		t.Fatalf("rebuild observability: %v", err)
	}
	app.buildHTTP(app.cfg, app.tfReg, provider)
}

// ─── fake kline feed: reconnect at the app seam ──────────────────────────────

type lifecycleKlineFeed struct {
	sink    func(string, candle.Candle)
	start   func([]string) error
	add     func([]string) error
	have    bool
	silent  float64
	restart bool
	stopped atomic.Bool
}

func (f *lifecycleKlineFeed) Start(symbols []string) error {
	if f.start != nil {
		return f.start(symbols)
	}
	return nil
}

func (f *lifecycleKlineFeed) AddSymbols(symbols []string) error {
	if f.add != nil {
		return f.add(symbols)
	}
	return nil
}

func (f *lifecycleKlineFeed) Mute([]string) {}
func (f *lifecycleKlineFeed) Stop() {
	f.stopped.Store(true)
	f.have = false
}
func (f *lifecycleKlineFeed) SecondsSinceLastFrame() (float64, bool) { return f.silent, f.have }
func (f *lifecycleKlineFeed) QuietestPartition() (float64, int)      { return f.silent, 1 }
func (f *lifecycleKlineFeed) NeedsRestart() bool                     { return f.restart }

// ─── fake recovery transport ─────────────────────────────────────────────────

type lifecycleHistorySource struct{}

func (lifecycleHistorySource) FetchOHLCV(context.Context, string, string, int64, int64) ([]candle.Candle, error) {
	return nil, nil
}

func (lifecycleHistorySource) FetchKlinesWithCVD(context.Context, string, string, int) ([]candle.Candle, bool, error) {
	return nil, false, nil
}
func (lifecycleHistorySource) Close() []error { return nil }

type lifecycleClientPool struct{ source recovery.HistorySource }

func (p lifecycleClientPool) Get(string, string) (recovery.HistorySource, error) {
	return p.source, nil
}
func (lifecycleClientPool) CloseAll() {}

type lifecycleFiller struct {
	mu     sync.Mutex
	reqs   []recovery.FillRequest
	candle candle.Candle
}

func (f *lifecycleFiller) Name() string { return "lifecycle" }

func (f *lifecycleFiller) Fill(_ context.Context, _ recovery.HistorySource, req recovery.FillRequest) (recovery.FillResult, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	f.mu.Unlock()
	c := f.candle
	c.Timestamp = req.GapStart
	return recovery.FillResult{Historical: []candle.Candle{c}}, nil
}

func (f *lifecycleFiller) requests() []recovery.FillRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recovery.FillRequest(nil), f.reqs...)
}

type lifecycleLatestTimestamp struct{ ts int64 }

func (p lifecycleLatestTimestamp) GetLatestTimestamp(context.Context, string, string, string, string) (int64, bool) {
	return p.ts, true
}

// lifecycleStorageWriter adapts the real Redis store to recovery.StorageWriter. The
// adapter is explicit rather than relying on the embedded fields of CombinedStore, so a
// port change fails here instead of at a call site.
type lifecycleStorageWriter struct{ store *redisstore.RedisStore }

func (w lifecycleStorageWriter) SeedCandlesBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, candles []candle.Candle) error {
	return w.store.SeedCandlesBatch(ctx, exchange, marketType, symbol, timeframe, candles)
}

func (w lifecycleStorageWriter) SeedCurrentBucketCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, c candle.Candle) error {
	return w.store.SeedCurrentBucketCandle(ctx, exchange, marketType, symbol, timeframe, c)
}

func (w lifecycleStorageWriter) GetCandlesByScoreRange(ctx context.Context, exchange, marketType, symbol, timeframe string, minScore, maxScore int64) ([]candle.Candle, error) {
	return w.store.GetCandlesByScoreRange(ctx, exchange, marketType, symbol, timeframe, minScore, maxScore)
}

func (w lifecycleStorageWriter) GetCandlesCount(ctx context.Context, exchange, marketType, symbol, timeframe string) (int64, error) {
	return w.store.GetCandlesCount(ctx, exchange, marketType, symbol, timeframe)
}

func waitForProcessed(t *testing.T, manager *recovery.Manager) recovery.GapRecoveryStats {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stats := manager.Stats()
		if stats.Processed > 0 {
			return stats
		}
		time.Sleep(5 * time.Millisecond)
	}
	stats := manager.Stats()
	t.Fatalf("recovery processed no job: %+v", stats)
	return stats
}

// ─── stage1 + 6: boot, readiness, cancellation ──────────────────────────────

// TestLifecycle_BootReadinessAndCancellation drives Run with the production managers,
// HTTP server and pprof server, then proves /ready follows the live connection set and
// that canceling the caller's context returns Run through the ordered shutdown.
func TestLifecycle_BootReadinessAndCancellation(t *testing.T) {
	cfg, _ := lifecycleConfig(t)
	cfg.Port = freeTCPPort(t)

	app := newLifecycleApp(t, cfg)
	app.marginBrackets = nil // the real catalog would call Binance; this case is not about it
	app.serverProbe = staticServerProbe{policy: "noeviction"}
	source := &lifecycleSource{}
	installLifecycleManager(t, app, source)

	var trace []string
	app.shutdownTrace = &trace

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- app.Run(ctx) }()

	client := lifecycleHTTPClient()
	base := "http://127.0.0.1:" + cfg.Port

	healthBody := waitForHTTPStatus(t, client, base+"/health", http.StatusOK)
	var health httpapi.HealthResponse
	if err := json.Unmarshal(healthBody, &health); err != nil {
		t.Fatalf("decode /health: %v", err)
	}
	if len(health.Connections) != 1 || !health.Connections[0].Healthy {
		t.Fatalf("/health did not see the live connection: %s", healthBody)
	}
	waitForHTTPStatus(t, client, base+"/ready", http.StatusOK)

	// A connected transport is gone: readiness must follow it, not stay green from a
	// cached verdict.
	source.connected.Store(false)
	waitForHTTPStatus(t, client, base+"/ready", http.StatusServiceUnavailable)
	waitForHTTPStatus(t, client, base+"/health", http.StatusServiceUnavailable)

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned %v after cancellation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the caller's context was canceled")
	}

	if got, want := strings.Join(trace, ","), "intake,timers,producers,stores,http"; got != want {
		t.Fatalf("shutdown order = %q, want %q", got, want)
	}
}

// ─── stage2: reconnect ──────────────────────────────────────────────────────

// TestLifecycle_ReconnectReplacesSilentFeedAndEnqueuesRecovery drives the watchdog's
// reconnect path against the real candle engine and the real recovery manager: the
// silent feed is stopped, a replacement is dialed, its first frame advances the engine,
// and the boot-gap path enqueues the window the silence opened.
func TestLifecycle_ReconnectReplacesSilentFeedAndEnqueuesRecovery(t *testing.T) {
	cfg, _ := lifecycleConfig(t)
	app := newLifecycleApp(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.streamCtx = ctx

	const (
		exchange   = "binance"
		marketType = "future"
		symbol     = "BTC/USDT"
	)
	// Guaranteed to be the PREVIOUS 5m bucket: now-5m always falls before the bucket
	// that contains now, so the replacement frame is a different timestamp and its
	// volume is not merged into the old bucket.
	latest := candle.BucketTimestamp(time.Now().Add(-5*time.Minute).UnixMilli(), "5m")
	app.candleEngine.UpdateCandle(exchange, marketType, symbol, "5m", candle.Candle{
		Timestamp: latest, Open: 100, High: 102, Low: 99, Close: 101, Volume: 10,
	})

	old := &lifecycleKlineFeed{have: true, restart: true}
	app.klineFeed = old
	app.klineScope = exchange + ":" + marketType
	app.klineSymbols = []string{symbol}

	freshCandle := candle.Candle{
		Timestamp: candle.BucketTimestamp(time.Now().UnixMilli(), "5m"),
		Open:      101, High: 104, Low: 100, Close: 103, Volume: 12,
	}
	fresh := &lifecycleKlineFeed{have: true}
	fresh.start = func(symbols []string) error {
		if len(symbols) != 1 || symbols[0] != symbol {
			t.Errorf("replacement dialed %v, want the live fleet [%s]", symbols, symbol)
		}
		fresh.sink(symbol, freshCandle)
		return nil
	}
	app.newKlineFeed = func(_ context.Context, _, _, _ string, sink func(string, candle.Candle)) klineFeed {
		fresh.sink = sink
		return fresh
	}

	if app.restartKlineFeedIfSilent(t.Context(), exchange, marketType) {
		t.Fatal("watchdog stopped although a replacement was available")
	}
	if app.klineFeed != fresh || !old.stopped.Load() {
		t.Fatal("the silent feed was not replaced by the live one")
	}
	if app.klineGapHeads != nil {
		t.Fatalf("successful restart kept a stale gap reservation: %v", app.klineGapHeads)
	}

	stats := app.gapRecovery.Stats()
	if stats.Enqueued == 0 || stats.Queued == 0 {
		t.Fatalf("reconnect did not enqueue recovery: %+v", stats)
	}

	got := app.candleEngine.GetCandlesAsc(exchange, marketType, symbol, "5m", 10)
	if len(got) == 0 || got[len(got)-1].Timestamp != freshCandle.Timestamp || got[len(got)-1].Volume != 12 {
		t.Fatalf("replacement frame did not reach the engine: %+v", got)
	}
}

// TestLifecycle_ReconnectEnqueueIsFilledByTheWorker closes the loop the previous case
// opens: the boot-gap job the reconnect enqueues is executed by the real worker, a history
// source returns the missing bucket, and the result reaches the hot tier. The gap is six
// seconds so the production admission delay is five seconds and below, not the thirty-second
// cap a multi-minute silence would produce.
func TestLifecycle_ReconnectEnqueueIsFilledByTheWorker(t *testing.T) {
	cfg, _ := lifecycleConfig(t)
	app := newLifecycleApp(t, cfg)

	tfReg := candle.NewTimeframeRegistry(cfg.CandleEngine.Timeframes)
	manager := recovery.NewManager(lifecycleStorageWriter{store: app.redisStore}, 1, tfReg)
	manager.SetClientPool(lifecycleClientPool{source: lifecycleHistorySource{}})
	filler := &lifecycleFiller{candle: candle.Candle{
		Open: 100, High: 102, Low: 99, Close: 101, Volume: 100,
	}}
	manager.SetFiller(filler)
	manager.Start(t.Context())
	t.Cleanup(manager.Stop)
	app.gapRecovery = manager

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.streamCtx = ctx

	const (
		exchange   = "binance"
		marketType = "future"
		symbol     = "BTC/USDT"
	)
	latestMs := time.Now().Add(-6 * time.Second).UnixMilli()
	app.candleEngine.UpdateCandle(exchange, marketType, symbol, "5m", candle.Candle{
		Timestamp: latestMs, Open: 99, High: 101, Low: 98, Close: 100, Volume: 7,
	})

	old := &lifecycleKlineFeed{have: true, restart: true}
	app.klineFeed = old
	app.klineScope = exchange + ":" + marketType
	app.klineSymbols = []string{symbol}
	fresh := &lifecycleKlineFeed{have: true}
	fresh.start = func([]string) error {
		// SAME bucket as the reserved head: the replacement proves the feed is alive
		// without closing the gapped bucket, so the only repair in flight is the
		// reconnect window under test (no boot-bucket settle or HasGap close).
		fresh.sink(symbol, candle.Candle{
			Timestamp: latestMs, Open: 99, High: 103, Low: 98, Close: 102, Volume: 9,
		})
		return nil
	}
	app.newKlineFeed = func(_ context.Context, _, _, _ string, sink func(string, candle.Candle)) klineFeed {
		fresh.sink = sink
		return fresh
	}

	if app.restartKlineFeedIfSilent(t.Context(), exchange, marketType) {
		t.Fatal("watchdog stopped during the reconnect case")
	}

	deadline := time.Now().Add(8 * time.Second)
	for len(filler.requests()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	requests := filler.requests()
	if len(requests) == 0 {
		t.Fatalf("the enqueued reconnect gap was never executed: %+v", manager.Stats())
	}

	missing := candle.BucketTimestamp(latestMs, "5m")
	var sawFiveMinuteRequest bool
	for _, req := range requests {
		if req.Timeframe != "5m" {
			continue
		}
		sawFiveMinuteRequest = true
		if req.GapStart != missing {
			t.Fatalf("reconnect repair asked for bucket %d, want %d", req.GapStart, missing)
		}
		if req.FetchLimit > 2 {
			t.Fatalf("reconnect repair swept %d buckets instead of the single named window", req.FetchLimit)
		}
	}
	if !sawFiveMinuteRequest {
		t.Fatalf("no 5m repair was executed: %+v", requests)
	}

	persisted, err := app.redisStore.GetCandlesAsc(context.Background(), exchange, marketType, symbol, "5m", 16)
	if err != nil {
		t.Fatalf("read back hot tier: %v", err)
	}
	if c, ok := findCandle(persisted, missing); !ok || c.Volume != 100 {
		t.Fatalf("reconnect gap was not filled in the hot tier: %+v (present=%t): %+v", c, ok, persisted)
	}
}

// ─── stage3: recovery ───────────────────────────────────────────────────────

// TestLifecycle_RecoveryFillsExactlyOneBucket proves the recovery path end to end: the
// real manager's worker asks a history source for one material window, and the returned
// candle is persisted in the real hot tier. The request is asserted for size, because a
// bulk sweep on a one-bucket hole is the wasteful shape the materiality rule exists to
// prevent.
func TestLifecycle_RecoveryFillsExactlyOneBucket(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := config.Config{
		RedisAddr:          mr.Addr(),
		RedisPoolSize:      10,
		RedisDialTimeoutS:  1,
		RedisReadTimeoutS:  1,
		RedisWriteTimeoutS: 1,
		RedisPoolTimeoutS:  1,
		RedisMaxRetries:    0,
	}
	const timeframe = "5m"
	tfReg := candle.NewTimeframeRegistry([]string{timeframe})
	store, err := redisstore.NewRedisStore(t.Context(), cfg, 800, tfReg)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	manager := recovery.NewManager(lifecycleStorageWriter{store: store}, 1, tfReg)
	manager.SetClientPool(lifecycleClientPool{source: lifecycleHistorySource{}})
	filler := &lifecycleFiller{candle: candle.Candle{
		Open: 10, High: 12, Low: 9, Close: 11, Volume: 100,
	}}
	manager.SetFiller(filler)
	manager.Start(t.Context())
	t.Cleanup(manager.Stop)

	current := candle.BucketTimestamp(time.Now().UnixMilli(), timeframe)
	missing := current - 5*60*1000 // one closed bucket behind the open one
	resp := manager.ForceBackfill(recovery.BackfillRequest{
		ExchangeID: "binance",
		MarketType: "future",
		Symbols:    []string{"BTC/USDT"},
		Timeframes: []string{timeframe},
		SinceMs:    missing,
		UntilMs:    missing + 5*60*1000,
	})
	if resp.Enqueued != 1 {
		t.Fatalf("ForceBackfill enqueued %d job(s), want 1: %+v", resp.Enqueued, resp)
	}

	waitForProcessed(t, manager)

	requests := filler.requests()
	if len(requests) != 1 {
		t.Fatalf("filler calls = %d, want exactly one (the named bucket)", len(requests))
	}
	req := requests[0]
	if req.Timeframe != timeframe || req.GapStart != missing {
		t.Fatalf("fill request = %+v, want timeframe %s at bucket %d", req, timeframe, missing)
	}
	if req.FetchLimit > 3 {
		t.Fatalf("fill request swept %d buckets for a one-bucket hole (gap start %d)", req.FetchLimit, req.GapStart)
	}
	if req.UntilMs != 0 {
		t.Fatalf("ordinary recovery became a bounded repair (UntilMs=%d): intent must be explicit", req.UntilMs)
	}

	persisted, err := store.GetCandlesAsc(context.Background(), "binance", "future", "BTC/USDT", timeframe, 10)
	if err != nil {
		t.Fatalf("read back hot tier: %v", err)
	}
	var found bool
	for _, c := range persisted {
		if c.Timestamp == missing {
			found = true
			if c.Close != 11 || c.Volume != 100 {
				t.Fatalf("persisted repair = %+v, want the filler's values", c)
			}
		}
	}
	if !found {
		t.Fatalf("the repaired bucket never reached the hot tier: %+v", persisted)
	}
}

// TestLifecycle_RecoveryMaterialitySkipsImmaterialWindowAndRepairsStale proves the two
// halves of the materiality rule through the app's real manager: a fresh timeframe whose
// short silence crossed no bucket boundary is skipped, while a genuinely stale timeframe
// is always repaired.
func TestLifecycle_RecoveryMaterialitySkipsImmaterialWindowAndRepairsStale(t *testing.T) {
	const timeframe = "1d"
	now := time.Now()
	bucket := candle.BucketTimestamp(now.UnixMilli(), timeframe)
	disconnect := time.UnixMilli(bucket + 1_000)
	reconnect := time.UnixMilli(bucket + 21_000) // 20s, far below 10% of a day

	run := func(t *testing.T, provider recovery.LatestTimestampProvider) []recovery.FillRequest {
		t.Helper()
		mr := miniredis.RunT(t)
		cfg := config.Config{
			RedisAddr:          mr.Addr(),
			RedisPoolSize:      10,
			RedisDialTimeoutS:  1,
			RedisReadTimeoutS:  1,
			RedisWriteTimeoutS: 1,
			RedisPoolTimeoutS:  1,
		}
		tfReg := candle.NewTimeframeRegistry([]string{timeframe})
		store, err := redisstore.NewRedisStore(t.Context(), cfg, 800, tfReg)
		if err != nil {
			t.Fatalf("NewRedisStore: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })

		manager := recovery.NewManager(lifecycleStorageWriter{store: store}, 1, tfReg)
		manager.SetClientPool(lifecycleClientPool{source: lifecycleHistorySource{}})
		manager.SetTimestampProvider(provider)
		filler := &lifecycleFiller{candle: candle.Candle{
			Open: 10, High: 12, Low: 9, Close: 11, Volume: 100,
		}}
		manager.SetFiller(filler)
		manager.Start(t.Context())
		t.Cleanup(manager.Stop)

		manager.ForceBackfill(recovery.BackfillRequest{
			ExchangeID: "binance",
			MarketType: "future",
			Symbols:    []string{"BTC/USDT"},
			Timeframes: []string{timeframe},
			SinceMs:    disconnect.UnixMilli(),
			UntilMs:    reconnect.UnixMilli(),
		})
		waitForProcessed(t, manager)
		return filler.requests()
	}

	t.Run("fresh short silence is skipped", func(t *testing.T) {
		requests := run(t, lifecycleLatestTimestamp{ts: now.UnixMilli()})
		if len(requests) != 0 {
			t.Fatalf("immaterial silence fetched %d timeframe(s): %+v", len(requests), requests)
		}
	})

	t.Run("genuinely stale bucket is repaired", func(t *testing.T) {
		stale := lifecycleLatestTimestamp{ts: now.Add(-10 * 24 * time.Hour).UnixMilli()}
		requests := run(t, stale)
		if len(requests) != 1 || requests[0].Timeframe != timeframe {
			t.Fatalf("stale timeframe was not repaired: %+v", requests)
		}
	})
}

// ─── stage4: publication ────────────────────────────────────────────────────

// TestLifecycle_CandleClosePublishesOrderedSequenceToRedisPubSub drives the real app sink
// through the real distribution producer and the real Redis adapter, and reads the
// frames from miniredis. Ordering and sequence are asserted on the wire, not on the
// producer's internal queue.
func TestLifecycle_CandleClosePublishesOrderedSequenceToRedisPubSub(t *testing.T) {
	cfg, mr := lifecycleConfig(t)
	app := newLifecycleApp(t, cfg)

	subscriber := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = subscriber.Close() })
	channel := realtime.Channel(cfg.RealtimeChannelPrefix, realtime.EventCandleClose)
	pubsub := subscriber.Subscribe(context.Background(), channel)
	t.Cleanup(func() { _ = pubsub.Close() })
	// Subscribe returns after the server confirmed the subscription, so no publish can
	// be missed by a race here.
	messages := pubsub.Channel()

	const events = 64
	key := engine.CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}
	base := candle.BucketTimestamp(time.Now().Add(-24*time.Hour).UnixMilli(), "5m")
	for i := 0; i < events; i++ {
		app.onCandleClose(key, candle.Candle{
			Timestamp: base + int64(i)*5*60*1000,
			Open:      100, High: 101, Low: 99, Close: 100.5, Volume: float64(i + 1),
		})
	}
	// Drain and stop the owner goroutines before reading: Stop publishes the queued tail
	// and then returns, which makes the wire contents complete and deterministic.
	app.realtimeProducer.Stop()

	var wantSeq uint64 = 1
	for i := 0; i < events; i++ {
		select {
		case msg, ok := <-messages:
			if !ok {
				t.Fatalf("Pub/Sub channel closed after %d/%d events", i, events)
			}
			if msg.Channel != channel {
				t.Fatalf("event %d published on %q, want %q", i, msg.Channel, channel)
			}
			env, err := realtime.Unmarshal([]byte(msg.Payload))
			if err != nil {
				t.Fatalf("event %d is not a valid envelope: %v", i, err)
			}
			if env.Seq != wantSeq {
				t.Fatalf("event %d carried sequence %d, want %d — a subscriber cannot distinguish reordering from loss", i, env.Seq, wantSeq)
			}
			wantSeq++
			if want := base + int64(i)*5*60*1000; env.Candle.Timestamp != want {
				t.Fatalf("event %d carried timestamp %d, want %d: the queue order must be the wire order", i, env.Candle.Timestamp, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("only %d/%d events reached Redis Pub/Sub", i, events)
		}
	}
}

// ─── stage5: candle close ───────────────────────────────────────────────────

func findCandle(candles []candle.Candle, ts int64) (candle.Candle, bool) {
	for _, c := range candles {
		if c.Timestamp == ts {
			return c, true
		}
	}
	return candle.Candle{}, false
}

// TestLifecycle_EngineCloseMergesFoldsAndPersists drives the production kline sink shape
// (UpdateCandle + FoldUp) against the real engine and the real tiered store: an open
// bucket merged twice is closed with the merged values, the 5m bucket is written to the
// hot tier, and the 15m parent is rebuilt from its three children and persisted when it
// closes.
func TestLifecycle_EngineCloseMergesFoldsAndPersists(t *testing.T) {
	cfg, _ := lifecycleConfig(t)
	app := newLifecycleApp(t, cfg)

	const (
		exchange   = "binance"
		marketType = "future"
		symbol     = "BTC/USDT"
	)
	base := candle.BucketTimestamp(time.Now().Add(-24*time.Hour).UnixMilli(), "15m")
	step := int64(5 * 60 * 1000)
	push := func(i int, volume float64) {
		ts := base + int64(i)*step
		app.candleEngine.UpdateCandle(exchange, marketType, symbol, "5m", candle.Candle{
			Timestamp: ts,
			Open:      100 + float64(i),
			High:      101 + float64(i),
			Low:       99 + float64(i),
			Close:     100.5 + float64(i),
			Volume:    volume,
		})
		app.candleEngine.FoldUp(exchange, marketType, symbol, "5m", ts)
	}

	push(0, 5)
	push(0, 3) // same open bucket: the delta merges into volume 8
	for i := 1; i <= 5; i++ {
		push(i, 5)
	}

	closed5m, ok := findCandle(app.candleEngine.GetCandlesAsc(exchange, marketType, symbol, "5m", 64), base)
	if !ok || closed5m.Volume != 8 {
		t.Fatalf("closed 5m bucket = %+v (present=%t), want the merged volume 8", closed5m, ok)
	}

	hot5m, err := app.redisStore.GetCandlesAsc(context.Background(), exchange, marketType, symbol, "5m", 64)
	if err != nil {
		t.Fatalf("read hot 5m: %v", err)
	}
	persisted5m, ok := findCandle(hot5m, base)
	if !ok || persisted5m.Volume != 8 {
		t.Fatalf("hot 5m bucket = %+v (present=%t), want the closed volume 8: %+v", persisted5m, ok, hot5m)
	}

	engine15m := app.candleEngine.GetCandlesAsc(exchange, marketType, symbol, "15m", 16)
	parent, ok := findCandle(engine15m, base)
	if !ok || parent.Volume != 18 {
		t.Fatalf("15m parent = %+v (present=%t), want volume 8+5+5=18: %+v", parent, ok, engine15m)
	}

	hot15m, err := app.redisStore.GetCandlesAsc(context.Background(), exchange, marketType, symbol, "15m", 16)
	if err != nil {
		t.Fatalf("read hot 15m: %v", err)
	}
	if persisted, ok := findCandle(hot15m, base); !ok || persisted.Volume != 18 {
		t.Fatalf("hot 15m parent = %+v (present=%t), want the folded volume 18: %+v", persisted, ok, hot15m)
	}
}

// ─── stage7: shutdown ───────────────────────────────────────────────────────

type lifecycleFailingPublisher struct{ calls atomic.Int64 }

func (p *lifecycleFailingPublisher) Publish(context.Context, string, []byte) error {
	p.calls.Add(1)
	return errors.New("redis publish refused")
}

// TestLifecycle_ShutdownDrainsTheProducerTailAndIsIdempotent proves the ordered shutdown
// on a real graph: every queued event is drained to Redis before the process stops, the
// phases run in the documented order, and a second/concurrent Shutdown is a no-op.
func TestLifecycle_ShutdownDrainsTheProducerTailAndIsIdempotent(t *testing.T) {
	cfg, mr := lifecycleConfig(t)
	app := newLifecycleApp(t, cfg)

	subscriber := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = subscriber.Close() })
	channel := realtime.Channel(cfg.RealtimeChannelPrefix, realtime.EventCandleClose)
	pubsub := subscriber.Subscribe(context.Background(), channel)
	t.Cleanup(func() { _ = pubsub.Close() })
	messages := pubsub.Channel()

	const events = 32
	key := engine.CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "ETH/USDT", Timeframe: "5m"}
	base := candle.BucketTimestamp(time.Now().Add(-24*time.Hour).UnixMilli(), "5m")
	for i := 0; i < events; i++ {
		app.onCandleClose(key, candle.Candle{
			Timestamp: base + int64(i)*5*60*1000,
			Open:      200, High: 201, Low: 199, Close: 200.5, Volume: float64(i + 1),
		})
	}

	var trace []string
	app.shutdownTrace = &trace
	if err := app.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got, want := strings.Join(trace, ","), "intake,timers,producers,stores,http"; got != want {
		t.Fatalf("shutdown order = %q, want %q", got, want)
	}

	for i := 0; i < events; i++ {
		select {
		case _, ok := <-messages:
			if !ok {
				t.Fatalf("Pub/Sub channel closed after %d/%d drained events", i, events)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("shutdown abandoned %d/%d queued events: durability drains before telemetry", events-i, events)
		}
	}

	// Concurrent second callers must observe the first outcome, not run the phases again.
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- app.Shutdown(t.Context())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("idempotent Shutdown returned %v", err)
		}
	}
	if got := len(trace); got != 5 {
		t.Fatalf("a repeated Shutdown re-ran %d phase(s)", got-5)
	}
}

// ─── stage8: dependency failures ────────────────────────────────────────────

// TestLifecycle_RedisUnavailableAtBootIsFatal pins the documented contract: the hot tier
// is required at construction, so the process refuses to start instead of serving
// without its only live store. The address is a miniredis that was just closed, so the
// case never reaches a shared server.
func TestLifecycle_RedisUnavailableAtBootIsFatal(t *testing.T) {
	mr := miniredis.RunT(t)
	addr := mr.Addr()
	mr.Close()

	cfg := config.Config{
		RedisAddr: addr,
		// PoolSize > 1 keeps go-redis's pool-level tryDial sleeper out of the test: that
		// library goroutine is only spawned once dial errors reach the pool size and it
		// retries on its own one-second backoff. The app-level behavior under test is the
		// constructor's refusal, not that library retry.
		RedisPoolSize:      10,
		RedisDialTimeoutS:  1,
		RedisReadTimeoutS:  1,
		RedisWriteTimeoutS: 1,
		RedisPoolTimeoutS:  1,
		RedisMaxRetries:    0,
		CandleEngine: config.CandleEngineConfig{
			Enabled:     true,
			Timeframes:  []string{"5m"},
			BufferSizes: map[string]int{"default": 16},
		},
	}
	app, err := NewApp(cfg, candle.NewTimeframeRegistry([]string{"5m"}), nil)
	if err == nil {
		if app != nil {
			_ = app.Shutdown(context.Background())
		}
		t.Fatal("NewApp accepted an unreachable hot tier")
	}
	if app != nil {
		t.Fatal("NewApp returned an App together with the error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "redis") {
		t.Fatalf("the fatal hot-tier error does not name the dependency: %v", err)
	}
}

// TestLifecycle_ColdTierUnavailableDegradesAndKeepsGuards proves the other side of the
// contract: a cold tier that cannot be reached degrades loudly, the process keeps
// serving, and every guard that does not depend on QuestDB stays armed.
func TestLifecycle_ColdTierUnavailableDegradesAndKeepsGuards(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)
	cfg, _ := lifecycleConfig(t, func(c *config.Config) {
		c.QuestDB = config.QuestDBConfig{Enabled: true}
		c.Exchanges = []config.ExchangeConfig{{
			ID:         "binance",
			MarketType: "future",
			Enabled:    true,
			Discovery:  config.DiscoveryConfig{Enabled: true, MaxAssets: 10},
		}}
	})

	app, err := NewApp(cfg, candle.NewTimeframeRegistry(cfg.CandleEngine.Timeframes), log,
		withColdTierConstructor(func(config.QuestDBConfig) (*questdb.Store, error) {
			return nil, errors.New("dial tcp 127.0.0.1:8812: connect: connection refused")
		}))
	if err != nil {
		t.Fatalf("an unavailable cold tier refused the boot: %v", err)
	}
	t.Cleanup(func() { _ = app.Shutdown(context.Background()) })
	if app.qdbStore != nil || app.qdbPipeline != nil || app.coldCompleteness != nil {
		t.Fatal("cold-tier failure left a partially wired durable graph")
	}

	// Hot tier and fan-out are independent of QuestDB: a close still persists and still
	// reaches the continuity baseline used for gap detection.
	key := engine.CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}
	base := candle.BucketTimestamp(time.Now().Add(-24*time.Hour).UnixMilli(), "5m")
	app.candleEngine.UpdateCandle(key.Exchange, key.MarketType, key.Symbol, "5m", candle.Candle{
		Timestamp: base, Open: 100, High: 101, Low: 99, Close: 100.5, Volume: 10,
	})
	app.candleEngine.UpdateCandle(key.Exchange, key.MarketType, key.Symbol, "5m", candle.Candle{
		Timestamp: base + 5*60*1000, Open: 101, High: 102, Low: 100, Close: 101.5, Volume: 11,
	})
	hot, err := app.redisStore.GetCandlesAsc(context.Background(), key.Exchange, key.MarketType, key.Symbol, "5m", 8)
	if err != nil || len(hot) == 0 || hot[0].Timestamp != base {
		t.Fatalf("cold-tier failure disabled the hot close write: %+v err=%v", hot, err)
	}

	// The preflight sole-copy guard cannot be measured without a durable tier. It must say
	// so at the highest non-fatal severity instead of silently passing.
	app.serverProbe = staticServerProbe{policy: "allkeys-lru"}
	if err := app.preflightGates(context.Background()); err != nil {
		t.Fatalf("unmeasured sole copy became a fatal verdict: %v", err)
	}
	var lines string
	for _, record := range sink.Records() {
		lines += record.Text() + "\n"
	}
	for _, want := range []string{"sole-copy-vs-evict-everything-policy", "COULD NOT BE MEASURED"} {
		if !strings.Contains(lines, want) {
			t.Errorf("degraded sole-copy guard did not report %q: %s", want, lines)
		}
	}

	// Expected scopes, coverage and liveness survive the cold-tier failure because they are
	// configuration + connection state, not QuestDB state.
	router := app.newHTTPRouter(app.derivStore, app.derivStore, app.derivStore)
	rec := newHealthRecorder(t, router)
	assertHealthComponent(t, rec, "expected_scopes", "starting")
	assertHealthComponent(t, rec, "coverage", "starting")
	assertHealthComponent(t, rec, "cold_write", "absent")

	failing, freshness := app.sampleFleetLivenessFromConfig(t.Context(), app.consumerLatestTimestamp(), nil, time.Now())
	app.metricsCollector.SetFleetLiveness(failing, freshness)
	rec = newHealthRecorder(t, router)
	assertHealthComponent(t, rec, "fleet_liveness", "starting")

	// With no durable tier there is no retention manager to license, and the license must
	// stay false: no destructive pass can be running behind a dependency failure.
	app.releaseRetentionUnderMigrationLicence(t.Context())
	if app.retentionMgr != nil {
		t.Fatal("a cold-tier failure still built a retention manager")
	}
	if app.gatesApproved == nil || app.gatesApproved.Load() {
		t.Fatal("the retention license was granted although there is no durable tier to prune")
	}
}

func newHealthRecorder(t *testing.T, router http.Handler) *httpapi.HealthResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil)
	router.ServeHTTP(rec, req)
	var body httpapi.HealthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode /health: %v", err)
	}
	return &body
}

func assertHealthComponent(t *testing.T, body *httpapi.HealthResponse, name, state string) {
	t.Helper()
	for _, c := range body.Components {
		if c.Name == name {
			if string(c.State) != state {
				t.Fatalf("health component %q = %q (%s), want %q", name, c.State, c.Reason, state)
			}
			return
		}
	}
	t.Fatalf("health reported no %q component: %+v", name, body.Components)
}

// TestLifecycle_ColdTierUnavailableDoesNotDisarmPreflightOrWatchdog is the negative half
// of the degradation contract: the failures that must still refuse the boot or restart
// the producer keep doing so when QuestDB is down.
func TestLifecycle_ColdTierUnavailableDoesNotDisarmPreflightOrWatchdog(t *testing.T) {
	cfg, _ := lifecycleConfig(t, func(c *config.Config) {
		c.QuestDB = config.QuestDBConfig{Enabled: true}
	})
	app, err := NewApp(cfg, candle.NewTimeframeRegistry(cfg.CandleEngine.Timeframes), nil,
		withColdTierConstructor(func(config.QuestDBConfig) (*questdb.Store, error) {
			return nil, errors.New("cold unavailable")
		}))
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	t.Cleanup(func() { _ = app.Shutdown(context.Background()) })

	// The eviction guard is a comparison of server facts and resident keys, none of which
	// needs QuestDB. It must still refuse the boot, and the refusal must happen before the
	// retention license exists.
	app.serverProbe = staticServerProbe{policy: "volatile-ttl"}
	if err := app.redisStore.Client().ZAdd(context.Background(),
		"candles:binance:future:BTC/USDT:1d", redis.Z{Score: 1, Member: "a"}).Err(); err != nil {
		t.Fatalf("seed immortal family: %v", err)
	}
	err = app.Run(context.Background())
	if !errors.Is(err, config.ErrStartupGate) {
		t.Fatalf("Run = %v, want the startup gate refusal", err)
	}
	if !strings.Contains(err.Error(), "eviction-policy-vs-immortal-families") {
		t.Fatalf("the refusal does not name the eviction guard: %v", err)
	}
	if app.gatesApproved == nil || app.gatesApproved.Load() {
		t.Fatal("the retention license survived a refused preflight — the cold-tier failure downgraded a guard")
	}
	if app.streamCtx != nil || app.httpServer != nil {
		t.Fatal("Run started serving after the preflight refused")
	}

	// The kline watchdog is local state + a controlled transport. It must keep replacing a
	// dead feed with the durable tier still down.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.streamCtx = ctx
	const (
		exchange   = "binance"
		marketType = "future"
		symbol     = "BTC/USDT"
	)
	old := &lifecycleKlineFeed{have: true, restart: true}
	app.klineFeed = old
	app.klineScope = exchange + ":" + marketType
	app.klineSymbols = []string{symbol}
	fresh := &lifecycleKlineFeed{have: true}
	app.newKlineFeed = func(context.Context, string, string, string, func(string, candle.Candle)) klineFeed {
		return fresh
	}
	if app.restartKlineFeedIfSilent(t.Context(), exchange, marketType) {
		t.Fatal("watchdog stopped while a replacement was available")
	}
	if app.klineFeed != fresh || !old.stopped.Load() {
		t.Fatal("cold-tier failure disabled the kline watchdog")
	}
}

// TestLifecycle_PublisherFailureDegradesWithoutDisablingGuards proves the last dependency
// failure: a Redis transport that refuses every publish is a telemetry loss, not an
// ingestion or lifecycle loss. Closes still persist, the guards stay armed, and the
// watchdog still restores a dead producer.
func TestLifecycle_PublisherFailureDegradesWithoutDisablingGuards(t *testing.T) {
	cfg, _ := lifecycleConfig(t)
	app := newLifecycleApp(t, cfg)

	failing := &lifecycleFailingPublisher{}
	app.realtimeProducer.Stop()
	app.realtimeProducer = realtime.NewAsyncProducer(failing, cfg.RealtimeChannelPrefix, func() int64 {
		return time.Now().UnixMilli()
	})
	t.Cleanup(func() { app.realtimeProducer.Stop() })

	key := engine.CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}
	base := candle.BucketTimestamp(time.Now().Add(-24*time.Hour).UnixMilli(), "5m")
	start := time.Now()
	for i := 0; i < 64; i++ {
		app.onCandleClose(key, candle.Candle{
			Timestamp: base + int64(i)*5*60*1000,
			Open:      100, High: 101, Low: 99, Close: 100.5, Volume: float64(i + 1),
		})
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("a refusing publisher blocked the close path for %s", elapsed)
	}

	deadline := time.Now().Add(2 * time.Second)
	for failing.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if failing.calls.Load() == 0 {
		t.Fatal("the failing publisher was never called; the failure was dropped before the transport")
	}
	if got, ok := app.continuityTracker().LatestClosed(key); !ok || got != base+63*5*60*1000 {
		t.Fatalf("publisher failure stopped the continuity baseline: %d (present=%t)", got, ok)
	}
	hot, err := app.redisStore.GetCandlesAsc(context.Background(), key.Exchange, key.MarketType, key.Symbol, "5m", 64)
	if err != nil || len(hot) == 0 {
		t.Fatalf("publisher failure disabled the hot-tier close write: %v", err)
	}

	// Expected scopes, coverage and liveness are configured independently of the transport.
	app.metricsCollector.SetExpectedScopes([]string{"binance:future"})
	livenessFailing, freshness := app.sampleFleetLivenessFromConfig(t.Context(), app.consumerLatestTimestamp(), nil, time.Now())
	app.metricsCollector.SetFleetLiveness(livenessFailing, freshness)
	router := app.newHTTPRouter(app.derivStore, app.derivStore, app.derivStore)
	rec := newHealthRecorder(t, router)
	assertHealthComponent(t, rec, "expected_scopes", "starting")
	assertHealthComponent(t, rec, "coverage", "starting")
	assertHealthComponent(t, rec, "fleet_liveness", "starting")

	// The retention license is granted by the boot stages, not by the publisher. Install a
	// real manager over a fake executor so the assertion is about the license mechanism
	// itself, not about a nil-safe early return.
	app.retentionMgr = retention.NewManager(retention.ManagerOptions{
		Executor:      countingRetentionExecutor{},
		Policies:      []retention.Policy{{Table: "candles", RetentionDays: 365}},
		GatesApproved: app.gatesApproved.Load,
		Interval:      time.Hour,
	})
	app.releaseRetentionUnderMigrationLicence(t.Context())
	if app.gatesApproved == nil || !app.gatesApproved.Load() {
		t.Fatal("publisher failure disabled the retention license")
	}
	deadlineRetention := time.Now().Add(time.Second)
	for app.retentionMgr.Passes() == 0 && time.Now().Before(deadlineRetention) {
		time.Sleep(5 * time.Millisecond)
	}
	if app.retentionMgr.Passes() == 0 {
		t.Fatal("the licensed retention loop did not run after a publisher failure")
	}

	// Watchdog: a dead feed is still replaced after the publisher has failed.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.streamCtx = ctx
	old := &lifecycleKlineFeed{have: true, restart: true}
	app.klineFeed = old
	app.klineScope = "binance:future"
	app.klineSymbols = []string{"BTC/USDT"}
	fresh := &lifecycleKlineFeed{have: true}
	app.newKlineFeed = func(context.Context, string, string, string, func(string, candle.Candle)) klineFeed {
		return fresh
	}
	if app.restartKlineFeedIfSilent(t.Context(), "binance", "future") {
		t.Fatal("watchdog stopped while a replacement was available")
	}
	if app.klineFeed != fresh || !old.stopped.Load() {
		t.Fatal("publisher failure disabled the kline watchdog")
	}
}
