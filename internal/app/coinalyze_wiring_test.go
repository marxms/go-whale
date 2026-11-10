package app

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/inbound/scheduler"
	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/derivatives/collector"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// THE WIRING, and what it refuses to do.
//
// The point is that the collector exists in the process, and not only in tests. But wiring it in
// cannot mean crashing: the streamer serves price and derivatives from the existing source, and
// refusing the whole boot because of a SECOND source's credential would trade a degradation for a
// crash.

// productionTimeframeRegistry is `candle_engine.timeframes` as configuration delivers it when nobody
// overrides it. That THESE are the eight is asserted where they are born —
// `internal/platform/config/config_test.go`, in the `expectedTFs` case —; here they are INPUT, and
// what is asserted is what the derivation does with them.
func productionTimeframeRegistry() []string {
	return []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"}
}

func appWithRegistry(timeframes []string) *App {
	return &App{cfg: config.Config{CandleEngine: config.CandleEngineConfig{Timeframes: timeframes}}}
}

// DURABLE COVERAGE IS DERIVED, AND THE MUTANT THIS CASE KILLS IS THE RETURN OF THE LITERAL.
//
// A typed list answers the SAME thing for any registry — it is that invariance the case attacks:
// two different registries must produce two different coverages, and each must be exactly the
// intersection with the declared support.
func TestDurableCoverageIsDerivedFromTheRegistryAndNotFromALiteral(t *testing.T) {
	tests := []struct {
		name     string
		registry []string
		want     []string
	}{
		{
			// The production registry. Seven of the eight have an equivalent in the durable source;
			// only the weekly does not, and it is by derivation that it stays out.
			name:     "production registry",
			registry: productionTimeframeRegistry(),
			want:     []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d"},
		},
		{
			// A SMALLER REGISTRY PRODUCES A SMALLER COVERAGE. A literal would return the seven here
			// too, and that is how it dies.
			name:     "reduced registry",
			registry: []string{"5m", "1h"},
			want:     []string{"5m", "1h"},
		},
		{
			// AND THE ORDER IS THE REGISTRY'S, not one invented here.
			name:     "the registry order is preserved",
			registry: []string{"1d", "1w", "5m"},
			want:     []string{"1d", "5m"},
		},
		{
			// A REGISTRY WITH NOTHING SERVABLE DOES NOT BECOME COVERAGE BY OMISSION.
			name:     "nothing servable",
			registry: []string{"1w", "3d"},
			want:     []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := appWithRegistry(tt.registry).durableTimeframes()
			if len(got) != len(tt.want) {
				t.Fatalf("coverage = %v, want %v", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("coverage = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// `30m` IS THE DEFECT THAT OPENED THIS FEATURE, and it must be covered.
//
// It was in the product registry and outside the literal, and lost 254 hours of series when the cap
// reached the only copy. A case of its own because the one above could keep passing with a
// derivation that excluded the short intervals.
func TestDurableCoverageIncludesTheTimeframeThatLostItsOnlyCopy(t *testing.T) {
	cobertura := appWithRegistry(productionTimeframeRegistry()).durableTimeframes()
	for _, want := range []string{"30m", "5m", "15m", "2h"} {
		found := false
		for _, got := range cobertura {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Errorf("%s was left without a durable tier: %v", want, cobertura)
		}
	}
}

// THE MISSING CONJUNCTION — every covered timeframe must be SERVABLE by the provider.
//
// `1w` was once in the list and the collection logged a WARN every cycle, forever: the provider's
// interval map ends at `daily`. Each half had a test — the list was asserted, the map was asserted —
// and nobody asserted that one fits the other. It is the same failure mode as the audit that was not
// called by the cycle, and it only appeared when running.
//
// The derivation makes that structural, and the ratchet stays: it refuses a derivation rule that
// started admitting `periodUndeclared` — "we never asked" — as coverage.
func TestEveryCollectedTimeframeIsServableByTheProvider(t *testing.T) {
	for _, timeframe := range appWithRegistry(productionTimeframeRegistry()).durableTimeframes() {
		if _, err := collector.CoinalyzeInterval(timeframe); err != nil {
			t.Errorf("%s is in the durable coverage and the provider does not serve it: %v", timeframe, err)
		}
	}
}

// THE CADENCE CANNOT BE SHORTER THAN THE SHORTEST BUCKET IT FILLS: pulling history faster than
// that spends budget rewriting the same point.
func TestCoinalyzeCycleIsNotFasterThanItsShortestBucket(t *testing.T) {
	if collector.CoinalyzeCycleInterval < 60*60*1e9 {
		t.Fatalf("the cycle (%v) is faster than the 1h bucket it fills", collector.CoinalyzeCycleInterval)
	}
}

// THE DECLARED BUDGET IS THE ONE MEASURED AT THE PROVIDER, and accounting is local because a 200
// response from this API carries no limit header — the only signal is `Retry-After` on a 429.
//
// AND IT HAS ONE PLACE. The wiring must not declare its own 40, because it is the production
// consumer — the collector's admission floor reads the symbol from the other package. Two equal
// sources diverge on the first edit, and the divergence is silent: the floor would report against one
// value and the bucket would spend against another.
//
// THE CASE IS ASSERTED THROUGH THE BUCKET'S BEHAVIOR, not by reading: the effective ceiling comes out
// on the refusal line that local accounting cannot explain, which is where this process publishes
// it.
func TestCoinalyzeBudgetIsThePublishedRate(t *testing.T) {
	opts := coinalyzeBudgetOptions()
	if opts.CallsPerMinute != 0 {
		t.Fatalf("the wiring declares %d call(s)/min of its own — the ceiling has ONE place "+
			"(`coinalyze.CoinalyzeCallsPerMinute`) and the bucket already inherits it as a default; a "+
			"second declaration is the copy the design removes", opts.CallsPerMinute)
	}
	if coinalyze.CoinalyzeCallsPerMinute != 40 {
		t.Fatalf("the measured budget is 40/min, it is %d", coinalyze.CoinalyzeCallsPerMinute)
	}

	// THE EFFECTIVE CEILING of the bucket built from the SAME declaration production uses —
	// `coinalyze.NewCoinalyzeBudget(coinalyzeBudgetOptions())`. Only the two time seams (clock and
	// wait) are rewritten here: without them `Do` would really sleep the refill. The ceiling is not
	// rewritten — it comes from inside the wiring.
	log, sink := logger.NewCapture(logger.LevelDebug)
	opts.Attempts = 1
	opts.Now = relogioQueAnda(time.Second)
	opts.Wait = func(context.Context, time.Duration) error { return nil }
	opts.Log = log
	budget := coinalyze.NewCoinalyzeBudget(opts)

	err := budget.Do(context.Background(), []string{"BTC/USDT"}, func(context.Context) error {
		return &coinalyze.CoinalyzeError{Status: http.StatusTooManyRequests, Message: "slow down"}
	})
	if err == nil {
		t.Fatal("the bucket swallowed the rate refusal")
	}

	teto := fmt.Sprintf("ceiling_per_minute=%d", coinalyze.CoinalyzeCallsPerMinute)
	if _, achou := lineContaining(sink, teto); !achou {
		t.Fatalf("the wiring's bucket does not budget %s — it stopped inheriting the single symbol; "+
			"captured: %v", teto, sink.Records())
	}
}

// relogioQueAnda returns a clock that advances `passo` on EVERY read.
//
// The bucket's refill credits by elapsed time, so a stopped clock would make `acquire` wait forever
// for the first token's balance.
func relogioQueAnda(passo time.Duration) func() time.Time {
	agora := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	return func() time.Time {
		atual := agora
		agora = agora.Add(passo)
		return atual
	}
}

// ============================================================================
// THE CADENCE, ADMISSION AND FLEET READINESS
//
// The RULES live in internal/derivatives/collector
// (coinalyze_policy_test.go). These cases cover the wiring: a visible refusal in the log and the
// production router.
// ============================================================================

// THE COUNTERWEIGHT TO REMOVING THE BOOT GUARD.
//
// `Validate` stopped killing the process for a missing `GOWHALE_COINALYZE_API_KEY` — the streamer
// serves the existing source, and bringing everything down for a SECOND source's credential trades
// degradation for a crash. What keeps that removal from becoming silence is THIS line: the absence
// appears at boot, at WARN, naming the variable.
//
// The level matters and is not decoration: the process default is `Error`, so an `Info` or `Debug`
// here would appear nowhere, and the operator would see HTTP 401 every cycle with the series not
// growing — a symptom that looks like "the provider has no data".
func TestCoinalyzeStartLogsTheRefusalAtWarnNamingTheVariable(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	app := &App{log: log, cfg: config.Config{
		Coinalyze: config.CoinalyzeConfig{Enabled: true, APIKey: ""},
		Exchanges: []config.ExchangeConfig{{MarketType: "future", Symbols: []string{"BTC/USDT"}}},
	}}
	app.startCoinalyze(context.Background())

	level, found := sink.LevelOf("GOWHALE_COINALYZE_API_KEY")
	if !found {
		t.Fatalf("the boot did not say the key is missing; captured: %v", sink.Records())
	}
	if level != logger.LevelWarn {
		t.Errorf("the refusal came out at %s; below WARN it does not appear at the process default level", level)
	}
}

// NEGATIVE CONTROL: with collection DISABLED by configuration, the line must not blame the key —
// otherwise the test above would pass by accident on every start.
func TestCoinalyzeStartDoesNotBlameTheKeyWhenDisabled(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	app := &App{log: log, cfg: config.Config{
		Coinalyze: config.CoinalyzeConfig{Enabled: false, APIKey: ""},
		Exchanges: []config.ExchangeConfig{{MarketType: "future", Symbols: []string{"BTC/USDT"}}},
	}}
	app.startCoinalyze(context.Background())

	if _, found := sink.LevelOf("GOWHALE_COINALYZE_API_KEY"); found {
		t.Errorf("disabled collection blamed the key: %v", sink.Records())
	}
	if _, found := sink.LevelOf("GOWHALE_COINALYZE_ENABLED"); !found {
		t.Errorf("disabled collection did not say why: %v", sink.Records())
	}
}

// THE PRODUCTION ASSEMBLY, ASSERTED — the part that failed three times.
//
// The reader built and not wired, the batch route outside the reader, and the durable timeframe list
// not declared: none of the three appears in a unit test of the part, and all three came from
// independent verification as LIVE mutants. `newHTTPRouter` exists so the assembly has somewhere to
// be checked.
func TestProductionRouter_DeclaresTheCollectedTimeframesToTheReadPath(t *testing.T) {
	app := appWithRegistry(productionTimeframeRegistry())
	app.defaultExchange = "binance"
	app.defaultMarketType = "future"
	router := app.newHTTPRouter(&routerDerivStore{}, &routerDerivStore{}, &routerDerivStore{})
	if router == nil {
		t.Fatal("the assembly returned a nil router")
	}

	// THE SAME DERIVATION governs collection and read. A collected timeframe the read does not
	// declare is deep history never fetched; a declared one the collection does not write is a
	// partition opened for nothing.
	for _, tf := range app.durableTimeframes() {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet,
			"/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe="+tf+"&limit=10", nil))
		if rec.Code != http.StatusOK {
			t.Errorf("%s: the assembled route returned %d", tf, rec.Code)
		}
	}
}

// AND THE COVERAGE REACHES THE HANDLER, it does not stay derived and ignored: a timeframe OUTSIDE it
// must not reach the durable tier. Without this assertion, removing `a.durableTimeframes()...` from
// the assembly passed — it was the mutant that survived.
func TestProductionRouter_ATimeframeOutsideTheListNeverReachesTheDurableTier(t *testing.T) {
	cold := &countingColdReader{}
	hot := &routerDerivStore{}
	app := appWithRegistry(productionTimeframeRegistry())
	app.defaultExchange = "binance"
	app.defaultMarketType = "future"
	app.windowOverride = derivatives.NewDerivativesWindowReader(hot, cold)
	router := app.newHTTPRouter(hot, hot, hot)

	// THE OUTSIDER IS NOW `1w`, not `5m`/`30m`. The derivation now covers the short ones, which is
	// exactly the defect that opened the feature; the weekly stays out because the durable source
	// does not serve it in any feed.
	for _, tf := range []string{"1w"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet,
			"/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe="+tf+"&limit=800", nil))
	}
	if cold.calls != 0 {
		t.Errorf("timeframes outside the coverage queried the durable tier %d time(s) — the derivation did not reach the handler", cold.calls)
	}

	for _, tf := range app.durableTimeframes() {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet,
			"/api/batch/derivatives-series?symbols=BTC%2FUSDT&timeframe="+tf+"&limit=800", nil))
	}
	if cold.calls == 0 {
		t.Error("no coverage timeframe reached the durable tier — the door refuses everything")
	}
}

// routerDerivStore is the stub ZSET, with the minimum the assembly exercises.
type routerDerivStore struct{}

func (s *routerDerivStore) SaveSnapshot(context.Context, *derivatives.DerivativesSnapshot) error {
	return nil
}

func (s *routerDerivStore) SaveSeriesOnly(context.Context, *derivatives.DerivativesSnapshot, string) error {
	return nil
}

func (s *routerDerivStore) GetLatestSnapshot(context.Context, string, string, string) (*derivatives.DerivativesSnapshot, error) {
	return nil, nil
}

func (s *routerDerivStore) GetSnapshotSeries(_ context.Context, _, _, _, _ string, _ int) ([]*derivatives.DerivativesSnapshot, error) {
	v := 1.0
	return []*derivatives.DerivativesSnapshot{{
		Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT",
		Timestamp: time.Now().UTC().Add(-24 * time.Hour).UnixMilli(),
		Source:    "venue", OpenInterestBase: &v,
	}}, nil
}

func (s *routerDerivStore) GetBaseline(context.Context, string, string, string, string) (*redisstore.DerivativesBaseline, error) {
	return nil, nil
}

func (s *routerDerivStore) PriceIncrement(context.Context, string, string, string, string, int64) (*float64, string, error) {
	return nil, "", nil
}

// countingColdReader counts how many times the durable tier was queried.
type countingColdReader struct{ calls int }

func (c *countingColdReader) QueryDerivativesRange(context.Context, derivatives.DerivativesScope, time.Time, time.Time) ([]*derivatives.DerivativesSnapshot, error) {
	c.calls++
	return nil, nil
}

// THE FLEET WAIT — what made a deploy deliver a thirtieth of the feature for an hour.
//
// MEASURED on the LAN: the first cycle is immediate on purpose, but at that instant discovery has
// not populated `active_symbols`. The deep pass covered the static list (one symbol) while the screen
// showed thirty, and the next cycle is only an hour away.
func TestAwaitFleet_ReturnsWhenThereIsNoRegistryToWaitFor(t *testing.T) {
	app := &App{}
	// Without `redisStore` there is no fleet to wait for — a static-fleet deployment.
	inicio := time.Now()
	app.awaitFleet(context.Background())
	if decorrido := time.Since(inicio); decorrido > time.Second {
		t.Errorf("without a fleet registry the wait took %s; it has nothing to wait for", decorrido)
	}
}

// AND IT HONORS CANCELLATION: a shutdown during the wait cannot hold the process for three
// minutes.
func TestAwaitFleet_HonoursCancellation(t *testing.T) {
	app := &App{redisStore: &redisstore.RedisStore{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pronto := make(chan struct{})
	go func() { app.awaitFleet(ctx); close(pronto) }()

	select {
	case <-pronto:
	case <-time.After(5 * time.Second):
		t.Fatal("the wait ignored cancellation; a shutdown would hold the process for three minutes")
	}
}

// THE CEILING IS SMALLER THAN THE CYCLE INTERVAL. A wait longer than the cycle itself would make the
// first pass miss the next tick, and "immediate" would become "later".
func TestAwaitFleet_TheWaitFitsInsideOneCycle(t *testing.T) {
	if collector.CoinalyzeFleetWait >= collector.CoinalyzeCycleInterval {
		t.Fatalf("the wait (%s) does not fit in one cycle (%s)", collector.CoinalyzeFleetWait, collector.CoinalyzeCycleInterval)
	}
}

// AND THE SETTLE INTERVAL fits twice inside the ceiling — otherwise the wait performs a single read.
func TestAwaitFleet_TheSettleIntervalFitsInsideTheWait(t *testing.T) {
	if collector.CoinalyzeFleetSettle*2 >= collector.CoinalyzeFleetWait {
		t.Fatalf("settle %s does not fit twice in ceiling %s", collector.CoinalyzeFleetSettle, collector.CoinalyzeFleetWait)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE CYCLE-DURATION SENSOR
//
// The defect these cases hold: `PollOnce` runs SYNCHRONOUSLY inside the select of the ticker that
// schedules it, and nothing measured what it cost. A cycle longer than the interval does not run
// late — the runtime DISCARDS the ticks that fall inside it —, so the overrun was invisible by
// construction. The arithmetic: 500 symbols × 3 timeframes × 3 endpoints = 4,500 symbol-calls at
// 40/min = 112min30s against a 60-minute ticker.
// ─────────────────────────────────────────────────────────────────────────────

// clockOf returns a clock that advances `elapsed` between the first and second reads, which are
// exactly the two the sensor makes.
func clockOf(elapsed time.Duration) func() time.Time {
	base := time.Date(2026, 8, 25, 3, 0, 0, 0, time.UTC)
	reads := 0
	return func() time.Time {
		reads++
		if reads == 1 {
			return base
		}
		return base.Add(elapsed)
	}
}

// segundosBatem compares histogram seconds with tolerance.
//
// The histogram sum is a process-wide float64 accumulator: subtracting the before value from the
// after value carries the rounding of samples other cases already added (measured:
// 45.000000000000014 for a 45s sample). The tolerance is one microsecond — loose for floating-point
// noise, tight enough to separate 45 from 90 and from zero.
func segundosBatem(got, want float64) bool { return math.Abs(got-want) < 1e-6 }

// lineContaining returns the first captured line containing `substr`.
func lineContaining(sink *logger.Sink, substr string) (string, bool) {
	for _, r := range sink.Records() {
		if strings.Contains(r.Text(), substr) {
			return r.Text(), true
		}
	}
	return "", false
}

// The duration of EVERY cycle becomes a continuous signal, and the published number is the measured
// one.
func TestMeasureCoinalyzeCycle_PublishesTheDurationOfEveryCycle(t *testing.T) {
	rec := newAppTestMetrics()
	report := scheduler.Measure(context.Background(),
		func(context.Context) error { return nil }, time.Hour, clockOf(90*time.Second))
	(&App{metrics: rec.appPort()}).reportCoinalyzeCycle(report)

	if report.Elapsed != 90*time.Second {
		t.Errorf("the cycle measured %s, and the clock advanced 90s", report.Elapsed)
	}

	if delta := rec.cycleCount(); delta != 1 {
		t.Errorf("%d duration sample(s) per cycle, wanted 1 — duration is not a continuous signal", delta)
	}
	if delta := rec.cycleSum(); !segundosBatem(delta, 90) {
		t.Errorf("the histogram received %v seconds, and the cycle took 90 — the published number is not the measured one", delta)
	}
}

// A FAILING cycle is also measured. "Every cycle" includes those returning an error, and measuring
// only the clean ones would hide exactly the cycle that was slow because it was failing.
func TestMeasureCoinalyzeCycle_AFailedCycleIsStillMeasured(t *testing.T) {
	rec := newAppTestMetrics()
	report := scheduler.Measure(context.Background(),
		func(context.Context) error { return errors.New("provider down") },
		time.Hour, clockOf(45*time.Second))
	(&App{metrics: rec.appPort()}).reportCoinalyzeCycle(report)

	if report.Err == nil {
		t.Fatal("the cycle report swallowed the collection error")
	}
	if delta := rec.cycleCount(); delta != 1 {
		t.Errorf("%d sample(s) for a cycle that failed, wanted 1", delta)
	}
	if delta := rec.cycleSum(); !segundosBatem(delta, 45) {
		t.Errorf("the failed cycle published %v seconds, and it took 45", delta)
	}
}

// The cycle exceeding the interval is reported WITH THE MEASURED NUMBER, and in a countable way.
//
// This is the case that kills the mutant that measures the duration and does not report the overrun.
// It cannot be covered by the error report: a cycle that overruns returns `nil`, because overrunning
// is not failing.
func TestMeasureCoinalyzeCycle_OverrunIsReportedWithTheMeasuredNumber(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	rec := newAppTestMetrics()

	// THE ARITHMETIC: 4,500 symbol-calls at 40/min against a one-hour ticker.
	const medido = 112*time.Minute + 30*time.Second
	report := scheduler.Measure(context.Background(),
		func(context.Context) error { return nil }, time.Hour, clockOf(medido))
	(&App{log: log, metrics: rec.appPort()}).reportCoinalyzeCycle(report)

	if report.Err != nil {
		t.Fatalf("the overrunning cycle returned an error (%v) — the case would measure the wrong branch", report.Err)
	}
	if !report.Overran() {
		t.Fatalf("a cycle of %s against an interval of %s was not considered an overrun", medido, time.Hour)
	}
	if delta := rec.overrun; delta != 1 {
		t.Errorf("the overrun added %v to the counter, wanted 1 — a log line does not aggregate", delta)
	}

	nivel, achou := sink.LevelOf("overran")
	if !achou {
		t.Fatalf("the overrun was not reported; captured: %v", sink.Records())
	}
	if nivel != logger.LevelWarn {
		t.Errorf("the overrun came out at %s; below WARN it does not appear at the process default level", nivel)
	}

	linha, _ := lineContaining(sink, "overran")
	// THE MEASURED NUMBER, not only the fact: the duration and how many cadences it consumed.
	if !strings.Contains(linha, "1h52m30s") {
		t.Errorf("the overrun report does not carry the measured duration (1h52m30s): %q", linha)
	}
	if !strings.Contains(linha, "1.88") {
		t.Errorf("the overrun report does not carry how many cadences the cycle consumed (1.88×): %q", linha)
	}
}

// NEGATIVE CONTROL: a cycle that FITS must not be reported as an overrun, otherwise the case above
// would pass on every start.
func TestMeasureCoinalyzeCycle_ACycleThatFitsIsNotReportedAsOverrun(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	rec := newAppTestMetrics()

	report := scheduler.Measure(context.Background(),
		func(context.Context) error { return nil }, time.Hour, clockOf(59*time.Minute))
	(&App{log: log, metrics: rec.appPort()}).reportCoinalyzeCycle(report)

	if report.Overran() {
		t.Errorf("a 59min cycle against a 60min interval was called an overrun")
	}
	if delta := rec.overrun; delta != 0 {
		t.Errorf("a fitting cycle added %v to the overrun counter", delta)
	}
	if _, achou := sink.LevelOf("overran"); achou {
		t.Errorf("a fitting cycle was reported as an overrun: %v", sink.Records())
	}
}

// THE CADENCE THE SENSOR COMPARES IS THE SAME ONE THE CYCLE IS SCHEDULED AT.
//
// Without this tie, measuring against an interval different from the `time.Ticker`'s would give a
// sensor that never accuses — the previous state with one more number.
func TestRunCoinalyzeCycles_MeasuresAgainstTheSchedulingInterval(t *testing.T) {
	if collector.CoinalyzeCycleInterval <= 0 {
		t.Fatalf("the cycle cadence is %s; a non-positive interval disables overrun detection", collector.CoinalyzeCycleInterval)
	}
	fits := scheduler.Report{Elapsed: collector.CoinalyzeCycleInterval, Interval: collector.CoinalyzeCycleInterval}
	if fits.Overran() {
		t.Error("a cycle lasting exactly the interval was called an overrun; the ticker only loses a tick above it")
	}
	over := scheduler.Report{Elapsed: collector.CoinalyzeCycleInterval + time.Second, Interval: collector.CoinalyzeCycleInterval}
	if !over.Overran() {
		t.Error("a cycle one second longer than the interval is not an overrun — it is exactly when the tick falls inside")
	}
	if got := over.LapFactor(); got <= 1 {
		t.Errorf("the cadence factor of an overrunning cycle is %v, and must exceed 1", got)
	}
	semCadencia := scheduler.Report{Elapsed: time.Hour}
	if semCadencia.LapFactor() != 0 {
		t.Errorf("a report without a declared cadence returned factor %v; it has nothing to compare against", semCadencia.LapFactor())
	}
}
