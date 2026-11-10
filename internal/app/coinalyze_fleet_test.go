package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	httpapi "github.com/marxms/go-whale/internal/adapters/inbound/httpapi"
	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/derivatives/collector"

	"github.com/marxms/go-whale/internal/platform/config"
)

// THE APP HALF OF THE CONJUNCTION RULE — the one that was missing.
//
// Returning `nil` from either `coinalyzeFleet` or `fleetCounts` returns the system to "1 of 30
// active" with the suite green AND both reachability ratchets green. The poller half had a test
// (`coinalyze_seed_test.go`); the app half could not, because it required a Redis.
//
// The remedy is a seam.

// fakeFleetRegistry is the stub subscription registry.
type fakeFleetRegistry struct {
	mu        sync.Mutex
	porEscopo map[string][]string
	erros     map[string]error
	leituras  int
}

func (f *fakeFleetRegistry) GetActiveSymbols(_ context.Context, exchange, marketType string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leituras++
	chave := exchange + ":" + marketType
	if err, ok := f.erros[chave]; ok {
		return nil, err
	}
	return f.porEscopo[chave], nil
}

func (f *fakeFleetRegistry) set(chave string, symbols []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.porEscopo == nil {
		f.porEscopo = map[string][]string{}
	}
	f.porEscopo[chave] = symbols
}

// frotaDoLan reproduces the measured configuration: one exchange by discovery and one static.
func frotaDoLan() []config.ExchangeConfig {
	return []config.ExchangeConfig{
		{ID: "binance", MarketType: "future", Discovery: config.DiscoveryConfig{Enabled: true, MaxAssets: 30}},
		{ID: "bingx", MarketType: "future", Symbols: []string{"BTC/USDT"}},
		{ID: "binance", MarketType: "spot", Discovery: config.DiscoveryConfig{Enabled: true}},
	}
}

func appComFrota(reg fleetRegistry) *App {
	return &App{cfg: config.Config{Exchanges: frotaDoLan()}, fleetOverride: reg}
}

// TestCoinalyzeFleet_ReadsEveryFutureScopeAndDeduplicates.
func TestCoinalyzeFleet_ReadsEveryFutureScopeAndDeduplicates(t *testing.T) {
	reg := &fakeFleetRegistry{}
	reg.set("binance:future", []string{"ETH/USDT", "BTC/USDT", "SOL/USDT"})
	reg.set("bingx:future", []string{"BTC/USDT"})
	// SPOT must not enter: this collector is for futures.
	reg.set("binance:spot", []string{"DOGE/USDT"})

	got := appComFrota(reg).coinalyzeFleet(context.Background())

	quer := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}
	if len(got) != len(quer) {
		t.Fatalf("fleet %v, want %v", got, quer)
	}
	for i := range quer {
		if got[i] != quer[i] {
			t.Fatalf("fleet %v, want %v (sorted and deduplicated)", got, quer)
		}
	}
}

// AN UNREADABLE EXCHANGE DOES NOT ERASE THE OTHERS. A momentary registry unavailability for one of
// them must not zero the whole fleet's collection.
func TestCoinalyzeFleet_AnUnreadableScopeDoesNotBlankTheRest(t *testing.T) {
	reg := &fakeFleetRegistry{erros: map[string]error{"bingx:future": errors.New("redis down")}}
	reg.set("binance:future", []string{"ETH/USDT"})

	got := appComFrota(reg).coinalyzeFleet(context.Background())
	if len(got) != 1 || got[0] != "ETH/USDT" {
		t.Fatalf("fleet %v; the readable exchange had to survive", got)
	}
}

// WITHOUT A REGISTRY it returns empty, which is what makes the poller fall back to the static list.
func TestCoinalyzeFleet_WithoutARegistryReturnsNothing(t *testing.T) {
	if got := (&App{cfg: config.Config{Exchanges: frotaDoLan()}}).coinalyzeFleet(context.Background()); got != nil {
		t.Errorf("without a registry the fleet came back %v", got)
	}
}

// TestFleetCounts_CountsPerExchangeAndOnlyFutures.
func TestFleetCounts_CountsPerExchangeAndOnlyFutures(t *testing.T) {
	reg := &fakeFleetRegistry{}
	reg.set("binance:future", make([]string, 30))
	reg.set("bingx:future", make([]string, 1))
	reg.set("binance:spot", make([]string, 99))

	got := appComFrota(reg).fleetCounts(context.Background())

	if got["binance"] != 30 || got["bingx"] != 1 {
		t.Fatalf("count %v, want binance=30 bingx=1", got)
	}
	// Spot shares the `binance` id; if it entered, the count would be 129.
	if got["binance"] == 129 {
		t.Error("the spot market was counted in a futures collector")
	}
}

// THE FULL CONJUNCTION: the count feeds `fleetReady`, and the pair decides the wait. The instant
// MEASURED on the LAN is the middle case.
func TestFleetCountsFeedsReadiness_AtTheMeasuredInstant(t *testing.T) {
	reg := &fakeFleetRegistry{}

	app := appComFrota(reg)
	scopes := app.fleetScopes()

	// 11:09:41 — only the static one published.
	reg.set("bingx:future", []string{"BTC/USDT"})
	if collector.FleetReady(scopes, app.fleetCounts(context.Background())) {
		t.Error("ready with a thirtieth of the fleet — that is the measured defect")
	}

	// 11:28:03 — discovery published TWO of thirty.
	reg.set("binance:future", make([]string, 2))
	if collector.FleetReady(scopes, app.fleetCounts(context.Background())) {
		t.Error("ready with two of thirty — discovery publishes incrementally")
	}

	// The whole target.
	reg.set("binance:future", make([]string, 30))
	if !collector.FleetReady(scopes, app.fleetCounts(context.Background())) {
		t.Error("the declared target was reached and the wait continued")
	}
}

// TestAwaitFleet_BlocksUntilTheDiscoveryPublishes — the wait measured as BEHAVIOR.
//
// The previous cases asserted degenerate exits (no registry, cancellation) and relations between
// constants, and ALL are satisfied by a no-op. This one exercises the wait for real: the fleet
// arrives in parts and the wait only ends at the target.
func TestAwaitFleet_BlocksUntilTheDiscoveryPublishes(t *testing.T) {
	reg := &fakeFleetRegistry{}
	reg.set("bingx:future", []string{"BTC/USDT"})
	reg.set("binance:future", make([]string, 2))

	app := appComFrota(reg)
	app.fleetSettle = 10 * time.Millisecond
	app.fleetWait = 5 * time.Second

	pronto := make(chan struct{})
	go func() { app.awaitFleet(context.Background()); close(pronto) }()

	// While the target does not arrive, the wait must NOT end.
	select {
	case <-pronto:
		t.Fatal("the wait ended with two of thirty — that is the defect this wait exists to fix")
	case <-time.After(120 * time.Millisecond):
	}

	reg.set("binance:future", make([]string, 30))
	select {
	case <-pronto:
	case <-time.After(3 * time.Second):
		t.Fatal("the target was published and the wait did not end")
	}
}

// AND IT GIVES UP AT THE CEILING, serving what exists — a deployment where discovery finds fewer
// eligible assets than the maximum cannot stay stuck forever.
func TestAwaitFleet_GivesUpAtTheDeadline(t *testing.T) {
	reg := &fakeFleetRegistry{}
	reg.set("binance:future", make([]string, 2)) // never reaches 30

	app := appComFrota(reg)
	app.fleetSettle = 5 * time.Millisecond
	app.fleetWait = 60 * time.Millisecond

	inicio := time.Now()
	app.awaitFleet(context.Background())
	decorrido := time.Since(inicio)

	if decorrido < 40*time.Millisecond {
		t.Errorf("the wait lasted %s; it had to try until the ceiling", decorrido)
	}
	if decorrido > 2*time.Second {
		t.Errorf("the wait lasted %s; the ceiling is %s", decorrido, app.fleetWait)
	}
	if reg.leituras < 2 {
		t.Errorf("the wait performed %d read(s); a no-op performs zero or one", reg.leituras)
	}
}

// THE HAPPY PATH OF startCoinalyze — the existing cases covered only REFUSALS.
//
// `coldDerivStore` returning `nil` survived, because the reachability ratchet sees the CALL and not
// the VALUE, and no test exercised a successful start. A durable WRITE tier switched off in silence
// is the whole second provenance writing only the ZSET again.
//
// The signal that the start happened is the fleet registry being READ: only the happy path
// dispatches the goroutine, and the first thing it does is `awaitFleet`. The context enters
// CANCELED so the goroutine dies at the first stop, without touching the network.
func TestStartCoinalyze_TheHappyPathDispatchesTheCycle(t *testing.T) {
	reg := &fakeFleetRegistry{}
	reg.set("binance:future", make([]string, 30))

	app := appComFrota(reg)
	app.cfg.Coinalyze = config.CoinalyzeConfig{Enabled: true, APIKey: "chave-de-teste"} //nolint:gosec // G101: test credential literal, not a secret.
	app.fleetSettle = time.Millisecond
	app.fleetWait = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	app.startCoinalyze(ctx)
	app.discoveryWg.Wait()

	reg.mu.Lock()
	leituras := reg.leituras
	reg.mu.Unlock()
	if leituras == 0 {
		t.Fatal("the fleet registry was not read; the start did not dispatch the cycle")
	}
}

// AND THE REFUSAL DISPATCHES NOTHING — without this control, "read the fleet" does not distinguish a
// start from anything.
func TestStartCoinalyze_ARefusalDispatchesNothing(t *testing.T) {
	reg := &fakeFleetRegistry{}
	reg.set("binance:future", make([]string, 30))

	app := appComFrota(reg)
	app.cfg.Coinalyze = config.CoinalyzeConfig{Enabled: true, APIKey: ""} // no credential

	// CANCELED CONTEXT, as in the sibling case. With a LIVE context, breaking the refusal did not
	// fail: it dispatched the goroutine, `discoveryWg.Wait()` hung and the package died with
	// `panic: test timed out` ten minutes later. A red that arrives as a hang is a red nobody reads.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	app.startCoinalyze(ctx)
	app.discoveryWg.Wait()

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.leituras != 0 {
		t.Errorf("the refusal read the fleet %d time(s); it had to exit first", reg.leituras)
	}
}

// TestColdDerivStore_CarriesTheDurableTierWhenItExists — the mutant that survived.
//
// The conversion only happens when there IS a store, and the reason is in the function comment: an
// interface carrying a nil pointer is NOT nil, and the `if w.cold == nil` on the other side would not
// catch it — the cycle would try to write and panic.
func TestColdDerivStore_CarriesTheDurableTierWhenItExists(t *testing.T) {
	semQuestDB := (&App{}).coldDerivStore()
	if semQuestDB != nil {
		t.Error("without QuestDB the durable tier came back non-nil; an interface with a nil pointer is not nil")
	}

	comQuestDB := (&App{qdbStore: &questdb.Store{}}).coldDerivStore()
	if comQuestDB == nil {
		t.Fatal("with QuestDB the durable tier came back nil — the second provenance writes only the ZSET again")
	}
}

// THE ORDER BETWEEN THE WAIT AND THE FIRST CYCLE — the mutant that survived everything.
//
// Moving `awaitFleet` to AFTER `PollOnce` is the LITERAL defect the wait exists to fix, and it passed
// the whole suite: the cases exercised the wait alone and the start alone, and none asserted that one
// happens BEFORE the other.
//
// The signal is the FLEET THE FIRST CYCLE SEES: the poller resolves one symbol at a time, and the
// resolution count is the fleet size at that instant. With the wait before, it sees thirty; with the
// wait after, it sees two.
//
// No network is touched: the catalog refuses every symbol, so `resolveSymbols` returns empty and
// `PollOnce` exits before any request.

// contandoResolver refuses every symbol and counts how many were asked of it.
type contandoResolver struct {
	mu  sync.Mutex
	por [][]string
}

func (c *contandoResolver) Resolve(_ context.Context, symbol string) (coinalyze.CoinalyzeMarket, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.por) == 0 {
		c.por = append(c.por, nil)
	}
	c.por[len(c.por)-1] = append(c.por[len(c.por)-1], symbol)
	return coinalyze.CoinalyzeMarket{}, errors.New("refused on purpose: this case does not touch the network")
}

func (c *contandoResolver) noPrimeiroCiclo() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.por) == 0 {
		return 0
	}
	return len(c.por[0])
}

type recusandoOrcamento struct{}

func (recusandoOrcamento) Do(_ context.Context, _ []string, _ func(context.Context) error) error {
	return nil
}

type descartandoStore struct{}

func (descartandoStore) WriteBatch(context.Context, derivatives.DerivativesBatch) error { return nil }

func TestRunCoinalyzeCycles_WaitsForTheFleetBeforeTheFirstCycle(t *testing.T) {
	reg := &fakeFleetRegistry{}
	reg.set("bingx:future", []string{"BTC/USDT"})
	reg.set("binance:future", trintaSimbolos(2))

	app := appComFrota(reg)
	app.fleetSettle = 5 * time.Millisecond
	app.fleetWait = 3 * time.Second

	catalogo := &contandoResolver{}
	poller := collector.NewCoinalyzePoller(collector.CoinalyzePollerOptions{
		Client:      coinalyze.NewCoinalyzeClient("chave-de-teste"),
		Catalog:     catalogo,
		Budget:      recusandoOrcamento{},
		Store:       descartandoStore{},
		Exchange:    "binance",
		MarketType:  "future",
		Symbols:     []string{"BTC/USDT"},
		SymbolsFunc: app.coinalyzeFleet,
		Timeframes:  []string{"1d"},
	})

	// Discovery completes the fleet AFTER the cycle has already started.
	go func() {
		time.Sleep(60 * time.Millisecond)
		reg.set("binance:future", trintaSimbolos(30))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	pronto := make(chan struct{})
	// NULL meter: this case measures the fleet wait, and a spend meter would require a live budget
	// without adding any assertion. The nil receiver is a declared contract.
	go func() { app.runCoinalyzeCycles(ctx, poller, nil); close(pronto) }()

	// Wait for the first cycle to happen, then shut down before the one-hour tick.
	deadline := time.After(4 * time.Second)
	for catalogo.noPrimeiroCiclo() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("the first cycle did not happen")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	cancel()
	<-pronto

	// 31 = the 30 from discovery plus BTC/USDT from the static exchange. With the wait AFTER the
	// cycle, the first one sees 3 (the two published plus the static one).
	if visto := catalogo.noPrimeiroCiclo(); visto != 31 {
		t.Fatalf("the first cycle saw %d symbol(s), want 31 — the wait did not happen BEFORE it", visto)
	}
}

// trintaSimbolos builds a fleet of distinct names, because resolution deduplicates.
func trintaSimbolos(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("SYM%02d/USDT", i))
	}
	return out
}

// THE PRODUCTION BRANCH OF THE SEAM.
//
// `fleetSource()` has two branches and all twelve cases enter through the test one
// (`fleetOverride`). Replacing the production one with `nil` leaves the suite green AND both ratchets
// green, with the production effect `coinalyzeFleet` → empty → poller on the static list → 1 of 30
// active: exactly the defect the seam exists to close.
//
// The ratchet is blind here and the reason is good: `GetActiveSymbols` remains reachable from
// `subscribe.go`, so nothing is orphaned. A new seam requires an assertion on the branch it does NOT
// traverse.
func TestFleetSource_FallsBackToTheProductionRegistry(t *testing.T) {
	// Without override and without Redis: nil, which is what makes the poller fall back to the
	// static list.
	if got := (&App{}).fleetSource(); got != nil {
		t.Error("without Redis the fleet source came back non-nil; an interface with a nil pointer is not nil")
	}

	// Without override and WITH Redis: the production registry, not nil.
	redis := &redisstore.RedisStore{}
	got := (&App{redisStore: redis}).fleetSource()
	if got == nil {
		t.Fatal("with Redis the fleet source came back nil — the poller would fall back to the static list and the fleet becomes 1 of 30 again")
	}
	if got != fleetRegistry(redis) {
		t.Errorf("the fleet source is not the production registry: %T", got)
	}

	// And the override WINS, otherwise the seam is useless.
	reg := &fakeFleetRegistry{}
	if (&App{redisStore: redis, fleetOverride: reg}).fleetSource() != fleetRegistry(reg) {
		t.Error("the override did not beat the production registry")
	}
}

// THE SERVER READS THE CONSTANT — nothing asserted it, and a constant does not appear in a ratchet.
//
// `WriteTimeout: httpapi.ServerWriteTimeout - 10*time.Second` survived the whole suite. Lowering the
// constant itself died (there is a case for its relation to the durable read budget), but subtracting
// at the point of use did not.
//
// The deadline matters: the durable read has its own budget, smaller than this one, so it degrades
// instead of blowing up in the middle of writing the body — and that is how the first deep read after
// a restart returned zero bytes with the connection down.
func TestNewHTTPServer_UsesTheDeclaredWriteTimeout(t *testing.T) {
	srv := (&App{cfg: config.Config{Port: "3000"}}).newHTTPServer(nil)

	if srv.WriteTimeout != httpapi.ServerWriteTimeout {
		t.Errorf("WriteTimeout = %s, want httpapi.ServerWriteTimeout (%s)",
			srv.WriteTimeout, httpapi.ServerWriteTimeout)
	}
	if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.IdleTimeout <= 0 {
		t.Errorf("a server deadline is zero: header=%s read=%s idle=%s",
			srv.ReadHeaderTimeout, srv.ReadTimeout, srv.IdleTimeout)
	}
	if srv.Addr != ":3000" {
		t.Errorf("Addr = %q, want :3000", srv.Addr)
	}
}
