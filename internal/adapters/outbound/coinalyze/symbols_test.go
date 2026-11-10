package coinalyze

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// The join that resolves the symbol, and the refusal by denomination.
//
// The cases below are written against what was MEASURED in `/future-markets` (4,958 markets,
// 759 from Binance), not against the documentation:
//
//   - BTC's USDT-M perpetual has `symbol_on_exchange: "BTCUSDT"` and id `BTCUSDT_PERP.A`;
//     concatenating `symbol_on_exchange + "." + exchange` gives `BTCUSDT.A`, an id that does
//     NOT EXIST, and the API answers a nonexistent id with HTTP 200 and `[]`;
//   - of Binance's 759 markets, 739 are `BASE_ASSET` and 20 are `QUOTE_ASSET` — and those 20
//     are exactly the COIN-M ones, whose `symbol_on_exchange` already carries `_PERP`;
//   - globally a third denomination, `CONTRACTS`, still exists in 6 markets.

// The fixture reproduces BOTH suffix families measured on Binance — a fixture with only one
// family would let a suffix rule pass — and ends with a LOOK-ALIKE from another venue. The
// look-alike comes LAST on purpose: a join blind to the exchange term would let it overwrite
// the Binance market, and it is the first case that breaks.
const coinalyzeMarketsFixture = `[
  {"symbol":"BTCUSDT_PERP.A","exchange":"A","symbol_on_exchange":"BTCUSDT","base_asset":"BTC","quote_asset":"USDT","is_perpetual":true,"margined":"STABLE","oi_lq_vol_denominated_in":"BASE_ASSET"},
  {"symbol":"ETHUSDT_PERP.A","exchange":"A","symbol_on_exchange":"ETHUSDT","base_asset":"ETH","quote_asset":"USDT","is_perpetual":true,"margined":"STABLE","oi_lq_vol_denominated_in":"BASE_ASSET"},
  {"symbol":"BTCUSD_PERP.A","exchange":"A","symbol_on_exchange":"BTCUSD_PERP","base_asset":"BTC","quote_asset":"USD","is_perpetual":true,"margined":"COIN","oi_lq_vol_denominated_in":"QUOTE_ASSET"},
  {"symbol":"XRPUSDT_PERP.A","exchange":"A","symbol_on_exchange":"XRPUSDT","base_asset":"XRP","quote_asset":"USDT","is_perpetual":true,"margined":"STABLE","oi_lq_vol_denominated_in":"CONTRACTS"},
  {"symbol":"SOLUSDT_PERP.A","exchange":"A","symbol_on_exchange":"SOLUSDT","base_asset":"SOL","quote_asset":"USDT","is_perpetual":true,"margined":"STABLE","oi_lq_vol_denominated_in":""},
  {"symbol":"BTCUSDT.6","exchange":"6","symbol_on_exchange":"BTCUSDT","base_asset":"BTC","quote_asset":"USDT","is_perpetual":true,"margined":"STABLE","oi_lq_vol_denominated_in":"BASE_ASSET"}
]`

// coinalyzeMarketsFake serves one body and counts how many times it was asked for.
type coinalyzeMarketsFake struct {
	mu       sync.Mutex
	hits     int
	body     string
	status   int
	clockNow time.Time
}

func newCoinalyzeCatalogFake(t *testing.T, body string) (*CoinalyzeSymbolCatalog, *coinalyzeMarketsFake) {
	t.Helper()

	fake := &coinalyzeMarketsFake{body: body, status: http.StatusOK, clockNow: time.Unix(1_700_000_000, 0)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		fake.hits++
		status, body := fake.status, fake.body
		fake.mu.Unlock()

		if r.URL.Path != "/future-markets" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	catalog := NewCoinalyzeSymbolCatalog(CoinalyzeSymbolCatalogOptions{
		Client:       NewCoinalyzeClient("chave-de-teste", WithCoinalyzeBaseURL(server.URL)),
		ExchangeCode: CoinalyzeBinanceCode,
		Now: func() time.Time {
			fake.mu.Lock()
			defer fake.mu.Unlock()
			return fake.clockNow
		},
	})
	return catalog, fake
}

func (f *coinalyzeMarketsFake) hitCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits
}

func (f *coinalyzeMarketsFake) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clockNow = f.clockNow.Add(d)
}

func (f *coinalyzeMarketsFake) serve(status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status, f.body = status, body
}

// THE JOIN, NEVER THE CONCATENATION. This is the reproduced defect: `BTCUSDT` + "." + "A" is
// `BTCUSDT.A`, a nonexistent id the API answers with 200 and `[]` — while the `symbol` field
// of the SAME market says `BTCUSDT_PERP.A`.
func TestCoinalyzeResolvesByJoinOnSymbolOnExchange(t *testing.T) {
	catalog, _ := newCoinalyzeCatalogFake(t, coinalyzeMarketsFixture)

	got, err := catalog.Resolve(context.Background(), "BTC/USDT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Symbol != "BTCUSDT_PERP.A" {
		t.Fatalf("expected the id from the `symbol` field (BTCUSDT_PERP.A), got %q — concatenation would give BTCUSDT.A", got.Symbol)
	}
	// And the resolved market's denomination is READ, not presumed.
	if got.DenominatedIn != "BASE_ASSET" {
		t.Fatalf("the market denomination did not reach the caller: %q", got.DenominatedIn)
	}
}

// THE SAME RULE COVERS THE OTHER SUFFIX FAMILY: here `_PERP` is already in
// `symbol_on_exchange`, and a rule that appended a suffix would produce `BTCUSD_PERP_PERP.A`.
func TestCoinalyzeResolvesTheSuffixedFamilyByTheSameJoin(t *testing.T) {
	catalog, _ := newCoinalyzeCatalogFake(t, coinalyzeMarketsFixture)

	_, err := catalog.Resolve(context.Background(), "BTCUSD_PERP")
	var refusal *CoinalyzeDenominationError
	if !errors.As(err, &refusal) {
		t.Fatalf("expected a denomination refusal, got %v", err)
	}
	// The join FOUND the market — the denomination is what blocks it, not absence.
	if refusal.CoinalyzeSymbol != "BTCUSD_PERP.A" {
		t.Fatalf("the join did not reach the suffixed family: %q", refusal.CoinalyzeSymbol)
	}
}

// AN UNEXPECTED DENOMINATION IS A REFUSAL, WITH THE FOUND VALUE NAMED. Binance's 20 COIN-M
// markets denominate open interest in the QUOTE asset; the decision axis is the base asset.
// Ingesting both on the same ruler mixes units.
func TestCoinalyzeRefusesQuoteAssetDenominationNamingTheValueFound(t *testing.T) {
	catalog, _ := newCoinalyzeCatalogFake(t, coinalyzeMarketsFixture)

	got, err := catalog.Resolve(context.Background(), "BTCUSD_PERP")

	var refusal *CoinalyzeDenominationError
	if !errors.As(err, &refusal) {
		t.Fatalf("expected CoinalyzeDenominationError, got %v", err)
	}
	if refusal.Found != "QUOTE_ASSET" {
		t.Fatalf("the found value must be named, got %q", refusal.Found)
	}
	if refusal.Expected != "BASE_ASSET" {
		t.Fatalf("the expected value must be named, got %q", refusal.Expected)
	}
	if !strings.Contains(err.Error(), "QUOTE_ASSET") {
		t.Fatalf("the message must name the found value: %v", err)
	}
	if got.Symbol != "" {
		t.Fatalf("a refused market must not return an id: %q", got.Symbol)
	}
}

// AND THE RULE IS "DIFFERENT FROM EXPECTED", NOT "EQUAL TO QUOTE_ASSET": there is a third
// denomination measured on the API, `CONTRACTS`, in 6 markets.
func TestCoinalyzeRefusesContractsDenominationNamingTheValueFound(t *testing.T) {
	catalog, _ := newCoinalyzeCatalogFake(t, coinalyzeMarketsFixture)

	_, err := catalog.Resolve(context.Background(), "XRP/USDT")

	var refusal *CoinalyzeDenominationError
	if !errors.As(err, &refusal) {
		t.Fatalf("expected CoinalyzeDenominationError, got %v", err)
	}
	if refusal.Found != "CONTRACTS" {
		t.Fatalf("expected CONTRACTS to be named, got %q", refusal.Found)
	}
}

// A MISSING DENOMINATION IS ALSO A REFUSAL, and the message says it was missing instead of
// faking a value. An empty field read as "probably base" is the same silent guess the
// refusal exists to prevent.
func TestCoinalyzeRefusesMarketWithNoDenominationDeclared(t *testing.T) {
	catalog, _ := newCoinalyzeCatalogFake(t, coinalyzeMarketsFixture)

	_, err := catalog.Resolve(context.Background(), "SOL/USDT")

	var refusal *CoinalyzeDenominationError
	if !errors.As(err, &refusal) {
		t.Fatalf("expected CoinalyzeDenominationError, got %v", err)
	}
	if refusal.Found != "" {
		t.Fatalf("the field came empty from the API and must remain empty in the error, got %q", refusal.Found)
	}
	if !strings.Contains(err.Error(), "not declared") {
		t.Fatalf("the message must say the denomination was missing: %v", err)
	}
}

// REFUSAL AND ABSENCE ARE TWO OUTCOMES, AND THE CALLER MUST SEPARATE THEM: a market that
// exists with the wrong unit is a modeling problem; a symbol that does not exist is a
// configuration problem. Collapsing the two hides both.
func TestCoinalyzeDenominationRefusalIsDistinctFromAbsence(t *testing.T) {
	catalog, _ := newCoinalyzeCatalogFake(t, coinalyzeMarketsFixture)

	_, absent := catalog.Resolve(context.Background(), "DOGE/USDT")
	if !errors.Is(absent, ErrCoinalyzeSymbolNotFound) {
		t.Fatalf("an absent symbol must be ErrCoinalyzeSymbolNotFound, got %v", absent)
	}
	var wrongType *CoinalyzeDenominationError
	if errors.As(absent, &wrongType) {
		t.Fatal("absence must not present itself as a denomination refusal")
	}
	if !strings.Contains(absent.Error(), "DOGE/USDT") {
		t.Fatalf("the absence must name the requested symbol: %v", absent)
	}

	_, refused := catalog.Resolve(context.Background(), "BTCUSD_PERP")
	if errors.Is(refused, ErrCoinalyzeSymbolNotFound) {
		t.Fatalf("a denomination refusal must not present itself as absence: %v", refused)
	}
}

// THE CATALOG IS A CACHE: the measured table has 4,958 markets, and fetching it per resolved
// symbol would spend one request per symbol of the cycle.
func TestCoinalyzeCatalogFetchesOnceForManyResolves(t *testing.T) {
	catalog, fake := newCoinalyzeCatalogFake(t, coinalyzeMarketsFixture)

	for _, symbol := range []string{"BTC/USDT", "ETH/USDT", "BTC/USDT"} {
		if _, err := catalog.Resolve(context.Background(), symbol); err != nil {
			t.Fatalf("unexpected error on %s: %v", symbol, err)
		}
	}
	if hits := fake.hitCount(); hits != 1 {
		t.Fatalf("three resolutions should cost ONE fetch, they cost %d", hits)
	}
}

// BUT IT IS NOT AN ETERNAL CACHE: a venue lists a new contract, and a table loaded once for
// the life of the process would never see it.
func TestCoinalyzeCatalogReloadsAfterTheInterval(t *testing.T) {
	catalog, fake := newCoinalyzeCatalogFake(t, coinalyzeMarketsFixture)

	if _, err := catalog.Resolve(context.Background(), "BTC/USDT"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := catalog.Resolve(context.Background(), "DOGE/USDT"); !errors.Is(err, ErrCoinalyzeSymbolNotFound) {
		t.Fatalf("DOGE is not listed yet: %v", err)
	}

	fake.serve(http.StatusOK, `[{"symbol":"DOGEUSDT_PERP.A","exchange":"A","symbol_on_exchange":"DOGEUSDT","oi_lq_vol_denominated_in":"BASE_ASSET"}]`)
	fake.advance(DefaultCoinalyzeMarketsReload + time.Minute)

	got, err := catalog.Resolve(context.Background(), "DOGE/USDT")
	if err != nil {
		t.Fatalf("after the interval the table should have been reread: %v", err)
	}
	if got.Symbol != "DOGEUSDT_PERP.A" {
		t.Fatalf("the new contract did not enter: %q", got.Symbol)
	}
	if hits := fake.hitCount(); hits != 2 {
		t.Fatalf("expected exactly one reread, there were %d fetches", hits)
	}
}

// AN UPSTREAM FAILURE IS AN ERROR, NOT AN EMPTY CATALOG. A 500 that became "no market" would
// make EVERY symbol of the cycle look nonexistent, and the cache would keep that lie.
func TestCoinalyzeUpstreamFailureIsAnErrorNotAnEmptyCatalog(t *testing.T) {
	catalog, fake := newCoinalyzeCatalogFake(t, coinalyzeMarketsFixture)
	fake.serve(http.StatusInternalServerError, "")

	_, err := catalog.Resolve(context.Background(), "BTC/USDT")
	if err == nil {
		t.Fatal("a 500 must fail")
	}
	if errors.Is(err, ErrCoinalyzeSymbolNotFound) {
		t.Fatalf("an upstream failure must not become symbol absence: %v", err)
	}

	// And the failure did not poison the cache: the next attempt resolves.
	fake.serve(http.StatusOK, coinalyzeMarketsFixture)
	got, err := catalog.Resolve(context.Background(), "BTC/USDT")
	if err != nil {
		t.Fatalf("the next fetch should resolve: %v", err)
	}
	if got.Symbol != "BTCUSDT_PERP.A" {
		t.Fatalf("got %q", got.Symbol)
	}
}

// EXPIRED TABLE + REFUSED RELOAD: serve the one that exists. Measured in production — a
// catalog 429 costs one fetch PER SYMBOL (outside the budget) and erases the derivatives
// hour.
func TestCoinalyzeCatalogServesTheStaleTableWhenTheReloadFails(t *testing.T) {
	catalog, fake := newCoinalyzeCatalogFake(t, coinalyzeMarketsFixture)

	if _, err := catalog.Resolve(context.Background(), "BTC/USDT"); err != nil {
		t.Fatalf("initial load: %v", err)
	}

	fake.serve(http.StatusTooManyRequests, "")
	fake.advance(DefaultCoinalyzeMarketsReload + time.Minute)

	got, err := catalog.Resolve(context.Background(), "BTC/USDT")
	if err != nil {
		t.Fatalf("the expired table should have been served, got error: %v", err)
	}
	if got.Symbol != "BTCUSDT_PERP.A" {
		t.Fatalf("got %q", got.Symbol)
	}

	// And the following symbols of the SAME cycle do not redo the fetch.
	if _, err := catalog.Resolve(context.Background(), "ETH/USDT"); err != nil {
		t.Fatalf("second symbol: %v", err)
	}
	if hits := fake.hitCount(); hits != 2 {
		t.Fatalf("expected 1 load + 1 refused reread, there were %d fetches", hits)
	}
}

// AFTER THE REFUSAL, THE NEXT ATTEMPT WAITS. Without the pause, every symbol of the cycle
// would hit the catalog again and the whole budget would be consumed by a single failure.
func TestCoinalyzeCatalogBacksOffAfterAFailedReload(t *testing.T) {
	catalog, fake := newCoinalyzeCatalogFake(t, coinalyzeMarketsFixture)

	if _, err := catalog.Resolve(context.Background(), "BTC/USDT"); err != nil {
		t.Fatalf("initial load: %v", err)
	}
	fake.serve(http.StatusTooManyRequests, "")
	fake.advance(DefaultCoinalyzeMarketsReload + time.Minute)
	if _, err := catalog.Resolve(context.Background(), "BTC/USDT"); err != nil {
		t.Fatalf("the expired table should have been served: %v", err)
	}
	hitsAfterFailure := fake.hitCount()

	// Inside the pause: no new fetch, even while resolving several symbols.
	fake.advance(DefaultCoinalyzeCatalogFailureBackoff - time.Minute)
	for _, symbol := range []string{"BTC/USDT", "ETH/USDT", "BTC/USDT"} {
		if _, err := catalog.Resolve(context.Background(), symbol); err != nil {
			t.Fatalf("%s inside the pause: %v", symbol, err)
		}
	}
	if hits := fake.hitCount(); hits != hitsAfterFailure {
		t.Fatalf("there must be no new fetch inside the pause: %d -> %d", hitsAfterFailure, hits)
	}

	// Once the pause passes, it tries again — and the new table enters.
	fake.serve(http.StatusOK, `[{"symbol":"DOGEUSDT_PERP.A","exchange":"A","symbol_on_exchange":"DOGEUSDT","oi_lq_vol_denominated_in":"BASE_ASSET"}]`)
	fake.advance(2 * time.Minute)
	got, err := catalog.Resolve(context.Background(), "DOGE/USDT")
	if err != nil {
		t.Fatalf("after the pause the fetch should run: %v", err)
	}
	if got.Symbol != "DOGEUSDT_PERP.A" {
		t.Fatalf("got %q", got.Symbol)
	}
}

// A TABLE WITHOUT THE REQUESTED EXCHANGE IS A JOIN ERROR, not the absence of each symbol one
// by one.
func TestCoinalyzeTableWithoutTheRequestedExchangeIsAnError(t *testing.T) {
	catalog, _ := newCoinalyzeCatalogFake(t, `[{"symbol":"BTCUSDT.6","exchange":"6","symbol_on_exchange":"BTCUSDT","oi_lq_vol_denominated_in":"BASE_ASSET"}]`)

	_, err := catalog.Resolve(context.Background(), "BTC/USDT")
	if err == nil {
		t.Fatal("a table without the requested exchange must fail")
	}
	if errors.Is(err, ErrCoinalyzeSymbolNotFound) {
		t.Fatalf("a broken join must not present itself as an absent symbol: %v", err)
	}
	if !strings.Contains(err.Error(), CoinalyzeBinanceCode) {
		t.Fatalf("the error must name the exchange that did not come: %v", err)
	}
}
