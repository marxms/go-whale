package coinalyze

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// The catalog that resolves an internal symbol to the Coinalyze id, BY JOIN.
//
// # WHY JOIN, NEVER CONCATENATION
//
// Binance's BTC USDT-M perpetual has `symbol_on_exchange: "BTCUSDT"` and id
// `BTCUSDT_PERP.A`. Building the id as `symbol_on_exchange + "." + exchange` gives
// `BTCUSDT.A`, which does not exist — and the API answers a nonexistent id with HTTP 200
// and `[]`, indistinguishable from "there is no data in this window". The error does not
// complain: it writes an empty series and calls coverage complete.
//
// No suffix rule rescues concatenation. Measured across Binance's 759 markets: 739 have a
// suffixless `symbol_on_exchange` and an id with `_PERP` (BTCUSDT -> BTCUSDT_PERP.A), and 20
// already carry `_PERP` in `symbol_on_exchange` itself (BTCUSD_PERP -> BTCUSD_PERP.A). Only
// the `symbol` field of the SAME record covers both families.
const coinalyzeFutureMarketsPath = "/future-markets"

// CoinalyzeBinanceCode is the code Coinalyze gives Binance — measured in `/exchanges`,
// which returns 28 venues. The code is the suffix of the id (`BTCUSDT_PERP.A`) and the
// exchange term of the join.
const CoinalyzeBinanceCode = "A"

// coinalyzeExpectedDenomination is the unit the decision axis requires: open interest in the
// BASE asset (contracts), not in the quote asset.
//
// Measured in `/future-markets`: of Binance's 759 markets, 739 are `BASE_ASSET` and 20 are
// `QUOTE_ASSET` — and those 20 are exactly the COIN-M ones. Globally a third value,
// `CONTRACTS`, still appears in 6 markets. Mixing two of those units on the same ruler
// produces a baseline that measures a unit change and calls it market variance.
const coinalyzeExpectedDenomination = "BASE_ASSET"

// DefaultCoinalyzeMarketsReload is the reread cadence of the markets table.
//
// The table changes when a venue lists a new contract. Loading it once for the life of the
// process would leave a newly configured symbol unresolvable forever, with nothing red. 6h
// is the same cadence `MarginBracketCatalog` already uses in this package.
const DefaultCoinalyzeMarketsReload = 6 * time.Hour

// ErrCoinalyzeSymbolNotFound says the join did not find the symbol — which is a DIFFERENT
// outcome from "found it, with the wrong unit". Collapsing the two hides both: one is
// configuration, the other is modeling.
var ErrCoinalyzeSymbolNotFound = errors.New("coinalyze: symbol absent from the markets catalog")

// CoinalyzeDenominationError refuses a market whose unit is not the decision axis, and NAMES
// the value found — an error that only says "unexpected denomination" sends the maintainer to
// the API to find out which.
type CoinalyzeDenominationError struct {
	// Symbol is the spelling requested by the caller (BTC/USDT).
	Symbol string
	// CoinalyzeSymbol is the id the join reached — the proof the market EXISTS.
	CoinalyzeSymbol string
	// Found is the value read from `oi_lq_vol_denominated_in`. Empty means the API did not
	// declare it, and empty stays: inventing a value here is the guess the refusal prevents.
	Found    string
	Expected string
}

func (e *CoinalyzeDenominationError) Error() string {
	found := e.Found
	if found == "" {
		found = "not declared"
	}
	return fmt.Sprintf("coinalyze: %s (%s) denominates open interest in %s, and the decision axis requires %s",
		e.Symbol, e.CoinalyzeSymbol, found, e.Expected)
}

// coinalyzeMarket is the slice of `/future-markets` this catalog reads. The other fields of
// the measured record (base_asset, margined, has_ohlcv_data…) are omitted because nothing here
// uses them.
type coinalyzeMarket struct {
	Symbol           string `json:"symbol"`
	Exchange         string `json:"exchange"`
	SymbolOnExchange string `json:"symbol_on_exchange"`
	DenominatedIn    string `json:"oi_lq_vol_denominated_in"`
}

// CoinalyzeMarket is what a successful resolution delivers.
//
// The denomination travels with it because it is persisted data, not only a port criterion:
// every stored point declares the unit in which it was measured.
type CoinalyzeMarket struct {
	Symbol        string
	DenominatedIn string
}

// CoinalyzeSymbolCatalogOptions configures the catalog. Every field has a default; tests
// replace the client, the cadence and the clock.
type CoinalyzeSymbolCatalogOptions struct {
	Client         *CoinalyzeClient
	ExchangeCode   string
	ReloadInterval time.Duration
	Now            func() time.Time
	Log            *slog.Logger
}

// DefaultCoinalyzeCatalogFailureBackoff is the pause after a refused catalog fetch.
//
// The `/future-markets` fetch does NOT go through the call budget. Without this pause, a
// single 429 on the catalog becomes one request per symbol of the cycle — 68 in 7 s, measured
// in production — blows the whole budget and erases the derivatives hour.
const DefaultCoinalyzeCatalogFailureBackoff = 5 * time.Minute

// CoinalyzeSymbolCatalog holds the markets table of ONE exchange and resolves symbols
// against it.
//
// The measured table has 4,958 markets (~1 MB). Fetching it per resolved symbol would spend
// one request per symbol of the cycle, on a budget of 40 calls per minute.
type CoinalyzeSymbolCatalog struct {
	client       *CoinalyzeClient
	exchangeCode string
	reload       time.Duration
	retryBackoff time.Duration
	now          func() time.Time
	log          *slog.Logger

	// mu is a Mutex, not an RWMutex, because a read can TRIGGER the fetch. Holding it during
	// the fetch deliberately serializes concurrent resolutions: the alternative is N
	// goroutines downloading the same 1 MB table at once, on the first cycle.
	mu       sync.Mutex
	markets  map[string]coinalyzeMarket
	loadedAt time.Time
	failedAt time.Time
}

// NewCoinalyzeSymbolCatalog builds the catalog with defaults filled in. It performs no I/O:
// the first fetch happens on the first resolution.
func NewCoinalyzeSymbolCatalog(opts CoinalyzeSymbolCatalogOptions) *CoinalyzeSymbolCatalog {
	if opts.ExchangeCode == "" {
		opts.ExchangeCode = CoinalyzeBinanceCode
	}
	if opts.ReloadInterval <= 0 {
		opts.ReloadInterval = DefaultCoinalyzeMarketsReload
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &CoinalyzeSymbolCatalog{
		client:       opts.Client,
		exchangeCode: opts.ExchangeCode,
		reload:       opts.ReloadInterval,
		retryBackoff: DefaultCoinalyzeCatalogFailureBackoff,
		now:          opts.Now,
		log:          logger.OrDiscard(opts.Log),
	}
}

// Resolve delivers the Coinalyze id for an internal symbol, or refuses.
//
// There are three outcomes, and they are not confused: resolved, absent
// (`ErrCoinalyzeSymbolNotFound`) and refused for unit (`*CoinalyzeDenominationError`).
func (c *CoinalyzeSymbolCatalog) Resolve(ctx context.Context, symbol string) (CoinalyzeMarket, error) {
	found, err := c.lookup(ctx, symbol)
	if err != nil {
		return CoinalyzeMarket{}, err
	}

	if !strings.EqualFold(found.DenominatedIn, coinalyzeExpectedDenomination) {
		refusal := &CoinalyzeDenominationError{
			Symbol:          symbol,
			CoinalyzeSymbol: found.Symbol,
			Found:           found.DenominatedIn,
			Expected:        coinalyzeExpectedDenomination,
		}
		// The log lives HERE because the decision to refuse lives here. The caller receives
		// the typed error and need not log it again nor reconstruct the reason from prose.
		logger.OrDiscard(c.log).Warn("coinalyze market refused",
			"symbol", refusal.Symbol,
			"coinalyze_symbol", refusal.CoinalyzeSymbol,
			"found_denomination", refusal.Found,
			"expected_denomination", refusal.Expected)
		return CoinalyzeMarket{}, refusal
	}

	return CoinalyzeMarket{Symbol: found.Symbol, DenominatedIn: found.DenominatedIn}, nil
}

// lookup guarantees the table and performs the join.
func (c *CoinalyzeSymbolCatalog) lookup(ctx context.Context, symbol string) (coinalyzeMarket, error) {
	key := coinalyzeJoinKey(symbol)
	if key == "" {
		return coinalyzeMarket{}, fmt.Errorf("coinalyze: an empty symbol cannot be resolved")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureLoadedLocked(ctx); err != nil {
		return coinalyzeMarket{}, err
	}

	market, ok := c.markets[key]
	if !ok {
		return coinalyzeMarket{}, fmt.Errorf("%w: %s on exchange %s", ErrCoinalyzeSymbolNotFound, symbol, c.exchangeCode)
	}
	return market, nil
}

// ensureLoadedLocked fetches the table when there is none or when the existing one expired.
//
// A fetch failure NEVER becomes an empty table: then every symbol of the cycle would look
// nonexistent, and the cache would keep that lie until the next interval. With a previous
// table in hand, the failure is ABSORBED — the expired one is served and the next attempt
// waits `retryBackoff`. With no table at all there is nothing to serve: the failure is
// returned so the cycle refuses instead of inventing absent symbols.
//
// The absorption is not tolerance of error: without it, every symbol of the cycle would redo
// the fetch (which does not go through the budget) and a single 429 on the catalog would
// erase the whole derivatives hour.
func (c *CoinalyzeSymbolCatalog) ensureLoadedLocked(ctx context.Context) error {
	if len(c.markets) > 0 && c.now().Sub(c.loadedAt) < c.reload {
		return nil
	}
	if len(c.markets) > 0 && !c.failedAt.IsZero() && c.now().Sub(c.failedAt) < c.retryBackoff {
		return nil
	}

	markets, err := c.fetch(ctx)
	if err != nil {
		c.failedAt = c.now()
		if len(c.markets) > 0 {
			return nil
		}
		return err
	}

	c.markets = markets
	c.loadedAt = c.now()
	c.failedAt = time.Time{}
	return nil
}

// fetch reads `/future-markets` and indexes by join key, inside the requested exchange.
func (c *CoinalyzeSymbolCatalog) fetch(ctx context.Context) (map[string]coinalyzeMarket, error) {
	var all []coinalyzeMarket
	if err := c.client.get(ctx, coinalyzeFutureMarketsPath, nil, &all); err != nil {
		return nil, err
	}

	byJoinKey := make(map[string]coinalyzeMarket, len(all))
	for _, market := range all {
		// THE EXCHANGE TERM IS PART OF THE JOIN. `BTCUSDT` is a `symbol_on_exchange` that
		// exists in dozens of the 28 measured venues; without this filter, the array's last
		// look-alike overwrites the right market.
		if !strings.EqualFold(market.Exchange, c.exchangeCode) {
			continue
		}
		key := coinalyzeJoinKey(market.SymbolOnExchange)
		if key == "" || market.Symbol == "" {
			continue
		}
		byJoinKey[key] = market
	}

	if len(byJoinKey) == 0 {
		return nil, fmt.Errorf("coinalyze: %s brought no market from exchange %q",
			coinalyzeFutureMarketsPath, c.exchangeCode)
	}
	return byJoinKey, nil
}

// coinalyzeJoinKey puts both spellings into the one the join uses.
//
// The internal symbol carries a slash (`BTC/USDT`) and `symbol_on_exchange` does not
// (`BTCUSDT`). Removing the slash is NOT the concatenation this file refuses: it rewrites
// OUR spelling into the provider's, it does not invent the provider's id.
func coinalyzeJoinKey(symbol string) string {
	return strings.ReplaceAll(strings.ToUpper(strings.TrimSpace(symbol)), "/", "")
}
