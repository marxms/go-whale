// Package binancerest exposes the Binance REST reads the streamer needs outside the
// streaming SDK, such as leverage brackets.
package binancerest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// DefaultMarginBracketsURL is Binance's PUBLIC bracket table.
//
// The signed route (/fapi/v1/leverageBracket) carries the same numbers but demands an API
// key. This process has never held one — every venue client is built without credentials —
// so the bapi route is not a shortcut around auth, it is the only door that does not force
// a credential into a service that reads public market data.
const DefaultMarginBracketsURL = "https://www.binance.com/bapi/futures/v1/friendly/future/common/brackets"

// DefaultMarginBracketsReload is the cadence of the periodic reload.
//
// The table changes rarely — BTCUSDT's tier 1 was last touched 2025-08-19 — but rarely is
// not never. Loading once and caching for the life of the process would leave the
// liquidation model drawing bands from a maintenance margin the exchange has already
// retired, and nothing in the system would say so.
const DefaultMarginBracketsReload = 6 * time.Hour

// marginBracketsTimeout bounds one fetch. The full table is ~1,6 MB.
const marginBracketsTimeout = 30 * time.Second

// maxMarginBracketsBody caps what a hostile or broken upstream can make us buffer.
const maxMarginBracketsBody = 32 << 20 // 32 MiB

// The brackets this endpoint publishes are Binance USDT-M futures. Serving them under any
// other exchange or market type would attribute one venue's maintenance margin to another
// venue's positions, which is a wrong number wearing the right name.
const (
	marginBracketsExchange   = "binance"
	marginBracketsMarketType = "future"
)

// marginBracketsEnvelope is the bapi response shape: a status envelope around the table.
type marginBracketsEnvelope struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Success bool   `json:"success"`
	Data    struct {
		Version  string                `json:"version"`
		Brackets []marginBracketSymbol `json:"brackets"`
	} `json:"data"`
}

type marginBracketSymbol struct {
	Symbol       string              `json:"symbol"`
	RiskBrackets []marginBracketTier `json:"riskBrackets"`
}

type marginBracketTier struct {
	NotionalFloor float64 `json:"bracketNotionalFloor"`
	Rate          float64 `json:"bracketMaintenanceMarginRate"`
}

// MarginBracketCatalogOptions configures the catalog. Every field has a default; tests
// override the URL, the retry budget and the reload cadence.
type MarginBracketCatalogOptions struct {
	URL            string
	ReloadInterval time.Duration
	HTTPClient     *http.Client
	Retry          exchange.RetryConfig
}

// MarginBracketCatalog holds the tier-1 maintenance margin rate per symbol, refreshed on a
// timer. A failed refresh keeps the previous table: a stale maintenance margin is a far
// smaller error than no maintenance margin at all, which is what sends the 100x band to
// the price where a 249x liquidates.
type MarginBracketCatalog struct {
	url    string
	reload time.Duration
	client *http.Client
	retry  exchange.RetryConfig
	log    *slog.Logger

	mu    sync.RWMutex
	rates map[string]float64
	wg    sync.WaitGroup
}

// NewMarginBracketCatalog builds a catalog with defaults filled in. It performs no I/O:
// the first fetch happens in Start or in an explicit Refresh.
func NewMarginBracketCatalog(opts MarginBracketCatalogOptions, log ...*slog.Logger) *MarginBracketCatalog {
	if opts.URL == "" {
		opts.URL = DefaultMarginBracketsURL
	}
	if opts.ReloadInterval <= 0 {
		opts.ReloadInterval = DefaultMarginBracketsReload
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: marginBracketsTimeout}
	}
	if opts.Retry.MaxAttempts <= 0 {
		opts.Retry = exchange.DefaultRetryConfig()
	}

	return &MarginBracketCatalog{
		url:    opts.URL,
		reload: opts.ReloadInterval,
		client: opts.HTTPClient,
		retry:  opts.Retry,
		log:    logger.OrDiscard(log...),
		rates:  map[string]float64{},
	}
}

// Tier1MaintenanceMarginRates returns the tier-1 rate per internal symbol (BTC/USDT), or
// nil for a scope this source does not describe. The map is a copy: the reload goroutine
// replaces the table underneath, and handing callers the live map would be a data race.
func (c *MarginBracketCatalog) Tier1MaintenanceMarginRates(exchangeID, marketType string) map[string]float64 {
	if !strings.EqualFold(exchangeID, marginBracketsExchange) || !strings.EqualFold(marketType, marginBracketsMarketType) {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.rates) == 0 {
		return nil
	}

	out := make(map[string]float64, len(c.rates))
	for symbol, rate := range c.rates {
		out[symbol] = rate
	}
	return out
}

// Size reports how many symbols the currently held table covers.
func (c *MarginBracketCatalog) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.rates)
}

// Refresh fetches the table once and swaps it in. On any failure it returns the error and
// leaves the previously loaded table untouched.
func (c *MarginBracketCatalog) Refresh(ctx context.Context) error {
	rates, err := exchange.WithRetryResult(ctx, c.log, c.retry, "margin-brackets fetch", func() (map[string]float64, error) {
		return c.fetch(ctx)
	})
	if err != nil {
		return err
	}

	if len(rates) == 0 {
		return fmt.Errorf("margin-brackets fetch: table carried no usable tier-1 rate")
	}

	c.mu.Lock()
	c.rates = rates
	c.mu.Unlock()
	return nil
}

// Start runs the reload loop until ctx is done. A failure is logged at error level and the
// loop continues — the collection cycle never dies because a table could not be read.
func (c *MarginBracketCatalog) Start(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// Load immediately so a restart is not blind for a whole interval.
		if err := c.Refresh(ctx); err != nil {
			logger.OrDiscard(c.log).Error("margin-brackets initial load failed; serving last known table",
				"symbols", c.Size(), "error", err)
		}

		ticker := time.NewTicker(c.reload)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.Refresh(ctx); err != nil {
					logger.OrDiscard(c.log).Error("margin-brackets reload failed; serving last known table",
						"symbols", c.Size(), "error", err)
				}
			}
		}
	}()
}

// Wait blocks until the reload loop has exited. Call it after canceling the context
// passed to Start.
func (c *MarginBracketCatalog) Wait() { c.wg.Wait() }

// fetch performs one unauthenticated GET and maps the body to tier-1 rates. No credential
// is attached, here or anywhere else in this file.

func (c *MarginBracketCatalog) fetch(ctx context.Context) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, fmt.Errorf("build margin bracket request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request margin brackets: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// The status code stays in the message on purpose: isRetryableError reads the
		// error string to decide whether 429 and 5xx get another attempt.
		return nil, fmt.Errorf("margin bracket source returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxMarginBracketsBody))
	if err != nil {
		return nil, fmt.Errorf("read margin bracket body: %w", err)
	}

	var envelope marginBracketsEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		// A body that does not parse is an upstream error, never an empty table.
		return nil, fmt.Errorf("decode margin bracket body: %w", err)
	}
	if !envelope.Success {
		return nil, fmt.Errorf("margin bracket source reported failure: code=%q message=%q", envelope.Code, envelope.Message)
	}

	return tier1RatesBySymbol(envelope.Data.Brackets), nil
}

// tier1RatesBySymbol maps the exchange table onto internal symbols, dropping every entry
// this service cannot address: dated futures, settled markets and anything whose tier-1
// rate is not a usable fraction.
func tier1RatesBySymbol(entries []marginBracketSymbol) map[string]float64 {
	rates := make(map[string]float64, len(entries))

	for _, entry := range entries {
		symbol := market.InternalSymbolFromConcatenated(entry.Symbol)
		if symbol == "" {
			continue
		}

		rate, ok := tier1Rate(entry.RiskBrackets)
		if !ok {
			continue
		}

		rates[symbol] = rate
	}

	return rates
}

// tier1Rate picks the bracket with the LOWEST notional floor instead of the first element
// of the array. Tier 1 is a property of the numbers, not of the transport's ordering, and
// the simulated positions carry no individual notional — so the tier where retail sits
// (below 300k USDT on BTCUSDT) is the one the model uses. Declared in the spec.
func tier1Rate(tiers []marginBracketTier) (float64, bool) {
	var (
		best  float64
		floor float64
		found bool
	)

	for _, tier := range tiers {
		if !isUsableMaintenanceMarginRate(tier.Rate) || math.IsNaN(tier.NotionalFloor) {
			continue
		}
		if !found || tier.NotionalFloor < floor {
			best, floor, found = tier.Rate, tier.NotionalFloor, true
		}
	}

	return best, found
}

// isUsableMaintenanceMarginRate accepts only a fraction strictly inside (0, 1). Zero makes
// 1/MMR infinite and would admit any leverage; one or more puts the liquidation price at
// or past the entry.
func isUsableMaintenanceMarginRate(rate float64) bool {
	return !math.IsNaN(rate) && !math.IsInf(rate, 0) && rate > 0 && rate < 1
}
