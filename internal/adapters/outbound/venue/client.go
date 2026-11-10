// Package venue is the outbound REST adapter for the venues this service reads:
// Binance spot, Binance USD-M futures and BingX spot/swap.
//
// The core consumes the ports declared by its own packages. This package owns the
// HTTP calls, symbol translation, request weights and error classification; no
// vendor type crosses the boundary.
package venue

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

const (
	defaultBinanceSpotURL    = "https://api.binance.com"
	defaultBinanceFuturesURL = "https://fapi.binance.com"
	defaultBingXURL          = "https://open-api.bingx.com"

	// defaultRequestTimeout bounds one venue call. The collector and recovery
	// already carry their own contexts; this is the backstop for a hung socket.
	defaultRequestTimeout = 30 * time.Second

	// bingXMinCallInterval keeps this process below the venue's 500 requests
	// per 10 seconds per IP for market data: 25 ms is 40 req/s.
	bingXMinCallInterval = 25 * time.Millisecond
)

// Options configures one client. The zero value is usable in production; tests
// inject the base URLs and the HTTP client.
type Options struct {
	BinanceSpotBaseURL    string
	BinanceFuturesBaseURL string
	BingXBaseURL          string
	HTTPClient            *http.Client
	Budget                *exchange.RestBudget
	Log                   *slog.Logger
	Metrics               Metrics
}

// Client is the façade the composition root consumes: it owns the transport, the request
// budget and the BingX pacer, and delegates every venue request to the driver the registry
// resolved for its scope. Its public signatures are the consumer ports, so no method here
// carries a venue branch.
type Client struct {
	venue      market.Exchange
	marketType market.MarketType

	http   *http.Client
	budget *exchange.RestBudget
	log    *slog.Logger

	binanceSpotURL    string
	binanceFuturesURL string
	bingXURL          string

	bingXPacer *intervalPacer
	metrics    Metrics

	// driver is the per-scope request implementation this façade delegates to. It is
	// resolved at construction so no request path contains a venue switch.
	driver driver

	// absenceLogged keeps the "capability absent" warning to one line per capability per
	// client. The seed path asks for CVD once per symbol and timeframe; without this the
	// absence would flood the log on every venue that does not publish it.
	absenceLogged sync.Map // market.Capability -> struct{}
}

// NewClient builds a client with the production defaults.
func NewClient(exchangeID string, marketType market.MarketType, logs ...*slog.Logger) (*Client, error) {
	return NewClientWithOptions(exchangeID, marketType, Options{Log: logger.OrDiscard(logs...)})
}

// NewClientWithOptions builds a client with explicit URLs and transport.
func NewClientWithOptions(exchangeID string, marketType market.MarketType, opts Options) (*Client, error) {
	venue, err := market.ParseExchange(exchangeID)
	if err != nil {
		return nil, err
	}
	if !marketType.Valid() {
		return nil, fmt.Errorf("venue: invalid market type %q", marketType)
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultRequestTimeout}
	}

	client := &Client{
		venue:             venue,
		marketType:        marketType,
		http:              httpClient,
		budget:            opts.Budget,
		log:               logger.OrDiscard(opts.Log),
		binanceSpotURL:    firstNonEmpty(opts.BinanceSpotBaseURL, defaultBinanceSpotURL),
		binanceFuturesURL: firstNonEmpty(opts.BinanceFuturesBaseURL, defaultBinanceFuturesURL),
		bingXURL:          firstNonEmpty(opts.BingXBaseURL, defaultBingXURL),
		metrics:           opts.Metrics,
	}
	if venue == market.BingX {
		client.bingXPacer = &intervalPacer{interval: bingXMinCallInterval}
	}

	resolved, err := registeredDrivers().resolve(venueScope{exchange: venue, marketType: marketType}, client)
	if err != nil {
		return nil, err
	}
	client.driver = resolved
	return client, nil
}

// SetMetrics injects the observability port. The composition root calls it
// before the first request.
func (c *Client) SetMetrics(m Metrics) { c.metrics = m }

// Close releases the driver's resources and idle connections. It is safe to
// call more than once. Drivers own no transport of their own; they must not
// call back into Client.Close.
func (c *Client) Close() []error {
	var errs []error
	if c.driver != nil {
		errs = append(errs, c.driver.Close()...)
	}
	c.http.CloseIdleConnections()
	return errs
}

func (c *Client) baseURL() string {
	switch c.venue {
	case market.Binance:
		if c.marketType == market.Spot {
			return c.binanceSpotURL
		}
		return c.binanceFuturesURL
	default:
		return c.bingXURL
	}
}

// acquireBinanceWeight reserves request weight for one call. A nil budget is
// unmetered, which is the case for tests and for venues we do not meter.
func (c *Client) acquireBinanceWeight(ctx context.Context, weight int) error {
	if c.venue != market.Binance || c.budget == nil {
		return nil
	}
	return c.budget.Acquire(ctx, weight)
}

func (c *Client) acquireKlines(ctx context.Context, limit int) error {
	if c.venue != market.Binance || c.budget == nil {
		return nil
	}
	return c.budget.AcquireKlines(ctx, limit)
}

func (c *Client) waitBingX(ctx context.Context) error {
	if c.bingXPacer == nil {
		return nil
	}
	return c.bingXPacer.wait(ctx)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// intervalPacer spaces calls by at least interval. It reserves a slot at entry
// so concurrent callers cannot all pass on the same instant.
type intervalPacer struct {
	mu       sync.Mutex
	next     time.Time
	interval time.Duration
}

func (p *intervalPacer) wait(ctx context.Context) error {
	p.mu.Lock()
	now := time.Now()
	if p.next.Before(now) {
		p.next = now
	}
	wait := p.next.Sub(now)
	p.next = p.next.Add(p.interval)
	p.mu.Unlock()

	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
