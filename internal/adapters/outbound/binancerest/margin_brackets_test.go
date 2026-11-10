package binancerest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
)

// fastRetry keeps the retry model in play without paying its production backoff.
func fastRetry() exchange.RetryConfig {
	return exchange.RetryConfig{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond, BackoffMult: 2.0}
}

// btcBody is the shape the live endpoint returns, trimmed to what the catalog reads.
// The tier-1 numbers are the measured ones: floor 0, cap 300.000 USDT, MMR 0,40%.
func btcBody(rate float64) string {
	return fmt.Sprintf(`{
      "code":"000000","message":null,"success":true,
      "data":{"version":"1787359927131","brackets":[
        {"symbol":"BTCUSDT","updateTime":1755585384254,"riskBrackets":[
          {"bracketSeq":1,"bracketNotionalFloor":0,"bracketNotionalCap":300000,"bracketMaintenanceMarginRate":%v,"maxOpenPosLeverage":150},
          {"bracketSeq":2,"bracketNotionalFloor":300000,"bracketNotionalCap":4000000,"bracketMaintenanceMarginRate":0.005,"maxOpenPosLeverage":100}
        ]}
      ]}}`, rate)
}

func newTestCatalog(t *testing.T, url string) *MarginBracketCatalog {
	t.Helper()
	return NewMarginBracketCatalog(MarginBracketCatalogOptions{
		URL:            url,
		ReloadInterval: time.Hour,
		Retry:          fastRetry(),
	})
}

// The tier-1 maintenance margin rate is obtained per symbol. BTCUSDT tier 1 is
// 0,40% — the number the liquidation model needs to put the 100x band at 0,602% instead
// of 1,000%.
func TestMarginBracketCatalog_RefreshLoadsTier1RatePerSymbol(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(btcBody(0.004)))
	}))
	defer server.Close()

	catalog := newTestCatalog(t, server.URL)
	if err := catalog.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v, want nil", err)
	}

	rates := catalog.Tier1MaintenanceMarginRates("binance", "future")
	if got := rates["BTC/USDT"]; got != 0.004 {
		t.Errorf("tier-1 MMR for BTC/USDT = %v, want 0.004", got)
	}
}

// The exchange publishes BTCUSDT; the catalog this service serves is keyed BTC/USDT.
// A rate under the wrong key is a rate nobody can join to a symbol.
func TestMarginBracketCatalog_KeysBySlashSymbolAndDropsUnaddressable(t *testing.T) {
	body := `{"code":"000000","success":true,"data":{"brackets":[
      {"symbol":"BTCUSDT","riskBrackets":[{"bracketNotionalFloor":0,"bracketMaintenanceMarginRate":0.004}]},
      {"symbol":"ETHUSDT_240329","riskBrackets":[{"bracketNotionalFloor":0,"bracketMaintenanceMarginRate":0.01}]},
      {"symbol":"CVCUSDTSETTLED","riskBrackets":[{"bracketNotionalFloor":0,"bracketMaintenanceMarginRate":0.02}]},
      {"symbol":"ETHUSDC","riskBrackets":[{"bracketNotionalFloor":0,"bracketMaintenanceMarginRate":0.0065}]}
    ]}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	catalog := newTestCatalog(t, server.URL)
	if err := catalog.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v, want nil", err)
	}

	rates := catalog.Tier1MaintenanceMarginRates("binance", "future")
	if got := rates["BTC/USDT"]; got != 0.004 {
		t.Errorf("rates[\"BTC/USDT\"] = %v, want 0.004", got)
	}
	if got := rates["ETH/USDC"]; got != 0.0065 {
		t.Errorf("rates[\"ETH/USDC\"] = %v, want 0.0065", got)
	}
	if len(rates) != 2 {
		t.Errorf("len(rates) = %d, want 2 — dated and settled markets must not be addressable", len(rates))
	}
}

// Tier 1 is the bracket with the lowest notional floor, not whatever the transport put
// first. Reading position 0 would hand the model the 0,5% tier for BTCUSDT and move every
// band.
func TestMarginBracketCatalog_PicksLowestNotionalFloorTier(t *testing.T) {
	body := `{"code":"000000","success":true,"data":{"brackets":[
      {"symbol":"BTCUSDT","riskBrackets":[
        {"bracketNotionalFloor":4000000,"bracketMaintenanceMarginRate":0.01},
        {"bracketNotionalFloor":300000,"bracketMaintenanceMarginRate":0.005},
        {"bracketNotionalFloor":0,"bracketMaintenanceMarginRate":0.004}
      ]}
    ]}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	catalog := newTestCatalog(t, server.URL)
	if err := catalog.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v, want nil", err)
	}

	if got := catalog.Tier1MaintenanceMarginRates("binance", "future")["BTC/USDT"]; got != 0.004 {
		t.Errorf("tier-1 MMR = %v, want 0.004 (floor-0 bracket), not the array's first element", got)
	}
}

// "Unauthenticated source; no credential is introduced." The request must carry no
// key, in any of the three places Binance accepts one.
func TestMarginBracketCatalog_RequestCarriesNoCredential(t *testing.T) {
	var (
		mu      sync.Mutex
		seen    http.Header
		rawQuer string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = r.Header.Clone()
		rawQuer = r.URL.RawQuery
		mu.Unlock()
		_, _ = w.Write([]byte(btcBody(0.004)))
	}))
	defer server.Close()

	catalog := newTestCatalog(t, server.URL)
	if err := catalog.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, header := range []string{"Authorization", "X-Mbx-Apikey", "Api-Key"} {
		if value := seen.Get(header); value != "" {
			t.Errorf("request carried credential header %s=%q, want absent", header, value)
		}
	}
	if rawQuer != "" {
		t.Errorf("request query = %q, want empty — no signature or key in the URL", rawQuer)
	}
}

// "A fetch failure does not kill the cycle: it logs an error and serves the last known
// value." The failing refresh must report the failure AND leave the loaded table standing.
func TestMarginBracketCatalog_FailedRefreshKeepsLastKnownTable(t *testing.T) {
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(btcBody(0.004)))
	}))
	defer server.Close()

	catalog := newTestCatalog(t, server.URL)
	if err := catalog.Refresh(context.Background()); err != nil {
		t.Fatalf("first Refresh() error = %v, want nil", err)
	}

	fail.Store(true)
	if err := catalog.Refresh(context.Background()); err == nil {
		t.Fatal("second Refresh() error = nil, want a reported failure")
	}

	if got := catalog.Tier1MaintenanceMarginRates("binance", "future")["BTC/USDT"]; got != 0.004 {
		t.Errorf("after failed refresh tier-1 MMR = %v, want the last known 0.004", got)
	}
}

// "The table is reloaded periodically, not cached indefinitely." A value changed
// upstream must reach the catalog without a restart.
func TestMarginBracketCatalog_ReloadsPeriodically(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		rate := 0.004
		if hits.Add(1) > 1 {
			rate = 0.006
		}
		_, _ = w.Write([]byte(btcBody(rate)))
	}))
	defer server.Close()

	catalog := NewMarginBracketCatalog(MarginBracketCatalogOptions{
		URL:            server.URL,
		ReloadInterval: 5 * time.Millisecond,
		Retry:          fastRetry(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	catalog.Start(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if catalog.Tier1MaintenanceMarginRates("binance", "future")["BTC/USDT"] == 0.006 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}

	t.Fatalf("tier-1 MMR never reloaded to 0.006 after %d fetches — the table is cached indefinitely", hits.Load())
}

// The reload loop must survive a failing upstream: it keeps ticking and picks the table up
// once the source recovers, instead of dying with the first error.
func TestMarginBracketCatalog_ReloadLoopSurvivesFailure(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(btcBody(0.004)))
	}))
	defer server.Close()

	catalog := NewMarginBracketCatalog(MarginBracketCatalogOptions{
		URL:            server.URL,
		ReloadInterval: 5 * time.Millisecond,
		Retry:          fastRetry(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	catalog.Start(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if catalog.Tier1MaintenanceMarginRates("binance", "future")["BTC/USDT"] == 0.004 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}

	t.Fatalf("catalog never recovered after the upstream came back (%d fetches)", hits.Load())
}

// Edge case (spec): a body that is not the expected structure is an upstream ERROR, not an
// empty table. Swallowing it would publish "no MMR for any symbol" as if it were fact.
func TestMarginBracketCatalog_MalformedBodyIsUpstreamError(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "not json", body: `<html>blocked</html>`},
		{name: "envelope reports failure", body: `{"code":"000001","message":"service busy","success":false,"data":null}`},
		{name: "empty bracket list", body: `{"code":"000000","success":true,"data":{"brackets":[]}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			catalog := newTestCatalog(t, server.URL)
			if err := catalog.Refresh(context.Background()); err == nil {
				t.Fatal("Refresh() error = nil, want an upstream error")
			}
			if got := catalog.Size(); got != 0 {
				t.Errorf("catalog size = %d, want 0 — a broken body must not publish a table", got)
			}
		})
	}
}

// A maintenance margin rate outside (0, 1) is unusable: zero makes 1/MMR infinite and
// admits any leverage, and one or more puts the liquidation price at or past the entry.
// Such a symbol is omitted, never defaulted.
func TestMarginBracketCatalog_OmitsUnusableRates(t *testing.T) {
	body := `{"code":"000000","success":true,"data":{"brackets":[
      {"symbol":"AAAUSDT","riskBrackets":[{"bracketNotionalFloor":0,"bracketMaintenanceMarginRate":0}]},
      {"symbol":"BBBUSDT","riskBrackets":[{"bracketNotionalFloor":0,"bracketMaintenanceMarginRate":-0.01}]},
      {"symbol":"CCCUSDT","riskBrackets":[{"bracketNotionalFloor":0,"bracketMaintenanceMarginRate":1}]},
      {"symbol":"BTCUSDT","riskBrackets":[
        {"bracketNotionalFloor":0,"bracketMaintenanceMarginRate":0},
        {"bracketNotionalFloor":300000,"bracketMaintenanceMarginRate":0.005}
      ]}
    ]}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	catalog := newTestCatalog(t, server.URL)
	if err := catalog.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v, want nil", err)
	}

	rates := catalog.Tier1MaintenanceMarginRates("binance", "future")
	for _, symbol := range []string{"AAA/USDT", "BBB/USDT", "CCC/USDT"} {
		if rate, ok := rates[symbol]; ok {
			t.Errorf("rates[%q] = %v, want absent — the rate is outside (0,1)", symbol, rate)
		}
	}
	if got := rates["BTC/USDT"]; got != 0.005 {
		t.Errorf("rates[\"BTC/USDT\"] = %v, want 0.005 — the unusable tier is skipped, the symbol is not", got)
	}
}

// This table describes Binance USDT-M futures. Any other scope gets nothing, so no other
// venue's positions are ever priced with Binance's maintenance margin.
func TestMarginBracketCatalog_ScopedToBinanceFutures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(btcBody(0.004)))
	}))
	defer server.Close()

	catalog := newTestCatalog(t, server.URL)
	if err := catalog.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v, want nil", err)
	}

	if got := catalog.Tier1MaintenanceMarginRates("bingx", "future"); got != nil {
		t.Errorf("rates for bingx/future = %v, want nil", got)
	}
	if got := catalog.Tier1MaintenanceMarginRates("binance", "spot"); got != nil {
		t.Errorf("rates for binance/spot = %v, want nil", got)
	}
	if got := catalog.Tier1MaintenanceMarginRates("binance", "future"); len(got) != 1 {
		t.Errorf("rates for binance/future = %v, want the loaded table", got)
	}
}

// The returned map is a copy. The reload goroutine replaces the table while handlers read
// it, so handing out the live map is a data race and lets a caller corrupt the source.
func TestMarginBracketCatalog_ReturnsDefensiveCopy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(btcBody(0.004)))
	}))
	defer server.Close()

	catalog := newTestCatalog(t, server.URL)
	if err := catalog.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v, want nil", err)
	}

	first := catalog.Tier1MaintenanceMarginRates("binance", "future")
	first["BTC/USDT"] = 0.99
	delete(first, "BTC/USDT")

	if got := catalog.Tier1MaintenanceMarginRates("binance", "future")["BTC/USDT"]; got != 0.004 {
		t.Errorf("after mutating the returned map, tier-1 MMR = %v, want 0.004", got)
	}
}
