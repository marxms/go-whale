package httpapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// stubBracketProvider stands in for the exchange-side catalog. Scope-aware, because the
// bracket table describes one venue and must not leak into another's market.
type stubBracketProvider struct {
	rates map[string]float64
}

func (s *stubBracketProvider) Tier1MaintenanceMarginRates(exchange, marketType string) map[string]float64 {
	if exchange != "binance" || marketType != "future" {
		return nil
	}
	return s.rates
}

// catalogStore serves the active-symbol catalog through the multi-exchange interface the
// handler prefers.
type catalogStore struct {
	mockStore
	symbols []candle.SymbolInfo
}

func (c *catalogStore) GetAllActiveSymbols(context.Context) ([]candle.SymbolInfo, error) {
	return c.symbols, nil
}

func (c *catalogStore) GetActiveSymbols(context.Context, string, string) ([]string, error) {
	out := make([]string, 0, len(c.symbols))
	for _, s := range c.symbols {
		out = append(out, s.Symbol)
	}
	return out, nil
}

func newCatalogHandler(store *catalogStore, brackets MarginBracketProvider) *Handler {
	return &Handler{
		candles:           store,
		batchCandles:      store,
		symbols:           store,
		allSymbols:        store,
		lastTrades:        store,
		defaultExchange:   "binance",
		defaultMarketType: "future",
		tfReg:             candle.NewTimeframeRegistry(testSupportedTimeframes()),
		datetimeCache:     newRFC3339Cache(),
		brackets:          brackets,
	}
}

func getSubscriptions(t *testing.T, handler *Handler) (SubscriptionsResponse, map[string]any) {
	t.Helper()

	rec := httptest.NewRecorder()
	handler.GetSubscriptions(rec, httptest.NewRequestWithContext(t.Context(), "GET", "/api/subscriptions", nil))

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var typed SubscriptionsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &typed); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw response: %v", err)
	}

	return typed, raw
}

// the tier-1 maintenance margin rate is EXPOSED in the catalog the service
// serves. A rate the analyzer cannot read is a rate the liquidation model does not have.
func TestGetSubscriptions_CarriesTier1MaintenanceMarginRate(t *testing.T) {
	store := &catalogStore{symbols: []candle.SymbolInfo{
		{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT"},
		{Exchange: "binance", MarketType: "future", Symbol: "ETH/USDT"},
	}}
	handler := newCatalogHandler(store, &stubBracketProvider{rates: map[string]float64{
		"BTC/USDT": 0.004,
		"ETH/USDT": 0.0065,
	}})

	resp, _ := getSubscriptions(t, handler)

	market := resp.Exchanges["binance"].Markets["future"]
	if got := market.MaintenanceMarginRate["BTC/USDT"]; got != 0.004 {
		t.Errorf("maintenance_margin_rate[\"BTC/USDT\"] = %v, want 0.004", got)
	}
	if got := market.MaintenanceMarginRate["ETH/USDT"]; got != 0.0065 {
		t.Errorf("maintenance_margin_rate[\"ETH/USDT\"] = %v, want 0.0065", got)
	}
	if len(market.Symbols) != 2 {
		t.Errorf("symbols = %v, want the 2 catalog symbols preserved", market.Symbols)
	}
}

// Without a bracket source the field is absent from the body, not zero. "No answer" is
// what lets the consumer fall back to its persisted value; a zero rate would be
// the silent default forbids.
func TestGetSubscriptions_OmitsRateWhenNoBracketSource(t *testing.T) {
	store := &catalogStore{symbols: []candle.SymbolInfo{
		{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT"},
	}}
	handler := newCatalogHandler(store, nil)

	_, raw := getSubscriptions(t, handler)

	exchanges := raw["exchanges"].(map[string]any)
	market := exchanges["binance"].(map[string]any)["markets"].(map[string]any)["future"].(map[string]any)
	if _, present := market["maintenance_margin_rate"]; present {
		t.Errorf("maintenance_margin_rate present = %v, want the key absent", market["maintenance_margin_rate"])
	}
	if _, present := market["symbols"]; !present {
		t.Error("symbols key missing — the existing catalog payload must be unchanged")
	}
}

// The served rates cover exactly the catalog's own symbols, and only the scope this
// bracket table describes. Shipping the exchange's full table would bloat a payload every
// consumer reads; serving another venue's rate would price its positions wrong.
func TestGetSubscriptions_RatesCoverOnlyCatalogueSymbolsInScope(t *testing.T) {
	store := &catalogStore{symbols: []candle.SymbolInfo{
		{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT"},
		{Exchange: "bingx", MarketType: "future", Symbol: "BTC/USDT"},
	}}
	handler := newCatalogHandler(store, &stubBracketProvider{rates: map[string]float64{
		"BTC/USDT":  0.004,
		"SOL/USDT":  0.02,
		"DOGE/USDT": 0.025,
	}})

	resp, _ := getSubscriptions(t, handler)

	binance := resp.Exchanges["binance"].Markets["future"]
	if len(binance.MaintenanceMarginRate) != 1 {
		t.Errorf("binance rates = %v, want only the 1 subscribed symbol", binance.MaintenanceMarginRate)
	}
	if got := binance.MaintenanceMarginRate["BTC/USDT"]; got != 0.004 {
		t.Errorf("binance BTC/USDT rate = %v, want 0.004", got)
	}

	bingx := resp.Exchanges["bingx"].Markets["future"]
	if bingx.MaintenanceMarginRate != nil {
		t.Errorf("bingx rates = %v, want nil — the table describes binance futures only", bingx.MaintenanceMarginRate)
	}
}
