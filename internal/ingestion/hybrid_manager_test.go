package ingestion

import (
	"context"
	"testing"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/trade"
)

type noopTradeHandler struct{}

func (n noopTradeHandler) HandleTrades(_ string, _ any) error {
	return nil
}

func (n noopTradeHandler) HandleTradesWithContext(_, _, _ string, _ any) error {
	return nil
}

type mockStreamingClient struct {
	unsubscribed []string
	closeCount   int
}

func (m *mockStreamingClient) Subscribe(_ context.Context, _ string) error {
	return nil
}

func (m *mockStreamingClient) Unsubscribe(symbol string) error {
	m.unsubscribed = append(m.unsubscribed, symbol)
	return nil
}

func (m *mockStreamingClient) GetTrades(_ context.Context, _ string) ([]trade.Trade, error) {
	return nil, nil
}

func (m *mockStreamingClient) HealthCheck() bool {
	return true
}

func (m *mockStreamingClient) Type() streaming.ClientType {
	return streaming.ClientTypeNative
}

func (m *mockStreamingClient) Close() error {
	m.closeCount++
	return nil
}

type mockSharedStreamingClient struct {
	mockStreamingClient
}

func (m *mockSharedStreamingClient) IsSharedSubscriptionClient() bool {
	return true
}

// The catalog belongs to ONE manager. Two managers built with different catalogs in
// the same process must resolve their own exchange to their own client; a package-level
// registry made the first lookup win for both.
func TestHybridManager_CatalogueIsOwnedPerManager(t *testing.T) {
	clientA := &mockStreamingClient{}
	clientB := &mockStreamingClient{}
	var callsA, callsB int

	managerA := NewHybridManager(noopTradeHandler{}, 1, map[string]streaming.ClientFactory{
		"binance": func(streaming.ClientConfig) (streaming.StreamingClient, error) {
			callsA++
			return clientA, nil
		},
	})
	managerB := NewHybridManager(noopTradeHandler{}, 1, map[string]streaming.ClientFactory{
		"binance": func(streaming.ClientConfig) (streaming.StreamingClient, error) {
			callsB++
			return clientB, nil
		},
	})
	t.Cleanup(managerA.Stop)
	t.Cleanup(managerB.Stop)

	if err := managerA.Subscribe(context.Background(), "BTC/USDT", "binance", market.Future); err != nil {
		t.Fatalf("manager A Subscribe: %v", err)
	}
	if err := managerB.Subscribe(context.Background(), "BTC/USDT", "binance", market.Future); err != nil {
		t.Fatalf("manager B Subscribe: %v", err)
	}

	key := "binance:future:BTC/USDT"
	if got := managerA.clients[key].Client; got != clientA {
		t.Fatal("manager A resolved its catalog entry to another manager's client")
	}
	if got := managerB.clients[key].Client; got != clientB {
		t.Fatal("manager B resolved its catalog entry to another manager's client")
	}
	if callsA != 1 || callsB != 1 {
		t.Fatalf("factory calls: A=%d B=%d, want one each", callsA, callsB)
	}
}

func TestUnsubscribeDoesNotCloseSharedSubscriptionClient(t *testing.T) {
	manager := NewHybridManager(noopTradeHandler{}, 1, nil)
	client := &mockSharedStreamingClient{}
	key := "binance:future:BTC/USDT"

	manager.clients[key] = &HybridClientState{
		Client:     client,
		Symbol:     "BTC/USDT",
		Exchange:   "binance",
		MarketType: market.Future,
		Key:        key,
	}

	err := manager.Unsubscribe("BTC/USDT", "binance", market.Future)
	if err != nil {
		t.Fatalf("unexpected unsubscribe error: %v", err)
	}
	if client.closeCount != 0 {
		t.Fatalf("expected shared client not to close, got closeCount=%d", client.closeCount)
	}
	if len(client.unsubscribed) != 1 || client.unsubscribed[0] != "BTC/USDT" {
		t.Fatalf("expected one unsubscribe for BTC/USDT, got %+v", client.unsubscribed)
	}
}

func TestUnsubscribeClosesNonSharedClientWithoutPeers(t *testing.T) {
	manager := NewHybridManager(noopTradeHandler{}, 1, nil)
	client := &mockStreamingClient{}
	key := "bingx:future:BTC/USDT"

	manager.clients[key] = &HybridClientState{
		Client:     client,
		Symbol:     "BTC/USDT",
		Exchange:   "bingx",
		MarketType: market.Future,
		Key:        key,
	}

	err := manager.Unsubscribe("BTC/USDT", "bingx", market.Future)
	if err != nil {
		t.Fatalf("unexpected unsubscribe error: %v", err)
	}
	if client.closeCount != 1 {
		t.Fatalf("expected non-shared client to close once, got closeCount=%d", client.closeCount)
	}
	if len(client.unsubscribed) != 1 || client.unsubscribed[0] != "BTC/USDT" {
		t.Fatalf("expected one unsubscribe for BTC/USDT, got %+v", client.unsubscribed)
	}
}
