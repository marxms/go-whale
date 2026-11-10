package binancesdk

import (
	"sync"
	"testing"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// oneMarketOneClient is the transport invariant the catalog exists for: a fleet of
// symbols on one market shares one WebSocket client instead of dialing one socket each.
func TestClientFactory_OneMarketSharesOneClient(t *testing.T) {
	factory := NewClientFactory()
	t.Cleanup(func() { _ = factory.Close() })

	first, err := factory.For(streaming.ClientConfig{MarketType: "swap"})
	if err != nil {
		t.Fatalf("For(swap): %v", err)
	}
	second, err := factory.For(streaming.ClientConfig{MarketType: "swap"})
	if err != nil {
		t.Fatalf("For(swap) second call: %v", err)
	}
	if first != second {
		t.Fatal("two calls for the same market returned different clients: the fleet would exhaust WebSocket connections")
	}
}

// Spot and futures must never share buffers: same factory, different market key.
func TestClientFactory_SpotAndFuturesAreSeparate(t *testing.T) {
	factory := NewClientFactory()
	t.Cleanup(func() { _ = factory.Close() })

	swap, err := factory.For(streaming.ClientConfig{MarketType: "swap"})
	if err != nil {
		t.Fatalf("For(swap): %v", err)
	}
	spot, err := factory.For(streaming.ClientConfig{MarketType: "spot"})
	if err != nil {
		t.Fatalf("For(spot): %v", err)
	}
	if swap == spot {
		t.Fatal("spot and futures resolved to the same client: their buffers would be shared")
	}
}

// Two factories in one process are two catalogs. This is the state the package-level
// registry made impossible to have, and the reason a test had to call a reset function.
func TestClientFactory_TwoFactoriesDoNotShareState(t *testing.T) {
	firstFactory := NewClientFactory()
	secondFactory := NewClientFactory()
	t.Cleanup(func() { _ = firstFactory.Close() })
	t.Cleanup(func() { _ = secondFactory.Close() })

	first, err := firstFactory.For(streaming.ClientConfig{MarketType: "swap"})
	if err != nil {
		t.Fatalf("first factory: %v", err)
	}
	second, err := secondFactory.For(streaming.ClientConfig{MarketType: "swap"})
	if err != nil {
		t.Fatalf("second factory: %v", err)
	}
	if first == second {
		t.Fatal("two factories returned the same client: they share package-level state")
	}

	// Closing one catalog must not empty the other.
	if err := firstFactory.Close(); err != nil {
		t.Fatalf("closing first factory: %v", err)
	}
	again, err := secondFactory.For(streaming.ClientConfig{MarketType: "swap"})
	if err != nil {
		t.Fatalf("second factory after the first closed: %v", err)
	}
	if again != second {
		t.Fatal("closing the first factory replaced the second factory's client")
	}
}

// Concurrent callers for the same market must resolve to exactly one client. Run with
// -race: the counter proves the mutex covers creation, not just lookup.
func TestClientFactory_ConcurrentCallsCreateExactlyOneClient(t *testing.T) {
	factory := NewClientFactory()
	t.Cleanup(func() { _ = factory.Close() })

	const callers = 64
	clients := make(chan streaming.StreamingClient, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := factory.For(streaming.ClientConfig{MarketType: "swap"})
			if err != nil {
				t.Errorf("For(swap): %v", err)
				return
			}
			clients <- client
		}()
	}
	wg.Wait()
	close(clients)

	got := make([]streaming.StreamingClient, 0, callers)
	for client := range clients {
		got = append(got, client)
	}
	if len(got) != callers {
		t.Fatalf("%d of %d concurrent calls returned a client", len(got), callers)
	}
	for _, client := range got[1:] {
		if client != got[0] {
			t.Fatal("concurrent factory calls created more than one client for one market")
		}
	}
}

func TestClientFactory_RejectsAnUnsupportedMarket(t *testing.T) {
	factory := NewClientFactory()
	t.Cleanup(func() { _ = factory.Close() })

	if _, err := factory.For(streaming.ClientConfig{MarketType: "margin"}); err == nil {
		t.Fatal("an unsupported market type was accepted")
	}
}
