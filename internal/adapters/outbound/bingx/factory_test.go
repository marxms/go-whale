package bingx

import (
	"sync"
	"testing"
)

func swapConfig() ClientConfig {
	return ClientConfig{ExchangeID: "bingx", MarketType: "swap"}
}

func TestClientFactory_OneMarketSharesOnePool(t *testing.T) {
	factory := NewClientFactory()
	t.Cleanup(func() { _ = factory.Close() })

	first, err := factory.For(swapConfig())
	if err != nil {
		t.Fatalf("For(swap): %v", err)
	}
	second, err := factory.For(swapConfig())
	if err != nil {
		t.Fatalf("For(swap) second call: %v", err)
	}
	if first != second {
		t.Fatal("two calls for the same market returned different pools")
	}
}

func TestClientFactory_SpotAndFuturesAreSeparatePools(t *testing.T) {
	factory := NewClientFactory()
	t.Cleanup(func() { _ = factory.Close() })

	swap, err := factory.For(swapConfig())
	if err != nil {
		t.Fatalf("For(swap): %v", err)
	}
	spot, err := factory.For(ClientConfig{ExchangeID: "bingx", MarketType: "spot"})
	if err != nil {
		t.Fatalf("For(spot): %v", err)
	}
	if swap == spot {
		t.Fatal("spot and futures resolved to the same pool")
	}
}

func TestClientFactory_TwoFactoriesDoNotShareState(t *testing.T) {
	firstFactory := NewClientFactory()
	secondFactory := NewClientFactory()
	t.Cleanup(func() { _ = firstFactory.Close() })
	t.Cleanup(func() { _ = secondFactory.Close() })

	first, err := firstFactory.For(swapConfig())
	if err != nil {
		t.Fatalf("first factory: %v", err)
	}
	second, err := secondFactory.For(swapConfig())
	if err != nil {
		t.Fatalf("second factory: %v", err)
	}
	if first == second {
		t.Fatal("two factories returned the same pool: they share package-level state")
	}

	if err := firstFactory.Close(); err != nil {
		t.Fatalf("closing first factory: %v", err)
	}
	again, err := secondFactory.For(swapConfig())
	if err != nil {
		t.Fatalf("second factory after the first closed: %v", err)
	}
	if again != second {
		t.Fatal("closing the first factory replaced the second factory's pool")
	}
}

func TestClientFactory_ConcurrentCallsCreateExactlyOnePool(t *testing.T) {
	factory := NewClientFactory()
	t.Cleanup(func() { _ = factory.Close() })

	const callers = 64
	pools := make(chan StreamingClient, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool, err := factory.For(swapConfig())
			if err != nil {
				t.Errorf("For(swap): %v", err)
				return
			}
			pools <- pool
		}()
	}
	wg.Wait()
	close(pools)

	got := make([]StreamingClient, 0, callers)
	for pool := range pools {
		got = append(got, pool)
	}
	if len(got) != callers {
		t.Fatalf("%d of %d concurrent calls returned a pool", len(got), callers)
	}
	for _, pool := range got[1:] {
		if pool != got[0] {
			t.Fatal("concurrent factory calls created more than one pool for one market")
		}
	}
}

func TestClientFactory_RejectsAnotherExchange(t *testing.T) {
	factory := NewClientFactory()
	t.Cleanup(func() { _ = factory.Close() })

	if _, err := factory.For(ClientConfig{ExchangeID: "binance", MarketType: "swap"}); err == nil {
		t.Fatal("a non-BingX exchange was accepted")
	}
}
