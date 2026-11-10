package bingx

import (
	"context"
	"sync"
	"testing"

	"github.com/marxms/go-whale/internal/trade"
)

// TestBingX_GetTrades_ReturnsIsolatedCopy is the DETERMINISTIC proof of the aliasing
// defect: returning the buffer (same backing array) and resetting the map slot to buf[:0]
// lets the next handleTradeMessage append overwrite buf[0..] — the very trades the consumer
// is still holding. Without the copy, got is overwritten to 99,99; with it, got stays 1,2.
func TestBingX_GetTrades_ReturnsIsolatedCopy(t *testing.T) {
	const sym = "BTC-USDT"
	c := &BingXClient{
		tradeBuffers:    map[string][]trade.Trade{sym: make([]trade.Trade, 0, 16)},
		subscriptions:   map[string]bool{sym: true},
		tradeBufferSize: 16,
		config:          ClientConfig{ExchangeID: "bingx", MarketType: "swap"},
	}
	c.tradeBuffers[sym] = append(c.tradeBuffers[sym], trade.Trade{Price: 1}, trade.Trade{Price: 2})

	got, err := c.GetTrades(context.Background(), sym)
	if err != nil || len(got) != 2 {
		t.Fatalf("GetTrades: err=%v len=%d", err, len(got))
	}

	// Simulate the next handleTradeMessage append into the reset slot (writes the same
	// backing array an unpadded slice would hand back).
	c.subsMu.Lock()
	c.tradeBuffers[sym] = append(c.tradeBuffers[sym], trade.Trade{Price: 99}, trade.Trade{Price: 99})
	c.subsMu.Unlock()

	if got[0].Price != 1 || got[1].Price != 2 {
		t.Fatalf("GetTrades returned an ALIASED slice — a later append overwrote it: got=%v (want [1 2])",
			[]float64{got[0].Price, got[1].Price})
	}
}

// TestBingX_GetTrades_NoAliasingRace is the concurrency guard (run with -race): concurrent
// drain + append on the same symbol; the reader READS each element (forcing a load of the
// backing array), so -race flags any aliasing.
func TestBingX_GetTrades_NoAliasingRace(t *testing.T) {
	const sym = "BTC-USDT"
	c := &BingXClient{
		tradeBuffers:    map[string][]trade.Trade{sym: make([]trade.Trade, 0, 2048)},
		subscriptions:   map[string]bool{sym: true},
		tradeBufferSize: 2048,
		config:          ClientConfig{ExchangeID: "bingx", MarketType: "swap"},
	}
	for i := 0; i < 64; i++ {
		c.tradeBuffers[sym] = append(c.tradeBuffers[sym], trade.Trade{Price: float64(i)})
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 3000; i++ {
			c.subsMu.Lock()
			if len(c.tradeBuffers[sym]) < c.tradeBufferSize {
				c.tradeBuffers[sym] = append(c.tradeBuffers[sym], trade.Trade{Price: 1})
			}
			c.subsMu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		var sink float64
		for i := 0; i < 3000; i++ {
			ts, _ := c.GetTrades(context.Background(), sym)
			for j := range ts {
				sink += ts[j].Price
			}
		}
		if sink < 0 {
			t.Error("unreachable")
		}
	}()
	wg.Wait()
}
