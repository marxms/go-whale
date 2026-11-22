package bingx

import (
	"errors"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/trade"
)

type bingxDropCapture struct {
	events []trade.DropEvent
}

func (d *bingxDropCapture) metrics() Metrics {
	return Metrics{
		TradesDroppedClassified: func(exchange, marketType, symbol string, partition int, reason string) {
			d.events = append(d.events, trade.DropEvent{
				Reason:     trade.DropReason(reason),
				Exchange:   exchange,
				MarketType: market.MarketType(marketType),
				Symbol:     symbol,
				Partition:  partition,
			})
		},
	}
}

// recordingGapQueue proves cleanup never reports a provider gap: a local
// discard is not a missing trade.
type recordingGapQueue struct {
	calls int
}

func (q *recordingGapQueue) EnqueueBatch(_, _ string, _ []string, _, _ time.Time) { q.calls++ }

// Every item cleanupOnError discards produces ONE CLEANUP_DISCARD with the
// client's pre-assigned pool partition. Map iteration order is irrelevant, and
// a second cleanup after the buffers were reset cannot double-count.
func TestBingXCleanupCountsEveryDiscardWithPoolPartition(t *testing.T) {
	// Twenty runs exercise Go's randomized map order; the per-symbol counts must
	// not depend on which symbol the loop visits first.
	for run := 0; run < 20; run++ {
		capture := &bingxDropCapture{}
		client := NewBingXClient(ClientConfig{ExchangeID: "bingx", MarketType: "swap"})
		client.SetMetrics(capture.metrics())
		client.SetPartition(2)
		queue := &recordingGapQueue{}
		client.gapRecovery = queue

		client.subsMu.Lock()
		client.subscriptions["BTC/USDT"] = true
		client.subscriptions["ETH/USDT"] = true
		client.tradeBuffers["BTC/USDT"] = make([]trade.Trade, 3)
		client.tradeBuffers["ETH/USDT"] = make([]trade.Trade, 2)
		client.subsMu.Unlock()

		client.cleanupOnError(errors.New("disconnect"))

		if len(capture.events) != 5 {
			t.Fatalf("run %d: classified=%d, want 5", run, len(capture.events))
		}
		perSymbol := map[string]int{}
		for _, event := range capture.events {
			if event.Reason != trade.DropCleanupDiscard {
				t.Fatalf("run %d: reason=%q, want CLEANUP_DISCARD", run, event.Reason)
			}
			if event.Exchange != "bingx" || event.MarketType != market.Future {
				t.Fatalf("run %d: venue=%q market=%q, want bingx/future", run, event.Exchange, event.MarketType)
			}
			if event.Partition != 2 {
				t.Fatalf("run %d: partition=%d, want the pre-Connect index 2", run, event.Partition)
			}
			perSymbol[event.Symbol]++
		}
		if perSymbol["BTC/USDT"] != 3 || perSymbol["ETH/USDT"] != 2 {
			t.Fatalf("run %d: per-symbol=%v, want BTC/USDT:3 ETH/USDT:2 — uma contagem por item", run, perSymbol)
		}
		if got := client.tradesDropped.Load(); got != 5 {
			t.Fatalf("run %d: tradesDropped=%d, want 5 (legacy == soma classificada)", run, got)
		}
		if got := client.ConnectionStats().TradesDropped; got != 5 {
			t.Fatalf("run %d: ConnectionStats=%d, want 5", run, got)
		}
		if queue.calls != 0 {
			t.Fatalf("run %d: cleanup enfileirou gap de provider (%d) — descarte local não é missing", run, queue.calls)
		}

		// Buffers were reset to empty: the same cleanup must not count again.
		client.cleanupOnError(errors.New("second disconnect"))
		if len(capture.events) != 5 {
			t.Fatalf("run %d: segundo cleanup duplicou drops (%d)", run, len(capture.events))
		}
	}
}
