package bingx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
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

// Parse failures and buffer overflows are LOCAL losses: each one is exactly one
// MALFORMED_FRAME/BUFFER_OVERFLOW with reason, symbol, venue, market and the
// pre-Connect partition. Double-counting or a single global log line fails here.
func TestBingXMalformedAndOverflowUseRecordDrop(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)
	client := NewBingXClient(ClientConfig{ExchangeID: "bingx", MarketType: "swap", TradeBufferSize: 1}, log)
	capture := &bingxDropCapture{}
	client.SetMetrics(capture.metrics())
	client.SetPartition(3)
	client.subsMu.Lock()
	client.subscriptions["BTC/USDT"] = true
	client.subscriptions["ETH/USDT"] = true
	client.subsMu.Unlock()

	valid := func(price string) map[string]interface{} {
		return map[string]interface{}{"p": price, "q": "1", "T": float64(7200000), "m": false}
	}
	client.handleTradeMessage(map[string]interface{}{
		"dataType": "BTC-USDT@trade",
		"data":     []interface{}{"not-a-map", valid("100"), valid("101")},
	})
	client.handleTradeMessage(map[string]interface{}{
		"dataType": "ETH-USDT@trade",
		"data":     []interface{}{valid("200"), valid("201")},
	})

	if len(capture.events) != 3 {
		t.Fatalf("classified=%d, want 3 (1 malformed + 2 overflow)", len(capture.events))
	}
	reasons := map[trade.DropReason]int{}
	for _, event := range capture.events {
		reasons[event.Reason]++
		if event.Exchange != "bingx" || event.MarketType != market.Future || event.Partition != 3 {
			t.Fatalf("location: %+v, want bingx/future/3", event)
		}
		if event.Symbol != "BTC/USDT" && event.Symbol != "ETH/USDT" {
			t.Fatalf("symbol: %+v", event)
		}
	}
	if reasons[trade.DropMalformedFrame] != 1 || reasons[trade.DropBufferOverflow] != 2 {
		t.Fatalf("reasons=%v, want 1 malformed / 2 overflow", reasons)
	}
	if got := client.tradesDropped.Load(); got != uint64(len(capture.events)) {
		t.Fatalf("tradesDropped=%d, classified=%d — legacy deve ser a soma classificada", got, len(capture.events))
	}
	if got := client.tradesIn.Load(); got != 2 {
		t.Fatalf("tradesIn=%d, want 2 (um por símbolo)", got)
	}
	overflowLines := 0
	malformedLines := 0
	for _, record := range sink.Records() {
		if strings.Contains(record.Text(), "trade buffer full") {
			overflowLines++
		}
		if strings.Contains(record.Text(), "trade parse failed") {
			malformedLines++
		}
	}
	if overflowLines != 2 {
		t.Fatalf("overflow log lines=%d, want 2 — first log por símbolo/local, não global", overflowLines)
	}
	if malformedLines != 1 {
		t.Fatalf("malformed log lines=%d, want 1 por local", malformedLines)
	}
}

// SetPartition(connIndex) must run BEFORE Connect. The spy observes the
// partition at Connect time; moving the assignment after Connect records 0.
func TestBingXPoolAssignsPartitionBeforeConnect(t *testing.T) {
	pool := newBingXPool(
		ClientConfig{ExchangeID: "bingx", MarketType: "swap", MaxSymbolsPerConnection: 1},
		logger.OrDiscard(),
	)
	// Occupy connection 0 so Subscribe must create connection 1.
	pool.connections = []*BingXClient{{}}
	pool.symbolMap["EXISTING/USDT"] = 0

	var observed []int
	sentinel := errors.New("stop before network")
	pool.connectClient = func(client *BingXClient, _ context.Context) error {
		observed = append(observed, client.partition)
		return sentinel
	}

	if err := pool.Subscribe(context.Background(), "BTC/USDT"); !errors.Is(err, sentinel) {
		t.Fatalf("Subscribe error=%v, want the connect seam's sentinel (no network dial)", err)
	}
	if len(observed) != 1 || observed[0] != 1 {
		t.Fatalf("partition at Connect=%v, want [1] — SetPartition precisa ocorrer antes do Connect", observed)
	}
}
