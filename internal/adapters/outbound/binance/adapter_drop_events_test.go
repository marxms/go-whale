package binancesdk

import (
	"testing"

	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/trade"
)

// dropCapture records the two counters that must move together: the legacy
// aggregate and the classified series carrying reason/symbol/partition.
type dropCapture struct {
	legacy       int
	classified   []trade.DropEvent
	panicMessage bool
}

func (d *dropCapture) metrics() Metrics {
	return Metrics{
		MessagesInTotal: func(_, _ string) {
			if d.panicMessage {
				panic("injected callback panic")
			}
		},
		TradesDroppedTotal: func(_, _ string) { d.legacy++ },
		TradesDroppedClassified: func(exchange, marketType, symbol string, partition int, reason string) {
			d.classified = append(d.classified, trade.DropEvent{
				Reason:     trade.DropReason(reason),
				Exchange:   exchange,
				MarketType: market.MarketType(marketType),
				Symbol:     symbol,
				Partition:  partition,
			})
		},
	}
}

// Every local loss carries one reason from the closed taxonomy and its full
// location, and the legacy aggregate is exactly the sum of the classified
// deltas. This is the invariant dashboards rely on to reconcile the two series.
func TestDropEventsFiveReasonsMatchLegacyAggregate(t *testing.T) {
	client := &SDKClient{buffers: NewBufferManager(4)}
	sink := &dropCapture{}
	client.metrics = sink.metrics()

	for _, reason := range trade.DropReasons() {
		client.recordDrop(reason, "BTC/USDT", 7)
	}

	if len(trade.DropReasons()) != 5 {
		t.Fatalf("taxonomia mudou: %d razões, o contrato tem 5", len(trade.DropReasons()))
	}
	if sink.legacy != 5 || len(sink.classified) != 5 {
		t.Fatalf("legacy=%d classified=%d, want 5/5", sink.legacy, len(sink.classified))
	}
	for index, want := range trade.DropReasons() {
		event := sink.classified[index]
		if event.Reason != want || !event.Reason.Valid() {
			t.Fatalf("evento %d: reason=%q, want %q", index, event.Reason, want)
		}
		if event.Exchange != "binance" || event.MarketType != market.Future {
			t.Fatalf("evento %d: venue=%q market=%q", index, event.Exchange, event.MarketType)
		}
		if event.Symbol != "BTC/USDT" || event.Partition != 7 {
			t.Fatalf("evento %d: symbol=%q partition=%d", index, event.Symbol, event.Partition)
		}
	}
}

// The callback's four reachable branches each spend exactly one classified
// drop with the REAL partition of the connection that delivered the frame.
func TestTradeCallbackRoutesDropsWithRealPartition(t *testing.T) {
	client := &SDKClient{buffers: NewBufferManager(1)}
	sink := &dropCapture{}
	client.metrics = sink.metrics()
	handler := client.tradeCallback("BTC/USDT", 3)
	valid := []byte(`{"e":"aggTrade","s":"BTC/USDT","a":1,"p":"101.5","q":"2","T":1793882096789,"m":false}`)

	handler([]byte(`{`))
	handler(valid)
	handler(valid)
	handler([]byte(`{"e":"aggTrade","s":"BTC/USDT","a":2,"p":"","q":"2","T":1793882096790,"m":false}`))

	want := []trade.DropReason{
		trade.DropMalformedFrame,
		trade.DropBufferOverflow,
		trade.DropNilTrade,
	}
	if len(sink.classified) != len(want) {
		t.Fatalf("classified=%d, want %d", len(sink.classified), len(want))
	}
	for index, reason := range want {
		event := sink.classified[index]
		if event.Reason != reason {
			t.Fatalf("evento %d: reason=%q, want %q", index, event.Reason, reason)
		}
		if event.Partition != 3 || event.Symbol != "BTC/USDT" {
			t.Fatalf("evento %d: symbol=%q partition=%d, want BTC/USDT/3", index, event.Symbol, event.Partition)
		}
	}
	if sink.legacy != len(want) {
		t.Fatalf("legacy=%d, want %d (agregado deve ser a soma dos classificados)", sink.legacy, len(want))
	}
	if got := client.tradesDropped.Load(); got != uint64(len(want)) {
		t.Fatalf("tradesDropped=%d, want %d", got, len(want))
	}
	// Ingested and dropped are disjoint: only the stored trade advanced tradesIn.
	if got := client.tradesIn.Load(); got != 1 {
		t.Fatalf("tradesIn=%d, want 1 — o drop não pode avançar o contador de ingeridos", got)
	}
}

// A panic anywhere in the callback is recovered and spent as PANIC with the
// same location, not silently swallowed. The panic is injected at a metrics
// seam so the recover branch is deterministic; the branch, not its source, is
// what this asserts.
func TestCallbackRecoverRoutesPanicWithPartition(t *testing.T) {
	client := &SDKClient{buffers: NewBufferManager(4)}
	sink := &dropCapture{panicMessage: true}
	client.metrics = sink.metrics()

	client.tradeCallback("BTC/USDT", 5)([]byte(`{"e":"aggTrade","s":"BTC/USDT","a":1,"p":"101.5","q":"2","T":1793882096789,"m":false}`))

	if len(sink.classified) != 1 || sink.classified[0].Reason != trade.DropPanic {
		t.Fatalf("panic não virou PANIC: %+v", sink.classified)
	}
	event := sink.classified[0]
	if event.Symbol != "BTC/USDT" || event.Partition != 5 {
		t.Fatalf("localização do panic: symbol=%q partition=%d", event.Symbol, event.Partition)
	}
	if sink.legacy != 1 || client.tradesDropped.Load() != 1 {
		t.Fatalf("legacy=%d counter=%d, want 1/1", sink.legacy, client.tradesDropped.Load())
	}
	if got := client.tradesIn.Load(); got != 0 {
		t.Fatalf("tradesIn=%d, want 0 — um panic não pode contar como ingerido", got)
	}
}
