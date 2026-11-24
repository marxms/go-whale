package binancesdk

import (
	"testing"

	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/trade"
)

// Spot conforms to the same reason/identity contract as futures: one reason
// from the closed taxonomy and the real partition per local loss, and the
// legacy aggregate is the sum of the classified deltas. Dedup keys are not
// touched here.
func TestSpotDropEventsFiveReasonsMatchLegacyAggregate(t *testing.T) {
	client := &SpotSDKClient{buffers: NewBufferManager(4)}
	sink := &dropCapture{}
	client.metrics = sink.metrics()

	for _, reason := range trade.DropReasons() {
		client.recordDrop(reason, "BTC/USDT", 11)
	}

	if len(sink.classified) != 5 || sink.legacy != 5 {
		t.Fatalf("classified=%d legacy=%d, want 5/5", len(sink.classified), sink.legacy)
	}
	for index, want := range trade.DropReasons() {
		event := sink.classified[index]
		if event.Reason != want {
			t.Fatalf("evento %d: reason=%q, want %q", index, event.Reason, want)
		}
		if event.Exchange != "binance" || event.MarketType != market.Spot {
			t.Fatalf("evento %d: venue=%q market=%q", index, event.Exchange, event.MarketType)
		}
		if event.Symbol != "BTC/USDT" || event.Partition != 11 {
			t.Fatalf("evento %d: symbol=%q partition=%d", index, event.Symbol, event.Partition)
		}
	}
	if got := client.tradesDropped.Load(); got != 5 {
		t.Fatalf("tradesDropped=%d, want 5", got)
	}
}

// The spot callback routes its reachable branches with the real partition and
// keeps ingested disjoint from dropped.
func TestSpotCallbackRoutesDropsWithRealPartition(t *testing.T) {
	client := &SpotSDKClient{buffers: NewBufferManager(1)}
	sink := &dropCapture{}
	client.metrics = sink.metrics()
	handler := client.tradeCallback("BTC/USDT", 4)
	valid := []byte(`{"e":"trade","s":"BTC/USDT","t":42,"p":"101.5","q":"2","T":1793882096789,"m":false}`)

	handler([]byte(`{`))
	handler(valid)
	handler(valid)
	handler([]byte(`{"e":"trade","s":"BTC/USDT","t":43,"p":"","q":"2","T":1793882096790,"m":false}`))

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
		if event.Reason != reason || event.MarketType != market.Spot {
			t.Fatalf("evento %d: reason=%q market=%q, want %q/spot", index, event.Reason, event.MarketType, reason)
		}
		if event.Partition != 4 || event.Symbol != "BTC/USDT" {
			t.Fatalf("evento %d: symbol=%q partition=%d, want BTC/USDT/4", index, event.Symbol, event.Partition)
		}
	}
	if sink.legacy != len(want) {
		t.Fatalf("legacy=%d, want %d", sink.legacy, len(want))
	}
	if got := client.tradesIn.Load(); got != 1 {
		t.Fatalf("tradesIn=%d, want 1 — drop não avança ingeridos", got)
	}
}
