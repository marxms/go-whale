package binancesdk

import (
	"strings"
	"testing"

	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/trade"
)

// cap-1, cap and cap+1 are the only three instants where a buffer can lie.
// The append that reaches capacity STORES its trade; the next one is the drop.
func TestTradeBufferStatsExactAtCapBoundaries(t *testing.T) {
	buffer := NewTradeBuffer("BTC/USDT", 3)

	buffer.Append(trade.Trade{ID: "1"})
	buffer.Append(trade.Trade{ID: "2"})
	stats := buffer.stats()
	if stats.Current != 2 || stats.Max != 2 || stats.Capacity != 3 {
		t.Fatalf("cap-1: %+v, want current=2 max=2 capacity=3", stats)
	}

	if buffer.Append(trade.Trade{ID: "3"}) {
		t.Fatal("the append that reaches cap STORED its trade; reporting a drop is the phantom-drop defect")
	}
	stats = buffer.stats()
	if stats.Current != 3 || stats.Max != 3 {
		t.Fatalf("cap: %+v, want current=3 max=3", stats)
	}

	if !buffer.Append(trade.Trade{ID: "4"}) {
		t.Fatal("cap+1 must be refused and reported as a drop")
	}
	stats = buffer.stats()
	if stats.Current != 3 || stats.Max != 3 {
		t.Fatalf("cap+1: %+v, want current=3 max=3 (high-water preserved, refused trade absent)", stats)
	}

	drained := buffer.Flush()
	if len(drained) != 3 {
		t.Fatalf("drained=%d, want 3", len(drained))
	}
	stats = buffer.stats()
	if stats.Current != 0 {
		t.Fatalf("after drain current=%d, want 0", stats.Current)
	}
	if stats.LastDrain.IsZero() {
		t.Fatal("drain clock was not stamped; a stalled drain would be invisible")
	}
}

// One refusal produces exactly one classified overflow carrying symbol and the
// real partition, the resolved cap/fill are observable, and the first log for
// that location appears exactly once.
func TestTradeBufferOverflowCountsOnceWithLocation(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)
	client := &SDKClient{buffers: NewBufferManager(2), log: log}
	capture := &dropCapture{}
	client.metrics = capture.metrics()
	handler := client.tradeCallback("BTC/USDT", 6)
	valid := []byte(`{"e":"aggTrade","s":"BTC/USDT","a":1,"p":"101.5","q":"2","T":1793882096789,"m":false}`)

	handler(valid)
	handler(valid)
	handler(valid) // refused at cap 2
	handler(valid) // refused again: same location, must not add a log line

	overflow := 0
	for _, event := range capture.classified {
		if event.Reason == trade.DropBufferOverflow {
			overflow++
			if event.Symbol != "BTC/USDT" || event.Partition != 6 {
				t.Fatalf("overflow location: %+v, want BTC/USDT/6", event)
			}
		}
	}
	if overflow != 2 {
		t.Fatalf("classified overflow=%d, want 2 (one per refused trade)", overflow)
	}
	lines := 0
	for _, record := range sink.Records() {
		if strings.Contains(record.Text(), "trade buffer overflow") {
			lines++
		}
	}
	if lines != 1 {
		t.Fatalf("first-overflow log lines=%d, want 1 per symbol/partition", lines)
	}
	stats, ok := client.GetBufferStats("BTC/USDT")
	if !ok {
		t.Fatal("buffer stats absent for a subscribed symbol")
	}
	if stats.Current != 2 || stats.Max != 2 || stats.Capacity != 2 {
		t.Fatalf("stats=%+v, want current=2 max=2 capacity=2", stats)
	}
	if got := client.tradesIn.Load(); got != 2 {
		t.Fatalf("tradesIn=%d, want 2 (refused trades are not ingested)", got)
	}
}
