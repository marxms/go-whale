package sourcetest

import (
	"testing"

	"github.com/marxms/go-whale/internal/ingestion"
	"github.com/marxms/go-whale/internal/trade"
)

// TestFakeSourcePassesTheContractSuite is the reference run: the fake must satisfy every
// clause before an adapter is allowed to use the suite as proof. If this fails, the
// contract or the suite is wrong — not the adapter.
func TestFakeSourcePassesTheContractSuite(t *testing.T) {
	RunFake(t)
}

// TestFakeSourceDropsNewestWhenFull pins the drop policy the suite relies on and that
// every adapter buffer implements: once full, NEW events are refused, never old ones
// silently displaced.
func TestFakeSourceDropsNewestWhenFull(t *testing.T) {
	fake := NewFakeSource(WithCapacity(2))
	defer func() { _ = fake.Close() }()

	if err := fake.Subscribe(t.Context(), "BTC/USDT"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fake.Deliver("BTC/USDT",
		trade.Trade{ID: "1"}, trade.Trade{ID: "2"}, trade.Trade{ID: "3"})

	got, err := fake.GetTrades(t.Context(), "BTC/USDT")
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(got) != 2 || got[0].ID != "1" || got[1].ID != "2" {
		t.Fatalf("buffered = %v, want the OLDEST two events (drop-newest)", ids(got))
	}
	if fake.Dropped() != 1 {
		t.Fatalf("Dropped = %d, want 1 — an unobservable drop is indistinguishable from silence", fake.Dropped())
	}
}

// TestFakeHarnessCarriesTheReconnectWindow keeps the harness from turning the reconnect
// case into a tautology: the recorder must carry the symbols and a monotonic window.
func TestFakeHarnessCarriesTheReconnectWindow(t *testing.T) {
	h := FakeHarness(t)
	if err := h.Source.Subscribe(t.Context(), "BTC/USDT"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if !h.ForceReconnect(t, "BTC/USDT") {
		t.Fatal("the fake harness must be able to force a reconnect")
	}
	batches := h.Reconnects()
	if len(batches) != 1 || !contains(batches[0].Symbols, "BTC/USDT") {
		t.Fatalf("reconnect batches = %+v, want one carrying BTC/USDT", batches)
	}
	if batches[0].ReconnectTime.Before(batches[0].DisconnectTime) {
		t.Fatalf("window inverted: %+v", batches[0])
	}
}

// guard: the fake is assignable to the consumer-owned contract without an adapter.
var _ ingestion.Source = (*FakeSource)(nil)

func ids(trades []trade.Trade) []string {
	out := make([]string, len(trades))
	for i, t := range trades {
		out[i] = t.ID
	}
	return out
}
