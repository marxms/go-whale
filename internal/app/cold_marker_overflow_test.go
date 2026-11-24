package app

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
)

// The production boot refuses ~1.000 cold bars, twice the 512-slot marker queue. Overflow must be
// COUNTED, never silently admitted, and the stop must drain what was admitted and be idempotent.
func TestColdMarkerOwnerBoundsAndDrainsTheBurst(t *testing.T) {
	release := make(chan struct{})
	calls := 0
	a := &App{coldMarkerWrite: func(context.Context, string, time.Duration) error {
		<-release
		calls++
		return nil
	}}

	const total = 600
	for i := 0; i < total; i++ {
		a.markColdPending(questdb.ColdID{
			Exchange: "binance", MarketType: "future",
			Symbol: fmt.Sprintf("S%d/USDT", i), Timeframe: "5m", Timestamp: 1_700_000_000_000,
		})
	}
	dropped := a.coldMarkerDropped.Load()
	if dropped == 0 {
		t.Fatalf("no drop counted for a %d-marker burst into the 512 queue", total)
	}

	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a.stopColdMarkers(ctx)
	a.stopColdMarkers(ctx) // idempotent: a second stop must not panic

	if got := int64(calls) + dropped; got != total {
		t.Fatalf("flushed %d + dropped %d = %d, want every marker accounted for (%d)", calls, dropped, got, total)
	}
}
