package binancesdk

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// TestSubscribeDoesNotDeadlock_Futures is a regression test for the
// connectMu re-entrancy (B1) and connWg self-join (B3) deadlocks.
//
// It points the futures WS base path at an unreachable, fast-failing address so
// Connect() returns immediately (connection refused) with no network and no long
// timeout. With the original code Subscribe deadlocks (the double-lock precedes
// the network dial); after the fix it returns within the timeout regardless of
// the connect error.
func TestSubscribeDoesNotDeadlock_Futures(t *testing.T) {
	c := NewSDKClient(streaming.ClientConfig{TradeBufferSize: 100})
	c.wsBasePath = "ws://127.0.0.1:1"
	defer func() { _ = c.Close() }()

	done := make(chan error, 1)
	go func() { done <- c.Subscribe(context.Background(), "BTC/USDT") }()
	select {
	case err := <-done: // returned (a connect error is fine) — the point is it did NOT deadlock
		if err == nil {
			t.Fatal("Subscribe succeeded against a closed port")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Subscribe deadlocked (connectMu re-entrancy / connWg self-join not fixed)")
	}
}

// TestSubscribeDoesNotDeadlock_Spot is the spot twin of the futures regression
// test above. See that test for the rationale.
func TestSubscribeDoesNotDeadlock_Spot(t *testing.T) {
	c := NewSpotSDKClient(streaming.ClientConfig{TradeBufferSize: 100})
	c.wsBasePath = "ws://127.0.0.1:1"
	defer func() { _ = c.Close() }()

	done := make(chan error, 1)
	go func() { done <- c.Subscribe(context.Background(), "BTC/USDT") }()
	select {
	case err := <-done: // returned (a connect error is fine) — the point is it did NOT deadlock
		if err == nil {
			t.Fatal("Subscribe succeeded against a closed port")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Subscribe deadlocked (connectMu re-entrancy / connWg self-join not fixed)")
	}
}
