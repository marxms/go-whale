package binancesdk

import "testing"

// TestBatchSubscribeCapability_IsDeclared guards the contract that makes the boot path
// able to attach the whole fleet in one handshake.
//
// : combinedURLConnectEnabled is ON in production, yet the log showed 215
// per-symbol SUBSCRIBE frames and zero combined-stream connects — because the flag was only
// ever consulted inside reconnectDial. The initial connect fell back to the paced path,
// where subscribeMinInterval (200ms) imposes a 100-second floor for 500 symbols; with the
// per-symbol REST seeding on top, the 4-minute init budget ran out at ~200 of 500 symbols
// and coverage silently sat at 40%.
//
// Declaring the capability on the SDK client is what lets the orchestrator hand it the
// whole set instead of dripping symbols one frame at a time.
func TestBatchSubscribeCapability_IsDeclared(t *testing.T) {
	t.Parallel()

	var c any = (*SDKClient)(nil)
	if _, ok := c.(interface {
		SubscribeAll(symbols []string) error
	}); !ok {
		t.Fatal("SDKClient does not expose SubscribeAll: the boot path cannot attach the fleet in one handshake and falls back to paced per-symbol frames")
	}
}
