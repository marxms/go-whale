package binancesdk

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// TestConnectionStats_SurfacesLastDisconnect verifies that each Binance SDK client surfaces
// its internal disconnectTime atomic on streaming.ConnectionStats, so the periodic-backstop
// proof metric can exclude disconnect-triggered enqueues.
func TestConnectionStats_SurfacesLastDisconnect(t *testing.T) {
	t.Run("futures: zero before any disconnect, set after", func(t *testing.T) {
		c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true}
		if got := c.ConnectionStats().LastDisconnect; !got.IsZero() {
			t.Fatalf("expected zero LastDisconnect before any disconnect, got %v", got)
		}
		ts := time.Unix(1_700_000_123, 0)
		c.disconnectTime.Store(ts)
		if got := c.ConnectionStats().LastDisconnect; !got.Equal(ts) {
			t.Fatalf("expected LastDisconnect=%v, got %v", ts, got)
		}
	})

	t.Run("spot: zero before any disconnect, set after", func(t *testing.T) {
		c := &SpotSDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true}
		if got := c.ConnectionStats().LastDisconnect; !got.IsZero() {
			t.Fatalf("expected zero LastDisconnect before any disconnect, got %v", got)
		}
		ts := time.Unix(1_700_000_456, 0)
		c.disconnectTime.Store(ts)
		if got := c.ConnectionStats().LastDisconnect; !got.Equal(ts) {
			t.Fatalf("expected LastDisconnect=%v, got %v", ts, got)
		}
	})

	// Compile-time guard: the field exists on the shared struct.
	_ = streaming.ConnectionStats{}.LastDisconnect
}
