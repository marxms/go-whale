package binancesdk

import (
	"testing"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// Compile-time interface assertions.
func TestSDKClientInterfaceCompliance(_ *testing.T) {
	var _ streaming.StreamingClient = (*SDKClient)(nil)
	var _ streaming.StatsProvider = (*SDKClient)(nil)
	var _ streaming.SharedSubscriptionClient = (*SDKClient)(nil)
}

func TestSpotSDKClientInterfaceCompliance(_ *testing.T) {
	var _ streaming.StreamingClient = (*SpotSDKClient)(nil)
	var _ streaming.StatsProvider = (*SpotSDKClient)(nil)
	var _ streaming.SharedSubscriptionClient = (*SpotSDKClient)(nil)
}
