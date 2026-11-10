package bingx

import "github.com/marxms/go-whale/internal/ingestion/streaming"

// Aliases keep the adapter's vocabulary local while the port definitions stay in the
// ingestion package.
type (
	// ClientConfig holds the configuration for creating clients.
	ClientConfig = streaming.ClientConfig
	// StreamingClient is the exchange-facing streaming contract.
	StreamingClient = streaming.StreamingClient
	// RecoveryCallback reports a disconnect window that needs gap recovery.
	RecoveryCallback = streaming.RecoveryCallback
	// GapRecoveryQueue enqueues gap-recovery work for disconnected symbols.
	GapRecoveryQueue = streaming.GapRecoveryQueue
)

// ClientType names the transport family a client implements.
type ClientType = streaming.ClientType

// ClientTypeNative marks a venue-native WebSocket client.
const ClientTypeNative = streaming.ClientTypeNative

type (
	// ConnectionStats is the per-connection transport snapshot.
	ConnectionStats = streaming.ConnectionStats
	// BufferFillAware exposes the trade buffer fill ratio.
	BufferFillAware = streaming.BufferFillAware
	// SharedSubscriptionClient exposes combined-stream subscription support.
	SharedSubscriptionClient = streaming.SharedSubscriptionClient
	// StatsProvider exposes connection statistics.
	StatsProvider = streaming.StatsProvider
)

func resolvePositive(v, fallback int) int {
	if v > 0 {
		return v
	}
	return fallback
}
