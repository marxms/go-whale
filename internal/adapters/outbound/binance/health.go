package binancesdk

import (
	"fmt"
)

// HealthState represents the granular health state of a connection.
type HealthState int

// HealthState values reported by the Binance connection monitor.
const (
	HealthHealthy HealthState = iota
	HealthStalled
	HealthReconnecting
	HealthDisconnected
)

// String returns the human-readable name of the health state.
func (h HealthState) String() string {
	switch h {
	case HealthHealthy:
		return "healthy"
	case HealthStalled:
		return "stalled"
	case HealthReconnecting:
		return "reconnecting"
	case HealthDisconnected:
		return "disconnected"
	default:
		return "unknown"
	}
}

// MarshalJSON implements json.Marshaler for HealthState.
func (h HealthState) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", h.String())), nil
}
