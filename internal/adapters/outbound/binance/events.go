package binancesdk

import (
	"log/slog"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// SDKEventType represents the type of event emitted by the SDKClient.
type SDKEventType int

// SDKEventType values emitted by the Binance SDK client.
const (
	EventConnected SDKEventType = iota
	EventDisconnected
	EventMessageReceived
	EventReconnectTriggered
	EventReconnectSuccess
	EventReconnectFailed
	EventMonitorStopped
	EventSubscribe
	EventError
	EventGapRecoveryTriggered
)

// String returns the human-readable name of the event type.
func (e SDKEventType) String() string {
	switch e {
	case EventConnected:
		return "connected"
	case EventDisconnected:
		return "disconnected"
	case EventMessageReceived:
		return "message_received"
	case EventReconnectTriggered:
		return "reconnect_triggered"
	case EventReconnectSuccess:
		return "reconnect_success"
	case EventReconnectFailed:
		return "reconnect_failed"
	case EventMonitorStopped:
		return "monitor_stopped"
	case EventSubscribe:
		return "subscribe"
	case EventError:
		return "error"
	case EventGapRecoveryTriggered:
		return "gap_recovery_triggered"
	default:
		return "unknown"
	}
}

// SDKEvent represents a structured event from the SDKClient.
type SDKEvent struct {
	Timestamp  time.Time              `json:"timestamp"`
	Type       SDKEventType           `json:"type"`
	TypeString string                 `json:"type_string"`
	Exchange   string                 `json:"exchange"`
	MarketType string                 `json:"market_type"`
	Symbol     string                 `json:"symbol,omitempty"`
	Error      string                 `json:"error,omitempty"`
	Reason     string                 `json:"reason,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`

	log *slog.Logger
}

// Log emits the event as a structured log entry.
func (e *SDKEvent) Log() {
	attrs := []any{
		"exchange", e.Exchange,
		"market_type", e.MarketType,
		"event", e.TypeString,
	}
	if e.Symbol != "" {
		attrs = append(attrs, "symbol", e.Symbol)
	}
	if e.Reason != "" {
		attrs = append(attrs, "reason", e.Reason)
	}
	if e.Error != "" {
		attrs = append(attrs, "error", e.Error)
	}

	switch e.Type {
	case EventError, EventReconnectFailed:
		logger.OrDiscard(e.log).Error("sdk event", attrs...)
	// EventReconnectSuccess logs at Warn so reconnect outcomes are visible under the
	// production log_level=warn (Info would suppress them).
	case EventReconnectTriggered, EventReconnectSuccess, EventDisconnected:
		logger.OrDiscard(e.log).Warn("sdk event", attrs...)
	default:
		logger.OrDiscard(e.log).Info("sdk event", attrs...)
	}
}

// NewSDKEvent creates a new SDKEvent with the current timestamp. The logger is owned by the
// client that emits the event; omitting it keeps the event safe to construct in a test.
func NewSDKEvent(eventType SDKEventType, exchange, marketType string, logs ...*slog.Logger) *SDKEvent {
	return &SDKEvent{
		Timestamp:  time.Now().UTC(),
		Type:       eventType,
		TypeString: eventType.String(),
		Exchange:   exchange,
		MarketType: marketType,
		Metadata:   make(map[string]interface{}),
		log:        logger.OrDiscard(logs...),
	}
}

// WithSymbol adds a symbol to the event.
func (e *SDKEvent) WithSymbol(symbol string) *SDKEvent {
	e.Symbol = symbol
	return e
}

// WithError adds an error to the event.
func (e *SDKEvent) WithError(err error) *SDKEvent {
	if err != nil {
		e.Error = err.Error()
	}
	return e
}

// WithReason adds a reason to the event.
func (e *SDKEvent) WithReason(reason string) *SDKEvent {
	e.Reason = reason
	return e
}

// WithMetadata adds metadata to the event.
func (e *SDKEvent) WithMetadata(key string, value interface{}) *SDKEvent {
	e.Metadata[key] = value
	return e
}
