package ingestion

import (
	"context"
	"errors"
	"fmt"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// BatchSubscriber is the OPTIONAL capability of a streaming client that can attach a whole
// symbol set in one transport operation instead of one frame per symbol.
//
// Optional rather than part of StreamingClient: only venues whose protocol supports
// attaching streams on the handshake can honor it, and forcing the method onto every
// implementation would mean five stubs that just loop — the loop the caller already has.
type BatchSubscriber interface {
	SubscribeAll(symbols []string) error
}

// SubscribeAll attaches every symbol of one exchange scope in a single transport operation
// when the client supports it, falling back to the per-symbol path otherwise.
//
// This is what removes the 100-second pacing floor from startup. The
// per-symbol path is kept intact and reachable — a venue without the capability, or a
// deployment with the combined-URL flag off, behaves exactly as before.
//
// Returns the number of symbols registered. A partial result is reported, never silently
// truncated: the caller decides whether a short fleet is fatal.
func (m *HybridManager) SubscribeAll(_ context.Context, symbols []string, exchangeID string, marketType market.MarketType) (int, error) {
	if len(symbols) == 0 {
		return 0, nil
	}
	if !m.hasNativeClient(exchangeID) {
		return 0, fmt.Errorf("no native client available for %s", exchangeID)
	}

	client, err := m.createNativeClient(exchangeID, marketType)
	if err != nil {
		return 0, fmt.Errorf("native client failed for %s: %w", exchangeID, err)
	}

	batch, ok := client.(BatchSubscriber)
	if !ok {
		return 0, errBatchUnsupported
	}
	if err := batch.SubscribeAll(symbols); err != nil {
		// The routing signal is defined by the port package, not by the Binance adapter:
		// any venue that cannot attach a whole set wraps streaming.ErrBatchUnsupported.
		// Keeping the check here vendor-agnostic is what lets ingestion stay free of
		// concrete adapter imports (GW-010) while still distinguishing "fall back" from
		// a real connect failure.
		if errors.Is(err, streaming.ErrBatchUnsupported) {
			return 0, errBatchUnsupported
		}
		return 0, err
	}

	registered := 0
	for _, symbol := range symbols {
		if m.registerSubscribed(client, symbol, exchangeID, marketType) {
			registered++
		}
	}
	logger.OrDiscard(m.log).Warn("hybrid manager: symbols attached in one batch without per-symbol pacing",
		"exchange", exchangeID, "market_type", marketType, "symbols", registered)
	return registered, nil
}

// registerSubscribed records a symbol as subscribed on an already-attached client.
// Returns false when the key was already registered (concurrent or repeated attach), so the
// caller counts what it actually added rather than what it attempted.
//
// Mirrors the bookkeeping tail of Subscribe, minus the per-symbol network call: with the
// batch path the transport attach already happened for the whole set.
func (m *HybridManager) registerSubscribed(client Source, symbol, exchangeID string, marketType market.MarketType) bool {
	key := fmt.Sprintf("%s:%s:%s", exchangeID, marketType, symbol)

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.clients[key]; exists {
		return false
	}
	m.clients[key] = &HybridClientState{
		Client:     client,
		Symbol:     symbol,
		Exchange:   exchangeID,
		MarketType: marketType,
		Key:        key,
	}
	return true
}

// errBatchUnsupported signals "this client cannot batch — use the per-symbol path". It is a
// routing signal, not a failure, and callers MUST fall back rather than surface it.
var errBatchUnsupported = fmt.Errorf("%w: batch subscribe not supported by this client", streaming.ErrBatchUnsupported)

// IsBatchUnsupported reports whether err means the caller should fall back to per-symbol
// subscription. Distinguishing it from a real error is what keeps a genuine connect failure
// from being silently downgraded to "just use the slow path".
func IsBatchUnsupported(err error) bool { return errors.Is(err, errBatchUnsupported) }
