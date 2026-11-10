package binancesdk

import (
	"errors"
	"fmt"
	"strings"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// SubscribeAll attaches the whole symbol set in ONE combined-stream handshake instead of
// one paced SUBSCRIBE frame per symbol.
//
// Why this exists: consulting `c.combinedURLConnectEnabled` only inside reconnectDial made the
// INITIAL connect fall back to the paced path, where subscribeMinInterval (200ms) is a hard
// 100-second floor for 500 symbols. Stacked with the per-symbol REST seeding, the 4-minute
// init budget ran out at ~200 of 500 and coverage sat silently at 40% for hours while /health
// reported healthy.
//
// Streams attached to the handshake URL cost ZERO inbound messages, so Binance's 10 msg/s
// inbound limit — the reason the paced path exists at all — simply does not apply here.
// binance-streaming-transport:7-15 keeps pacing for incremental runtime add/remove, which
// is the case it was actually written for.
//
// Falls back to the per-symbol paced path when the flag is off, so the paced behavior
// remains reachable by configuration alone.
func (c *SDKClient) SubscribeAll(symbols []string) error {
	if len(symbols) == 0 {
		return nil
	}

	normalized := make([]string, 0, len(symbols))
	c.symbolsMu.Lock()
	for _, s := range symbols {
		s = strings.ToUpper(s)
		c.symbols[s] = true
		normalized = append(normalized, s)
	}
	c.symbolsMu.Unlock()

	if !c.combinedURLConnectEnabled {
		// Fallback: the caller keeps driving per-symbol Subscribe(). Reported so a boot that
		// silently takes the slow path is visible instead of inferred from frame counts.
		logger.OrDiscard(c.log).Warn("binance sdk: combined-URL connect is off; using paced per-symbol frames",
			"symbols", len(normalized), "pacing_floor", subscribeMinInterval*500)
		return errBatchSubscribeUnavailable
	}

	c.connectMu.Lock()
	defer c.connectMu.Unlock()

	live, err := c.connectAndResubscribeCombined(normalized)
	if err != nil {
		return fmt.Errorf("combined-stream connect for %d symbols: %w", len(normalized), err)
	}
	logger.OrDiscard(c.log).Warn("binance sdk: combined-stream connect attached symbols without post-connect subscribe frames",
		"attached", live, "requested", len(normalized))
	return nil
}

// errBatchSubscribeUnavailable signals that the batch path is disabled by configuration and
// the caller must fall back to per-symbol Subscribe. It is a control signal, not a failure:
// callers check for it explicitly rather than treating it as a subscribe error.
var errBatchSubscribeUnavailable = fmt.Errorf("%w: combined-URL connect disabled", streaming.ErrBatchUnsupported)

// IsBatchSubscribeUnavailable reports whether err is the "batch path disabled" signal, so a
// caller can fall back to per-symbol subscription without swallowing real connect errors.
func IsBatchSubscribeUnavailable(err error) bool {
	return errors.Is(err, errBatchSubscribeUnavailable)
}
