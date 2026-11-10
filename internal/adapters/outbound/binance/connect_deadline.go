package binancesdk

import (
	"context"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/binancews"
)

// Connect-deadline tuning (package vars so tests can shorten them). These bound
// the context-aware dial below the SDK handshake limit, scaling with stream
// count so a large partition gets proportionally more budget while a small one
// fails fast (letting the reconnect retry loop spin sooner under load).
const (
	connectDeadlineBase      = 8 * time.Second
	connectDeadlinePerStream = 30 * time.Millisecond
	connectDeadlineMax       = 40 * time.Second // stay under the SDK's 45s hard limit
)

// connectDeadline returns the per-partition connect budget for streamCount streams.
func connectDeadline(streamCount int) time.Duration {
	d := connectDeadlineBase + time.Duration(streamCount)*connectDeadlinePerStream
	if d > connectDeadlineMax {
		d = connectDeadlineMax
	}
	return d
}

// connectMarketStream bounds the complete handshake and binds it to both the
// caller and the client lifecycle. Handlers are installed before the reader
// starts, so no frame can arrive without a route.
func connectMarketStream(ctx, lifecycle context.Context, conn *binancews.Connection, handlers map[string]binancews.Handler) error {
	dialCtx, cancel := context.WithTimeout(ctx, connectDeadline(len(handlers)))
	defer cancel()
	stop := context.AfterFunc(lifecycle, cancel)
	defer stop()
	if err := lifecycle.Err(); err != nil {
		return err
	}
	err := conn.Connect(dialCtx, handlers)
	if err != nil {
		if canceled := lifecycle.Err(); canceled != nil {
			return canceled
		}
		if canceled := dialCtx.Err(); canceled != nil {
			return canceled
		}
		if deadline, ok := dialCtx.Deadline(); ok && !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
	}
	return err
}
