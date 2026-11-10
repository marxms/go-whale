package recovery

import (
	"context"
	"time"
)

const (
	// SeedConcurrency bounds how many symbols are seeded over REST at once.
	//
	// THIS NUMBER IS A WEIGHT BUDGET. Binance bills klines by limit TIER and the seed
	// asks for seedDepth+1 = 801 bars, which costs weight 5. Measured: 200 symbols x 8
	// timeframes = 1.600 calls in 702s serial = 2,28 calls/s = 684 weight/min. At 4
	// concurrent that is 2.736 weight/min, over the 2.400 cap; at 2 it is 1.368 (57%).
	// Raising this spends headroom that discovery, the derivatives poller and gap
	// recovery also draw from — re-measure before changing it.
	SeedConcurrency = 2

	// ReseedRetryWindow is how far back a re-seed request reaches when the backstop
	// picks it up. A symbol kept unseeded after a rate limit has NO history at all, so
	// the window only has to be wide enough that the backstop's staleness gate treats
	// it as lagging.
	ReseedRetryWindow = time.Hour

	// BootDeliveryGrace bounds, FOR THE WHOLE SCOPE, how long the boot will wait for
	// attached streams to start delivering before it seeds them anyway.
	//
	// It is an ABSOLUTE deadline computed once, at attach, and shared by every symbol —
	// not a per-symbol budget. 45s is sized off the measured shape of the attach: the
	// transport's own resubscribe verification uses a 10s window and reported 335/500
	// delivering at 10s on the 500-symbol boot.
	BootDeliveryGrace = 45 * time.Second

	// BootDeliveryPoll is how often the waiting seed goroutine re-checks its own
	// symbol. It polls the in-process map the ingest path already maintains.
	BootDeliveryPoll = 250 * time.Millisecond
)

// DeliveryProbe is the "is this stream actually delivering?" signal, taken as an
// interface so the gate is testable without the streaming stack.
type DeliveryProbe interface {
	Delivering(exchangeID, marketType, symbol string) bool
	DeliveringCount(exchangeID, marketType string, symbols []string) int
}

// AwaitSymbolDelivery blocks until this symbol's stream has delivered its first
// trade, the shared boot deadline passes, or ctx is canceled. It reports whether
// delivery was CONFIRMED.
//
// The seed's REST snapshot covers [bucket start, T_fetch]; the live stream covers
// [T_deliver, ...]. Those windows are stitched by a MAX, and a MAX can only stitch
// windows that OVERLAP. Waiting for delivery before fetching makes T_deliver <
// T_fetch, so the windows overlap and the MAX stitches.
func AwaitSymbolDelivery(
	ctx context.Context,
	probe DeliveryProbe,
	exchangeID, marketType, symbol string,
	deadline time.Time,
	poll time.Duration,
) bool {
	if probe == nil {
		return false
	}
	if poll <= 0 {
		poll = BootDeliveryPoll
	}

	for {
		// Probed BEFORE the deadline test on purpose: a symbol already delivering must
		// proceed even when the shared grace has expired.
		if probe.Delivering(exchangeID, marketType, symbol) {
			return true
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		wait := poll
		if remaining < wait {
			wait = remaining
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}
