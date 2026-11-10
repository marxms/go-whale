package sourcetest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/ingestion"
	"github.com/marxms/go-whale/internal/trade"
)

const (
	// adapterDeliveryTimeout tolerates an in-process transport plus the adapter's own
	// parse/dispatch goroutines. It is still two orders of magnitude below a live
	// network timeout, so a stalled implementation fails instead of hiding.
	adapterDeliveryTimeout = 10 * time.Second

	// adapterReconnectTimeout covers Binance's 2s socket-cleanup delay plus a fresh dial
	// and resubscribe, and BingX's 1s initial backoff with jitter, with plenty of margin.
	adapterReconnectTimeout = 20 * time.Second

	pollInterval = 5 * time.Millisecond

	// suiteSymbol is the sample instrument every case subscribes to.
	suiteSymbol = "BTC/USDT"
)

// Run executes the shared Source conformance suite. Every subtest constructs a fresh
// Harness, so no transport or sequence state crosses cases.
//
// Covered: subscribe, per-symbol delivery order, reconnect notification, backpressure
// signal, unsubscribe/cancel, idempotent close, and close-after-cancel.
func Run(t *testing.T, newHarness NewHarness) {
	t.Helper()

	t.Run("SubscribeAndOrderedDelivery", func(t *testing.T) {
		h := newHarness(t)
		src := h.Source
		symbol := suiteSymbol
		ctx := context.Background()

		if err := src.Subscribe(ctx, symbol); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		// Idempotent by contract: a second attach must not fail or duplicate events.
		if err := src.Subscribe(ctx, symbol); err != nil {
			t.Fatalf("second Subscribe: %v", err)
		}

		want := []trade.Trade{
			{ID: "1", Symbol: symbol, Price: 100, Amount: 1, Side: trade.Buy, Timestamp: 1_700_000_000_001},
			{ID: "2", Symbol: symbol, Price: 101, Amount: 2, Side: trade.Sell, Timestamp: 1_700_000_000_002},
			{ID: "3", Symbol: symbol, Price: 102, Amount: 3, Side: trade.Buy, Timestamp: 1_700_000_000_003},
		}
		h.Deliver(t, symbol, want...)

		got := collect(t, src, symbol, len(want))
		for i := range want {
			if got[i].ID != want[i].ID {
				t.Fatalf("delivery order broken at %d: got id %q, want %q — per-symbol arrival order is the contract", i, got[i].ID, want[i].ID)
			}
		}

		// Consume semantics: a second read has nothing left.
		requireEventually(t, adapterDeliveryTimeout, func() bool {
			rest, err := src.GetTrades(ctx, symbol)
			return err == nil && len(rest) == 0
		}, "GetTrades must clear what it returned")
	})

	t.Run("SubscribeHonoursCancelledContext", func(t *testing.T) {
		h := newHarness(t)
		src := h.Source

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := src.Subscribe(ctx, suiteSymbol)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe(canceled) = %v, want context.Canceled — a canceled caller must not see a successful attach", err)
		}

		// A canceled read must also surface cancellation instead of blocking.
		if err := src.Subscribe(context.Background(), "ETH/USDT"); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if _, err := src.GetTrades(ctx, "ETH/USDT"); !errors.Is(err, context.Canceled) {
			t.Fatalf("GetTrades(canceled) = %v, want context.Canceled", err)
		}
	})

	t.Run("UnsubscribeDetachesWithCanonicalError", func(t *testing.T) {
		h := newHarness(t)
		src := h.Source
		symbol := suiteSymbol
		ctx := context.Background()

		if err := src.Subscribe(ctx, symbol); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		h.Deliver(t, symbol, trade.Trade{ID: "1", Symbol: symbol, Price: 100, Amount: 1, Timestamp: 1})
		_ = collect(t, src, symbol, 1)

		if err := src.Unsubscribe(symbol); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
		// Idempotent teardown.
		if err := src.Unsubscribe(symbol); err != nil {
			t.Fatalf("second Unsubscribe: %v", err)
		}

		got, err := src.GetTrades(ctx, symbol)
		if !errors.Is(err, ingestion.ErrNotSubscribed) {
			t.Fatalf("GetTrades after Unsubscribe = %v, want ingestion.ErrNotSubscribed", err)
		}
		if len(got) != 0 {
			t.Fatalf("GetTrades after Unsubscribe returned %d trades, want none", len(got))
		}
	})

	t.Run("ReconnectNotification", func(t *testing.T) {
		h := newHarness(t)
		src := h.Source
		symbol := suiteSymbol

		if err := src.Subscribe(context.Background(), symbol); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if !h.ForceReconnect(t, symbol) {
			t.Skip("this harness cannot drop the transport without live network")
		}

		var batch ReconnectBatch
		requireEventually(t, adapterReconnectTimeout, func() bool {
			for _, b := range h.Reconnects() {
				if contains(b.Symbols, symbol) {
					batch = b
					return true
				}
			}
			return false
		}, "a completed reconnect must be reported through the configured ReconnectSink; a silent reconnect leaves the consumer unable to repair the gap")
		if batch.ReconnectTime.Before(batch.DisconnectTime) {
			t.Fatalf("reconnect window inverted: disconnect=%v reconnect=%v", batch.DisconnectTime, batch.ReconnectTime)
		}
	})

	t.Run("BackpressureSignal", func(t *testing.T) {
		h := newHarness(t)
		src := h.Source
		symbol := suiteSymbol
		ctx := context.Background()

		if err := src.Subscribe(ctx, symbol); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		bp, ok := src.(ingestion.BackpressureSource)
		if !ok {
			t.Skip("source does not expose the optional backpressure capability")
		}
		if h.Capacity < 2 {
			t.Skipf("harness capacity %d is too small to fill", h.Capacity)
		}

		fill := h.Capacity / 2
		trades := orderedTrades(symbol, fill)
		h.Deliver(t, symbol, trades...)

		var ratio float64
		requireEventually(t, adapterDeliveryTimeout, func() bool {
			ratio = bp.GetBufferFillRatio(symbol)
			return ratio > 0 && ratio <= 1
		}, fmt.Sprintf("buffer fill ratio must report (0,1] once %d/%d events are buffered", fill, h.Capacity))

		_ = collect(t, src, symbol, fill)
		requireEventually(t, adapterDeliveryTimeout, func() bool {
			return bp.GetBufferFillRatio(symbol) == 0
		}, "draining the buffer must bring the fill ratio back to zero")
	})

	t.Run("IdempotentClose", func(t *testing.T) {
		h := newHarness(t)
		src := h.Source

		for i := 0; i < 3; i++ {
			if err := src.Close(); err != nil {
				t.Fatalf("Close call %d: %v — Close must be idempotent and return nil", i+1, err)
			}
		}
		if src.HealthCheck() {
			t.Fatal("a closed source must not report healthy")
		}
	})

	t.Run("CloseAfterCancel", func(t *testing.T) {
		h := newHarness(t)
		src := h.Source
		ctx := context.Background()

		if err := src.Subscribe(ctx, suiteSymbol); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := src.Unsubscribe(suiteSymbol); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
		if err := src.Close(); err != nil {
			t.Fatalf("Close after cancel: %v", err)
		}
		if err := src.Close(); err != nil {
			t.Fatalf("second Close after cancel: %v", err)
		}
		// Teardown after Close is allowed and must not fail.
		if err := src.Unsubscribe(suiteSymbol); err != nil {
			t.Fatalf("Unsubscribe after Close: %v", err)
		}

		if _, err := src.GetTrades(ctx, suiteSymbol); !errors.Is(err, ingestion.ErrClosed) {
			t.Fatalf("GetTrades after Close = %v, want ingestion.ErrClosed", err)
		}
	})

	t.Run("ClosedSourceRejectsWork", func(t *testing.T) {
		h := newHarness(t)
		src := h.Source

		if err := src.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := src.Subscribe(context.Background(), suiteSymbol); !errors.Is(err, ingestion.ErrClosed) {
			t.Fatalf("Subscribe after Close = %v, want ingestion.ErrClosed", err)
		}
	})
}

// collect reads until want trades have arrived, accumulating across calls. Adapters
// deliver asynchronously, so a single call may return a prefix; the suite still proves
// the concatenation preserves arrival order.
func collect(t *testing.T, src ingestion.Source, symbol string, want int) []trade.Trade {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(adapterDeliveryTimeout)
	out := make([]trade.Trade, 0, want)
	for len(out) < want {
		got, err := src.GetTrades(ctx, symbol)
		if err != nil {
			t.Fatalf("GetTrades while collecting %d/%d for %s: %v", len(out), want, symbol, err)
		}
		out = append(out, got...)
		if len(out) >= want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out collecting trades for %s: got %d, want %d", symbol, len(out), want)
		}
		time.Sleep(pollInterval)
	}
	if len(out) > want {
		t.Fatalf("received %d trades, want exactly %d — the source duplicated or over-delivered", len(out), want)
	}
	return out
}

func orderedTrades(symbol string, n int) []trade.Trade {
	out := make([]trade.Trade, n)
	for i := range out {
		out[i] = trade.Trade{
			ID:        fmt.Sprintf("bp-%d", i+1),
			Symbol:    symbol,
			Price:     100 + float64(i),
			Amount:    1,
			Side:      trade.Buy,
			Timestamp: int64(1_700_000_000_000 + i),
		}
	}
	return out
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

func requireEventually(t *testing.T, timeout time.Duration, condition func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatal(msg)
}
