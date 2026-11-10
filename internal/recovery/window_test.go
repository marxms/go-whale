package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/market"
)

// t11Now sits mid-bucket so a short silence can be placed wholly inside the open 5m
// bucket or across its close without depending on when the suite runs.
func t11Now() int64 {
	return time.Date(2026, 6, 1, 12, 2, 30, 0, time.UTC).UnixMilli()
}

func windowJob(start, disconnect, reconnect time.Time) RecoveryJob {
	return RecoveryJob{
		ExchangeID:     "binance",
		MarketType:     "future",
		Symbol:         "BTC/USDT",
		DisconnectTime: disconnect,
		ReconnectTime:  reconnect,
		window:         recoveryWindow{startMs: start.UnixMilli(), gapDuration: reconnect.Sub(disconnect)},
	}
}

// TestDeriveWindowAdmittedWindowWins pins REC-01/REC-03: a job carrying an admitted window
// always fetches [window.start, now], and the one-bucket straddle shortcut never narrows it.
func TestDeriveWindowAdmittedWindowWins(t *testing.T) {
	const tf = "5m"
	const tfMs = int64(300_000)
	current := candle.BucketTimestamp(t11Now(), tf)

	t.Run("window older than the persisted head is the lower bound", func(t *testing.T) {
		start := time.UnixMilli(current - 24*3600_000)
		head := current - 3600_000
		job := windowJob(start, start, time.UnixMilli(t11Now()))
		since, gapStart, fetchLimit := deriveWindow(job, tf, head, t11Now())
		if since != start.UnixMilli() || gapStart != start.UnixMilli() {
			t.Fatalf("since=%d gapStart=%d, want the admitted start %d", since, gapStart, start.UnixMilli())
		}
		if want := int((t11Now()-start.UnixMilli())/tfMs) + 2; fetchLimit != want {
			t.Fatalf("fetchLimit=%d, want %d (the whole window, not the head)", fetchLimit, want)
		}
	})

	t.Run("window newer than an older persisted head keeps the older head", func(t *testing.T) {
		start := time.UnixMilli(current - 3600_000)
		head := current - 2*3600_000
		job := windowJob(start, start, time.UnixMilli(t11Now()))
		since, gapStart, fetchLimit := deriveWindow(job, tf, head, t11Now())
		if since != head || gapStart != head {
			t.Fatalf("since=%d gapStart=%d, want the older head %d", since, gapStart, head)
		}
		if want := int((t11Now()-head)/tfMs) + 2; fetchLimit != want {
			t.Fatalf("fetchLimit=%d, want %d", fetchLimit, want)
		}
	})

	t.Run("straddled close does not replace an admitted window with one bucket", func(t *testing.T) {
		start := time.UnixMilli(current - 2*3600_000)
		disconnect := time.UnixMilli(current - 10_000)
		reconnect := time.UnixMilli(current + 10_000)
		job := windowJob(start, disconnect, reconnect)
		since, gapStart, fetchLimit := deriveWindow(job, tf, current, t11Now())
		if since != start.UnixMilli() || gapStart != start.UnixMilli() {
			t.Fatalf("straddle narrowed the admitted window: since=%d gapStart=%d want=%d", since, gapStart, start.UnixMilli())
		}
		if want := int((t11Now()-start.UnixMilli())/tfMs) + 2; fetchLimit != want {
			t.Fatalf("fetchLimit=%d, want %d — a straddle may repair one bucket only without a window", fetchLimit, want)
		}
	})

	t.Run("a window in the future is ignored and the head decides", func(t *testing.T) {
		start := time.UnixMilli(t11Now() + 3600_000)
		disconnect := time.UnixMilli(t11Now() - 600_000)
		reconnect := time.UnixMilli(t11Now())
		job := windowJob(start, disconnect, reconnect)
		head := disconnect.UnixMilli()
		since, gapStart, fetchLimit := deriveWindow(job, tf, head, t11Now())
		if since != head || gapStart != candle.BucketTimestamp(head, tf) {
			t.Fatalf("future window was not ignored: since=%d gapStart=%d want=%d", since, gapStart, head)
		}
		if want := int((t11Now()-head)/tfMs) + 2; fetchLimit != want {
			t.Fatalf("fetchLimit=%d, want %d (derived from the head)", fetchLimit, want)
		}
	})

	t.Run("a targeted repair fetches exactly its named bucket", func(t *testing.T) {
		target := current - 7*tfMs
		job := RecoveryJob{TargetedTimeframe: tf, TargetedBucket: target}
		since, gapStart, fetchLimit := deriveWindow(job, tf, current, t11Now())
		if since != target || gapStart != target || fetchLimit != 2 {
			t.Fatalf("targeted window = (%d,%d,%d), want (%d,%d,2)", since, gapStart, fetchLimit, target, target)
		}
	})
}

// TestDeriveWindowStraddleOnlyWithoutWindow pins the branch that repairs only the bucket
// whose close the silence crossed: it applies when no window was admitted, and a fresh,
// immaterial silence that crosses no boundary is skipped instead of producing a fetch.
func TestDeriveWindowStraddleOnlyWithoutWindow(t *testing.T) {
	const tf = "5m"
	const tfMs = int64(300_000)
	current := candle.BucketTimestamp(t11Now(), tf)

	t.Run("straddle without a window repairs the one closed bucket", func(t *testing.T) {
		disconnect := time.UnixMilli(current - 10_000)
		reconnect := time.UnixMilli(current + 10_000)
		job := RecoveryJob{DisconnectTime: disconnect, ReconnectTime: reconnect}
		since, gapStart, fetchLimit := deriveWindow(job, tf, current, t11Now())
		want := current - tfMs
		if since != want || gapStart != want || fetchLimit != 2 {
			t.Fatalf("straddle = (%d,%d,%d), want (%d,%d,2)", since, gapStart, fetchLimit, want, want)
		}
	})

	t.Run("silence inside the open bucket is skipped", func(t *testing.T) {
		disconnect := time.UnixMilli(t11Now() - 20_000)
		reconnect := time.UnixMilli(t11Now() - 10_000)
		job := RecoveryJob{DisconnectTime: disconnect, ReconnectTime: reconnect}
		since, gapStart, fetchLimit := deriveWindow(job, tf, current, t11Now())
		if since != 0 || gapStart != 0 || fetchLimit != 0 {
			t.Fatalf("immaterial silence produced a fetch: (%d,%d,%d)", since, gapStart, fetchLimit)
		}
	})

	t.Run("stale head is fetched regardless of the gate", func(t *testing.T) {
		head := current - 10*tfMs
		disconnect := time.UnixMilli(t11Now() - 20_000)
		reconnect := time.UnixMilli(t11Now() - 10_000)
		job := RecoveryJob{DisconnectTime: disconnect, ReconnectTime: reconnect}
		since, gapStart, fetchLimit := deriveWindow(job, tf, head, t11Now())
		if since != head || gapStart != head {
			t.Fatalf("stale head = (%d,%d), want %d", since, gapStart, head)
		}
		if want := int((t11Now()-head)/tfMs) + 2; fetchLimit != want {
			t.Fatalf("fetchLimit=%d, want %d", fetchLimit, want)
		}
	})
}

type recordingTimestampProvider struct {
	latest int64
	ok     bool
	calls  []string
}

func (p *recordingTimestampProvider) GetLatestTimestamp(_ context.Context, exchange, marketType, symbol, tf string) (int64, bool) {
	p.calls = append(p.calls, exchange+"|"+marketType+"|"+symbol+"|"+tf)
	return p.latest, p.ok
}

// TestBackstopUsesPersistedTimestamp pins REC-10: the staleness gate reads the persisted
// watermark through the provider, keyed exactly as the worker reads it. A live-looking
// engine buffer cannot make the backstop skip a symbol whose persisted candle is stale.
func TestBackstopUsesPersistedTimestamp(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	scope := Scope{ExchangeID: "binance", MarketType: market.Future}
	subs := []string{"binance:future:BTC/USDT"}
	const wantKey = "binance|future|BTC/USDT|1m"

	t.Run("fresh persisted candle is not enqueued", func(t *testing.T) {
		provider := &recordingTimestampProvider{latest: now.Add(-time.Minute).UnixMilli(), ok: true}
		orch := &recordingOrchestrator{}
		backstop := NewBackstop(orch, provider, []Scope{scope}, []string{"1m", "5m"}, Metrics{})

		if got := backstop.ReconcileOnce(t.Context(), subs, now, 30*time.Minute, nil); got != 0 {
			t.Fatalf("fresh persisted candle reconciled %d scopes, want 0", got)
		}
		if len(orch.calls) != 0 {
			t.Fatalf("fresh persisted candle enqueued: %+v", orch.calls)
		}
		if len(provider.calls) != 1 || provider.calls[0] != wantKey {
			t.Fatalf("provider keys = %v, want [%s] (the persisted read path, fastest timeframe)", provider.calls, wantKey)
		}
	})

	t.Run("stale persisted candle is enqueued", func(t *testing.T) {
		provider := &recordingTimestampProvider{latest: now.Add(-4 * time.Minute).UnixMilli(), ok: true}
		orch := &recordingOrchestrator{}
		backstop := NewBackstop(orch, provider, []Scope{scope}, []string{"1m", "5m"}, Metrics{})

		if got := backstop.ReconcileOnce(t.Context(), subs, now, 30*time.Minute, nil); got != 1 {
			t.Fatalf("stale persisted candle reconciled %d scopes, want 1", got)
		}
		if len(orch.calls) != 1 || len(orch.calls[0].symbols) != 1 || orch.calls[0].symbols[0] != "BTC/USDT" {
			t.Fatalf("stale symbol not enqueued: %+v", orch.calls)
		}
		if provider.calls[0] != wantKey {
			t.Fatalf("provider key = %q, want %q", provider.calls[0], wantKey)
		}
	})
}
