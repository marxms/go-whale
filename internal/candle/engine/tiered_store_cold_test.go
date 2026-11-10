package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/retention"
)

// fakeColdAppender records what the TieredStore routes to the cold tier.
type fakeColdAppender struct {
	got []candle.Candle
	err error
}

func (f *fakeColdAppender) EnqueueClosedBlocking(_ context.Context, _, _, _, _ string, c candle.Candle) error {
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, c)
	return nil
}

func TestTieredStore_appendClosedToCold(t *testing.T) {
	ctx := context.Background()
	cs := []candle.Candle{{Timestamp: 1000}, {Timestamp: 2000}, {Timestamp: 3000}}

	t.Run("routes every closed candle when no retention clamp", func(t *testing.T) {
		fake := &fakeColdAppender{}
		ts := &TieredStore{coldAppender: fake}
		if err := ts.appendClosedToCold(ctx, "binance", "future", "BTC/USDT", "1h", cs); err != nil {
			t.Fatalf("unexpected error on full success: %v", err)
		}
		if len(fake.got) != 3 {
			t.Fatalf("want 3 routed to cold, got %d", len(fake.got))
		}
	})

	// The write floor is the table's POLICY, not a standalone duration. `RetentionDays: 1`
	// cuts at `now − 2 days` (the window plus the spare day of the active partition). What is
	// outside the cut is not written; what is inside is.
	t.Run("retention clamp skips candles older than the cutoff", func(t *testing.T) {
		fake := &fakeColdAppender{}
		ts := &TieredStore{coldAppender: fake, coldBoundary: retention.Policy{Table: "candles", RetentionDays: 1}}
		now := time.Now().UnixMilli()
		old := now - 3*int64(24*time.Hour/time.Millisecond)    // outside retention
		recent := now - int64(10*time.Minute/time.Millisecond) // inside retention
		if err := ts.appendClosedToCold(ctx, "binance", "future", "BTC/USDT", "1h",
			[]candle.Candle{{Timestamp: old}, {Timestamp: recent}}); err != nil {
			t.Fatalf("retention-clamp success must not error: %v", err)
		}
		if len(fake.got) != 1 || fake.got[0].Timestamp != recent {
			t.Fatalf("retention clamp failed: got %+v (want only ts=%d)", fake.got, recent)
		}
	})

	t.Run("nil appender is a safe no-op", func(t *testing.T) {
		ts := &TieredStore{} // no appender wired (QuestDB disabled)
		if err := ts.appendClosedToCold(ctx, "binance", "future", "BTC/USDT", "1h", cs); err != nil {
			t.Fatalf("nil appender must be a nil-error no-op, got %v", err)
		}
	})

	// appendClosedToCold writes EVERY in-retention batch candle UNCONDITIONALLY — there is no
	// per-batch cold-existence probe. The write is idempotent (QuestDB DEDUP) and the derived
	// cumulative fields are anchored so a re-seed matches the authoritative value. The probe the
	// skip would require doubles cold-start seed time. This asserts that the cold tier already
	// holding those buckets does NOT suppress any write.
	t.Run("writes ALL batch candles even when cold already holds them (no skip)", func(t *testing.T) {
		fake := &fakeColdAppender{}
		// Cold tier already holds ts=1000 and ts=2000 with an authoritative cumulative.
		// The removed guard would have skipped these; the simplified path must NOT.
		cold := &mockColdStore{candles: []candle.Candle{
			{Timestamp: 1000},
			{Timestamp: 2000},
		}}
		ts := &TieredStore{coldAppender: fake, cold: cold}
		batch := []candle.Candle{
			{Timestamp: 1000},
			{Timestamp: 2000},
			{Timestamp: 3000},
		}
		if err := ts.appendClosedToCold(ctx, "binance", "future", "BTC/USDT", "1h", batch); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fake.got) != 3 {
			t.Fatalf("want ALL 3 batch candles routed to cold (no skip), got %d: %+v", len(fake.got), fake.got)
		}
	})

	// appendClosedToCold must not depend on the cold store. There is no read at all, so a
	// misbehaving cold store cannot affect the write.
	t.Run("does not probe the cold store (writes regardless of cold-read failure)", func(t *testing.T) {
		fake := &fakeColdAppender{}
		cold := &mockColdStore{queryErr: errors.New("cold read must never be reached")}
		ts := &TieredStore{coldAppender: fake, cold: cold}
		batch := []candle.Candle{{Timestamp: 1000}, {Timestamp: 2000}}
		if err := ts.appendClosedToCold(ctx, "binance", "future", "BTC/USDT", "1h", batch); err != nil {
			t.Fatalf("no cold probe means a cold-read error is irrelevant, got %v", err)
		}
		if len(fake.got) != 2 {
			t.Fatalf("want all candles written without probing cold, got %d", len(fake.got))
		}
	})

	// A mid-batch abort (ctx-cancel / pipeline shutdown) MUST surface the error so
	// SeedCandlesBatch propagates it and gap-recovery retries. Logging and returning silently
	// would report the Redis success while post-abort candles were permanently lost from the
	// cold tier.
	t.Run("surfaces the error on mid-batch abort (observable + retryable)", func(t *testing.T) {
		sentinel := errors.New("pipeline stopped")
		fake := &fakeColdAppender{err: sentinel}
		ts := &TieredStore{coldAppender: fake}
		rec := newEngineTestMetrics()
		ts.SetMetrics(rec.port())
		before := rec.coldAborted
		err := ts.appendClosedToCold(ctx, "binance", "future", "BTC/USDT", "1h", cs)
		if err == nil {
			t.Fatal("expected a non-nil error on abort, got nil (silent drop regression)")
		}
		if !errors.Is(err, sentinel) {
			t.Fatalf("abort error must wrap the appender cause, got %v", err)
		}
		if len(fake.got) != 0 {
			t.Fatalf("expected no candles recorded on error, got %d", len(fake.got))
		}
		if after := rec.coldAborted; after <= before {
			t.Fatalf("cold_append_aborted_total not incremented: before=%v after=%v", before, after)
		}
	})
}

// NO POLICY IS NOT A PANIC. The boundary became a port (interface);
// a TieredStore assembled without a durable tier — or a test that only wires
// the appender — must write everything, which is the same contract as a
// zeroed policy. Calling a method on a nil interface would crash the process
// in the middle of the seed.
func TestTieredStore_ColdWriteFloorWithoutBoundaryIsNoClamp(t *testing.T) {
	ts := &TieredStore{}
	if _, clamped := ts.coldWriteFloor(time.Now()); clamped {
		t.Fatal("without an injected policy the write floor applied a clamp")
	}
}
