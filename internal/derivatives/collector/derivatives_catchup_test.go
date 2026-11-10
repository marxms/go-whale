package collector

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE RESTART CATCH-UP
//
// The tick at 05:00 falls inside a process restart and never runs. The next scheduled
// cycle is one full period away, and the series carries a hole nobody sees. The catch-up
// compares the newest PERSISTED snapshot with the poll period: when it is older, the
// first cycle IS the catch-up. It writes the CURRENT reading, never a backdated one, and
// records the missed interval so the hole is visible.
//
// Idempotency is about the period, not the clock: a second catch-up would write a second
// snapshot for the same period, and the series would double-count it.
// ─────────────────────────────────────────────────────────────────────────────

const catchUpIntervalSec = 300

// catchUpStore is the two things the catch-up reads: the newest persisted snapshot and the
// write path. Only SaveSnapshot is exercised; SaveSeriesOnly exists to satisfy the
// consumer port.
type catchUpStore struct {
	mu      sync.Mutex
	latest  *derivatives.DerivativesSnapshot
	readErr error
	saved   []*derivatives.DerivativesSnapshot
}

var _ pollerStore = (*catchUpStore)(nil)

func (s *catchUpStore) SaveSnapshot(_ context.Context, snapshot *derivatives.DerivativesSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved = append(s.saved, snapshot)
	s.latest = snapshot // the real store advances the persisted head
	return nil
}

func (s *catchUpStore) SaveSeriesOnly(context.Context, *derivatives.DerivativesSnapshot, string) error {
	return nil
}

func (s *catchUpStore) GetLatestSnapshot(context.Context, string, string, string) (*derivatives.DerivativesSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest, s.readErr
}

func (s *catchUpStore) snapshots() []*derivatives.DerivativesSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*derivatives.DerivativesSnapshot(nil), s.saved...)
}

// newCatchUpPoller wires one symbol, one venue client and the injected clock. The fake
// venue answers the live reads, so a cycle that runs always writes.
func newCatchUpPoller(store pollerStore, now time.Time) (*DerivativesPoller, *logger.Sink) {
	log, sink := logger.NewCapture(logger.LevelDebug)
	return &DerivativesPoller{
		cfg:     config.DerivativesConfig{Enabled: true, PollIntervalSec: catchUpIntervalSec, MaxSeriesSize: 200},
		store:   store,
		symbols: []SymbolScope{testScope()},
		clients: map[string]derivatives.Source{"binance": &fakeRestClient{
			funding:      derivatives.FundingRate{Rate: fptr(0.0001), MarkPrice: fptr(100)},
			openInterest: derivatives.OpenInterest{Amount: fptr(1000)},
		}},
		fundingProfiles: NewFundingProfileRegistry(),
		now:             func() time.Time { return now },
		log:             log,
	}, sink
}

func staleSnapshot(now time.Time, age time.Duration) *derivatives.DerivativesSnapshot {
	return &derivatives.DerivativesSnapshot{
		Exchange:   "binance",
		MarketType: "future",
		Symbol:     "BTC/USDT",
		Timestamp:  now.Add(-age).UnixMilli(),
	}
}

func catchUpWarning(sink *logger.Sink, message string) (logger.Record, bool) {
	for _, record := range sink.Records() {
		if record.Level == logger.LevelWarn && strings.Contains(record.Message, message) {
			return record, true
		}
	}
	return logger.Record{}, false
}

func TestDerivativesCatchUpOnRestart(t *testing.T) {
	now := time.Now()
	store := &catchUpStore{latest: staleSnapshot(now, 2*catchUpIntervalSec*time.Second)}
	poller, _ := newCatchUpPoller(store, now)

	before := time.Now().UnixMilli()
	poller.PollOnce(context.Background())
	after := time.Now().UnixMilli()

	saved := store.snapshots()
	if len(saved) != 1 {
		t.Fatalf("the restart ran %d snapshot writes, want exactly 1 catch-up cycle", len(saved))
	}
	// The catch-up writes the CURRENT reading. A backdated value would invent a
	// measurement the venue never made for the missed period.
	if got := saved[0].Timestamp; got < before || got > after {
		t.Fatalf("the catch-up wrote timestamp %d, want the current instant in [%d, %d]",
			got, before, after)
	}
}

func TestDerivativesCatchUpIdempotent(t *testing.T) {
	now := time.Now()
	store := &catchUpStore{latest: staleSnapshot(now, 2*catchUpIntervalSec*time.Second)}
	poller, sink := newCatchUpPoller(store, now)

	if !poller.catchUp(context.Background()) {
		t.Fatal("the first catch-up did not run with a snapshot older than the period")
	}
	if poller.catchUp(context.Background()) {
		t.Fatal("a second catch-up ran for the same period")
	}

	if got := len(store.snapshots()); got != 1 {
		t.Fatalf("the store received %d snapshots, want 1 — the catch-up duplicated a period", got)
	}
	if _, ok := catchUpWarning(sink, "missed period recorded"); !ok {
		t.Fatalf("the missed period was not recorded; captured: %v", sink.Records())
	}
	// One record, not one per call: the second call must not repeat the gap either.
	records := 0
	for _, record := range sink.Records() {
		if strings.Contains(record.Message, "missed period recorded") {
			records++
		}
	}
	if records != 1 {
		t.Fatalf("the gap was recorded %d times, want 1", records)
	}
}

func TestDerivativesCatchUpRecordsGap(t *testing.T) {
	t.Run("a missed period is recorded with its bounds", func(t *testing.T) {
		now := time.Now()
		const age = 2 * catchUpIntervalSec * time.Second
		latest := staleSnapshot(now, age)
		store := &catchUpStore{latest: latest}
		poller, sink := newCatchUpPoller(store, now)

		poller.catchUp(context.Background())

		record, ok := catchUpWarning(sink, "missed period recorded")
		if !ok {
			t.Fatalf("the missed period produced no warning; captured: %v", sink.Records())
		}
		if got, _ := record.Attr("gap_from_ms"); got.Int64() != latest.Timestamp {
			t.Errorf("gap_from_ms = %d, want the last persisted %d", got.Int64(), latest.Timestamp)
		}
		if got, _ := record.Attr("gap_to_ms"); got.Int64() != now.UnixMilli() {
			t.Errorf("gap_to_ms = %d, want the catch-up instant %d", got.Int64(), now.UnixMilli())
		}
		if got, _ := record.Attr("missed_ms"); got.Int64() != age.Milliseconds() {
			t.Errorf("missed_ms = %d, want %d", got.Int64(), age.Milliseconds())
		}
	})

	t.Run("a period that cannot be found is recorded, not silenced", func(t *testing.T) {
		now := time.Now()
		store := &catchUpStore{} // no snapshot at all
		poller, sink := newCatchUpPoller(store, now)

		poller.catchUp(context.Background())

		if _, ok := catchUpWarning(sink, "no readable snapshot to compare"); !ok {
			t.Fatalf("the absent period was silenced; captured: %v", sink.Records())
		}
	})
}

// TestDerivativesCatchUpIdempotentAcrossRestart proves the period guard is the STORE's
// persisted head, not process memory: a second process reading the snapshot the first
// one wrote must not write it again.
func TestDerivativesCatchUpIdempotentAcrossRestart(t *testing.T) {
	now := time.Now()
	store := &catchUpStore{latest: staleSnapshot(now, 2*catchUpIntervalSec*time.Second)}

	first, _ := newCatchUpPoller(store, now)
	if !first.catchUp(context.Background()) {
		t.Fatal("the first process did not catch up")
	}

	second, _ := newCatchUpPoller(store, now)
	if second.catchUp(context.Background()) {
		t.Fatal("a restarted process duplicated the period the first one wrote")
	}
	if got := len(store.snapshots()); got != 1 {
		t.Fatalf("store received %d snapshots across two processes, want 1", got)
	}
}
