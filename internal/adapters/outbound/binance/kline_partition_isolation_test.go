package binancesdk

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/adapters/outbound/binancews"
	"github.com/marxms/go-whale/internal/candle"
)

func fabricatedPartition(id int, age time.Duration, symbols ...string) *klinePartition {
	p := &klinePartition{client: binancews.NewConnection(id, "ws://127.0.0.1:1"), symbols: len(symbols), members: symbols}
	p.lastFrameAt.Store(time.Now().Add(-age).UnixMilli())
	return p
}

// The watchdog must be able to tell "one partition died" from "the feed died". Restarting a single
// partition when the whole fleet is silent would heal one and leave the rest dead, one tick at a
// time, while every log line claimed a recovery.
func TestSilentPartitionRequiresALiveSister(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		parts    []*klinePartition
		wantOK   bool
		wantID   int
		wantSyms int
	}{
		{
			name:     "one silent partition beside a delivering one",
			parts:    []*klinePartition{fabricatedPartition(3, 90*time.Second, "AAA/USDT", "BBB/USDT"), fabricatedPartition(4, time.Second, "CCC/USDT")},
			wantOK:   true,
			wantID:   3,
			wantSyms: 2,
		},
		{
			name:   "a single partition going silent is a fleet problem",
			parts:  []*klinePartition{fabricatedPartition(0, 90*time.Second, "AAA/USDT")},
			wantOK: false,
		},
		{
			name:   "every partition silent is a fleet problem",
			parts:  []*klinePartition{fabricatedPartition(0, 90*time.Second, "AAA/USDT"), fabricatedPartition(1, 120*time.Second, "BBB/USDT")},
			wantOK: false,
		},
		{
			name:   "nothing silent",
			parts:  []*klinePartition{fabricatedPartition(0, time.Second, "AAA/USDT"), fabricatedPartition(1, 2*time.Second, "BBB/USDT")},
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			feed := &KlineFeed{parts: tc.parts}
			stat, members, ok := feed.SilentPartition(60)
			if ok != tc.wantOK {
				t.Fatalf("found = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if stat.ID != tc.wantID || len(members) != tc.wantSyms {
				t.Fatalf("partition %d with %d members, want %d with %d", stat.ID, len(members), tc.wantID, tc.wantSyms)
			}
		})
	}
}

// A terminal transport error must be actionable on the next owner tick, not only after the 60 s
// silence limit. The victim is the partition that faulted and has not delivered since; a fleet
// where every partition faulted is still the owner's problem, not one partition's.
func TestFaultedPartitionRequiresADeliveringSister(t *testing.T) {
	t.Parallel()

	faulted := func(id int, lastFrameAgo, faultAgo time.Duration, symbols ...string) *klinePartition {
		p := fabricatedPartition(id, lastFrameAgo, symbols...)
		p.faultedAt.Store(time.Now().Add(-faultAgo).UnixMilli())
		return p
	}

	cases := []struct {
		name     string
		parts    []*klinePartition
		wantOK   bool
		wantID   int
		wantSyms int
	}{
		{
			name:     "one faulted partition beside a delivering one",
			parts:    []*klinePartition{faulted(3, 5*time.Second, time.Second, "AAA/USDT", "BBB/USDT"), fabricatedPartition(4, time.Second, "CCC/USDT")},
			wantOK:   true,
			wantID:   3,
			wantSyms: 2,
		},
		{
			name:   "a single faulted partition is a fleet problem",
			parts:  []*klinePartition{faulted(0, 5*time.Second, time.Second, "AAA/USDT")},
			wantOK: false,
		},
		{
			name:   "every partition faulted is a fleet problem",
			parts:  []*klinePartition{faulted(0, 5*time.Second, time.Second, "AAA/USDT"), faulted(1, 7*time.Second, 2*time.Second, "BBB/USDT")},
			wantOK: false,
		},
		{
			name:   "a stale fault is already handled elsewhere",
			parts:  []*klinePartition{faulted(0, 5*time.Minute, 2*time.Minute, "AAA/USDT"), fabricatedPartition(1, time.Second, "BBB/USDT")},
			wantOK: false,
		},
		{
			name:   "a frame after the fault heals it",
			parts:  []*klinePartition{faulted(0, time.Second, 5*time.Second, "AAA/USDT"), fabricatedPartition(1, time.Second, "BBB/USDT")},
			wantOK: false,
		},
		{
			// Production closes a busy stream with the last frame in the same millisecond as
			// the error (last_frame_age_ms=0). Strict < made the fault invisible.
			name: "last frame in the same millisecond as the fault",
			parts: func() []*klinePartition {
				p := fabricatedPartition(0, 5*time.Second, "AAA/USDT")
				now := time.Now().UnixMilli()
				p.lastFrameAt.Store(now)
				p.faultedAt.Store(now)
				return []*klinePartition{p, fabricatedPartition(1, time.Second, "BBB/USDT")}
			}(),
			wantOK:   true,
			wantID:   0,
			wantSyms: 1,
		},
		{
			name:   "nothing faulted",
			parts:  []*klinePartition{fabricatedPartition(0, time.Second, "AAA/USDT"), fabricatedPartition(1, time.Second, "BBB/USDT")},
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			feed := &KlineFeed{parts: tc.parts}
			stat, members, ok := feed.FaultedPartition(60)
			if ok != tc.wantOK {
				t.Fatalf("found = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if stat.ID != tc.wantID || len(members) != tc.wantSyms {
				t.Fatalf("partition %d with %d members, want %d with %d", stat.ID, len(members), tc.wantID, tc.wantSyms)
			}
		})
	}
}

// The watchdog can be delayed (~90 s measured on 2026-11-24). The production window must still
// consider a fault actionable at that point; the old 60 s window had already expired and only the
// slower silence path restarted the partition.
func TestProductionFaultWindowOutlivesADelayedWatchdog(t *testing.T) {
	victim := fabricatedPartition(0, 5*time.Minute, "AAA/USDT")
	victim.faultedAt.Store(time.Now().Add(-2 * time.Minute).UnixMilli())
	feed := &KlineFeed{parts: []*klinePartition{victim, fabricatedPartition(1, time.Second, "BBB/USDT")}}

	if _, _, ok := feed.FaultedPartition(60); ok {
		t.Fatal("a 2-minute-old fault must not fit the old 60 s window")
	}
	if _, members, ok := feed.FaultedPartition(KlineFaultRedialWindow); !ok || len(members) != 1 {
		t.Fatalf("a 2-minute-old fault must stay actionable inside the production window; got ok=%v members=%v", ok, members)
	}
}

func TestNoteFrameClearsTheFault(t *testing.T) {
	feed := &KlineFeed{}
	p := fabricatedPartition(1, time.Second, "AAA/USDT")
	p.faults.Store(2)
	p.faultedAt.Store(time.Now().UnixMilli())

	feed.noteFrame(p)

	if p.faults.Load() != 0 || p.faultedAt.Load() != 0 {
		t.Fatalf("faults=%d faultedAt=%d, want both cleared by a delivered frame", p.faults.Load(), p.faultedAt.Load())
	}
}

// Regression: one partition's fault closed the healthy sister, resetting its counters and marking
// its symbols. A scoped restart must leave the sister's connection untouched.
func TestRestartPartitionClosesOnlyTheVictim(t *testing.T) {
	var mu sync.Mutex
	var held []*websocket.Conn
	server, connections := newMarketWSServer(t, func(conn *websocket.Conn, _ *http.Request, _ int) {
		mu.Lock()
		held = append(held, conn)
		mu.Unlock()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})

	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {})
	feed.wsBasePath = wsServerURL(server)
	defer feed.Stop()

	// Enough long-named symbols to produce several partitions under the production budgets. The
	// scenario needs a victim and at least one sister, not a hardcoded partition count: the count
	// follows whichever budget (stream count or URL length) binds first.
	symbols := make([]string, 0, 320)
	for i := 0; i < 320; i++ {
		symbols = append(symbols, fmt.Sprintf("TEST%04d/USDT", i))
	}
	if err := feed.Start(symbols); err != nil {
		t.Fatalf("Start: %v", err)
	}

	feed.mu.Lock()
	wantParts := len(feed.parts)
	if wantParts < 2 {
		feed.mu.Unlock()
		t.Fatalf("partitions = %d, want at least 2 (the scenario needs a sister)", wantParts)
	}
	victim, sister := feed.parts[0], feed.parts[1]
	victimID := victim.client.ID
	victimMembers := append([]string(nil), victim.members...)
	feed.mu.Unlock()

	if len(victimMembers) == 0 {
		t.Fatal("victim has no members")
	}
	before := connections.Load()

	if err := feed.RestartPartition(victimID); err != nil {
		t.Fatalf("RestartPartition: %v", err)
	}

	feed.mu.Lock()
	defer feed.mu.Unlock()
	if len(feed.parts) != wantParts {
		t.Fatalf("partitions after the scoped restart = %d, want %d", len(feed.parts), wantParts)
	}
	sisterAlive := false
	for _, p := range feed.parts {
		if p == sister {
			sisterAlive = true
		}
	}
	if !sisterAlive {
		t.Fatal("the sister partition was replaced: the restart was not scoped")
	}
	if sister.ctx.Err() != nil {
		t.Fatal("the sister partition was cancelled by the victim's restart")
	}
	if victim.ctx.Err() == nil {
		t.Fatal("the victim partition is still alive")
	}
	for _, symbol := range victimMembers {
		p, ok := feed.membership[symbol]
		if !ok || p == victim {
			t.Fatalf("symbol %s was not remapped to a live partition", symbol)
		}
	}
	if connections.Load() <= before {
		t.Fatal("the victim was not re-dialed: no new connection reached the server")
	}
}

func TestRestartPartitionRejectsAnUnknownPartition(t *testing.T) {
	t.Parallel()

	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {})
	defer feed.Stop()
	feed.parts = []*klinePartition{fabricatedPartition(0, time.Second, "AAA/USDT")}
	if err := feed.RestartPartition(9); err == nil {
		t.Fatal("an unknown partition id must not be accepted silently")
	}
}
