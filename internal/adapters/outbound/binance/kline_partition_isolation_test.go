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

	// Long enough names to exceed the combined-URL budget before the stream-count budget, which is
	// how production ends up with two uneven partitions.
	symbols := make([]string, 0, 320)
	for i := 0; i < 320; i++ {
		symbols = append(symbols, fmt.Sprintf("TEST%04d/USDT", i))
	}
	if err := feed.Start(symbols); err != nil {
		t.Fatalf("Start: %v", err)
	}

	feed.mu.Lock()
	if len(feed.parts) != 2 {
		feed.mu.Unlock()
		t.Fatalf("partitions = %d, want 2 (the scenario needs a sister)", len(feed.parts))
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
	if len(feed.parts) != 2 {
		t.Fatalf("partitions after the scoped restart = %d, want 2", len(feed.parts))
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
