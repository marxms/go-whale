package binancesdk

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/candle"
)

// Regression for the production loop: AddSymbols used to re-use id 0 for the discovery
// connection, so RestartPartition(id 0) could remove the first Start partition instead of the
// silent victim. Every live connection must carry a unique id for the life of the feed.
func TestPartitionIDsAreUniqueAcrossStartAndAddSymbols(t *testing.T) {
	server, connections := newMarketWSServer(t, func(conn *websocket.Conn, _ *http.Request, _ int) {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {})
	feed.wsBasePath = wsServerURL(server)
	defer feed.Stop()

	symbols := make([]string, 0, 320)
	for i := 0; i < 320; i++ {
		symbols = append(symbols, fmt.Sprintf("TEST%04d/USDT", i))
	}
	if err := feed.Start(symbols); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := feed.AddSymbols([]string{"NEWCOIN/USDT", "OTHER/USDT"}); err != nil {
		t.Fatalf("AddSymbols: %v", err)
	}

	feed.mu.Lock()
	defer feed.mu.Unlock()
	seen := make(map[int]struct{}, len(feed.parts))
	for _, p := range feed.parts {
		if p.client == nil {
			continue
		}
		if _, dup := seen[p.client.ID]; dup {
			t.Fatalf("partition id %d is not unique: a restart can remove the wrong victim", p.client.ID)
		}
		seen[p.client.ID] = struct{}{}
	}
	if len(seen) < 3 {
		t.Fatalf("live partitions = %d, want at least 3 (Start split + AddSymbols)", len(seen))
	}
	if connections.Load() < int64(len(seen)) {
		t.Fatalf("server saw %d connections, want at least %d", connections.Load(), len(seen))
	}
}

// A duplicate id is a programming error. Removing either candidate would be a guess, and the
// production failure was exactly a guess; the restart must refuse and leave the feed untouched.
func TestRestartPartitionRefusesADuplicateID(t *testing.T) {
	feed := NewKlineFeed(context.Background(), "binance", "future", "5m", func(string, candle.Candle) {})
	defer feed.Stop()
	feed.parts = []*klinePartition{
		fabricatedPartition(0, time.Second, "AAA/USDT"),
		fabricatedPartition(0, time.Second, "BBB/USDT", "CCC/USDT"),
	}
	if err := feed.RestartPartition(0); err == nil {
		t.Fatal("a duplicate partition id must be refused, not guessed")
	}
	if len(feed.parts) != 2 {
		t.Fatalf("a refused restart removed a partition: parts = %d, want 2", len(feed.parts))
	}
}
