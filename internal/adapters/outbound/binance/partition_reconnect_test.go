package binancesdk

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/adapters/outbound/binancews"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// Regression: a partition-0 socket error also closed partition 1, resetting a connection for a
// fault that was not hers. The scoped reconnect must replace only the victim and leave the sister's
// connection, counters and clock intact.
func TestReconnectOnePartitionKeepsTheSister(t *testing.T) {
	server, connections := newMarketWSServer(t, func(conn *websocket.Conn, _ *http.Request, _ int) {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})

	c := NewSDKClient(streaming.ClientConfig{TradeBufferSize: 10, MarketType: "swap"})
	c.wsBasePath = wsServerURL(server)
	c.livenessLedgerEnabled = true
	t.Cleanup(func() { _ = c.Close() })

	// Long names so the combined URL hits its budget before the stream-count budget: this is how
	// production ends up with two uneven partitions.
	symbols := make([]string, 0, 340)
	for i := 0; i < 340; i++ {
		symbols = append(symbols, fmt.Sprintf("TEST%04d/USDT", i))
	}
	c.symbolsMu.Lock()
	for _, s := range symbols {
		c.symbols[s] = true
	}
	c.symbolsMu.Unlock()

	c.connectMu.Lock()
	if _, err := c.connectAndResubscribeCombined(symbols); err != nil {
		c.connectMu.Unlock()
		t.Fatalf("combined connect: %v", err)
	}
	c.connectMu.Unlock()

	c.connectMu.Lock()
	if len(c.partitions) != 2 {
		c.connectMu.Unlock()
		t.Fatalf("partitions = %d, want 2 (the scenario needs a sister)", len(c.partitions))
	}
	victim, sister := c.partitions[0], c.partitions[1]
	c.connectMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.connectMu.Lock()
	c.connCtx, c.connCancel = ctx, cancel
	c.connectMu.Unlock()

	members := c.ledger.SymbolsForPartition(0)
	if len(members) == 0 {
		t.Fatal("the ledger recorded no members for partition 0")
	}
	before := connections.Load()

	if !c.reconnectOnePartition(0, "test") {
		t.Fatal("the partition rebuild was declined; the fleet fallback would have closed the sister")
	}

	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	if c.partitions[1] != sister {
		t.Fatal("the sister partition was replaced: the reconnect was not scoped")
	}
	// The pointer alone proves nothing: a fleet-wide close would keep the pointer and still kill
	// the sister's socket. Its DONE channel is the observable that says the owner closed it.
	select {
	case <-sister.Done():
		t.Fatal("the sister partition was CLOSED by the victim's reconnect")
	default:
	}
	if c.partitions[0] == victim {
		t.Fatal("the victim partition was not replaced")
	}
	if c.wsConn != c.partitions[0] {
		t.Fatal("wsConn still points at the dead partition")
	}
	if ev := victim.CloseEvidence(); ev.Initiator != binancews.InitiatorLocalShutdown {
		t.Fatalf("victim evidence = %+v, want OUR close recorded on the victim only", ev)
	}
	// The server's counter is incremented by its own handler goroutine after the upgrade; a
	// direct read here raced it once the stability delay changed the interleaving. Waiting keeps
	// the assertion (a NEW connection must arrive) without depending on scheduler order.
	if !waitFor(t, 2*time.Second, func() bool { return connections.Load() > before }) {
		t.Fatal("the victim was not re-dialed: no new connection reached the server")
	}
	if got := c.ledger.SymbolsForPartition(0); len(got) != len(members) {
		t.Fatalf("partition 0 membership changed during the redial: %d -> %d", len(members), len(got))
	}
}

// A partition the ledger does not know about must NOT be rebuilt from a guess: the caller falls
// back to the fleet path, which restores every symbol.
func TestReconnectOnePartitionDeclinesWithoutMembership(t *testing.T) {
	c := NewSDKClient(streaming.ClientConfig{TradeBufferSize: 10, MarketType: "swap"})
	t.Cleanup(func() { _ = c.Close() })

	if c.reconnectOnePartition(3, "test") {
		t.Fatal("a partition with no recorded members must not be rebuilt")
	}
}
