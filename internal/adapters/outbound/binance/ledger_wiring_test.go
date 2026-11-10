package binancesdk

import (
	"testing"

	"github.com/marxms/go-whale/internal/health"
)

// TestTradeCallbackStampsLedgerWhenEnabled proves the ingest hot path stamps the
// freshness ledger on message arrival (D1) when the flag is on — even for a
// message that fails to parse into a trade (liveness = "a message arrived").
func TestTradeCallbackStampsLedgerWhenEnabled(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, ledger: health.New(), buffers: NewBufferManager(10)}
	c.tradeCallback("BTC/USDT")([]byte(`{}`))

	if _, ok := c.ledger.LastSeen("BTC/USDT"); !ok {
		t.Fatal("tradeCallback did not stamp the ledger on message arrival")
	}
}

// TestTradeCallbackNoStampWhenDisabled proves the ledger is untouched (and the hot path pays
// only a bool check) when the flag is off, keeping the default behavior.
func TestTradeCallbackNoStampWhenDisabled(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: false, ledger: health.New(), buffers: NewBufferManager(10)}
	c.tradeCallback("BTC/USDT")([]byte(`{}`))

	if _, ok := c.ledger.LastSeen("BTC/USDT"); ok {
		t.Fatal("ledger stamped while the flag was disabled")
	}
}
