package binancesdk

import (
	"strings"
	"testing"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// The counter keeps every loss; the log keeps the first one. A sustained overflow must not drown
// the line that says it started, but silence is not allowed either.
func TestTradeBufferFirstDropLogsOnce(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)
	client := &SDKClient{buffers: NewBufferManager(0), log: log}
	payload := []byte(`{"e":"aggTrade","s":"BTC/USDT","a":1,"p":"101.5","q":"2","T":1793882096789,"m":false}`)

	handler := client.tradeCallback("BTC/USDT")
	handler(payload)
	handler(payload)
	handler(payload)

	overflowLines := 0
	for _, record := range sink.Records() {
		if strings.Contains(record.Text(), "trade buffer overflow") {
			overflowLines++
		}
	}
	if overflowLines != 1 {
		t.Fatalf("overflow log lines = %d, want 1", overflowLines)
	}
	if got := client.tradesDropped.Load(); got != 3 {
		t.Fatalf("tradesDropped = %d, want 3", got)
	}
}
