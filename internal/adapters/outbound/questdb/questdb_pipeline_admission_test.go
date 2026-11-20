package questdb

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

func TestPipelineEnqueueReturnsAdmissionAndIdentity(t *testing.T) {
	pipeline := NewPipeline(nil, 1, time.Hour)
	closed := candle.Candle{Timestamp: 1_700_000_000_000, Close: 100}

	admitted, id := pipeline.Enqueue("binance", "future", "BTC/USDT", "5m", closed)
	if !admitted {
		t.Fatal("the first enqueue must be admitted")
	}
	if id.Exchange != "binance" || id.MarketType != "future" || id.Symbol != "BTC/USDT" ||
		id.Timeframe != "5m" || id.Timestamp != closed.Timestamp {
		t.Fatalf("identity = %+v, want the closed candle's scope", id)
	}

	admitted, id = pipeline.Enqueue("binance", "future", "ETH/USDT", "5m", closed)
	if admitted {
		t.Fatal("an enqueue beyond the buffer capacity must be refused")
	}
	if id.Symbol != "ETH/USDT" || id.Timestamp != closed.Timestamp {
		t.Fatalf("refused identity = %+v, want the refused candle", id)
	}
	if got := pipeline.Stats().Dropped; got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
}
