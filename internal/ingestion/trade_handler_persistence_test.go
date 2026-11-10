package ingestion

import (
	"context"
	"errors"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

type persistenceFailingStore struct {
	batchErr error
	lastErr  error

	batchCalls int
	lastCalls  int
}

func (s *persistenceFailingStore) AddStreamingCandlesBatch(_ context.Context, _ []redisstore.CandleUpdate) error {
	s.batchCalls++
	return s.batchErr
}

func (s *persistenceFailingStore) SetLastTrade(_ context.Context, _, _, _ string, _ candle.Trade) error {
	s.lastCalls++
	return s.lastErr
}

func TestProcessCandleTrades_ReturnsCriticalErrorOnBatchPersistenceFailure(t *testing.T) {
	tfReg := candle.NewTimeframeRegistry([]string{"1m"})

	store := &persistenceFailingStore{batchErr: context.DeadlineExceeded}
	handler := NewStreamTradeHandler(store, "binance", "future", tfReg)

	before := handler.PersistenceObservability().Stats()

	err := handler.processCandleTrades(
		"binance",
		"future",
		"BTC/USDT",
		[]candle.Trade{{ID: "1", Timestamp: time.Now().UnixMilli(), Price: 100, Amount: 1, Side: "buy"}},
		tradeGap{},
	)
	if err == nil {
		t.Fatalf("expected critical persistence error")
	}
	var criticalErr *streaming.CriticalPersistenceError
	if !errors.As(err, &criticalErr) {
		t.Fatalf("expected critical persistence error type, got: %v", err)
	}
	if store.batchCalls != 1 {
		t.Fatalf("expected one batch persistence attempt, got %d", store.batchCalls)
	}
	if store.lastCalls != 0 {
		t.Fatalf("expected no last-trade persistence attempt after critical batch failure, got %d", store.lastCalls)
	}

	after := handler.PersistenceObservability().Stats()
	if after.TotalFailures < before.TotalFailures+1 {
		t.Fatalf("expected total failures to increase by at least 1 (before=%d after=%d)", before.TotalFailures, after.TotalFailures)
	}
	if after.CriticalFailures < before.CriticalFailures+1 {
		t.Fatalf("expected critical failures to increase by at least 1 (before=%d after=%d)", before.CriticalFailures, after.CriticalFailures)
	}
}

func TestProcessCandleTrades_LastTradeFailureIsObservedButNotFatal(t *testing.T) {
	tfReg := candle.NewTimeframeRegistry([]string{"1m"})

	store := &persistenceFailingStore{lastErr: errors.New("metadata write failed")}
	handler := NewStreamTradeHandler(store, "binance", "future", tfReg)

	before := handler.PersistenceObservability().Stats()

	err := handler.processCandleTrades(
		"binance",
		"future",
		"ETH/USDT",
		[]candle.Trade{{ID: "2", Timestamp: time.Now().UnixMilli(), Price: 200, Amount: 2, Side: "sell"}},
		tradeGap{},
	)
	if err != nil {
		t.Fatalf("expected non-fatal last-trade failure, got %v", err)
	}
	if store.batchCalls != 1 {
		t.Fatalf("expected one batch persistence attempt, got %d", store.batchCalls)
	}
	if store.lastCalls != 1 {
		t.Fatalf("expected one last-trade persistence attempt, got %d", store.lastCalls)
	}

	after := handler.PersistenceObservability().Stats()
	if after.TotalFailures < before.TotalFailures+1 {
		t.Fatalf("expected total failures to increase by at least 1 (before=%d after=%d)", before.TotalFailures, after.TotalFailures)
	}
	if after.CriticalFailures != before.CriticalFailures {
		t.Fatalf("expected critical failures unchanged for last-trade write failures (before=%d after=%d)", before.CriticalFailures, after.CriticalFailures)
	}
}
