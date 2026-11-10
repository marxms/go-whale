package streaming

import (
	"context"
	"errors"
	"testing"
)

func TestRecordPersistenceFailure_BatchTimeoutIsCriticalAndCounted(t *testing.T) {
	obs := NewPersistenceObservability()

	event := obs.Record(
		PersistenceOperationBatchWrite,
		"binance:future:BTC/USDT",
		context.DeadlineExceeded,
	)

	if event.Kind != PersistenceFailureKindBatchWriteTimeout {
		t.Fatalf("expected kind %s, got %s", PersistenceFailureKindBatchWriteTimeout, event.Kind)
	}
	if event.Class != PersistenceFailureClassTimeout {
		t.Fatalf("expected class %s, got %s", PersistenceFailureClassTimeout, event.Class)
	}
	if !event.Critical {
		t.Fatalf("expected critical=true for batch write failure")
	}

	stats := obs.Stats()
	if stats.TotalFailures != 1 {
		t.Fatalf("expected total_failures=1, got %d", stats.TotalFailures)
	}
	if stats.CriticalFailures != 1 {
		t.Fatalf("expected critical_failures=1, got %d", stats.CriticalFailures)
	}
	if stats.ByKind.BatchWriteTimeout != 1 {
		t.Fatalf("expected batch_write_timeout=1, got %d", stats.ByKind.BatchWriteTimeout)
	}
	if stats.LastFailure == nil {
		t.Fatalf("expected last_failure to be present")
	}
	if stats.LastFailure.Kind != PersistenceFailureKindBatchWriteTimeout {
		t.Fatalf("expected last_failure kind %s, got %s", PersistenceFailureKindBatchWriteTimeout, stats.LastFailure.Kind)
	}
}

func TestRecordPersistenceFailure_LastTradeCanceledIsNonCritical(t *testing.T) {
	obs := NewPersistenceObservability()

	err := context.Canceled
	event := obs.Record(
		PersistenceOperationLastTradeWrite,
		"binance:future:ETH/USDT",
		err,
	)

	if event.Kind != PersistenceFailureKindLastTradeWriteCanceled {
		t.Fatalf("expected kind %s, got %s", PersistenceFailureKindLastTradeWriteCanceled, event.Kind)
	}
	if event.Class != PersistenceFailureClassCanceled {
		t.Fatalf("expected class %s, got %s", PersistenceFailureClassCanceled, event.Class)
	}
	if event.Critical {
		t.Fatalf("expected critical=false for last-trade persistence failure")
	}

	stats := obs.Stats()
	if stats.TotalFailures != 1 {
		t.Fatalf("expected total_failures=1, got %d", stats.TotalFailures)
	}
	if stats.CriticalFailures != 0 {
		t.Fatalf("expected critical_failures=0, got %d", stats.CriticalFailures)
	}
	if stats.ByKind.LastTradeWriteCanceled != 1 {
		t.Fatalf("expected last_trade_write_canceled=1, got %d", stats.ByKind.LastTradeWriteCanceled)
	}
}

func TestRecordPersistenceFailure_UnknownErrorFallsBackToGenericKind(t *testing.T) {
	obs := NewPersistenceObservability()

	event := obs.Record(
		PersistenceOperationBatchWrite,
		"binance:future:SOL/USDT",
		errors.New("boom"),
	)

	if event.Kind != PersistenceFailureKindBatchWriteError {
		t.Fatalf("expected kind %s, got %s", PersistenceFailureKindBatchWriteError, event.Kind)
	}
	if event.Class != PersistenceFailureClassOther {
		t.Fatalf("expected class %s, got %s", PersistenceFailureClassOther, event.Class)
	}

	stats := obs.Stats()
	if stats.ByKind.BatchWriteError != 1 {
		t.Fatalf("expected batch_write_error=1, got %d", stats.ByKind.BatchWriteError)
	}
}
