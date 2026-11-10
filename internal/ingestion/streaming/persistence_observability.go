package streaming

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// PersistenceOperation identifies where persistence failed.
type PersistenceOperation string

// The instrumented persistence write paths.
const (
	PersistenceOperationBatchWrite     PersistenceOperation = "batch_write"
	PersistenceOperationLastTradeWrite PersistenceOperation = "last_trade_write"
)

// PersistenceFailureClass groups failure root-causes for operator action.
type PersistenceFailureClass string

// Failure classes group persistence failures for operator action.
const (
	PersistenceFailureClassTimeout  PersistenceFailureClass = "timeout"
	PersistenceFailureClassCanceled PersistenceFailureClass = "canceled"
	PersistenceFailureClassOther    PersistenceFailureClass = "other"
)

// PersistenceFailureKind provides actionable per-operation failure categories.
type PersistenceFailureKind string

// Failure kinds are the actionable per-operation categories exposed to dashboards.
const (
	PersistenceFailureKindBatchWriteTimeout      PersistenceFailureKind = "batch_write_timeout"
	PersistenceFailureKindBatchWriteCanceled     PersistenceFailureKind = "batch_write_canceled"
	PersistenceFailureKindBatchWriteError        PersistenceFailureKind = "batch_write_error"
	PersistenceFailureKindLastTradeWriteTimeout  PersistenceFailureKind = "last_trade_write_timeout"
	PersistenceFailureKindLastTradeWriteCanceled PersistenceFailureKind = "last_trade_write_canceled"
	PersistenceFailureKindLastTradeWriteError    PersistenceFailureKind = "last_trade_write_error"
	PersistenceFailureKindUnknownError           PersistenceFailureKind = "unknown_error"
)

// PersistenceFailureEvent is a single classified failure event.
type PersistenceFailureEvent struct {
	Operation PersistenceOperation    `json:"operation"`
	Kind      PersistenceFailureKind  `json:"kind"`
	Class     PersistenceFailureClass `json:"class"`
	Scope     string                  `json:"scope"`
	Error     string                  `json:"error"`
	Critical  bool                    `json:"critical"`
	AtUnixMS  int64                   `json:"at_unix_ms"`
}

// PersistenceFailureKindCounters is a fixed-shape view for dashboards.
type PersistenceFailureKindCounters struct {
	BatchWriteTimeout      uint64 `json:"batch_write_timeout"`
	BatchWriteCanceled     uint64 `json:"batch_write_canceled"`
	BatchWriteError        uint64 `json:"batch_write_error"`
	LastTradeWriteTimeout  uint64 `json:"last_trade_write_timeout"`
	LastTradeWriteCanceled uint64 `json:"last_trade_write_canceled"`
	LastTradeWriteError    uint64 `json:"last_trade_write_error"`
	UnknownError           uint64 `json:"unknown_error"`
}

// PersistenceObservabilityStats exposes cumulative persistence health.
type PersistenceObservabilityStats struct {
	TotalFailures    uint64                         `json:"total_failures"`
	CriticalFailures uint64                         `json:"critical_failures"`
	ByKind           PersistenceFailureKindCounters `json:"by_kind"`
	LastFailure      *PersistenceFailureEvent       `json:"last_failure,omitempty"`
}

// PersistenceObservability accumulates persistence failure counters. It is owned by the
// trade handler, which logs each recorded failure and exposes the counters to tests.
type PersistenceObservability struct {
	totalFailures    atomic.Uint64
	criticalFailures atomic.Uint64

	batchWriteTimeout      atomic.Uint64
	batchWriteCanceled     atomic.Uint64
	batchWriteError        atomic.Uint64
	lastTradeWriteTimeout  atomic.Uint64
	lastTradeWriteCanceled atomic.Uint64
	lastTradeWriteError    atomic.Uint64
	unknownError           atomic.Uint64
	hasLastFailure         atomic.Bool

	lastFailure atomic.Value // stores PersistenceFailureEvent
}

// NewPersistenceObservability returns an empty accumulator.
func NewPersistenceObservability() *PersistenceObservability { return &PersistenceObservability{} }

// CriticalPersistenceError marks a persistence event that should surface to the worker loop.
type CriticalPersistenceError struct {
	Event PersistenceFailureEvent
	Err   error
}

func (e *CriticalPersistenceError) Error() string {
	if e == nil {
		return "critical persistence failure"
	}
	if e.Err == nil {
		return fmt.Sprintf("critical persistence failure kind=%s scope=%s", e.Event.Kind, e.Event.Scope)
	}
	return fmt.Sprintf("critical persistence failure kind=%s scope=%s: %v", e.Event.Kind, e.Event.Scope, e.Err)
}

func (e *CriticalPersistenceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// NewCriticalPersistenceError wraps the underlying cause so callers can propagate explicitly.
func NewCriticalPersistenceError(event PersistenceFailureEvent, err error) error {
	if err == nil {
		return nil
	}
	return &CriticalPersistenceError{Event: event, Err: err}
}

// Record classifies, counts and snapshots a failure event.
func (o *PersistenceObservability) Record(op PersistenceOperation, scope string, err error) PersistenceFailureEvent {
	event := PersistenceFailureEvent{
		Operation: op,
		Scope:     strings.TrimSpace(scope),
		AtUnixMS:  time.Now().UnixMilli(),
	}

	if err != nil {
		event.Error = err.Error()
	}

	kind, class, critical := classifyPersistenceFailure(op, err)
	event.Kind = kind
	event.Class = class
	event.Critical = critical

	o.totalFailures.Add(1)
	if critical {
		o.criticalFailures.Add(1)
	}
	o.incrementKind(kind)
	o.lastFailure.Store(event)
	o.hasLastFailure.Store(true)

	return event
}

// Stats returns cumulative persistence counters and the latest event.
func (o *PersistenceObservability) Stats() PersistenceObservabilityStats {
	stats := PersistenceObservabilityStats{
		TotalFailures:    o.totalFailures.Load(),
		CriticalFailures: o.criticalFailures.Load(),
		ByKind: PersistenceFailureKindCounters{
			BatchWriteTimeout:      o.batchWriteTimeout.Load(),
			BatchWriteCanceled:     o.batchWriteCanceled.Load(),
			BatchWriteError:        o.batchWriteError.Load(),
			LastTradeWriteTimeout:  o.lastTradeWriteTimeout.Load(),
			LastTradeWriteCanceled: o.lastTradeWriteCanceled.Load(),
			LastTradeWriteError:    o.lastTradeWriteError.Load(),
			UnknownError:           o.unknownError.Load(),
		},
	}

	if o.hasLastFailure.Load() {
		last := o.lastFailure.Load()
		event, ok := last.(PersistenceFailureEvent)
		if ok {
			eventCopy := event
			stats.LastFailure = &eventCopy
		}
	}

	return stats
}

func classifyPersistenceFailure(op PersistenceOperation, err error) (PersistenceFailureKind, PersistenceFailureClass, bool) {
	class := PersistenceFailureClassOther
	switch {
	case errors.Is(err, context.DeadlineExceeded) || isTimeoutError(err):
		class = PersistenceFailureClassTimeout
	case errors.Is(err, context.Canceled):
		class = PersistenceFailureClassCanceled
	}

	switch op {
	case PersistenceOperationBatchWrite:
		switch class {
		case PersistenceFailureClassTimeout:
			return PersistenceFailureKindBatchWriteTimeout, class, true
		case PersistenceFailureClassCanceled:
			return PersistenceFailureKindBatchWriteCanceled, class, true
		default:
			return PersistenceFailureKindBatchWriteError, class, true
		}
	case PersistenceOperationLastTradeWrite:
		switch class {
		case PersistenceFailureClassTimeout:
			return PersistenceFailureKindLastTradeWriteTimeout, class, false
		case PersistenceFailureClassCanceled:
			return PersistenceFailureKindLastTradeWriteCanceled, class, false
		default:
			return PersistenceFailureKindLastTradeWriteError, class, false
		}
	default:
		return PersistenceFailureKindUnknownError, class, false
	}
}

func (o *PersistenceObservability) incrementKind(kind PersistenceFailureKind) {
	switch kind {
	case PersistenceFailureKindBatchWriteTimeout:
		o.batchWriteTimeout.Add(1)
	case PersistenceFailureKindBatchWriteCanceled:
		o.batchWriteCanceled.Add(1)
	case PersistenceFailureKindBatchWriteError:
		o.batchWriteError.Add(1)
	case PersistenceFailureKindLastTradeWriteTimeout:
		o.lastTradeWriteTimeout.Add(1)
	case PersistenceFailureKindLastTradeWriteCanceled:
		o.lastTradeWriteCanceled.Add(1)
	case PersistenceFailureKindLastTradeWriteError:
		o.lastTradeWriteError.Add(1)
	default:
		o.unknownError.Add(1)
	}
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	type timeoutIface interface {
		Timeout() bool
	}
	var timeoutErr timeoutIface
	if errors.As(err, &timeoutErr) {
		return timeoutErr.Timeout()
	}
	return false
}
