package recovery

import (
	"log/slog"
	"sync"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// Default thresholds — can be overridden via configuration.
const (
	DefaultCircuitBreakerThreshold = 5
	DefaultCircuitBreakerTimeout   = 60 * time.Second
)

// SimpleCircuitBreaker is a per-key failure counter that opens after N
// consecutive failures and stays open for a fixed duration.
type SimpleCircuitBreaker struct {
	threshold int
	timeout   time.Duration
	failures  map[string]int
	openUntil map[string]time.Time
	mu        sync.Mutex
	log       *slog.Logger
}

// NewSimpleCircuitBreaker creates a circuit breaker with sensible defaults.
func NewSimpleCircuitBreaker(logs ...*slog.Logger) *SimpleCircuitBreaker {
	return &SimpleCircuitBreaker{
		log:       logger.OrDiscard(logs...),
		threshold: DefaultCircuitBreakerThreshold,
		timeout:   DefaultCircuitBreakerTimeout,
		failures:  make(map[string]int),
		openUntil: make(map[string]time.Time),
	}
}

// IsOpen reports whether the circuit is open for the given scope key.
func (cb *SimpleCircuitBreaker) IsOpen(key string) bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if until, ok := cb.openUntil[key]; ok && time.Now().Before(until) {
		return true
	}
	return false
}

// RecordFailure increments the failure counter. Returns true if the circuit
// just opened as a result of this failure.
func (cb *SimpleCircuitBreaker) RecordFailure(key string) bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures[key]++
	if cb.failures[key] >= cb.threshold {
		cb.openUntil[key] = time.Now().Add(cb.timeout)
		logger.OrDiscard(cb.log).Warn("gap recovery: circuit open",
			"key", key, "failures", cb.failures[key], "closes_at", cb.openUntil[key].Format(time.RFC3339))
		return true
	}
	return false
}

// RecordSuccess resets the failure state for the given key.
func (cb *SimpleCircuitBreaker) RecordSuccess(key string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.failures[key] > 0 {
		logger.OrDiscard(cb.log).Info("gap recovery: circuit reset", "key", key, "previous_failures", cb.failures[key])
	}
	delete(cb.failures, key)
	delete(cb.openUntil, key)
}
