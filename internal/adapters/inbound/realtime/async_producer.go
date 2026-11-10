package realtime

import (
	"log/slog"
	"time"

	"github.com/marxms/go-whale/internal/distribution"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// Publisher is the minimal transport the producer needs. It is owned by distribution;
// the alias keeps the app's wiring vocabulary local and the Redis adapter free of a
// compile-time dependency on the producer.
type Publisher = distribution.Publisher

// AsyncProducer is the transport-agnostic per-channel owner-goroutine producer.
type AsyncProducer = distribution.AsyncProducer

// AsyncProducerOption configures the producer.
type AsyncProducerOption = distribution.AsyncProducerOption

// NewAsyncProducer wires the distribution producer to the realtime observability surface.
// The producer itself imports neither Prometheus nor the logger: those side effects are
// injected here, which is what lets callers redirect the registry without touching the
// ordering/backpressure machinery.
func NewAsyncProducer(pub Publisher, prefix string, clock func() int64, opts ...AsyncProducerOption) *AsyncProducer {
	return NewAsyncProducerWithLogger(pub, prefix, clock, Metrics{}, nil, opts...)
}

// NewAsyncProducerWithLogger is NewAsyncProducer with the process logger injected into the
// producer's side-effect hooks.
func NewAsyncProducerWithLogger(pub Publisher, prefix string, clock func() int64, metrics Metrics, log *slog.Logger, opts ...AsyncProducerOption) *AsyncProducer {
	h := realtimeProducerHooks{metrics: metrics, log: logger.OrDiscard(log)}
	opts = append([]AsyncProducerOption{distribution.WithHooks(distribution.ProducerHooks{
		OnDrop:         h.OnDrop,
		OnPublishError: h.OnPublishError,
		OnMarshalError: h.OnMarshalError,
		OnDrainTimeout: h.OnDrainTimeout,
	})}, opts...)
	return distribution.NewAsyncProducer(pub, prefix, clock, opts...)
}

// realtimeProducerHooks is the realtime edge's observer for the producer's side effects.
type realtimeProducerHooks struct {
	metrics Metrics
	log     *slog.Logger
}

func (h realtimeProducerHooks) OnDrop(channel string, total int64) {
	h.metrics.publishDropped(channel)
	if total%100 == 1 {
		logger.OrDiscard(h.log).Warn("realtime: outbound queue full; dropped events; subscribers will detect the sequence gap and rehydrate",
			"channel", channel, "dropped", total)
	}
}

func (h realtimeProducerHooks) OnPublishError(channel string, err error) {
	logger.OrDiscard(h.log).Warn("realtime: publish failed", "channel", channel, "error", err)
}

func (h realtimeProducerHooks) OnMarshalError(typ distribution.EventType, err error) {
	logger.OrDiscard(h.log).Error("realtime: marshal failed", "event_type", typ, "error", err)
}

func (h realtimeProducerHooks) OnDrainTimeout(timeout time.Duration) {
	logger.OrDiscard(h.log).Warn("realtime: outbound drain timed out at shutdown; abandoning the queued tail",
		"timeout", timeout)
}
