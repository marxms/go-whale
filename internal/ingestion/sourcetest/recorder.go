package sourcetest

import (
	"sync"
	"time"
)

// ReconnectBatch is one reconnect notification observed through the source's configured
// ReconnectSink.
type ReconnectBatch struct {
	ExchangeID     string
	MarketType     string
	Symbols        []string
	DisconnectTime time.Time
	ReconnectTime  time.Time
}

// ReconnectRecorder is a thread-safe ingestion.ReconnectSink that records reconnect
// notifications for the suite (and for any consumer test that needs the same window).
type ReconnectRecorder struct {
	mu      sync.Mutex
	batches []ReconnectBatch
}

// NewReconnectRecorder builds an empty recorder.
func NewReconnectRecorder() *ReconnectRecorder { return &ReconnectRecorder{} }

// EnqueueBatch implements ingestion.ReconnectSink.
func (r *ReconnectRecorder) EnqueueBatch(
	exchangeID, marketType string,
	symbols []string,
	disconnectTime, reconnectTime time.Time,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, ReconnectBatch{
		ExchangeID:     exchangeID,
		MarketType:     marketType,
		Symbols:        append([]string(nil), symbols...),
		DisconnectTime: disconnectTime,
		ReconnectTime:  reconnectTime,
	})
}

// Batches returns a copy of everything recorded so far.
func (r *ReconnectRecorder) Batches() []ReconnectBatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ReconnectBatch(nil), r.batches...)
}
