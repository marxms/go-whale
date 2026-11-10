package ingestion

// Metrics is the ingestion observability port. The zero value is a safe no-op.
//
// tradesDiscarded counts trades dropped by deduplication, by reason. These drop points are
// the ONLY places an inbound trade can disappear before it reaches candle aggregation, and
// both were silent — no error, no log, no counter. That silence is what let a real defect
// hide: concurrent draining of the same symbol let the id watermark advance past a batch
// that had never been processed, and the batch was discarded wholesale.
//
// tradesMissing answers a different question: aggregate trades the EXCHANGE issued and this
// process never received. tradesDiscarded says "we got it and threw it away"; tradesMissing
// says "we never got it".
//
// admittedVolume is the sum of the QUANTITIES of every trade this process admitted into the
// aggregator, by scope. Its partner lives in the engine
// (engine_applied_delta_volume_total). A VOLUME, not a count, on purpose: the residual
// defect has a signature of whole one-sided aggTrades missing, where one taker leg is exact
// and the other carries the entire deficit at roughly twice the volume's relative error.
type Metrics struct {
	TradesDiscarded   func(exchange, marketType, reason string)
	TradesMissing     func(exchange, marketType string, n float64)
	TradesMissingTail func(exchange, marketType string, n float64)
	AdmittedVolume    func(exchange, marketType string, volume float64)

	// Pre-registration materializes a series at ZERO. Absence and zero are different
	// answers, and only zero is evidence.
	PreregisterDiscarded func(exchange, marketType, reason string)
	PreregisterMissing   func(exchange, marketType string)
	PreregisterAdmitted  func(exchange, marketType string)
	ForgetAdmitted       func(exchange, marketType string)
}

// discardReasons is the CLOSED SET of ways a trade can be dropped before aggregation. It is
// a list rather than scattered string literals so pre-registration cannot drift from the
// increment sites: adding a reason without adding it here leaves that path invisible at
// zero, which is the condition this exists to remove.
func discardReasons() []string {
	return []string{"stale_timestamp", "duplicate_id", "beyond_window", "unparseable"}
}

func (m Metrics) incDiscard(exchange, marketType, reason string) {
	if m.TradesDiscarded != nil {
		m.TradesDiscarded(exchange, marketType, reason)
	}
}

func (m Metrics) addMissing(exchange, marketType string, n float64) {
	if m.TradesMissing != nil {
		m.TradesMissing(exchange, marketType, n)
	}
}

func (m Metrics) addMissingTail(exchange, marketType string, n float64) {
	if m.TradesMissingTail != nil {
		m.TradesMissingTail(exchange, marketType, n)
	}
}

func (m Metrics) addAdmitted(exchange, marketType string, volume float64) {
	if m.AdmittedVolume != nil {
		m.AdmittedVolume(exchange, marketType, volume)
	}
}

// PreregisterDiscardReasons creates the tradesDiscarded and tradesMissing series at zero.
//
// A Prometheus CounterVec materializes a series only on its first WithLabelValues call, so
// a discard path that never fires produces no series at all. Measured in production after
// 4h55m and 500 symbols: /metrics carried NO tradesDiscarded series. That reads as "no
// data", which is indistinguishable from "not wired" — and it is why the acceptance gate
// that requires "zero silent discard" came back NOT-VERIFIED rather than PASS.
//
// Idempotent by construction: the existing child is returned when one is present, so calling
// this per subscription never resets a counter that has already moved.
func (h *StreamTradeHandler) PreregisterDiscardReasons(exchange, marketType string) {
	for _, reason := range discardReasons() {
		if h.metrics.PreregisterDiscarded != nil {
			h.metrics.PreregisterDiscarded(exchange, marketType, reason)
		}
	}
	if h.metrics.PreregisterMissing != nil {
		h.metrics.PreregisterMissing(exchange, marketType)
	}
}

// PreregisterAdmittedVolume materializes the admitted-volume series at zero, for the same
// reason every other counter in this file is pre-registered.
func (h *StreamTradeHandler) PreregisterAdmittedVolume(exchange, marketType string) {
	if h.metrics.PreregisterAdmitted != nil {
		h.metrics.PreregisterAdmitted(exchange, marketType)
	}
}

// ForgetAdmittedVolume removes this scope's series entirely. Once the venue owns the
// candles, any value this series still exports is an artifact of the boot window and reads
// as a permanent accusation. Absent is the honest state: it means "not measured here".
func (h *StreamTradeHandler) ForgetAdmittedVolume(exchange, marketType string) {
	if h.metrics.ForgetAdmitted != nil {
		h.metrics.ForgetAdmitted(exchange, marketType)
	}
}
