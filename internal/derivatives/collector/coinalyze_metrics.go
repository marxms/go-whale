package collector

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE CYCLE'S SPEND BECOMES A MEASURED SIGNAL.
//
// # WHY MEASURE, IF THE CEILING IS DOCUMENTED
//
// The ceiling is documented: 40 calls per minute PER KEY. What the constant does NOT say is how
// many processes share that key. Measured once: the key fingerprint was the SAME in production
// and on the LAN (`sha256` = `0ec152beb58edff8`), both had `GOWHALE_COINALYZE_ENABLED=true`, and
// both emitted — 991 and 623 refusals in 24 h. Two processes budgeting 40 each on a 40-key
// saturate the quota by construction.
//
// So the LOCAL budget is honest accounting and still insufficient, because it describes this
// process while the quota belongs to the pair. The only way out is to measure both sides — what was
// spent and what came back — and leave the comparison visible.
//
// # THE REPORTED RATE DERIVES FROM THE MEASURED DURATION
//
// Never from the constant. A rate computed from `CoinalyzeCallsPerMinute` would report 40/min
// forever, including in a cycle where the process could spend nothing — exactly the saturation
// regime that must appear.
// ─────────────────────────────────────────────────────────────────────────────

// coinalyzeTallySource is the budget's local accounting, narrowed to what this signal reads. An
// interface because the signal must be checkable without a live budget.
type coinalyzeTallySource interface {
	Tally() coinalyze.CoinalyzeBudgetTally
}

// CoinalyzeCycleSignal is what ONE cycle actually spent and got back.
type CoinalyzeCycleSignal struct {
	// Elapsed is the MEASURED cycle duration.
	Elapsed time.Duration
	// CallsSpent is the calls debited IN THIS cycle — the difference between two readings of the
	// accumulated accounting, never the accumulated value.
	CallsSpent int64
	// RateRefusals is the 429s received IN THIS cycle.
	RateRefusals int64
	// ExternalQuotaRefusals is, of those, the ones local accounting does not explain.
	ExternalQuotaRefusals int64
}

// ObservedCallsPerMinute is the rate the cycle ACTUALLY sustained.
//
// Derived from the measured duration, not from the declared ceiling: a cycle that spent the whole
// hour waiting for a penalty sustained a low rate, and that is the rate that must appear. A
// non-positive duration returns zero — not knowing is not zero, and zero here is literal: no time
// passed, no rate was sustained.
func (s CoinalyzeCycleSignal) ObservedCallsPerMinute() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.CallsSpent) / s.Elapsed.Minutes()
}

// CoinalyzeSpendMeter turns the budget's ACCUMULATED accounting into ONE cycle's spend, by
// difference.
//
// The difference lives here and not in the budget because the accumulated value must stay
// monotonic: zeroing it at the end of every cycle would make two concurrent readings steal counts
// from each other, and the Prometheus counter would lose the only invariant it has.
type CoinalyzeSpendMeter struct {
	source  coinalyzeTallySource
	last    coinalyze.CoinalyzeBudgetTally
	metrics Metrics
	log     *slog.Logger
}

// SetMetrics injects the observability port.
func (m *CoinalyzeSpendMeter) SetMetrics(metrics Metrics) {
	if m != nil {
		m.metrics = metrics
	}
}

// NewCoinalyzeSpendMeter assembles the meter. A nil source returns a nil meter — degradation is
// declared, and a meter reporting zero forever would be worse than not existing.
func NewCoinalyzeSpendMeter(source coinalyzeTallySource, logs ...*slog.Logger) *CoinalyzeSpendMeter {
	if source == nil {
		return nil
	}
	return &CoinalyzeSpendMeter{source: source, log: logger.OrDiscard(logs...)}
}

// Cycle closes a cycle: it reads the accounting, takes the difference and returns the signal.
//
// A NIL receiver returns the empty signal — the same contract as `DurableFillProbe.Report`, so that
// the caller does not need an `if` it would forget.
func (m *CoinalyzeSpendMeter) Cycle(elapsed time.Duration) CoinalyzeCycleSignal {
	if m == nil {
		return CoinalyzeCycleSignal{}
	}
	agora := m.source.Tally()
	sinal := CoinalyzeCycleSignal{
		Elapsed:               elapsed,
		CallsSpent:            agora.CallsSpent - m.last.CallsSpent,
		RateRefusals:          agora.RateRefusals - m.last.RateRefusals,
		ExternalQuotaRefusals: agora.ExternalQuotaRefusals - m.last.ExternalQuotaRefusals,
	}
	m.last = agora

	m.metrics.addCoinalyzeCallsSpent(float64(sinal.CallsSpent))
	m.metrics.addCoinalyzeRateRefusals(float64(sinal.RateRefusals))
	m.metrics.addCoinalyzeExternalQuotaRefusals(float64(sinal.ExternalQuotaRefusals))
	reportCoinalyzeSpend(sinal, m.log)
	return sinal
}

// reportCoinalyzeSpend puts the requirement's three quantities on the SAME line: duration, spend
// and refusals.
//
// Together and not on three lines because the question is the relation between them. "I spent 120
// calls" says nothing; "I spent 120 calls in 2 minutes and received 37 refusals" says the window is
// being consumed by another process.
func reportCoinalyzeSpend(s CoinalyzeCycleSignal, log *slog.Logger) {
	log = logger.OrDiscard(log)
	if s.Elapsed <= 0 && s.CallsSpent == 0 && s.RateRefusals == 0 {
		// A cycle that spent nothing and lasted nothing: there is no measurement to report, and
		// one line per empty cycle would drown the ones with content.
		return
	}
	if s.RateRefusals > 0 {
		log.Warn("coinalyze cycle spent calls and received rate refusals",
			"elapsed", s.Elapsed.Round(time.Second),
			"calls_spent", s.CallsSpent,
			"calls_per_minute", fmt.Sprintf("%.1f", s.ObservedCallsPerMinute()),
			"rate_refusals", s.RateRefusals,
			"external_quota_refusals", s.ExternalQuotaRefusals)
		return
	}
	log.Info("coinalyze cycle spent calls without rate refusals",
		"elapsed", s.Elapsed.Round(time.Second),
		"calls_spent", s.CallsSpent,
		"calls_per_minute", fmt.Sprintf("%.1f", s.ObservedCallsPerMinute()))
}
