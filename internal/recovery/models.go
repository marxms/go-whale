package recovery

import (
	"context"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// ============================================================================
// PUBLIC MODELS (used by HTTP admin handlers, metrics, and callers)
// ============================================================================

// RecoveryJob represents a single symbol gap recovery task.
type RecoveryJob struct {
	ExchangeID     string
	MarketType     string
	Symbol         string
	DisconnectTime time.Time
	ReconnectTime  time.Time
	Delay          time.Duration
	window         recoveryWindow
	// RetryAttempt carries the retry attempt count when a job is re-enqueued from
	// the retry scheduler, so the budget (RetryMaxAttempts) is preserved across the
	// BackfillRetryJob -> RecoveryJob round-trip instead of resetting to 1.
	RetryAttempt int

	// TargetedTimeframe scopes TargetedBucket. A RecoveryJob is per SYMBOL and the worker
	// iterates every timeframe on it, but a bucket open-time only means something for the
	// timeframe that produced it — applying one timeframe's bucket to the others would fetch
	// the wrong window for each.
	//
	// Empty selects bulk recovery. Targeted retry identity includes timeframe/bucket.
	TargetedTimeframe string

	// TargetedBucket names ONE closed bucket to re-read, by open-time. Zero means an ordinary
	// catch-up job, which keeps its admitted lower bound and reconnect materiality.
	//
	// It is an EXPLICIT field rather than a property inferred from DisconnectTime and
	// ReconnectTime. The first attempt inferred it — "the window ends at or before the ring
	// head must mean an interior repair" — and that predicate fired on ordinary RECONNECT jobs
	// too, bypassing the materiality gate that stops a 33-second blip re-fetching daily and
	// weekly candles for the whole fleet. Two tests caught it immediately.
	//
	// Intent that changes which code path runs has to be stated, not deduced from a coincidence
	// of timestamps that a different caller can reproduce by accident.
	TargetedBucket int64
}

// BackfillRetryJob represents a deferred retry entry.
type BackfillRetryJob struct {
	ExchangeID  string
	MarketType  string
	Symbol      string
	Timeframe   string
	SinceMs     int64
	FetchLimit  int
	Attempt     int
	NextRetryAt time.Time
	window      recoveryWindow

	// TargetedTimeframe/TargetedBucket carry a single-bucket repair ACROSS the retry, and their
	// absence made every retry of one a guaranteed no-op.
	//
	// reenqueueRetryJob rebuilt a generic RecoveryJob with DisconnectTime == ReconnectTime ==
	// now. That is worse than losing the priority lane: fallbackGapDuration becomes zero, so
	// the materiality gate `continue`s on EVERY timeframe and the filler is never called at
	// all. Three live paths schedule these retries — REST budget exhausted, Fill error, and
	// validation still reporting missing buckets — and the last is literally "the repair did
	// not work", so the one case that most needs a second attempt was guaranteed to waste all
	// twelve.
	TargetedTimeframe string
	TargetedBucket    int64
}

// recoveryWindow preserves an admitted lower bound without making it targeted.
// Materiality uses the original duration, never the time spent waiting for retry.
// A zero start leaves an unscoped catch-up job derived from the live head.
type recoveryWindow struct {
	startMs     int64
	gapDuration time.Duration
}

func (w recoveryWindow) merge(other recoveryWindow) recoveryWindow {
	if w.startMs <= 0 {
		return other
	}
	if other.startMs <= 0 {
		return w
	}
	if other.startMs < w.startMs {
		w.startMs = other.startMs
	}
	if other.gapDuration > w.gapDuration {
		w.gapDuration = other.gapDuration
	}
	return w
}

// BackfillRequest is the admin HTTP payload for manual backfill.
type BackfillRequest struct {
	ExchangeID string   `json:"exchange_id"`
	MarketType string   `json:"market_type"`
	Symbols    []string `json:"symbols"`
	Timeframes []string `json:"timeframes,omitempty"`
	SinceMs    int64    `json:"since_ms"`
	UntilMs    int64    `json:"until_ms,omitempty"`
}

// BackfillResponse is the admin HTTP response for manual backfill.
type BackfillResponse struct {
	Enqueued   int      `json:"enqueued"`
	Failed     int      `json:"failed"`
	Symbols    []string `json:"symbols"`
	Timeframes []string `json:"timeframes"`
	Message    string   `json:"message"`
}

// GapRecoveryStats holds runtime statistics.
type GapRecoveryStats struct {
	Queued               int    `json:"queued"`
	Pending              int    `json:"pending"`
	Backlog              int    `json:"backlog"`
	BacklogHighWatermark int    `json:"backlog_high_watermark"`
	Enqueued             uint64 `json:"enqueued"`
	Coalesced            uint64 `json:"coalesced"`
	Retried              uint64 `json:"retried"`
	Failed               uint64 `json:"failed"`
	Processed            uint64 `json:"processed"`
	Dropped              uint64 `json:"dropped"`
	Errors               uint64 `json:"errors"`
	Workers              int    `json:"workers"`
	CandlesBackfilled    uint64 `json:"candles_backfilled"`
	MissingAfterValidate int    `json:"missing_after_validate"`
}

// ============================================================================
// FILLER INTERFACE (moved here from streaming to avoid circular imports)
// ============================================================================

// FillRequest contains parameters for a single timeframe gap fill.
//
// Symbol is the canonical internal form (BTC/USDT). The filler's HistorySource
// owns the venue-symbol translation; carrying a pre-translated symbol here
// would let two layers disagree about the same contract.
type FillRequest struct {
	ExchangeID    string
	MarketType    string
	Symbol        string
	Timeframe     string
	SinceMs       int64
	FetchLimit    int
	GapStart      int64
	CurrentBucket int64
	// UntilMs is an inclusive fetch bound. Zero keeps ordinary recovery ending at
	// CurrentBucket, which remains the true open bucket for result classification.
	UntilMs int64
}

// FillResult contains the filled candle.
type FillResult struct {
	Historical []candle.Candle
	Current    *candle.Candle
	// TakerVolumesAuthoritative is true when the filler measured both taker
	// volumes. Zero is a valid measured value, so it cannot imply absence.
	TakerVolumesAuthoritative bool
}

// GapFiller is the strategy interface for filling historical candle gaps.
type GapFiller interface {
	Fill(ctx context.Context, client HistorySource, req FillRequest) (FillResult, error)
	Name() string
}

// LatestTimestampProvider gives the most recent candle timestamp for a stream key.
type LatestTimestampProvider interface {
	GetLatestTimestamp(ctx context.Context, exchange, marketType, symbol, tf string) (int64, bool)
}
