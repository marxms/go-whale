package recovery

import (
	"context"
	"log/slog"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// Compile-time check: NoOpFiller implements GapFiller.
var _ GapFiller = (*NoOpFiller)(nil)

// NoOpFiller is a no-operation gap filler that logs a warning and returns
// an empty result. It serves as a safe fallback when no real filler is
// configured, preventing nil-pointer panics in the worker pool.
type NoOpFiller struct {
	log *slog.Logger
}

// NewNoOpFiller creates a new NoOpFiller.
func NewNoOpFiller(logs ...*slog.Logger) *NoOpFiller {
	return &NoOpFiller{log: logger.OrDiscard(logs...)}
}

// Fill logs a warning and returns an empty FillResult with no error.
// This allows gap-recovery workers to continue operating when a specific
// exchange filler is unavailable (e.g., BingX without a REST filler).
func (f *NoOpFiller) Fill(_ context.Context, _ HistorySource, req FillRequest) (FillResult, error) {
	logger.OrDiscard(f.log).Warn("gap recovery disabled: no real filler configured",
		"exchange", req.ExchangeID, "market_type", req.MarketType, "symbol", req.Symbol, "timeframe", req.Timeframe)
	return FillResult{}, nil
}

// Name returns "no-op".
func (f *NoOpFiller) Name() string {
	return "no-op"
}
