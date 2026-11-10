package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ============================================================================
// BASIC OHLCV FILLER (fallback — current behavior using the HistorySource port)
// ============================================================================

// BasicOHLCVFiller uses the OHLCV-only history read (six fields) as the
// fallback method.
type BasicOHLCVFiller struct {
	log *slog.Logger
}

// NewBasicOHLCVFiller creates a new basic filler.
func NewBasicOHLCVFiller(logs ...*slog.Logger) *BasicOHLCVFiller {
	return &BasicOHLCVFiller{log: logger.OrDiscard(logs...)}
}

// Name returns "basic-ohlcv".
func (f *BasicOHLCVFiller) Name() string {
	return "basic-ohlcv"
}

// Fill fetches OHLCV and converts to candle.Candle (6 fields only).
func (f *BasicOHLCVFiller) Fill(ctx context.Context, client HistorySource, req FillRequest) (FillResult, error) {
	ohlcvs, err := client.FetchOHLCV(ctx, req.Symbol, req.Timeframe, req.SinceMs, int64(req.FetchLimit))
	if err != nil {
		return FillResult{}, fmt.Errorf("fetch OHLCV: %w", err)
	}

	var result FillResult
	now := time.Now().UnixMilli()

	for _, ohlcv := range ohlcvs {
		if ohlcv.Timestamp < req.GapStart || (req.UntilMs > 0 && ohlcv.Timestamp > req.UntilMs) {
			continue // skip pre-gap candles
		}

		c := candle.Candle{
			Timestamp:  ohlcv.Timestamp,
			Open:       ohlcv.Open,
			High:       ohlcv.High,
			Low:        ohlcv.Low,
			Close:      ohlcv.Close,
			Volume:     ohlcv.Volume,
			CreatedAt:  now,
			LastUpdate: now,
			Source:     "gap-recovery",
		}

		if ohlcv.Timestamp == req.CurrentBucket {
			result.Current = &c
		} else {
			result.Historical = append(result.Historical, c)
		}
	}

	logger.OrDiscard(f.log).Debug("gap filler: filled",
		"filler", f.Name(),
		"historical", len(result.Historical),
		"current", boolToInt(result.Current != nil),
		"symbol", req.Symbol,
		"timeframe", req.Timeframe)

	return result, nil
}

// ============================================================================
// UTILITIES
// ============================================================================

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
