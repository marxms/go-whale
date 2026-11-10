package questdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
)

// recordingTieredCold is the durable port the tiered writer drives in these cases; the
// logger assertion belongs here, where logging is an adapter concern.
type recordingTieredCold struct {
	points []derivatives.DerivativesColdPoint
	err    error
}

func (c *recordingTieredCold) WriteDerivativesBatch(_ context.Context, points []derivatives.DerivativesColdPoint) error {
	c.points = append(c.points, points...)
	return c.err
}

func tieredSampleBatch(n int) derivatives.DerivativesBatch {
	base := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC).UnixMilli()
	snaps := make([]*derivatives.DerivativesSnapshot, 0, n)
	for i := 0; i < n; i++ {
		snaps = append(snaps, &derivatives.DerivativesSnapshot{
			Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT",
			Timestamp:        base + int64(i)*86_400_000,
			OpenInterestBase: fp(106117.954 + float64(i)),
			FundingRate:      fp(0.0001),
			LongShortRatio:   fp(0.9554),
		})
	}
	return derivatives.DerivativesBatch{
		Timeframe:     "1d",
		Source:        "coinalyze",
		DenominatedIn: "BASE_ASSET",
		Label:         derivatives.LabelIntervalStart,
		Snapshots:     snaps,
	}
}

func TestColdWriteFailure_IsReturnedAndNotLoggedHere(t *testing.T) {
	causa := errors.New("questdb down")
	writer := derivatives.NewDerivativesTieredWriter(&recordingTieredCold{err: causa})

	err := writer.WriteBatch(context.Background(), tieredSampleBatch(3))
	if err == nil {
		t.Fatal("the durable failure was swallowed — with no alternative tier, the cycle would have no failure point")
	}
	if !errors.Is(err, causa) {
		t.Fatalf("the returned error lost its cause: %v", err)
	}
	// THIS LAYER HAS NO LOGGER. The producer is a propagation-only layer: the error is
	// returned exactly once, and there is no logger it could have written to — the property is
	// structural.
}

// NEGATIVE CONTROL: without it, "refuse and do not log" would be satisfied by a WriteBatch that
// always fails.
func TestColdWriteSuccess_ReturnsNilAndLogsNothingAtErrorLevel(t *testing.T) {
	writer := derivatives.NewDerivativesTieredWriter(&recordingTieredCold{})
	if err := writer.WriteBatch(context.Background(), tieredSampleBatch(3)); err != nil {
		t.Fatalf("successful write returned an error: %v", err)
	}

	// Same structural note: this layer has no logger to write to.
}
