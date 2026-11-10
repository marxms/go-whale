package recovery

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// valueRecordingWriter keeps the candles the manager actually hands to the store, so a
// test can assert the values that crossed the write boundary instead of only a count.
type valueRecordingWriter struct {
	batch   []candle.Candle
	current []candle.Candle
}

func (w *valueRecordingWriter) SeedCandlesBatch(_ context.Context, _, _, _, _ string, cs []candle.Candle) error {
	w.batch = append(w.batch, cs...)
	return nil
}

func (w *valueRecordingWriter) SeedCurrentBucketCandle(_ context.Context, _, _, _, _ string, c candle.Candle) error {
	w.current = append(w.current, c)
	return nil
}

func (w *valueRecordingWriter) GetCandlesByScoreRange(context.Context, string, string, string, string, int64, int64) ([]candle.Candle, error) {
	return nil, nil
}

func (w *valueRecordingWriter) GetCandlesCount(context.Context, string, string, string, string) (int64, error) {
	return 0, nil
}

// TestPrepareCandlesNormalizes pins SD-01/SD-02: a venue row whose high sits below its own
// open/close, or whose low sits above them, is repaired before it can reach the store.
func TestPrepareCandlesNormalizes(t *testing.T) {
	tests := []struct {
		name     string
		candle   candle.Candle
		wantHigh float64
		wantLow  float64
	}{
		{
			name:     "high below open and close",
			candle:   candle.Candle{Timestamp: 1, Open: 110, High: 100, Low: 90, Close: 105, Volume: 1},
			wantHigh: 110,
			wantLow:  90,
		},
		{
			name:     "low above open and close",
			candle:   candle.Candle{Timestamp: 1, Open: 100, High: 110, Low: 108, Close: 105, Volume: 1},
			wantHigh: 110,
			wantLow:  100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prepared, rejected := prepareCandles(DefaultCandlesValidator{}, []candle.Candle{tt.candle}, "BCH/USDT", "1w")
			if len(rejected) != 0 {
				t.Fatalf("normalizable candle rejected: %+v", rejected)
			}
			if len(prepared) != 1 {
				t.Fatalf("prepared %d candles, want 1", len(prepared))
			}
			if prepared[0].High != tt.wantHigh {
				t.Errorf("high = %v, want %v", prepared[0].High, tt.wantHigh)
			}
			if prepared[0].Low != tt.wantLow {
				t.Errorf("low = %v, want %v", prepared[0].Low, tt.wantLow)
			}
		})
	}
}

// TestPrepareCandlesRejectsImpossible pins SD-03: a candle that is still impossible after
// normalization is dropped with the validator's reason, never silently repaired past the
// point the data can support.
func TestPrepareCandlesRejectsImpossible(t *testing.T) {
	tests := []struct {
		name       string
		candle     candle.Candle
		wantReason string
	}{
		{
			name:       "non-positive open",
			candle:     candle.Candle{Timestamp: 1, Open: 0, High: 10, Low: 5, Close: 10, Volume: 1},
			wantReason: "open",
		},
		{
			name:       "non-positive timestamp",
			candle:     candle.Candle{Timestamp: 0, Open: 10, High: 11, Low: 9, Close: 10, Volume: 1},
			wantReason: "timestamp",
		},
		{
			name:       "negative volume",
			candle:     candle.Candle{Timestamp: 1, Open: 10, High: 11, Low: 9, Close: 10, Volume: -1},
			wantReason: "volume",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prepared, rejected := prepareCandles(DefaultCandlesValidator{}, []candle.Candle{tt.candle}, "BTC/USDT", "5m")
			if len(prepared) != 0 {
				t.Fatalf("impossible candle kept: %+v", prepared)
			}
			if len(rejected) != 1 {
				t.Fatalf("rejected %d candles, want 1", len(rejected))
			}
			if !strings.Contains(rejected[0].Reason, tt.wantReason) {
				t.Errorf("reason = %q, want it to name %q", rejected[0].Reason, tt.wantReason)
			}
		})
	}
}

// TestSeedBCHWeeklyImpossibleOHLC pins SD-04 with the real row fapi returned for BCHUSDT
// 1w 2020-01-13: open 339.65, high 215.69, low 182.65, close 339.62. The high sits below
// both the open and the close. The seed must persist the repaired line instead of dropping
// the bucket, and must leave the venue's open/close untouched.
func TestSeedBCHWeeklyImpossibleOHLC(t *testing.T) {
	var got []candle.Candle
	store := &provenanceStore{onHistorical: func(cs []candle.Candle) { got = cs }}
	line := candle.Candle{
		Timestamp: 1578873600000, // 2020-01-13T00:00:00Z
		Open:      339.65,
		High:      215.69,
		Low:       182.65,
		Close:     339.62,
		Volume:    975831.371,
	}

	if err := applySeedWindow(context.Background(), Metrics{}, store, "binance", "future", "BCH/USDT", "1w",
		&seedWindow{historical: []candle.Candle{line}}); err != nil {
		t.Fatalf("applySeedWindow: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("seeded %d candles, want 1", len(got))
	}
	if got[0].High != 339.65 {
		t.Errorf("persisted high = %v, want 339.65 (max of open and close)", got[0].High)
	}
	if got[0].Low != 182.65 {
		t.Errorf("persisted low = %v, want 182.65", got[0].Low)
	}
	if got[0].Open != 339.65 || got[0].Close != 339.62 {
		t.Errorf("venue prices changed: open=%v close=%v", got[0].Open, got[0].Close)
	}
}

// TestRejectedCandleReasonLabeled pins SD-06 on the recovery path: the metric carries the
// validator's actual reason, not a generic label, and the rejected candle never reaches the
// store.
func TestRejectedCandleReasonLabeled(t *testing.T) {
	writer := newRecordingWriter()
	invalid := candle.Candle{
		Timestamp: candle.BucketTimestamp(time.Now().Add(-5*time.Minute).UnixMilli(), "1m"),
		Open:      0, High: 1, Low: 0.5, Close: 1, Volume: 1,
	}
	normalized := candle.NormalizeOHLC(invalid)
	_, wantReason := candle.ValidateCandle(normalized.Timestamp, normalized.Open, normalized.High,
		normalized.Low, normalized.Close, normalized.Volume, "BTC/USDT", "1m")

	m := newTestManager(fakeFiller{historical: []candle.Candle{invalid}}, writer, nil)
	var reasons []string
	m.SetMetrics(Metrics{RejectedCandles: func(exchange, marketType, timeframe, reason string) {
		if exchange != "binance" || marketType != "future" || timeframe != "1m" {
			t.Errorf("rejection labels = %s/%s/%s, want binance/future/1m", exchange, marketType, timeframe)
		}
		reasons = append(reasons, reason)
	}})

	m.process(context.Background(), 0, RecoveryJob{
		ExchangeID:     "binance",
		MarketType:     "future",
		Symbol:         "BTC/USDT",
		DisconnectTime: time.Now().Add(-10 * time.Minute),
		ReconnectTime:  time.Now(),
	})

	if len(reasons) != 1 {
		t.Fatalf("rejected-candle metric = %v, want one reason", reasons)
	}
	if reasons[0] != wantReason {
		t.Errorf("metric reason = %q, want the validator reason %q", reasons[0], wantReason)
	}
	if writer.seededBatch != 0 || len(writer.seededCur) != 0 {
		t.Fatalf("rejected candle reached the store: historical=%d current=%d",
			writer.seededBatch, len(writer.seededCur))
	}
}

// TestPrepareCandlesUsedByRecovery pins SD-05 on the gap-recovery side: the manager routes
// fetched candles through the same gate the seed uses, so an impossible high is normalized
// before the store sees it.
func TestPrepareCandlesUsedByRecovery(t *testing.T) {
	writer := &valueRecordingWriter{}
	impossible := candle.Candle{
		Timestamp: candle.BucketTimestamp(time.Now().Add(-5*time.Minute).UnixMilli(), "1m"),
		Open:      100,
		High:      90,
		Low:       80,
		Close:     105,
		Volume:    1,
	}
	m := newTestManager(fakeFiller{historical: []candle.Candle{impossible}}, writer, nil)

	m.process(context.Background(), 0, RecoveryJob{
		ExchangeID:     "binance",
		MarketType:     "future",
		Symbol:         "BTC/USDT",
		DisconnectTime: time.Now().Add(-10 * time.Minute),
		ReconnectTime:  time.Now(),
	})

	if len(writer.batch) != 1 {
		t.Fatalf("recovery persisted %d candles, want 1", len(writer.batch))
	}
	if writer.batch[0].High != 105 {
		t.Errorf("persisted high = %v, want 105 (max of open and close)", writer.batch[0].High)
	}
	if writer.batch[0].Low != 80 {
		t.Errorf("persisted low = %v, want 80", writer.batch[0].Low)
	}
}
