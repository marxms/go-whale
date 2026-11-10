package recovery

import (
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// TestSplitHistoricalAndCurrent verifies /: the open in-progress bucket
// is honestly derived from the fetched klines. Binance returns the open bucket
// as the most recent kline (its open-time == the current bucket start) when the
// request's endTime reaches "now". That kline is NOT closed, so it must be moved
// from Historical into Current so the manager seeds the latest timestamp and the
// symbol's persisted freshness advances (clearing staleness, stopping re-enqueue).
func TestSplitHistoricalAndCurrent(t *testing.T) {
	const bucket = 1_700_000_000_000

	tests := []struct {
		name          string
		in            []candle.Candle
		currentBucket int64
		wantHistTs    []int64
		wantCurrentTs int64 // -1 means expect nil Current
	}{
		{
			name:          "empty input yields nothing",
			in:            nil,
			currentBucket: bucket,
			wantHistTs:    nil,
			wantCurrentTs: -1,
		},
		{
			name: "last kline is the open bucket — promoted to Current",
			in: []candle.Candle{
				{Timestamp: bucket - 120_000, Close: 1},
				{Timestamp: bucket - 60_000, Close: 2},
				{Timestamp: bucket, Close: 3},
			},
			currentBucket: bucket,
			wantHistTs:    []int64{bucket - 120_000, bucket - 60_000},
			wantCurrentTs: bucket,
		},
		{
			name: "all candles closed (none at current bucket) — Current nil",
			in: []candle.Candle{
				{Timestamp: bucket - 120_000, Close: 1},
				{Timestamp: bucket - 60_000, Close: 2},
			},
			currentBucket: bucket,
			wantHistTs:    []int64{bucket - 120_000, bucket - 60_000},
			wantCurrentTs: -1,
		},
		{
			name: "only the open bucket fetched — no historical",
			in: []candle.Candle{
				{Timestamp: bucket, Close: 3},
			},
			currentBucket: bucket,
			wantHistTs:    nil,
			wantCurrentTs: bucket,
		},
		{
			name: "currentBucket unknown (0) — never promote, all historical",
			in: []candle.Candle{
				{Timestamp: bucket - 60_000, Close: 2},
				{Timestamp: bucket, Close: 3},
			},
			currentBucket: 0,
			wantHistTs:    []int64{bucket - 60_000, bucket},
			wantCurrentTs: -1,
		},
		{
			name: "candle strictly beyond current bucket is never promoted (defensive)",
			in: []candle.Candle{
				{Timestamp: bucket - 60_000, Close: 2},
				{Timestamp: bucket + 60_000, Close: 4},
			},
			currentBucket: bucket,
			wantHistTs:    []int64{bucket - 60_000, bucket + 60_000},
			wantCurrentTs: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hist, current := splitHistoricalAndCurrent(tt.in, tt.currentBucket)

			gotHistTs := make([]int64, 0, len(hist))
			for _, c := range hist {
				gotHistTs = append(gotHistTs, c.Timestamp)
			}
			if len(gotHistTs) != len(tt.wantHistTs) {
				t.Fatalf("historical len = %d (%v), want %d (%v)", len(gotHistTs), gotHistTs, len(tt.wantHistTs), tt.wantHistTs)
			}
			for i := range gotHistTs {
				if gotHistTs[i] != tt.wantHistTs[i] {
					t.Errorf("historical[%d] ts = %d, want %d", i, gotHistTs[i], tt.wantHistTs[i])
				}
			}

			if tt.wantCurrentTs == -1 {
				if current != nil {
					t.Errorf("expected nil Current, got ts=%d", current.Timestamp)
				}
				return
			}
			if current == nil {
				t.Fatalf("expected Current ts=%d, got nil", tt.wantCurrentTs)
			}
			if current.Timestamp != tt.wantCurrentTs {
				t.Errorf("Current ts = %d, want %d", current.Timestamp, tt.wantCurrentTs)
			}
		})
	}
}
