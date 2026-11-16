package distribution

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// `hasGap` alone tells the consumer that a bar is unproven, not WHETHER ITS PRICES ARE. A
// volume-only shortfall leaves OHLC exact and may be folded into a parent; a provisional value may
// be wrong in every field and must block. Without the cause on the wire the consumer can only
// choose between trusting everything and trusting nothing.
func TestCandleDTOCarriesTheGapCause(t *testing.T) {
	t.Parallel()

	base := candle.Candle{Timestamp: 1_790_000_000_000, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 3}

	cases := []struct {
		name    string
		kind    candle.GapKind
		marked  bool
		want    string
		back    candle.GapKind
		omitted bool
	}{
		{"clean bucket omits the field", candle.GapUnknown, false, "", candle.GapUnknown, true},
		{"volume-only shortfall", candle.GapVolumeIncomplete, true, "volume_incomplete", candle.GapVolumeIncomplete, false},
		{"provisional value", candle.GapProvisional, true, "provisional", candle.GapProvisional, false},
		{"unrecorded cause stays honest", candle.GapUnknown, true, "unknown", candle.GapUnknown, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := base
			c.HasGap, c.GapKind = tc.marked, tc.kind

			dto := NewCandleDTO(c)
			if dto.GapCause != tc.want {
				t.Fatalf("GapCause = %q, want %q", dto.GapCause, tc.want)
			}
			raw, err := json.Marshal(dto)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			hasField := strings.Contains(string(raw), `"gapCause"`)
			if hasField == tc.omitted {
				t.Fatalf("gapCause present = %v, want %v (payload %s)", hasField, !tc.omitted, raw)
			}
			if got := dto.ToCandle().GapKind; got != tc.back {
				t.Fatalf("round-trip GapKind = %v, want %v", got, tc.back)
			}
		})
	}
}
