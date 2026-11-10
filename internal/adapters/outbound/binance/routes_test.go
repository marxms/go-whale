package binancesdk

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/market"
)

func TestReadIdleFromPolicy(t *testing.T) {
	cases := []struct {
		exchange string
		mt       string
		want     time.Duration
	}{
		{"binance", marketTypeSpot, 60 * time.Second},
		{"binance", marketTypeFuture, 4 * time.Minute},
		{"bingx", marketTypeSpot, 0},
		{"bingx", marketTypeFuture, 0},
	}
	for _, tc := range cases {
		if got := readIdleFor(tc.exchange, tc.mt); got != tc.want {
			t.Errorf("readIdleFor(%s, %s) = %v, want %v", tc.exchange, tc.mt, got, tc.want)
		}
	}
}

// TestSpotDeadlineBelowPing guards the defect that made a quiet socket look
// dead: the read deadline must never be shorter than the venue's ping cadence.
func TestSpotDeadlineBelowPing(t *testing.T) {
	for _, marketType := range []string{marketTypeSpot, marketTypeFuture} {
		mt, err := market.ParseMarketType(marketType)
		if err != nil {
			t.Fatalf("ParseMarketType(%s): %v", marketType, err)
		}
		policy, ok := market.ConnPolicyFor(venueBinance, mt)
		if !ok {
			t.Fatalf("no policy for binance %s", marketType)
		}
		if policy.ReadIdle <= policy.ServerPing {
			t.Errorf("binance %s read idle %v must exceed the %v server ping", marketType, policy.ReadIdle, policy.ServerPing)
		}
	}
}
