package recovery

import (
	"errors"
	"testing"
)

// TestTransientFetchErr_SeparatesRefusalFromVerdict is the guard for THE FLEET WIPE.
//
// # THE DEFECT
//
// Callers use SeedResult.AllFailed to ROLL BACK a subscription: "every timeframe failed,
// so this symbol is unusable — unsubscribe it". That inference only holds when the failure
// is ABOUT THE SYMBOL. A rate limit is about US, and it fails EVERY symbol identically, so
// rolling back on it deletes the whole fleet.
//
// Measured twice:
//   - the 500-symbol deploy took 4,602 HTTP 429s during its boot burst and 117 valid
//     symbols were unsubscribed (500 -> 383);
//   - after the power outage, with the IP still penalized, the same path cut the fleet
//     to 34.
//
// In both cases the symbols were fine — Binance was refusing us for a few minutes, and the
// penalty decays on its own. Keeping the subscription costs a retry; rolling it back throws
// away live data.
func TestTransientFetchErr_SeparatesRefusalFromVerdict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		err       error
		transient bool
	}{
		// The exchange refusing US — must never justify a rollback.
		{
			name:      "vendor-wrapped 429 with code -1003",
			err:       errors.New(`upstream: 429 Too Many Requests {"code":-1003,"msg":"Too many requests; current limit of IP"}`),
			transient: true,
		},
		{
			name:      "escalated IP ban is 418, not 429",
			err:       errors.New("binance 418 I'm a teapot: IP banned until 1785349452617"),
			transient: true,
		},
		{
			name:      "request timeout says nothing about the symbol",
			err:       errors.New("context deadline exceeded: request timeout"),
			transient: true,
		},
		{
			name:      "connection reset is a network condition, not a verdict",
			err:       errors.New("read tcp 198.51.100.5:443: connection reset by peer"),
			transient: true,
		},

		// Genuinely about the symbol — rollback stays available.
		{
			name:      "invalid symbol is a real verdict",
			err:       errors.New(`binance {"code":-1121,"msg":"Invalid symbol."}`),
			transient: false,
		},
		{
			name:      "unparseable timeframe is our own bug, not a refusal",
			err:       errors.New("invalid timeframe: 3x"),
			transient: false,
		},
		{
			name:      "no error at all",
			err:       nil,
			transient: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isTransientFetchErr(tt.err); got != tt.transient {
				verdict := "a verdict about the symbol"
				if tt.transient {
					verdict = "the exchange refusing us"
				}
				t.Errorf("isTransientFetchErr(%v) = %v, want %v — this error is %s, and getting it "+
					"wrong either wipes a valid fleet or keeps a genuinely dead symbol subscribed",
					tt.err, got, tt.transient, verdict)
			}
		})
	}
}
