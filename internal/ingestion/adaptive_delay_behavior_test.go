package ingestion

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/trade"
)

type fillSource struct{ fill float64 }

func (f *fillSource) Subscribe(context.Context, string) error                  { return nil }
func (f *fillSource) Unsubscribe(string) error                                 { return nil }
func (f *fillSource) GetTrades(context.Context, string) ([]trade.Trade, error) { return nil, nil }
func (f *fillSource) HealthCheck() bool                                        { return true }
func (f *fillSource) Type() streaming.ClientType                               { return streaming.ClientTypeNative }
func (f *fillSource) Close() error                                             { return nil }
func (f *fillSource) GetBufferFillRatio(string) float64                        { return f.fill }

type plainSource struct{ fillSource }

func managerWithFills(fills map[string]float64) *HybridManager {
	m := NewHybridManager(nil, 1, nil)
	for sym, fill := range fills {
		m.clients[sym] = &HybridClientState{Client: &fillSource{fill: fill}, Symbol: sym}
	}
	return m
}

// The production defect was AVERAGING the fleet's fill: one hot symbol at 100% behind hundreds
// of idle ones read as "market idle" and kept the 200 ms delay. The decision must use the
// FULLEST buffer. This drives the real manager, not a source-string guard.
func TestAdaptiveDelayUsesTheFullestSymbol(t *testing.T) {
	fills := map[string]float64{"HOT": 1.0}
	for _, sym := range []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"} {
		fills[sym] = 0
	}
	if got := managerWithFills(fills).calculateAdaptiveDelay(); got != defaultMinProcessDelay {
		t.Fatalf("one hot symbol among idle ones = %v, want %v (the average would read idle)",
			got, defaultMinProcessDelay)
	}
}

func TestAdaptiveDelayEmptyFleetIsConservative(t *testing.T) {
	if got := managerWithFills(nil).calculateAdaptiveDelay(); got != defaultMaxProcessDelay {
		t.Fatalf("empty fleet = %v, want %v", got, defaultMaxProcessDelay)
	}
	m := NewHybridManager(nil, 1, nil)
	m.clients["plain"] = &HybridClientState{Client: &plainSource{}, Symbol: "plain"}
	if got := m.calculateAdaptiveDelay(); got != defaultMaxProcessDelay {
		t.Fatalf("fleet without fill capability = %v, want %v", got, defaultMaxProcessDelay)
	}
}

// The threshold boundaries and the INTERPOLATION DIRECTION are part of the contract: a hotter
// buffer must poll sooner. The old test used only 0.5, the symmetric point, where an inverted
// formula returns the same value.
func TestProcessDelayForFillBoundaries(t *testing.T) {
	minD, maxD := 50*time.Millisecond, 200*time.Millisecond
	cases := []struct {
		fill float64
		want time.Duration
	}{
		{0.0, maxD},
		{0.299, maxD},
		{0.3, maxD},
		{0.4, 162500 * time.Microsecond},
		{0.6, 87500 * time.Microsecond},
		{0.699, 50375 * time.Microsecond},
		{0.7, minD},
		{1.0, minD},
	}
	for _, c := range cases {
		got := processDelayForFill(c.fill, minD, maxD)
		// ±1 µs absorbs the nanosecond rounding of the float interpolation; the direction and
		// the thresholds are still discriminated (an inverted curve misses by tens of ms).
		if diff := got - c.want; diff < -time.Microsecond || diff > time.Microsecond {
			t.Fatalf("fill %.3f → %v, want %v", c.fill, got, c.want)
		}
	}
}
