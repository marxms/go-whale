package candle

import (
	"testing"
	"time"
)

// TestBootGapMarkTimeframesCoversEveryStreamedBucket: the boot must mark every
// timeframe in flight, not just the feed's.
func TestBootGapMarkTimeframesCoversEveryStreamedBucket(t *testing.T) {
	got := BootGapMarkTimeframes("5m", []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"})

	for _, tf := range []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"} {
		if !containsTF(got, tf) {
			t.Errorf("%s was left without the boot-gap mark", tf)
		}
	}
}

// TestBootGapMarkTimeframesAlwaysIncludesTheFeedInterval: the feed interval is in
// flight by construction even when the configuration does not list it.
func TestBootGapMarkTimeframesAlwaysIncludesTheFeedInterval(t *testing.T) {
	got := BootGapMarkTimeframes("5m", []string{"1h", "4h"})
	if !containsTF(got, "5m") {
		t.Fatalf("the feed interval was left out: %v", got)
	}
}

func TestBootGapMarkTimeframesDeduplicates(t *testing.T) {
	got := BootGapMarkTimeframes("5m", []string{"5m", "5m", "1h", "1h"})
	seen := map[string]int{}
	for _, tf := range got {
		seen[tf]++
	}
	for tf, n := range seen {
		if n != 1 {
			t.Errorf("%s appears %d times", tf, n)
		}
	}
}

func TestResidentReach(t *testing.T) {
	tests := []struct {
		name       string
		timeframe  string
		inherited  int
		declared   int
		want       time.Duration
		measurable bool
	}{
		{name: "8h of 1h with inherited depth", timeframe: "1h", inherited: 8, declared: 0, want: 8 * time.Hour, measurable: true},
		{name: "declared cap above inherited adds reach", timeframe: "4h", inherited: 8, declared: 100, want: 400 * time.Hour, measurable: true},
		{name: "declared cap below inherited cannot remove reach", timeframe: "4h", inherited: 800, declared: 1, want: 3200 * time.Hour, measurable: true},
		{name: "unparseable timeframe is not measurable", timeframe: "banana", inherited: 800, declared: 0, want: 0, measurable: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, measurable := ResidentReach(tt.timeframe, tt.inherited, tt.declared)
			if measurable != tt.measurable || got != tt.want {
				t.Fatalf("ResidentReach(%q, %d, %d) = (%v, %v), want (%v, %v)",
					tt.timeframe, tt.inherited, tt.declared, got, measurable, tt.want, tt.measurable)
			}
		})
	}
}

func containsTF(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}
