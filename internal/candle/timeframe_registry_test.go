package candle

import (
	"encoding/hex"
	"testing"
)

func TestTimeframeRegistry_All_ReturnsCopy(t *testing.T) {
	reg := NewTimeframeRegistry([]string{"1h", "30m", "15m"})
	all := reg.All()

	if len(all) != 3 {
		t.Fatalf("expected 3 timeframes, got %d", len(all))
	}

	// Mutation of returned slice must not affect internal state
	all[0] = "tampered"
	second := reg.All()
	for _, tf := range second {
		if tf == "tampered" {
			t.Error("mutation of returned slice leaked into registry")
		}
	}
}

func TestTimeframeRegistry_IsSupported(t *testing.T) {
	reg := NewTimeframeRegistry([]string{"5m", "15m", "1h"})

	tests := []struct {
		tf    string
		want  bool
		label string
	}{
		{"5m", true, "exact match"},
		{"15M", true, "case-insensitive"},
		{" 1h ", true, "whitespace-trimmed"},
		{"2h", false, "unsupported timeframe"},
		{"", false, "empty string"},
		{"  ", false, "whitespace-only"},
	}

	for _, tc := range tests {
		t.Run(tc.label, func(t *testing.T) {
			got := reg.IsSupported(tc.tf)
			if got != tc.want {
				t.Errorf("IsSupported(%q) = %v, want %v", tc.tf, got, tc.want)
			}
		})
	}
}

func TestTimeframeRegistry_Checksum_IsIdempotent(t *testing.T) {
	reg := NewTimeframeRegistry([]string{"1h", "30m", "15m"})
	cs1 := reg.Checksum()
	cs2 := reg.Checksum()

	if cs1 != cs2 {
		t.Errorf("checksum not idempotent: %q vs %q", cs1, cs2)
	}

	// Verify hex format
	if _, err := hex.DecodeString(cs1); err != nil {
		t.Errorf("checksum is not valid hex: %v", err)
	}
}

func TestTimeframeRegistry_Checksum_SameOrder(t *testing.T) {
	// Reordering the same set must yield the same checksum
	a := NewTimeframeRegistry([]string{"1h", "30m", "15m"})
	b := NewTimeframeRegistry([]string{"15m", "1h", "30m"})

	if a.Checksum() != b.Checksum() {
		t.Errorf("same timeframes in different order should have identical checksums: %q vs %q",
			a.Checksum(), b.Checksum())
	}
}

func TestTimeframeRegistry_Checksum_DifferentSet(t *testing.T) {
	a := NewTimeframeRegistry([]string{"1h", "30m"})
	b := NewTimeframeRegistry([]string{"1h", "15m"})

	if a.Checksum() == b.Checksum() {
		t.Error("different timeframe sets must not share a checksum")
	}
}

func TestTimeframeRegistry_Default(t *testing.T) {
	reg := NewTimeframeRegistry([]string{"15m", "1h"})
	if got := reg.Default(); got != "15m" {
		t.Errorf("Default() = %q, want %q", got, "15m")
	}
}

func TestTimeframeRegistry_Default_EmptyRegistry(t *testing.T) {
	reg := NewTimeframeRegistry([]string{})
	if got := reg.Default(); got != "1h" {
		t.Errorf("Default() = %q, want %q", got, "1h")
	}
}

func TestTimeframeRegistry_All_PreservesInputOrder(t *testing.T) {
	input := []string{"30m", "1h", "15m"}
	reg := NewTimeframeRegistry(input)
	all := reg.All()

	for i, tf := range input {
		if all[i] != tf {
			t.Fatalf("index %d: got %q, want %q", i, all[i], tf)
		}
	}
}

func TestTimeframeRegistry_Checksum_NormalizesCase(t *testing.T) {
	lower := NewTimeframeRegistry([]string{"1h", "30m"})
	mixed := NewTimeframeRegistry([]string{"1H", "30M"})

	if lower.Checksum() != mixed.Checksum() {
		t.Errorf("mixed case should produce same checksum: %q vs %q",
			lower.Checksum(), mixed.Checksum())
	}
}
