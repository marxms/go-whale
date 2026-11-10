package candle

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
)

// TimeframeRegistry holds the canonical set of supported timeframes and
// provides safe, immutable access. It is the single source of truth for
// timeframe configuration within the GoWhale process.
type TimeframeRegistry struct {
	timeframes []string
}

// NewTimeframeRegistry creates a registry from the given timeframes.
// The input is defensively copied and each entry is trimmed.
func NewTimeframeRegistry(timeframes []string) *TimeframeRegistry {
	tfs := make([]string, len(timeframes))
	for i, tf := range timeframes {
		tfs[i] = strings.TrimSpace(tf)
	}
	return &TimeframeRegistry{timeframes: tfs}
}

// All returns a defensive copy of the internal timeframe slice.
// Mutating the returned slice does not affect the registry.
func (r *TimeframeRegistry) All() []string {
	out := make([]string, len(r.timeframes))
	copy(out, r.timeframes)
	return out
}

// Default returns the first configured timeframe, or "1h" if none are set.
func (r *TimeframeRegistry) Default() string {
	if len(r.timeframes) > 0 {
		return r.timeframes[0]
	}
	return "1h"
}

// IsSupported reports whether the given timeframe exists in the registry.
// Comparison is case-insensitive and whitespace-trimmed.
func (r *TimeframeRegistry) IsSupported(tf string) bool {
	normalized := strings.TrimSpace(strings.ToLower(tf))
	for _, t := range r.timeframes {
		if strings.TrimSpace(strings.ToLower(t)) == normalized {
			return true
		}
	}
	return false
}

// Checksum returns a SHA-256 hex digest of the timeframes.
// Timeframes are trimmed, lowercased, sorted alphabetically, and then
// joined with commas before hashing.  The result is idempotent for the
// same set of timeframes regardless of input order.
func (r *TimeframeRegistry) Checksum() string {
	tfs := make([]string, len(r.timeframes))
	for i, tf := range r.timeframes {
		tfs[i] = strings.ToLower(strings.TrimSpace(tf))
	}
	sort.Strings(tfs)
	combined := strings.Join(tfs, ",")
	hash := sha256.Sum256([]byte(combined))
	return fmt.Sprintf("%x", hash)
}
