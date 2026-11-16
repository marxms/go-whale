package candle

import "math"

// SanityViolation names a way a folded bucket contradicts the children it was built from.
type SanityViolation string

const (
	// ViolationOHLCOrder means the bucket's own prices are ordered impossibly.
	ViolationOHLCOrder SanityViolation = "ohlc_order"
	// ViolationHighBelowChildren means the bucket's high is below a child's high.
	ViolationHighBelowChildren SanityViolation = "high_below_children"
	// ViolationLowAboveChildren means the bucket's low is above a child's low.
	ViolationLowAboveChildren SanityViolation = "low_above_children"
	// ViolationVolumeMismatch means a bucket declaring itself COMPLETE does not carry the sum of
	// its children's volume.
	ViolationVolumeMismatch SanityViolation = "volume_mismatch"
)

// volumeTolerance is relative: parent and children are float64 sums of the same values, so a few
// ulps is arithmetic, not a defect.
const volumeTolerance = 1e-9

// ValidateFoldedParent reports every way `parent` contradicts `kids`.
//
// Only children with volume are compared: an untraded child carries the previous close on every
// price field, so including it would invent violations.
//
// A parent that contradicts its children must be caught here: nothing else validates a published
// bar against the children it was built from, so an impossible candle would reach a decision with
// every counter healthy.
func ValidateFoldedParent(parent Candle, kids []Candle) []SanityViolation {
	seen := make(map[SanityViolation]struct{}, 4)
	out := make([]SanityViolation, 0, 4)
	add := func(v SanityViolation) {
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}

	positive := func(v float64) bool { return v > 0 }
	if positive(parent.High) && positive(parent.Low) && parent.High < parent.Low {
		add(ViolationOHLCOrder)
	}
	if positive(parent.High) && positive(parent.Open) && parent.Open > parent.High {
		add(ViolationOHLCOrder)
	}
	if positive(parent.High) && positive(parent.Close) && parent.Close > parent.High {
		add(ViolationOHLCOrder)
	}
	if positive(parent.Low) && positive(parent.Open) && parent.Open < parent.Low {
		add(ViolationOHLCOrder)
	}
	if positive(parent.Low) && positive(parent.Close) && parent.Close < parent.Low {
		add(ViolationOHLCOrder)
	}

	var (
		traded  int
		maxHigh float64
		minLow  float64
		haveLow bool
		sumVol  float64
	)
	for _, k := range kids {
		if k.Volume <= 0 {
			continue
		}
		traded++
		if k.High > maxHigh {
			maxHigh = k.High
		}
		if k.Low > 0 && (!haveLow || k.Low < minLow) {
			minLow, haveLow = k.Low, true
		}
		sumVol += k.Volume
	}
	if traded > 0 {
		if positive(parent.High) && positive(maxHigh) && parent.High < maxHigh {
			add(ViolationHighBelowChildren)
		}
		if positive(parent.Low) && haveLow && parent.Low > minLow {
			add(ViolationLowAboveChildren)
		}
		// A bucket that declares itself COMPLETE must be the sum of its parts. A marked bucket is
		// exempt: its whole reason to exist is that a part is missing.
		if !parent.HasGap && !nearlyEqualVolume(parent.Volume, sumVol) {
			add(ViolationVolumeMismatch)
		}
	}
	return out
}

func nearlyEqualVolume(a, b float64) bool {
	if a == b {
		return true
	}
	scale := math.Max(math.Abs(a), math.Abs(b))
	return math.Abs(a-b) <= volumeTolerance*scale
}
