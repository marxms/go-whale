package retention

import "time"

// Policy construction and derived horizons.
//
// The composition root converts configuration into typed TimeframeReach values and
// calls these builders. The decisions — which table gets a policy, how deep the
// deliberate horizon is, and which cells a sole-copy guard must cover — stay here.

// TimeframeReach is one timeframe's resident reach and whether it could be measured.
type TimeframeReach struct {
	Timeframe string
	Reach     time.Duration
	// Measurable false means the timeframe duration could not be derived. It is
	// included by the guard rather than silently treated as shallow.
	Measurable bool
}

// CandlesDeliberateHorizon is the deepest resident reach across the configured
// timeframes. Zero means no timeframe could be measured, and an invented horizon
// would be retention disabled under another name.
func CandlesDeliberateHorizon(reaches []TimeframeReach) time.Duration {
	var deepest time.Duration
	for _, r := range reaches {
		if !r.Measurable {
			continue
		}
		if r.Reach > deepest {
			deepest = r.Reach
		}
	}
	return deepest
}

// SoleCopyGuardTimeframes returns the cells whose hot ZSET reaches FURTHER BACK than
// the durable window. A cell is guarded when the measurement is missing or the
// durable window is undeclared: absence of measurement is not approval.
func SoleCopyGuardTimeframes(reaches []TimeframeReach, durableWindow time.Duration) []string {
	var out []string
	for _, r := range reaches {
		if durableWindow <= 0 || !r.Measurable || r.Reach > durableWindow {
			out = append(out, r.Timeframe)
		}
	}
	return out
}

// CandlesPolicy is the single declaration of the `candles` time boundary.
func CandlesPolicy(table string, retentionDays int, deliberateHorizon time.Duration) (Policy, bool) {
	if table == "" || retentionDays <= 0 {
		return Policy{}, false
	}
	p := Policy{
		Table:         table,
		RetentionDays: retentionDays,
	}
	if deliberateHorizon > 0 {
		p.DeliberateHorizon = deliberateHorizon
		p.HorizonReason = "the hot ZSET reaches that far and the sole-copy migration copies exactly that reach to the durable tier"
	}
	return p, true
}

// DerivativesRetentionDays resolves the `derivatives` window: declared wins over
// derived, and derived wins over a literal.
func DerivativesRetentionDays(declaredDays int, seedHorizon time.Duration) int {
	if declaredDays > 0 {
		return declaredDays
	}
	return int(seedHorizon.Hours() / 24)
}

// DerivativesPolicy declares the `derivatives` retention. The deliberate horizon is
// the deep-seed reach and survives a declared window: lowering the window must not
// let the cut reach the seed.
func DerivativesPolicy(table string, declaredDays int, seedHorizon time.Duration) (Policy, bool) {
	days := DerivativesRetentionDays(declaredDays, seedHorizon)
	if days <= 0 {
		return Policy{}, false
	}
	return Policy{
		Table:             table,
		RetentionDays:     days,
		DeliberateHorizon: seedHorizon,
		HorizonReason:     "the second-provenance seed reaches ~2,000 buckets of the deepest timeframe, on purpose",
	}, true
}

// RetainedWindow is the depth retention actually leaves in a table. It mirrors
// Policy.Cutoff: one day of slack over the declared window, and the deliberate
// horizon pulling the cut back when it is deeper.
func RetainedWindow(retentionDays int, deliberateHorizon time.Duration) time.Duration {
	var window time.Duration
	if retentionDays > 0 {
		window = time.Duration(retentionDays+1) * 24 * time.Hour
	}
	if deliberateHorizon > window {
		return deliberateHorizon
	}
	return window
}
