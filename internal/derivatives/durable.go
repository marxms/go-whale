package derivatives

import "time"

// DurableReadColumns is the list, IN ORDER, of columns the durable read requests.
//
// THE ORDER IS A CONTRACT with the adapter's positional `Scan`. It lives here — not inside the
// query — because the "readable" half of coverage is a property of this list: whoever decides
// whether a cell can be capped needs to know which columns the route actually requests.
func DurableReadColumns() []string {
	return []string{
		"timestamp",
		"open_interest_base",
		"funding_rate",
		"funding_rate_8h",
		"long_short_ratio",
		"funding_interval_hours",
		"n_step",
		"open_interest",
		"next_funding_rate",
		"interest_rate",
		"funding_at_anchor",
		"ls_long_pct",
		"ls_short_pct",
	}
}

// ErosionGuardedColumns names the columns that partial-rewrite protection carries forward.
//
// A column declared as durable coverage but absent here is a column dedup erases on the first
// degraded cycle. The startup gate compares the two sets.
func ErosionGuardedColumns() []string {
	return []string{
		"open_interest_base", "funding_rate", "funding_rate_8h", "long_short_ratio",
		"funding_interval_hours", "n_step",
		"open_interest", "next_funding_rate", "interest_rate", "funding_at_anchor",
		"ls_long_pct", "ls_short_pct",
	}
}

// DerivativesTimeframeSpan is the snapshot of ONE timeframe in the durable table.
type DerivativesTimeframeSpan struct {
	Rows            int64
	DistinctBuckets int64
	Oldest          time.Time
	Newest          time.Time
}
