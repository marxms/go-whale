package collector

import (
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// DERIVATIVES DISK COST — AS A FUNCTION, NEVER AS A NUMBER.
//
//	rows(A)  = A × Σ_tf (retained window ÷ tf bucket)
//	bytes(A) = rows(A) × bytes per row
//
// The factors are measured or derived, never typed twice: the durable coverage comes
// from the intersection between candle_engine.timeframes and the declared support of
// the durable source; the retained window is derived by the SAME rule retention.Policy
// applies; each timeframe bucket comes from the strict candle parser; bytes per row is
// a declared measurement.

// DerivativesBytesPerRow is the cost of ONE `derivatives` row on disk.
//
// Measured on the LAN with `SELECT * FROM table_storage()`:
// diskSize = 120.240.651 over rowCount = 200.854 → 598,6 B/row, in 2.002 daily
// partitions. It is a CEILING PER ROW (the fixed partition cost is spread over fewer
// rows in a dense fleet); the composed number is the table's steady state, and that
// is what must fit the volume.
const DerivativesBytesPerRow = 598.6

// DerivativesBudgetAssetPoints are the points at which the requirement demands the
// function be instantiated. They are not a ceiling nor a fleet projection.
func DerivativesBudgetAssetPoints() []int {
	return []int{500, 600, 800}
}

// DerivativesSeriesRows is the cost of ONE timeframe, in buckets per series.
type DerivativesSeriesRows struct {
	Timeframe string
	// Buckets is how many instants the series carries IN STEADY STATE: the retained
	// window divided by the bucket. NOT how many the seed brings on its first pass —
	// the tail fills the rest and retention stops the accumulation.
	Buckets int64
}

// DerivativesDiskBudget is the function, with its factors visible.
type DerivativesDiskBudget struct {
	Assets      int
	Window      time.Duration
	Series      []DerivativesSeriesRows
	BytesPerRow float64
	// Unparsed names covered timeframes whose duration could not be derived. They
	// appear on the sheet instead of costing zero silently.
	Unparsed []string
}

// DeepestSeries is the timeframe that accumulates the most rows — always the FINEST,
// because the window is the same for all. It sizes the distance between seed and
// steady state.
func (b DerivativesDiskBudget) DeepestSeries() int64 {
	var largest int64
	for _, s := range b.Series {
		if s.Buckets > largest {
			largest = s.Buckets
		}
	}
	return largest
}

// BucketsPerAsset is the sum PER TIMEFRAME, and it is why the account stopped being
// wrong: one number for seven is only correct when all seven share a bucket.
func (b DerivativesDiskBudget) BucketsPerAsset() int64 {
	var total int64
	for _, s := range b.Series {
		total += s.Buckets
	}
	return total
}

// Rows is the steady-state row count for the fleet.
func (b DerivativesDiskBudget) Rows() int64 {
	return int64(b.Assets) * b.BucketsPerAsset()
}

// Bytes is the steady-state on-disk size.
func (b DerivativesDiskBudget) Bytes() int64 {
	return int64(float64(b.Rows()) * b.BytesPerRow)
}

// NewDerivativesDiskBudget instantiates the function for `assets` under the retained
// window.
func NewDerivativesDiskBudget(assets int, durableTimeframes []string, window time.Duration) DerivativesDiskBudget {
	b := DerivativesDiskBudget{Assets: assets, Window: window, BytesPerRow: DerivativesBytesPerRow}
	for _, timeframe := range durableTimeframes {
		ms, ok := candle.ParseTimeframeMs(timeframe)
		if !ok || ms <= 0 {
			// STRICT. ParseTimeframeToSeconds returns 1h for an unrecognized input, and a
			// fine timeframe budgeted as hourly disappears from the account by 12x.
			b.Unparsed = append(b.Unparsed, timeframe)
			continue
		}
		bucket := time.Duration(ms) * time.Millisecond
		b.Series = append(b.Series, DerivativesSeriesRows{
			Timeframe: timeframe,
			Buckets:   int64(window / bucket),
		})
	}
	return b
}
