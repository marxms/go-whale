package coinalyze

import "time"

// CoinalyzeCatalogReloadInterval is how often the market table is refreshed.
//
// The table has ~1 MB and 4,958 rows; reloading it every six hours is expensive
// enough not to do it per symbol and fresh enough that a new listing appears the same
// day. It is a vendor-table property, so it lives with the adapter that reads it.
const CoinalyzeCatalogReloadInterval = 6 * time.Hour
