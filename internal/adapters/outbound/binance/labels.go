package binancesdk

// Venue and market labels carried by metrics, events, logs and client configuration. Both
// Binance clients emit the same venue identity, so these literals must not drift between
// files.
const (
	venueBinance     = "binance"
	marketTypeFuture = "future"
	marketTypeSpot   = "spot"
)
