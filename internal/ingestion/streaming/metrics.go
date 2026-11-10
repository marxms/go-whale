package streaming

// Metrics is the exchange-agnostic streaming observability port. Every field is a function
// so the package never imports Prometheus and a zero value is a usable no-op. These live in
// the shared streaming package (rather than a per-exchange subpackage like binancesdk)
// because they describe liveness and startup health for every exchange scope and are
// updated from cross-cutting call sites (the /health handler and the startup subscription
// loop).
type Metrics struct {
	// StreamLiveness reports whether a healthy, connected stream currently exists for an
	// exchange:market_type scope. 1 means at least one connection for the scope is both
	// Connected and Healthy; 0 means the scope is missing, disconnected, or unhealthy.
	StreamLiveness func(exchange, marketType string, live bool)

	// StreamSymbolsTotal / StreamSymbolsStale expose PER-SYMBOL fleet freshness for an
	// exchange:market_type scope: how many subscribed symbols exist and how many have a
	// stale most-recent PERSISTED candle on the fastest timeframe.
	StreamSymbolsTotal func(exchange, marketType string, n float64)
	StreamSymbolsStale func(exchange, marketType string, n float64)

	// StartupFailures counts fatal failures encountered while bringing an exchange scope
	// online during startup, labeled by the stage at which the failure occurred.
	StartupFailures func(exchange, marketType, stage string)

	// BootSeedDeliveryConfirmed / BootSeedDeliveryBlind split the boot seed pass by whether
	// the symbol's live stream was CONFIRMED delivering before its REST snapshot was taken.
	BootSeedDeliveryConfirmed func(exchange, marketType string, n float64)
	BootSeedDeliveryBlind     func(exchange, marketType string, n float64)
}

// SetLiveness records whether a scope is receiving frames.
func (m Metrics) SetLiveness(exchange, marketType string, live bool) {
	if m.StreamLiveness != nil {
		m.StreamLiveness(exchange, marketType, live)
	}
}

// SetSymbolsTotal records the number of symbols in a scope.
func (m Metrics) SetSymbolsTotal(exchange, marketType string, n float64) {
	if m.StreamSymbolsTotal != nil {
		m.StreamSymbolsTotal(exchange, marketType, n)
	}
}

// SetSymbolsStale records how many symbols are stale in a scope.
func (m Metrics) SetSymbolsStale(exchange, marketType string, n float64) {
	if m.StreamSymbolsStale != nil {
		m.StreamSymbolsStale(exchange, marketType, n)
	}
}

// IncStartupFailure counts a startup failure at its stage.
func (m Metrics) IncStartupFailure(exchange, marketType, stage string) {
	if m.StartupFailures != nil {
		m.StartupFailures(exchange, marketType, stage)
	}
}

// SetBootSeedConfirmed records a boot seed whose stream was confirmed delivering first.
func (m Metrics) SetBootSeedConfirmed(exchange, marketType string, n float64) {
	if m.BootSeedDeliveryConfirmed != nil {
		m.BootSeedDeliveryConfirmed(exchange, marketType, n)
	}
}

// SetBootSeedBlind records a boot seed taken without delivery confirmation.
func (m Metrics) SetBootSeedBlind(exchange, marketType string, n float64) {
	if m.BootSeedDeliveryBlind != nil {
		m.BootSeedDeliveryBlind(exchange, marketType, n)
	}
}

// Startup failure stage labels for StartupFailures. Kept as named constants so call sites do
// not drift on the cardinality-bearing label value.
const (
	// StartupStageDiscovery marks a failure in symbol discovery.
	StartupStageDiscovery = "discovery"
	// StartupStageNoSymbols marks an exchange that resolved zero symbols.
	StartupStageNoSymbols = "no_symbols"
	// StartupStageRestClient marks a failure constructing the REST client.
	StartupStageRestClient = "rest_client"
	// StartupStageLoadMarkets marks a failure loading markets metadata.
	StartupStageLoadMarkets = "load_markets"
	// StartupStageSubscribe marks a failure subscribing a symbol's stream.
	StartupStageSubscribe = "subscribe"
	// StartupStageSeed marks a failure seeding historical candles for a symbol.
	StartupStageSeed = "seed"
	// StartupStageInit marks a failure initializing a backing dependency (e.g.
	// the QuestDB cold store) that degrades durability without stopping startup.
	StartupStageInit = "init"
)
