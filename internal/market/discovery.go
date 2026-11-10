package market

// Discovery admission rules. They are pure because they decide whether a discovered
// symbol set may drive destructive reconciliation and how large a scope's declared
// fleet is; the composition root only converts configuration into their inputs and
// logs the outcome.

// MinDiscoveryFraction is the discovery-completeness safety floor: a discovered
// result is only trusted to DRIVE DELETIONS (orphan reconcile / derank prune) when
// it returns at least this fraction of the expected symbol count. A single
// FetchTickers HTTP call can return a degraded-but-200 response with NO error and a
// TRUNCATED set. The existing len(...) > 0 guards only catch the total-failure case;
// this fraction catches the partial one.
const MinDiscoveryFraction = 0.5

// DiscoveryHealthy reports whether a discovery result is complete enough to drive
// destructive reconciliation.
//
// maxAssets <= 0 is the static / explicit-symbol path: the count is exact and
// discovery did not bound it, so it is always trusted. Otherwise the result must
// reach MinDiscoveryFraction of the configured ceiling.
func DiscoveryHealthy(discoveredCount, maxAssets int) bool {
	if maxAssets <= 0 {
		return true
	}
	floor := int(float64(maxAssets) * MinDiscoveryFraction)
	return discoveredCount >= floor
}

// PeriodicDerankHealthy adds the stronger signal the periodic path has: even when
// the configured ceiling would pass, a discovered set that collapsed far below the
// currently-subscribed count is treated as a partial response. A zero current count
// makes the second basis a no-op — on a healthy first cycle subscriptions are still
// pacing in.
func PeriodicDerankHealthy(discoveredCount, currentCount, maxAssets int) bool {
	if !DiscoveryHealthy(discoveredCount, maxAssets) {
		return false
	}
	if currentCount <= 0 {
		return true
	}
	return discoveredCount >= int(float64(currentCount)*MinDiscoveryFraction)
}

// EnforceMaxAssets hard-caps a discovered symbol slice to the configured ceiling.
// It returns the kept slice and how many symbols were dropped, so the caller owns
// the warning (this package is pure). A non-positive ceiling disables the cap.
func EnforceMaxAssets(symbols []string, maxAssets int) ([]string, int) {
	if maxAssets <= 0 || len(symbols) <= maxAssets {
		return symbols, 0
	}
	return symbols[:maxAssets], len(symbols) - maxAssets
}

// DeclaredSymbolCount reports how many symbols a scope is DECLARED to stream: the
// discovery ceiling when discovery is enabled, otherwise the explicit list size.
// Zero means "no expectation" and is never reported as a coverage deficit.
func DeclaredSymbolCount(discoveryEnabled bool, maxAssets, staticSymbols int) int {
	if discoveryEnabled && maxAssets > 0 {
		return maxAssets
	}
	return staticSymbols
}
