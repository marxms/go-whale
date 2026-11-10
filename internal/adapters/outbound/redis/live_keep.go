package redisstore

import "strings"

// LiveKeepSet builds the race-free retain set for a scope: the union of the
// manager's subscriptions (scoped by the "exchange:marketType:" prefix) and the
// authoritative active_symbols registry.
//
// A freshly subscribed symbol is in the manager before its first data write reaches
// active_symbols, so the union never sweeps it. The Redis key layout lives here, with
// the adapter that owns the keyspace.
func LiveKeepSet(managerSubs, activeSymbols []string, prefix string) map[string]struct{} {
	keep := make(map[string]struct{}, len(managerSubs)+len(activeSymbols))
	for _, key := range managerSubs {
		if strings.HasPrefix(key, prefix) {
			keep[strings.TrimPrefix(key, prefix)] = struct{}{}
		}
	}
	for _, sym := range activeSymbols {
		keep[sym] = struct{}{}
	}
	return keep
}
