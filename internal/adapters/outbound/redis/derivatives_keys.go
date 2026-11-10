package redisstore

import (
	"fmt"
	"strings"
)

// ============================================================================
// DERIVATIVES KEY LAYOUT — ONE SOURCE OF TRUTH
// ============================================================================
//
// THREE sweeps delete derivatives keys, not two: UnlinkStaleDerivatives and
// CleanupSymbolDerivatives live in this package, and pruner.PruneRedis lives in
// another one and parses the symbol POSITIONALLY out of a glob. A key family added
// to the writer and forgotten by any one of them is a leak that nothing reports,
// because a key nobody deletes still answers every read correctly.
//
// So the layout is declared once here, and every sweep derives its patterns and its
// positional parse from these functions instead of re-typing the string.
//
// # WHY THE VERSION IS A SUFFIX
//
// The parse is positional. A version placed INFIX shifts every field after it and
// silently changes which segment the sweeps read as the symbol — the sweep keeps
// running, keeps reporting success, and deletes nothing. As a suffix it lands past
// the symbol, so parts[4] stays the symbol in both the versioned and unversioned
// families.
const (
	derivLatestPrefix   = "derivatives:snapshot:latest"
	derivSeriesPrefix   = "derivatives:series"
	derivBaselinePrefix = "derivatives:baseline"

	// derivBaselineKeyVersion is a SUFFIX. See the note above.
	derivBaselineKeyVersion = "v1"
)

// THE SECOND PROVENANCE LIVES IN ITS OWN NAMESPACE.
//
// # WHY NOT WRITE TO THE SAME KEYS
//
// `mergeDerivativesSnapshots` (redis_derivatives.go) is last-writer-wins and has no
// provenance term — the file's own comment declares that as the mechanism. Two sources in
// the same key do not add up: they overwrite each other, and the series starts oscillating
// between the two every cycle. That oscillation enters the MAD baseline as if it were
// market variance, and the `zOpenInterest` derived from it is the scoring jury's decision
// axis. The error appears nowhere — the read keeps answering.
//
// # WHY THE DISCRIMINATOR IS A SEGMENT, AND RIGHT AFTER `derivatives`
//
// Putting `coinalyze` as the second segment keeps each family with its own LITERAL prefix
// (`derivatives:coinalyze:series:` versus `derivatives:series:`), so no glob of one family
// reaches the other by accident — Redis `*` spans `:`, and a discriminator tucked in the
// middle of the key would be reached by the neighboring family's wildcard. The price is
// that the symbol drops one position (parts[5], not parts[4]), which is why this file's
// parse lists each family explicitly instead of counting segments.
const (
	derivCoinalyzeLatestPrefix   = "derivatives:coinalyze:snapshot:latest"
	derivCoinalyzeSeriesPrefix   = "derivatives:coinalyze:series"
	derivCoinalyzeBaselinePrefix = "derivatives:coinalyze:baseline"
)

// derivLatestScopedKey is the real-time snapshot key (SET, TTL safety net).
func derivLatestScopedKey(exchange, marketType, symbol string) string {
	return fmt.Sprintf("%s:%s:%s:%s", derivLatestPrefix, exchange, marketType, symbol)
}

// derivSeriesZsetKey returns the ZSET key for a per-timeframe derivatives series.
// Pattern matches candle storage: one ZSET per (exchange, marketType, symbol, timeframe).
func derivSeriesZsetKey(exchange, marketType, symbol, timeframe string) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s", derivSeriesPrefix, exchange, marketType, symbol, timeframe)
}

// derivBaselineKey returns the key holding the persisted robust baseline for one
// (symbol, timeframe). It is its OWN key rather than a field of the series so that
// recomputing the ruler never rewrites the observations it was computed from.
func derivBaselineKey(exchange, marketType, symbol, timeframe string) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s:%s", derivBaselinePrefix, exchange, marketType, symbol, timeframe, derivBaselineKeyVersion)
}

// derivCoinalyzeLatestKey is the real-time snapshot key of the Coinalyze feed.
func derivCoinalyzeLatestKey(exchange, marketType, symbol string) string {
	return fmt.Sprintf("%s:%s:%s:%s", derivCoinalyzeLatestPrefix, exchange, marketType, symbol)
}

// The Coinalyze series family is WRITE-ONLY: no production path writes it, and its resident
// keys only disappear through an explicit `UNLINK` — they have no TTL. The PREFIX stays
// because the three sweeps in this file must keep covering the family. The key shape, for
// anyone who needs to build it in a sweep or a test, is
// `derivCoinalyzeSeriesPrefix:{ex}:{mt}:{symbol}:{tf}`.

// derivSymbolKeyPatterns returns the globs covering every derivatives key of ONE
// symbol — what a per-symbol cleanup has to walk.
//
// The latest keys are excluded because they are addressed directly, not scanned —
// derivSymbolDirectKeys names them.
func derivSymbolKeyPatterns(exchange, marketType, symbol string) []string {
	return []string{
		fmt.Sprintf("%s:%s:%s:%s:*", derivSeriesPrefix, exchange, marketType, symbol),
		fmt.Sprintf("%s:%s:%s:%s:*", derivBaselinePrefix, exchange, marketType, symbol),
		fmt.Sprintf("%s:%s:%s:%s:*", derivCoinalyzeSeriesPrefix, exchange, marketType, symbol),
		fmt.Sprintf("%s:%s:%s:%s:*", derivCoinalyzeBaselinePrefix, exchange, marketType, symbol),
	}
}

// derivSymbolDirectKeys returns the keys of ONE symbol that a cleanup can address by
// name instead of scanning for.
//
// They stay out of derivSymbolKeyPatterns because a SCAN walks the whole keyspace even when
// the MATCH is literal: turning two known keys into two full scans costs the entire keyspace
// per deranked symbol, to find what is already known how to write.
func derivSymbolDirectKeys(exchange, marketType, symbol string) []string {
	return []string{
		derivLatestScopedKey(exchange, marketType, symbol),
		derivCoinalyzeLatestKey(exchange, marketType, symbol),
	}
}

// derivScopeKeyPatterns returns the globs covering every derivatives key of one
// (exchange, marketType) — what a stale-symbol DISCOVERY sweep has to walk.
func derivScopeKeyPatterns(exchange, marketType string) []string {
	return []string{
		fmt.Sprintf("%s:%s:%s:*", derivLatestPrefix, exchange, marketType),
		fmt.Sprintf("%s:%s:%s:*", derivSeriesPrefix, exchange, marketType),
		fmt.Sprintf("%s:%s:%s:*", derivBaselinePrefix, exchange, marketType),
		fmt.Sprintf("%s:%s:%s:*", derivCoinalyzeLatestPrefix, exchange, marketType),
		fmt.Sprintf("%s:%s:%s:*", derivCoinalyzeSeriesPrefix, exchange, marketType),
		fmt.Sprintf("%s:%s:%s:*", derivCoinalyzeBaselinePrefix, exchange, marketType),
	}
}

// CoinalyzeSeriesKeyPattern is the glob of the WRITE-ONLY `derivatives:coinalyze:series`
// family.
//
// Exported because the destructive sweep that deletes the resident keys lives outside this
// package. A sweep that deletes must not type its own glob: a family covered by two of the
// three sweeps and not by the third is how stale keys survive.
func CoinalyzeSeriesKeyPattern() string {
	return derivCoinalyzeSeriesPrefix + ":*:*:*:*"
}

// DerivativesKeyPatterns returns the globs covering every derivatives key family,
// for sweeps that walk the whole keyspace without knowing the exchange or market
// type — today that is the pruner.
//
// The snapshot glob is deliberately the broad `snapshot:*:*:*` rather than the
// narrower `snapshot:latest:...`: Redis globs let `*` span `:`, so the broad form
// also matches any older snapshot shape still sitting in a long-lived database.
func DerivativesKeyPatterns() []string {
	return []string{
		"derivatives:snapshot:*:*:*",
		derivSeriesPrefix + ":*:*:*:*",
		derivBaselinePrefix + ":*:*:*:*:*",
		derivCoinalyzeLatestPrefix + ":*:*:*",
		CoinalyzeSeriesKeyPattern(),
		derivCoinalyzeBaselinePrefix + ":*:*:*:*:*",
	}
}

// SymbolFromDerivativesKey extracts the symbol from any derivatives key, returning
// "" when the key is not one of the declared families.
//
// The parse is positional and every family is listed here explicitly, so a key shape
// that nobody taught this function is skipped rather than mis-parsed into some other
// symbol's name — a wrong symbol here would delete live data.
func SymbolFromDerivativesKey(key string) string {
	parts := strings.Split(key, ":")
	if len(parts) < 5 || parts[0] != "derivatives" {
		return ""
	}
	switch parts[1] {
	case "snapshot":
		// derivatives:snapshot:latest:{exchange}:{marketType}:{symbol}
		if len(parts) >= 6 && parts[2] == "latest" {
			return parts[5]
		}
	case "series":
		// derivatives:series:{exchange}:{marketType}:{symbol}:{timeframe}
		if len(parts) >= 6 {
			return parts[4]
		}
	case "baseline":
		// derivatives:baseline:{exchange}:{marketType}:{symbol}:{timeframe}:{version}
		if len(parts) >= 7 {
			return parts[4]
		}
	case "coinalyze":
		// The provenance discriminator pushes EVERY field one position. That is why the
		// second family has its own branch instead of falling into the branch above: reusing
		// the index would return the marketType in place of the symbol, and the sweep would
		// delete the keys of a symbol named "future".
		return coinalyzeSymbolFromParts(parts)
	}
	return ""
}

// coinalyzeSymbolFromParts parses the second-provenance family positionally.
func coinalyzeSymbolFromParts(parts []string) string {
	if len(parts) < 6 {
		return ""
	}
	switch parts[2] {
	case "snapshot":
		// derivatives:coinalyze:snapshot:latest:{exchange}:{marketType}:{symbol}
		if len(parts) >= 7 && parts[3] == "latest" {
			return parts[6]
		}
	case "series":
		// derivatives:coinalyze:series:{exchange}:{marketType}:{symbol}:{timeframe}
		if len(parts) >= 7 {
			return parts[5]
		}
	case "baseline":
		// derivatives:coinalyze:baseline:{exchange}:{marketType}:{symbol}:{timeframe}:{version}
		if len(parts) >= 8 {
			return parts[5]
		}
	}
	return ""
}
