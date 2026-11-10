package pruning

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ============================================================================
// QUESTDB COLD-STORAGE NOTE
// ============================================================================
//
// QuestDB 8.2.3 has NO row-level DELETE (DELETE FROM <table> WHERE ... errors
// with "unexpected token [FROM]"), so the Pruner does NOT touch QuestDB. Cold-
// storage retention — including the eventual expiry of deranked/delisted
// symbols — is handled entirely by internal/retention via time-based
// ALTER TABLE ... DROP PARTITION (retention_days). A stale symbol stops
// receiving writes, its partitions age past the retention window, and are
// dropped natively; OHLCV is re-fetchable, so this preserves maximum record
// integrity without ever issuing an (impossible) per-row delete.
//
// The Pruner is therefore Redis-only: it keeps the dashboard-facing hot data
// (derivatives snapshots/series, last-trade keys, and — via the subscribe
// derank path — the Redis active-set that /api/symbols reads) accurate.

// ============================================================================
// INTERFACES — small, mockable surface
// ============================================================================

// KeyStore is the Redis surface this package needs.
//
// The port is declared HERE, by the consumer, and implemented by
// adapters/outbound/redis. The business path never sees a *redis.Client: the
// cursor-carrying signature and the UNLINK semantics are the contract, not the
// driver.
//
// ScanKeys ACCEPTS the cursor as well as returning the next one. A signature that returns a
// cursor it cannot receive can only ever start from 0, which turns a paginated SCAN into a
// full-keyspace walk.
type KeyStore interface {
	ScanKeys(ctx context.Context, cursor uint64, pattern string) ([]string, uint64, error)
	UnlinkKeys(ctx context.Context, keys ...string) (int64, error)
}

// AnchorFloorStore persists the last accepted active-set cardinality.
//
// It is separate from KeyStore because only the orphan sweep needs it, and a
// port that grows to serve one caller is how every test double starts
// implementing methods it never exercises. The Redis adapter implements both.
type AnchorFloorStore interface {
	AnchorFloor(ctx context.Context) (int, bool, error)
	SetAnchorFloor(ctx context.Context, n int) error
}

// ============================================================================
// PRUNER
// ============================================================================

// Pruner removes stale hot data from Redis for symbols that are no longer in the
// active discovery list. It is Redis-only: QuestDB cold storage is retained and
// expired by internal/retention (see the cold-storage note above).
type Pruner struct {
	log   *slog.Logger
	redis KeyStore
	floor AnchorFloorStore
	// gatesApproved is the license to act, and it starts DENIED. See PruneRedis.
	gatesApproved func() bool
}

// PrunerOption configures a declared dependency.
type PrunerOption func(*Pruner)

// WithGatesApproved injects the integrity-gates predicate. Without it the sweep never removes
// anything: the safe default for a destructive path is not to act.
func WithGatesApproved(gates func() bool) PrunerOption {
	return func(p *Pruner) { p.gatesApproved = gates }
}

// WithLogger injects the logger this pruner reports through. A nil logger is a silent sink.
func WithLogger(log *slog.Logger) PrunerOption {
	return func(p *Pruner) { p.log = logger.OrDiscard(log) }
}

// NewPruner creates a Pruner over a port implementation owned by the adapter.
func NewPruner(store KeyStore, opts ...PrunerOption) *Pruner {
	p := &Pruner{redis: store, log: logger.OrDiscard()}
	if floor, ok := store.(AnchorFloorStore); ok {
		p.floor = floor
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// gatesAllow reads the license defensively: a nil predicate (a Pruner built
// without the option) is a denied license, never an unconditional sweep.
func (p *Pruner) gatesAllow() bool {
	return p.gatesApproved != nil && p.gatesApproved()
}

// maxUnlinksPerPass is the removal ceiling for ONE pass.
//
// THE MEASURED COHORT IS 5,303 ORPHAN KEYS, so a legitimate cleanup converges in three passes and
// the sweep runs once per discovery cycle. The cap does not exist to contain cleanup; it exists to
// contain the DEFECT: a wrong positional parse marks the whole fleet as orphaned, and 81,860 keys
// leave in one pass. With the cap, 2,000 leave and the warning line is the difference between a
// defect and a catastrophe.
const maxUnlinksPerPass = 2000

// PruneRedis removes orphaned symbol data from Redis using UNLINK.
// It scans for keys matching derivative patterns and removes
// those belonging to symbols no longer in the active set.
func (p *Pruner) PruneRedis(ctx context.Context, activeSymbols []string) error {
	// THE LICENSE COMES FIRST. The sweep is a destructive path, and a pruner started before the
	// integrity gates speak cannot even begin to sweep: a read-only pass still measures the wrong
	// keyspace, and the next one removes with the blessing of a license never granted.
	if !p.gatesAllow() {
		logger.OrDiscard(p.log).Warn("pruner: pass refused; integrity gates have not approved")
		return nil
	}

	activeSet := make(map[string]struct{}, len(activeSymbols))
	for _, s := range activeSymbols {
		activeSet[s] = struct{}{}
	}

	// THE GUARD COMES BEFORE ANY SCAN. See orphan_guard.go for the three rules and for why it
	// exists before `allkeys-lru` is possible.
	if p.floor == nil {
		return fmt.Errorf("pruner: anchor guard without floor storage")
	}
	verdict, err := checkAnchor(ctx, p.floor, len(activeSet))
	if err != nil {
		return fmt.Errorf("pruner: anchor guard: %w", err)
	}
	if !verdict.Sweep {
		logger.OrDiscard(p.log).Warn("pruner: pass without removal",
			"reason", verdict.Reason, "anchor", verdict.Seen, "floor", verdict.Floor)
		return nil
	}

	// The derivatives patterns come from storage, which owns the key layout. A second copy of a
	// positional parse here is how a new key family ends up swept by two sweeps out of three.
	patterns := append(redisstore.DerivativesKeyPatterns(), orphanSweepPatterns()...)

	var totalUnlinked int64
	var capped bool

	for _, pattern := range patterns {
		var cursor uint64
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			keys, nextCursor, err := p.redis.ScanKeys(ctx, cursor, pattern)
			if err != nil {
				return fmt.Errorf("redis scan %s: %w", pattern, err)
			}

			var toUnlink []string
			for _, key := range keys {
				sym := extractSymbolFromKey(key)
				if sym == "" {
					continue
				}
				if _, active := activeSet[sym]; !active {
					toUnlink = append(toUnlink, key)
				}
			}

			// THE CAP IS APPLIED BEFORE REMOVING, and truncates the batch instead of discarding
			// it: stopping in the middle of a pass is the desired behavior, and the next pass
			// continues where this one stopped.
			if restante := maxUnlinksPerPass - int(totalUnlinked); len(toUnlink) > restante {
				toUnlink = toUnlink[:max(restante, 0)]
				capped = true
			}

			if len(toUnlink) > 0 {
				n, err := p.redis.UnlinkKeys(ctx, toUnlink...)
				if err != nil {
					logger.OrDiscard(p.log).Warn("pruner: unlink failed", "pattern", pattern, "error", err)
				} else {
					totalUnlinked += n
				}
			}

			if capped {
				break
			}

			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}
		if capped {
			break
		}
	}

	if capped {
		logger.OrDiscard(p.log).Warn("pruner: removal ceiling reached in this pass; the next pass continues",
			"max_unlinks_per_pass", maxUnlinksPerPass)
	}
	if totalUnlinked > 0 {
		logger.OrDiscard(p.log).Info("pruner: stale redis keys unlinked",
			"unlinked", totalUnlinked, "anchor", verdict.Seen, "floor", verdict.Floor)
	}
	return nil
}

// orphanSweepPatterns are the families `redisstore.DerivativesKeyPatterns()` does NOT cover.
//
// # THE COHORT, MEASURED
//
// 5,303 orphan keys (134 deranked symbols) totaling 97.3 MiB, of which 95.8 MiB are in
// `indicators` — the family this sweep did not look at. The other missing ones are
// `analyzer:lastAnalyzedBar` (1,235 keys) and `signalEvidence` (154).
//
// # WHY `candles` STAYS OUT
//
// It has 112 orphan keys and, of the cohort's 97.3 MiB, accounts for a fraction of a MiB — but
// 95,811 members of `candles:*:1d` and 33,379 of `:1w` PREDATE QuestDB coverage and exist ONLY in
// Redis. Until the migration runs, deleting one of those keys deletes a sole copy to recover ~0
// MiB — exactly the trade the sole-copy rule forbids. It enters after the migration, or not at
// all.
//
// The glob is the BROAD one (`family:*`), not one shape per variant: Redis's `*` crosses `:`, so
// the broad pattern also reaches the old form someone wrote and nobody remembers. The parse
// decides, refusing the shape it does not know.
func orphanSweepPatterns() []string {
	return []string{
		"last_trade:*:*:*",
		"indicators:*",
		"analyzer:lastAnalyzedBar:*",
		"signalEvidence:*",
	}
}

// extractSymbolFromKey parses a Redis key and extracts the symbol, returning "" when
// the key belongs to no known family — an unknown shape is skipped, never guessed at,
// because a wrong symbol here UNLINKs live data.
//
// The derivatives families are parsed by redisstore.SymbolFromDerivativesKey rather than
// re-derived here: this parse is positional, so two copies agreeing is only a convention, and
// conventions do not survive a key gaining a segment.
//
// Supported patterns:
//
//	derivatives:snapshot:latest:{exchange}:{marketType}:{symbol}
//	derivatives:series:{exchange}:{marketType}:{symbol}:{timeframe}
//	derivatives:baseline:{exchange}:{marketType}:{symbol}:{timeframe}:{version}
//	last_trade:{exchange}:{marketType}:{symbol}
//	indicators:{symbol}:{timeframe}:{name}
//	indicators:{exchange}:{marketType}:{symbol}:{timeframe}:{name}
//	analyzer:lastAnalyzedBar:{symbol}:{timeframe}
//	signalEvidence:{symbol}:{timeframe}
//	signalEvidence:{producerId}:{symbol}:{timeframe}
//
// THE SHAPES OF THE LAST THREE FAMILIES ARE WHAT THE ANALYZER BUILDERS PRODUCE —
// `apps/analyzer/src/services/state/redis-keys.ts` (`boundedIndicatorKey`, `signalEvidenceKey`)
// and `services/realtime/analyzed-bar-tracker.ts` (`lastAnalyzedBarKey`). They are distinguished
// by SEGMENT COUNT, and the count works because a symbol carries `/` and never `:`. Any other
// count returns "" and the key is SKIPPED: a wrong symbol here deletes live data, and the two
// `indicators` variants differ precisely in where the symbol falls.
func extractSymbolFromKey(key string) string {
	if sym := redisstore.SymbolFromDerivativesKey(key); sym != "" {
		return sym
	}
	parts := strings.Split(key, ":")
	switch parts[0] {
	case "last_trade":
		if len(parts) == 4 {
			return parts[3]
		}
	case "indicators":
		switch len(parts) {
		case 4: // indicators:{symbol}:{tf}:{name}
			return parts[1]
		case 6: // indicators:{exchange}:{marketType}:{symbol}:{tf}:{name}
			return parts[3]
		}
	case "analyzer":
		if len(parts) == 4 && parts[1] == "lastAnalyzedBar" {
			return parts[2]
		}
	case "signalEvidence":
		switch len(parts) {
		case 3: // signalEvidence:{symbol}:{tf}
			return parts[1]
		case 4: // signalEvidence:{producerId}:{symbol}:{tf}
			return parts[2]
		}
	}
	return ""
}
