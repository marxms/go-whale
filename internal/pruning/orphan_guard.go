// Package pruning sweeps Redis keys outside the active symbol set under an anchor guard.
package pruning

import "context"

// ============================================================================
// THE ANCHOR GUARD
// ============================================================================
//
// # WHY IT EXISTS
//
// The sweep deletes everything OUTSIDE the active set. That is safe exactly to the extent the
// active set is trustworthy. The real anchor is `active_symbols:{ex}:{mt}`, read from Redis. Under
// `volatile-ttl` it is IMMORTAL (TTL −1 measured on 4/4 `active_symbols*` keys), so it is never
// evicted and "empty set" happens only on a cold boot. Under `allkeys-lru` it becomes evictable
// like any other key, and then "short anchor" and "destructive sweep" compose: the idlest key in
// the keyspace is evicted first, and the next sweep reads 0 (or 12) symbols and marks the whole
// fleet as orphaned. The guard therefore exists before that eviction policy is possible.
//
// # THE THREE RULES
//
//  1. EMPTY ANCHOR ⇒ ZERO REMOVALS. It is not an error case; it is the normal state of a boot
//     before discovery. The right answer is to do nothing.
//  2. ANCHOR BELOW 90% OF THE FLOOR ⇒ ZERO REMOVALS. The floor is the last cardinality observed
//     in an ACCEPTED pass, persisted. The sibling scopes `binance:spot`, `bingx:future` and
//     `bingx:spot` have 1 member each in production, so "short set" is not hypothetical.
//  3. REFUSAL DOES NOT LOWER THE FLOOR. Lowering would be the entire defect in one line: the
//     short pass becomes the new normal and the next one deletes with the guard's blessing.
//
// The accepted consequence of rule 3: a LEGITIMATE, permanent reduction of the universe (500 →
// 300) makes the guard refuse indefinitely, with both numbers in the log. A refusal the operator
// sees is strictly better than a removal they do not.
//
// # WHY A PASS WITHOUT A FLOOR DOES NOT SWEEP
//
// Without a floor there is nothing to compare against, and it is precisely in the first pass
// after an eviction of the floor key itself that the anchor is most likely to be short too. The
// pass records the floor and removes nothing; the next one has something to measure against.

// anchorFloorPercent is the relative floor: below 90% of the last accepted cardinality, the pass
// removes nothing.
const anchorFloorPercent = 90

// AnchorFloorKey stores the last observed cardinality.
//
// DECLARED RETENTION: no TTL, on purpose, because the floor must survive the restart that would
// empty it. Its absence is SAFE by construction — without a floor the sweep refuses to remove —
// so it is the only immortal family this path creates and the only one whose loss degrades to the
// conservative side.
//
// The name is EXPORTED because persisting it belongs to the adapter (the port does not know Redis
// keys), and a second string with this name would be a divergence the guard would not notice: it
// would read a floor nobody writes and refuse forever.
const AnchorFloorKey = "pruner:v1:anchor_floor"

// anchorVerdict is what the guard decides.
type anchorVerdict struct {
	// Sweep says whether the pass may remove.
	Sweep bool
	// Reason names the refusal, for the log and for the test.
	Reason string
	Floor  int
	Seen   int
}

// checkAnchor applies the three rules and maintains the floor.
func checkAnchor(ctx context.Context, store AnchorFloorStore, seen int) (anchorVerdict, error) {
	if seen == 0 {
		return anchorVerdict{Reason: "empty anchor", Seen: 0}, nil
	}

	floor, known, err := store.AnchorFloor(ctx)
	if err != nil {
		// An unreadable floor is a dead instrument. Sweeping without it would be sweeping without
		// a guard.
		return anchorVerdict{Reason: "unreadable floor", Seen: seen}, err
	}
	if !known {
		if err := store.SetAnchorFloor(ctx, seen); err != nil {
			return anchorVerdict{Reason: "floor not persisted", Seen: seen}, err
		}
		return anchorVerdict{Reason: "first observation of the floor", Seen: seen}, nil
	}

	if seen*100 < floor*anchorFloorPercent {
		// WITHOUT updating the floor. See rule 3 in the header block.
		return anchorVerdict{Reason: "anchor below the floor", Floor: floor, Seen: seen}, nil
	}

	if seen != floor {
		if err := store.SetAnchorFloor(ctx, seen); err != nil {
			return anchorVerdict{Reason: "floor not persisted", Floor: floor, Seen: seen}, err
		}
	}
	return anchorVerdict{Sweep: true, Floor: floor, Seen: seen}, nil
}
