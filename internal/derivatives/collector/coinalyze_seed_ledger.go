package collector

import (
	"context"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE SEEDED MARK, OUTSIDE PROCESS MEMORY.
//
// # WHAT KEEPING IT IN MEMORY COST
//
// `seededSymbols` is a process map written at the END of the cycle. This collection cycle
// measures `499 symbols × 3 endpoints × 3 timeframes ÷ 40 calls/min = 112.3 min` against a
// 60-minute ticker — 1.87x. So NO deploy shorter than ~2 h preserves the seeding: the new
// process wakes without the mark, re-requests 2000 buckets for the whole fleet, and the next
// cycle never arrives.
//
// # WHY THE MARK IS DERIVED FROM THE DATA, NOT STORED ELSEWHERE
//
// The two obvious alternatives are refused:
//
//   - A REDIS KEY. Without a TTL it becomes another resident family with no expiration — the
//     exact condition the sole-copy gate refuses. With a TTL it is the cohort `volatile-*`
//     evicts FIRST, on a server whose peak already exceeded `maxmemory` and has no AOF.
//   - A NEW TABLE. `EnsureTable` contract, DDL, a schema job in CI — all to store a fact the
//     durable table already contains.
//
// The fact is already written: a DEEP pass leaves depth a TAIL pass could not have left.
// Reading the data follows the same rule the startup migration uses — a completion criterion read
// from storage, never from a flag.
//
// # THE RULE IS ADDITIVE, AND THAT IS WHAT MAKES IT SAFE
//
// The ledger can only TRANSFORM "awaiting seeding" into "seeded". It never unmarks anyone: a
// symbol this process already seeded stays seeded even if the query fails, and a new listing —
// whose entire history fits inside the tail — falls back to the process mark. The worst outcome of
// a bad read is the previous behavior.
// ─────────────────────────────────────────────────────────────────────────────

// CoinalyzeSeedLedger is where the seeded mark survives between processes: the durable tier
// itself.
//
// It returns, per INTERNAL symbol, the oldest point the durable table has for that scope. An empty
// map is a legitimate answer — it means "nothing written yet" — and differs from an error, which
// means "I do not know".
type CoinalyzeSeedLedger interface {
	OldestDerivativesBySymbol(ctx context.Context, source, exchange, marketType, timeframe string) (map[string]time.Time, error)
}

// durableSeeded returns the provider ids the DURABLE TIER proves have already received the deep
// pass.
//
// The proof is of depth: the tail can only write within `[to − tail, to]`, so a point STRICTLY
// older than that boundary can only have come from a seeding run.
//
// THE REQUIREMENT IS ON EVERY TIMEFRAME, and it is what closes the ROW hole measured in production:
// four batches of 20 died on 429 and left 40 symbols with `4h` full and ZERO `1h`. Counting as
// seeded whoever has depth in only one timeframe makes that hole permanent.
//
// A read error returns NOTHING, not "nobody seeded": not knowing is not zero. The cycle falls back
// to the process mark.
func (p *CoinalyzePoller) durableSeeded(
	ctx context.Context,
	resolved map[string]coinalyzeTarget,
	fleet []string,
	to time.Time,
) map[string]struct{} {
	if p.seedLedger == nil || len(fleet) == 0 || len(p.timeframes) == 0 {
		return nil
	}

	// It starts with the whole fleet and each timeframe REMOVES those without depth in it.
	candidatos := make(map[string]struct{}, len(fleet))
	for _, id := range fleet {
		candidatos[id] = struct{}{}
	}

	for _, timeframe := range p.timeframes {
		oldest, err := p.seedLedger.OldestDerivativesBySymbol(ctx, coinalyzeSourceName, p.exchange, p.marketType, timeframe)
		if err != nil {
			logger.OrDiscard(p.log).Warn("coinalyze: durable depth unreadable; seeding returns to process lifetime only",
				"timeframe", timeframe, "error", err)
			return nil
		}
		fronteira := to.Add(-p.windowFor(timeframe, false))
		for id := range candidatos {
			target, resolvido := resolved[id]
			if !resolvido {
				delete(candidatos, id)
				continue
			}
			// PRESENCE IS CHECKED, not the value: `time.Time`'s zero is year 1, older than any
			// boundary — a symbol with NO ROW AT ALL would pass as seeded. That is how the row
			// hole measured in production (40 symbols with `4h` full and zero `1h`) would become
			// permanent.
			ponto, temLinha := oldest[target.internal]
			if !temLinha || !ponto.Before(fronteira) {
				delete(candidatos, id)
			}
		}
		if len(candidatos) == 0 {
			return nil
		}
	}

	logger.OrDiscard(p.log).Info("coinalyze: symbols already have durable depth; seeding is skipped for them",
		"seeded", len(candidatos), "fleet", len(fleet))
	return candidatos
}
