package collector

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// paraDeFalhar returns storage to normal — the chronically failing symbol recovering.
func (s *storeQueFalhaPorSimbolo) paraDeFalhar() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.falhaEm = ""
}

// janelasPorSimbolo reads the requested depth in each request, indexed by that request's
// `symbols`. It is what separates "who re-seeded" from "who went on the tail" when the cycle
// starts emitting more than one request per timeframe.
func janelasPorSimbolo(t *testing.T, queries []url.Values) map[string]int64 {
	t.Helper()
	out := make(map[string]int64, len(queries))
	for _, q := range queries {
		from, errFrom := strconv.ParseInt(q.Get("from"), 10, 64)
		to, errTo := strconv.ParseInt(q.Get("to"), 10, 64)
		if errFrom != nil || errTo != nil {
			t.Fatalf("unreadable window in request %v", q)
		}
		out[q.Get("symbols")] = to - from
	}
	return out
}

// SEEDING IS MARKED PER WRITTEN SYMBOL, NOT PER WHOLE CYCLE.
//
// `markSeeded` only ran when the cycle was 100% clean: `falhou` was a CYCLE boolean, not a symbol
// one. A single symbol with a persistent write error kept the whole fleet in seeding mode, and
// every hourly cycle re-requested and rewrote ~2000 points × 31 symbols × 3 quantities in both
// tiers. The write is idempotent thanks to DEDUP, so nothing breaks — it is recurring write
// amplification, the kind of cost nobody sees rising.
//
// The criterion has two halves and both are asserted here: the symbols that wrote LEAVE seeding,
// and the one that failed re-seeds ALONE — the deep window covers only it, and the others go on
// the tail.
//
// THE OTHER HALF IS THE PACE: "re-seeds ALONE" applies to the cycle in which the backoff WAIT
// advances, not to the immediately following cycle. Without that, a failing symbol re-requested
// the 2000 buckets every cycle, forever:
//
//	cycle 1  nobody seeded          → one deep request for both
//	cycle 2  BTC failed in 1        → BACKOFF: both go on the tail in the same request
//	cycle 3  the wait advanced      → BTC re-seeds ALONE; ETH stays on the tail
//	cycle 4  both wrote             → one tail request for both
func TestSeed_OnlyTheSymbolThatFailedReseedsInTheNextCycle(t *testing.T) {
	poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{"1d"}, map[string]string{
		"/future-markets":           coinalyzeDoisMercadosBody,
		"/open-interest-history":    coinalyzeDoisOpenInterestBody,
		"/funding-rate-history":     coinalyzeDoisFundingBody,
		"/long-short-ratio-history": coinalyzeDoisLongShortBody,
	})
	poller.symbolsFunc = func(context.Context) []string { return []string{"BTC/USDT", "ETH/USDT"} }

	store := &storeQueFalhaPorSimbolo{falhaEm: "BTC/USDT", err: errors.New("durable tier unavailable")}
	poller.store = store

	const profunda = int64(2000 * 86400) // the 2000 daily buckets of the seeding pass

	// Cycle 1 — nobody seeded: one deep request for both symbols.
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 1: %v", err)
	}
	cicloUm := janelasPorSimbolo(t, fake.queriesFor("/open-interest-history"))
	if len(cicloUm) != 1 {
		t.Fatalf("cycle 1 emitted %d distinct request(s); with the whole fleet awaiting seeding it is one: %v", len(cicloUm), cicloUm)
	}
	if got := cicloUm["BTCUSDT_PERP.A,ETHUSDT_PERP.A"]; got != profunda {
		t.Fatalf("cycle 1 requested %d s for both symbols, expected the deep pass (%d s)", got, profunda)
	}

	// Storage returns — cycle 1's failure is what puts BTC in backoff.
	store.paraDeFalhar()

	// Cycle 2 — ETH wrote and left seeding; BTC failed and is in BACKOFF: both go on the tail, in
	// the SAME request, and neither requests the deep window.
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 2: %v", err)
	}
	todas := fake.queriesFor("/open-interest-history")
	cicloDois := janelasPorSimbolo(t, todas[1:])
	if len(cicloDois) != 1 {
		t.Fatalf("cycle 2 emitted %d request(s); with the failed symbol in backoff both go on the "+
			"tail in one: %v", len(cicloDois), cicloDois)
	}
	juntos := false
	for pedido := range cicloDois {
		if strings.Contains(pedido, "BTCUSDT_PERP.A") && strings.Contains(pedido, "ETHUSDT_PERP.A") {
			juntos = true
		}
	}
	if !juntos {
		t.Fatalf("cycle 2 pinned the whole fleet in seeding because one symbol failed: %v", cicloDois)
	}
	for pedido, janela := range cicloDois {
		if janela >= profunda {
			t.Errorf("the cycle after the failure requested the deep window (%d s) in %q — repeated "+
				"failure amplifies the request at the same pace", janela, pedido)
		}
	}

	// Cycle 3 — the wait ADVANCED (one cycle): NOW only BTC re-seeds, and ETH stays on the tail.
	// This half separates "the mark is per symbol" from "the mark is per cycle", and backoff did
	// not lose it: it postponed it.
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 3: %v", err)
	}
	todas = fake.queriesFor("/open-interest-history")
	cicloTres := janelasPorSimbolo(t, todas[len(todas)-2:])
	if got := cicloTres["BTCUSDT_PERP.A"]; got != profunda {
		t.Errorf("after the wait, the failed symbol requested %d s, expected the deep pass "+
			"(%d s) — it re-seeds ALONE: %v", got, profunda, cicloTres)
	}
	if got, pediu := cicloTres["ETHUSDT_PERP.A"]; !pediu || got >= profunda {
		t.Errorf("the symbol that wrote requested %d s in cycle 3 (present=%v); it left seeding and "+
			"had to go on the tail", got, pediu)
	}
	if len(cicloTres) != 2 {
		t.Errorf("cycle 3 emitted %d request(s); expected two — BTC's deep one and ETH's "+
			"tail: %v", len(cicloTres), cicloTres)
	}

	// Cycle 4 — BTC wrote the deep pass in cycle 3 and leaves seeding too.
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 4: %v", err)
	}
	todas = fake.queriesFor("/open-interest-history")
	cicloQuatro := janelasPorSimbolo(t, todas[len(todas)-1:])
	if got := cicloQuatro["BTCUSDT_PERP.A,ETHUSDT_PERP.A"]; got == 0 || got >= profunda {
		t.Fatalf("with both writing, the next cycle requested %v; expected a single tail request for both", cicloQuatro)
	}
}

// AND THE WHOLE FLEET FAILING STILL DOES NOT BURN THE SEEDING — the sibling of
// `TestSeed_AFailedCycleDoesNotBurnTheSeeding`, now from the WRITE side.
//
// The per-symbol mark must not become a per-attempt mark: a symbol whose write failed on every
// cycle still OWES the deep pass, otherwise its history never arrives.
//
// AND IT IS THE PACE THAT CHANGES: "keeps requesting indefinitely" becomes "keeps owing, and asks
// again when it stops failing". The two halves asserted here:
//
//	while failing   → ONE deep request, the first cycle's; and never the seeded mark
//	when it stops   → the deep pass returns, within the backoff ceiling
func TestSeed_ASymbolWhoseWriteKeepsFailingStillOwesTheDeepPassAndAsksBackWithinTheCeiling(t *testing.T) {
	poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{"1d"}, map[string]string{
		"/future-markets":           coinalyzeDoisMercadosBody,
		"/open-interest-history":    coinalyzeDoisOpenInterestBody,
		"/funding-rate-history":     coinalyzeDoisFundingBody,
		"/long-short-ratio-history": coinalyzeDoisLongShortBody,
	})
	poller.symbolsFunc = func(context.Context) []string { return []string{"BTC/USDT", "ETH/USDT"} }
	store := &storeQueFalhaPorSimbolo{falhaEm: "BTC/USDT", err: errors.New("durable tier unavailable")}
	poller.store = store

	const profunda = int64(2000 * 86400)
	const ciclosFalhando = 3

	for i := 1; i <= ciclosFalhando; i++ {
		if err := poller.PollOnce(context.Background()); err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
	}

	profundos := 0
	for pedido, janela := range janelasPorSimbolo(t, fake.queriesFor("/open-interest-history")) {
		if janela >= profunda {
			profundos++
			if !strings.Contains(pedido, "BTCUSDT_PERP.A") {
				t.Errorf("the deep window was requested by a symbol that is not failing: %q", pedido)
			}
		}
	}
	// ONE DEEP REQUEST ACROSS THREE FAILING CYCLES: the first cycle's, when nothing had failed
	// yet. Repeating it every cycle was the defect — the plan is deterministic and the symbol never
	// leaves seeding, so the next two cycles emitted the SAME 2000-bucket request.
	if profundos != 1 {
		t.Fatalf("the chronically failing symbol had %d deep request(s) across %d failing cycles; "+
			"with backoff it is ONE — the same pace is the defect removed here", profundos, ciclosFalhando)
	}
	// AND IT WAS NEVER MARKED AS SEEDED — why the deep pass remains owed, and why the mark is per
	// WRITTEN symbol and not per attempt.
	poller.seededMu.Lock()
	_, marcado := poller.seededSymbols["BTC/USDT"]
	poller.seededMu.Unlock()
	if marcado {
		t.Fatal("the symbol was marked as seeded without ever having written the deep pass")
	}

	// Storage returns, and the deep pass RETURNS with it. The backoff ceiling is the verifiable
	// promise: it cannot stay postponed forever, because it is the only path by which that symbol's
	// 2000 points arrive.
	store.paraDeFalhar()
	tetoDoRecuo := coinalyzeSeedBackoffCycles(coinalyzeSeedBackoffSteps + 1)
	voltaEm := 0
	for i := 1; i <= tetoDoRecuo+1; i++ {
		antes := len(fake.queriesFor("/open-interest-history"))
		if err := poller.PollOnce(context.Background()); err != nil {
			t.Fatalf("recovery cycle %d: %v", i, err)
		}
		janelas := janelasPorSimbolo(t, fake.queriesFor("/open-interest-history")[antes:])
		if got, pediu := janelas["BTCUSDT_PERP.A"]; pediu && got == profunda {
			voltaEm = i
			break
		}
	}
	if voltaEm == 0 {
		t.Fatalf("the symbol that never wrote did not request the deep pass again within %d cycles "+
			"after it stopped failing: backoff became abandonment of the history", tetoDoRecuo+1)
	}
}
