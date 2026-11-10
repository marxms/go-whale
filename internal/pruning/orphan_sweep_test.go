package pruning

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// ============================================================================
// A REAL KEYSPACE, NOT A LIST OF CALLS
// ============================================================================
//
// The doubles in this package return keys by pattern and record what was requested. That answers
// "what did the sweep order deleted", which is NOT this task's question — it is "what remained".
// A mutant that orders the right family plus one more passes a call-recording double and fails a
// keyspace that applies the removals.
//
// So this double keeps state: `UNLINK` removes, `SCAN` returns only what still exists, and the
// test checks the SURVIVING SET against the seeded one.
type fakeKeyspace struct {
	keys map[string]struct{}
	// pageSize is the SCAN page size, which is NOT the UNLINK batch size: precisely because they
	// are independent, the batch has to accumulate across pages.
	pageSize int
	// iter stores the list of ONE iteration, captured when the cursor returns to 0.
	//
	// It is the correct SCAN model, not convenience: Redis guarantees returning every key present
	// from the start to the end of the iteration, so deleting during the scan does not make the
	// cursor skip neighbors. A double that recomputed the list on every page would reproduce a
	// Redis that does not exist — and would fail the sweep for a defect in the double.
	iter        map[string][]string
	unlinkCalls [][]string
}

func newFakeKeyspace(pageSize int) *fakeKeyspace {
	return &fakeKeyspace{keys: map[string]struct{}{}, pageSize: pageSize}
}

func (f *fakeKeyspace) add(keys ...string) {
	for _, k := range keys {
		f.keys[k] = struct{}{}
	}
}

func (f *fakeKeyspace) sorted() []string {
	out := make([]string, 0, len(f.keys))
	for k := range f.keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// globToRegexp reproduces the rule that matters here: in Redis `*` crosses `:`.
func globToRegexp(pattern string) *regexp.Regexp {
	var b strings.Builder
	b.WriteString("^")
	for _, r := range pattern {
		if r == '*' {
			b.WriteString(".*")
			continue
		}
		b.WriteString(regexp.QuoteMeta(string(r)))
	}
	b.WriteString("$")
	return regexp.MustCompile(b.String())
}

func (f *fakeKeyspace) ScanKeys(_ context.Context, cursor uint64, pattern string) ([]string, uint64, error) {
	if cursor == 0 {
		re := globToRegexp(pattern)
		var matched []string
		for _, k := range f.sorted() {
			if re.MatchString(k) {
				matched = append(matched, k)
			}
		}
		if f.iter == nil {
			f.iter = map[string][]string{}
		}
		f.iter[pattern] = matched
	}
	matched := f.iter[pattern]
	start := int(cursor) //nolint:gosec // G115: cursor is this fake's own page index, bounded by len(matched).
	if start >= len(matched) {
		return nil, 0, nil
	}
	end := start + f.pageSize
	if end >= len(matched) {
		return matched[start:], 0, nil
	}
	return matched[start:end], uint64(end), nil //nolint:gosec // G115: end is a non-negative slice index.
}

func (f *fakeKeyspace) UnlinkKeys(_ context.Context, keys ...string) (int64, error) {
	f.unlinkCalls = append(f.unlinkCalls, append([]string(nil), keys...))
	var n int64
	for _, k := range keys {
		if _, ok := f.keys[k]; ok {
			delete(f.keys, k)
			n++
		}
	}
	return n, nil
}

// ============================================================================
// THE ORPHAN SWEEP, AND THE GUARD THAT MAKES IT SAFE
// ============================================================================
//
// An earlier version of this sweep's test passed for a sweeper that deletes 100%. It seeded one
// orphan and demanded zero survivors OF IT — a claim that "delete everything" satisfies. What
// distinguishes the sweep from disaster is the other side: how many ACTIVE keys survived. That is
// why every case here seeds N active + M orphans and asserts the whole surviving set.

// symbolFamilies returns ONE symbol's keys across all swept families.
func symbolFamilies(symbol string) []string {
	return []string{
		fmt.Sprintf("indicators:binance:future:%s:1h:wt1", symbol),
		fmt.Sprintf("indicators:%s:1h:mf", symbol),
		fmt.Sprintf("analyzer:lastAnalyzedBar:%s:1h", symbol),
		fmt.Sprintf("signalEvidence:%s:1h", symbol),
		fmt.Sprintf("signalEvidence:market-cipher-reversal:%s:1h", symbol),
		fmt.Sprintf("derivatives:snapshot:latest:binance:future:%s", symbol),
		fmt.Sprintf("last_trade:binance:future:%s", symbol),
	}
}

func symbolsNamed(prefix string, n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("%s%03d/USDT", prefix, i))
	}
	return out
}

// seedFleet seeds each symbol's families and returns the keyspace.
func seedFleet(pageSize int, symbols ...[]string) *fakeKeyspace {
	f := newFakeKeyspace(pageSize)
	for _, grupo := range symbols {
		for _, s := range grupo {
			f.add(symbolFamilies(s)...)
		}
	}
	return f
}

func survivorsOf(f *fakeKeyspace, symbols []string) []string {
	var vivos []string
	for _, s := range symbols {
		for _, k := range symbolFamilies(s) {
			if _, ok := f.keys[k]; ok {
				vivos = append(vivos, k)
			}
		}
	}
	sort.Strings(vivos)
	return vivos
}

// TestOrphanSweep_NActiveAndMOrphansLeavesExactlyN is THE case.
//
// KILLS the sweeper that deletes 100% — which passed the earlier test ("seed 1 orphan, demand 0
// survivors"), because deleting everything satisfies that claim.
func TestOrphanSweep_NActiveAndMOrphansLeavesExactlyN(t *testing.T) {
	t.Parallel()

	ativas := symbolsNamed("ACT", 120)
	orfas := symbolsNamed("ORF", 30)
	f := seedFleet(100, ativas, orfas)

	p := &Pruner{redis: f, floor: &fakeAnchorFloor{value: 120, known: true}, gatesApproved: allowAllGates}
	if err := p.PruneRedis(context.Background(), ativas); err != nil {
		t.Fatalf("PruneRedis: %v", err)
	}

	vivos := survivorsOf(f, ativas)
	if len(vivos) != len(ativas)*len(symbolFamilies("x")) {
		t.Fatalf("%d active key(s) survived, wanted %d — the sweep ate live data",
			len(vivos), len(ativas)*len(symbolFamilies("x")))
	}
	if mortas := survivorsOf(f, orfas); len(mortas) != 0 {
		t.Fatalf("%d orphan key(s) survived, wanted 0: %v", len(mortas), mortas[:1])
	}
}

// TestOrphanSweep_TheIndicatorFamilyIsSwept: 95.8 of the orphan cohort's 97.3 MiB sit in
// `indicators`, and `DerivativesKeyPatterns()` did not sweep it.
//
// KILLS the sweep that earns the guard but not the coverage — the one that would leave 98% of the
// reclaimable space standing while the report says "clean".
func TestOrphanSweep_TheIndicatorFamilyIsSwept(t *testing.T) {
	t.Parallel()

	ativas := symbolsNamed("ACT", 10)
	orfas := symbolsNamed("ORF", 4)
	f := seedFleet(100, ativas, orfas)

	p := &Pruner{redis: f, floor: &fakeAnchorFloor{value: 10, known: true}, gatesApproved: allowAllGates}
	if err := p.PruneRedis(context.Background(), ativas); err != nil {
		t.Fatalf("PruneRedis: %v", err)
	}

	for _, familia := range []string{"indicators:", "analyzer:lastAnalyzedBar:", "signalEvidence:"} {
		var orfasVivas []string
		for k := range f.keys {
			if !strings.HasPrefix(k, familia) {
				continue
			}
			for _, orfa := range orfas {
				if strings.Contains(k, orfa) {
					orfasVivas = append(orfasVivas, k)
				}
			}
		}
		if len(orfasVivas) != 0 {
			t.Errorf("family %s kept %d orphan key(s): %v", familia, len(orfasVivas), orfasVivas)
		}
		var ativasVivas int
		for k := range f.keys {
			if strings.HasPrefix(k, familia) {
				ativasVivas++
			}
		}
		if ativasVivas == 0 {
			t.Errorf("family %s was left empty — the active keys went with it", familia)
		}
	}
}

// TestOrphanSweep_CandlesAreNotSwept is the retention rule in code.
//
// 95,811 members of `candles:*:1d` and 33,379 of `:1w` predate QuestDB coverage and exist ONLY in
// Redis. The 112 orphan candle keys are worth a fraction of a MiB of the cohort's 97.3; deleting
// them before migration trades a sole copy for ~0 MiB.
//
// KILLS the sweep that includes `candles` along with the other "missing" families.
func TestOrphanSweep_CandlesAreNotSwept(t *testing.T) {
	t.Parallel()

	ativas := symbolsNamed("ACT", 10)
	orfas := symbolsNamed("ORF", 4)
	f := seedFleet(100, ativas, orfas)
	for _, s := range append(append([]string{}, ativas...), orfas...) {
		f.add("candles:binance:future:"+s+":1d", "candles:binance:future:"+s+":1w")
	}

	p := &Pruner{redis: f, floor: &fakeAnchorFloor{value: 10, known: true}, gatesApproved: allowAllGates}
	if err := p.PruneRedis(context.Background(), ativas); err != nil {
		t.Fatalf("PruneRedis: %v", err)
	}

	for _, s := range orfas {
		for _, tf := range []string{"1d", "1w"} {
			k := "candles:binance:future:" + s + ":" + tf
			if _, viva := f.keys[k]; !viva {
				t.Fatalf("the sweep deleted %s — a sole copy before migration", k)
			}
		}
	}
}

// TestOrphanSweep_AnEmptyAnchorRemovesNothing: an empty anchor is the normal state of a boot
// before discovery, and the right answer is to do nothing.
//
// KILLS the sweeper that treats an empty active set as "everything is an orphan".
func TestOrphanSweep_AnEmptyAnchorRemovesNothing(t *testing.T) {
	t.Parallel()

	orfas := symbolsNamed("ORF", 12)
	f := seedFleet(100, orfas)

	p := &Pruner{redis: f, floor: &fakeAnchorFloor{value: 500, known: true}, gatesApproved: allowAllGates}
	if err := p.PruneRedis(context.Background(), nil); err != nil {
		t.Fatalf("PruneRedis: %v", err)
	}
	if len(f.unlinkCalls) != 0 {
		t.Fatalf("the pass with an empty anchor issued %d UNLINK(s)", len(f.unlinkCalls))
	}
	if len(survivorsOf(f, orfas)) != len(orfas)*len(symbolFamilies("x")) {
		t.Fatal("the pass with an empty anchor removed keys")
	}
}

// TestOrphanSweep_AnAnchorBelowNinetyPercentOfTheFloorRemovesNothing: after the flip to
// `allkeys-lru` the anchor itself becomes evictable, and a partial anchor is indistinguishable
// from a fleet that shrank — except by the floor.
//
// KILLS the sweeper that only checks for an EMPTY set. The sibling scopes `binance:spot` and
// `bingx:*` have 1 member each in production: "short but not empty" is not hypothetical.
func TestOrphanSweep_AnAnchorBelowNinetyPercentOfTheFloorRemovesNothing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		nome   string
		vistas int
		piso   int
		remove bool
	}{
		{"449 against floor 500 (89.8%)", 449, 500, false},
		{"1 against floor 500", 1, 500, false},
		{"450 against floor 500 (90.0%)", 450, 500, true},
		{"500 against floor 500", 500, 500, true},
	} {
		tc := tc
		t.Run(tc.nome, func(t *testing.T) {
			t.Parallel()

			ativas := symbolsNamed("ACT", tc.vistas)
			orfas := symbolsNamed("ORF", 4)
			f := seedFleet(200, ativas, orfas)

			p := &Pruner{redis: f, floor: &fakeAnchorFloor{value: tc.piso, known: true}, gatesApproved: allowAllGates}
			if err := p.PruneRedis(context.Background(), ativas); err != nil {
				t.Fatalf("PruneRedis: %v", err)
			}

			removeu := len(f.unlinkCalls) > 0
			if removeu != tc.remove {
				t.Fatalf("removed=%v, wanted %v (anchor=%d, floor=%d)", removeu, tc.remove, tc.vistas, tc.piso)
			}
			if !tc.remove && len(survivorsOf(f, orfas)) == 0 {
				t.Fatal("the refusal removed the orphans anyway")
			}
		})
	}
}

// TestOrphanSweep_ARefusalDoesNotLowerTheFloor is rule 3, and it is what keeps the guard from
// disarming itself: if a short pass became the new floor, the next one would delete with its
// blessing.
//
// KILLS the sweeper that persists the observed cardinality BEFORE deciding.
func TestOrphanSweep_ARefusalDoesNotLowerTheFloor(t *testing.T) {
	t.Parallel()

	curtas := symbolsNamed("ACT", 12)
	orfas := symbolsNamed("ORF", 4)
	f := seedFleet(200, curtas, orfas)
	piso := &fakeAnchorFloor{value: 500, known: true}

	p := &Pruner{redis: f, floor: piso, gatesApproved: allowAllGates}
	for volta := 1; volta <= 2; volta++ {
		if err := p.PruneRedis(context.Background(), curtas); err != nil {
			t.Fatalf("PruneRedis pass %d: %v", volta, err)
		}
		if len(f.unlinkCalls) != 0 {
			t.Fatalf("pass %d removed with the short anchor", volta)
		}
	}
	if piso.value != 500 {
		t.Fatalf("the floor became %d after two refusals, wanted 500", piso.value)
	}
	if len(piso.writes) != 0 {
		t.Fatalf("the refusal wrote the floor %v", piso.writes)
	}
}

// TestOrphanSweep_TheFirstPassLearnsTheFloorAndRemovesNothing: without a floor there is nothing
// to compare against, and it is right after an eviction of the floor key itself that the anchor
// is most likely to be short too.
//
// KILLS the sweeper that treats a missing floor as floor zero — zero passes any comparison.
func TestOrphanSweep_TheFirstPassLearnsTheFloorAndRemovesNothing(t *testing.T) {
	t.Parallel()

	ativas := symbolsNamed("ACT", 120)
	orfas := symbolsNamed("ORF", 4)
	f := seedFleet(200, ativas, orfas)
	piso := &fakeAnchorFloor{}

	p := &Pruner{redis: f, floor: piso, gatesApproved: allowAllGates}
	if err := p.PruneRedis(context.Background(), ativas); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if len(f.unlinkCalls) != 0 {
		t.Fatalf("the first pass removed %d batch(es) with no recorded floor", len(f.unlinkCalls))
	}
	if piso.value != 120 || !piso.known {
		t.Fatalf("the first pass did not record the floor: %+v", piso)
	}

	// AND THE SECOND ONE SWEEPS — without this control, "never sweeps" would satisfy the case
	// above.
	if err := p.PruneRedis(context.Background(), ativas); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if len(survivorsOf(f, orfas)) != 0 {
		t.Fatal("the second pass, now with a floor, did not remove the orphans")
	}
	if len(survivorsOf(f, ativas)) != len(ativas)*len(symbolFamilies("x")) {
		t.Fatal("the second pass ate active keys")
	}
}

// TestOrphanSweep_AnUnreadableFloorAbortsWithoutRemoving: sweeping without being able to read
// the floor is sweeping without a guard.
func TestOrphanSweep_AnUnreadableFloorAbortsWithoutRemoving(t *testing.T) {
	t.Parallel()

	ativas := symbolsNamed("ACT", 120)
	orfas := symbolsNamed("ORF", 4)
	f := seedFleet(200, ativas, orfas)

	p := &Pruner{redis: f, floor: &fakeAnchorFloor{readErr: errors.New("redis down")}, gatesApproved: allowAllGates}
	if err := p.PruneRedis(context.Background(), ativas); err == nil {
		t.Fatal("the pass continued with an unreadable floor")
	}
	if len(f.unlinkCalls) != 0 {
		t.Fatalf("the pass removed %d batch(es) with an unreadable floor", len(f.unlinkCalls))
	}
	if len(survivorsOf(f, orfas)) == 0 {
		t.Fatal("the orphans vanished despite the abort")
	}
}

// TestOrphanSweep_StopsAtThePassCap: the cap does not contain cleanup (the measured cohort is
// 5,303 and converges in three passes), it contains the DEFECT — a wrong parse would mark all
// 81,860 keys as orphans.
//
// KILLS the sweeper without a cap, whose worst case is the whole keyspace in one pass.
func TestOrphanSweep_StopsAtThePassCap(t *testing.T) {
	t.Parallel()

	ativas := symbolsNamed("ACT", 500)
	// 400 orphans × 7 families = 2,800 keys, above the 2,000 cap.
	orfas := symbolsNamed("ORF", 400)
	f := seedFleet(500, ativas, orfas)

	p := &Pruner{redis: f, floor: &fakeAnchorFloor{value: 500, known: true}, gatesApproved: allowAllGates}
	if err := p.PruneRedis(context.Background(), ativas); err != nil {
		t.Fatalf("PruneRedis: %v", err)
	}

	var removidas int
	for _, call := range f.unlinkCalls {
		removidas += len(call)
	}
	if removidas > maxUnlinksPerPass {
		t.Fatalf("the pass removed %d key(s), above the cap of %d", removidas, maxUnlinksPerPass)
	}
	if removidas == 0 {
		t.Fatal("the pass removed nothing — the cap became a refusal")
	}
	if len(survivorsOf(f, ativas)) != len(ativas)*len(symbolFamilies("x")) {
		t.Fatal("the cap was spent on active keys")
	}
}

// TestExtractSymbolFromKey_TheAnalyzerFamilies pins the positional parse of the three families,
// INCLUDING the forms it must refuse.
//
// A wrong symbol here deletes live data, and the two `indicators` variants differ precisely in
// where the symbol falls: the scoped one puts it at parts[3], the legacy one at parts[1].
func TestExtractSymbolFromKey_TheAnalyzerFamilies(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		nome string
		key  string
		want string
	}{
		{"scoped indicator", "indicators:binance:future:BTC/USDT:1h:wt1", "BTC/USDT"},
		{"legacy indicator", "indicators:BTC/USDT:1h:wt1", "BTC/USDT"},
		{"indicator with unknown part count", "indicators:binance:future:BTC/USDT:1h", ""},
		{"lastAnalyzedBar", "analyzer:lastAnalyzedBar:BTC/USDT:1h", "BTC/USDT"},
		{"another analyzer key", "analyzer:somethingElse:BTC/USDT:1h", ""},
		{"signalEvidence from the default producer", "signalEvidence:BTC/USDT:1h", "BTC/USDT"},
		{"signalEvidence with a producer", "signalEvidence:market-cipher-reversal:BTC/USDT:1h", "BTC/USDT"},
		{"signalEvidence with unknown part count", "signalEvidence:a:b:c:d", ""},
		{"candle is NOT swept", "candles:binance:future:BTC/USDT:1d", ""},
	} {
		tc := tc
		t.Run(tc.nome, func(t *testing.T) {
			t.Parallel()
			if got := extractSymbolFromKey(tc.key); got != tc.want {
				t.Fatalf("extractSymbolFromKey(%q) = %q, wanted %q", tc.key, got, tc.want)
			}
		})
	}
}
