package questdb

import (
	"context"
	"errors"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/derivatives"
)

// emptyPriorSource is the empty table: nothing written, nothing to erode.
type emptyPriorSource struct{}

func (emptyPriorSource) QueryDerivativesRange(context.Context, derivatives.DerivativesScope, time.Time, time.Time) (
	[]*derivatives.DerivativesSnapshot, error,
) {
	return nil, nil
}

// recordingPriorSource returns already written rows and records what was asked.
type recordingPriorSource struct {
	rows  []*derivatives.DerivativesSnapshot
	err   error
	calls []derivatives.DerivativesScope
	spans []bucketSpan
}

func (r *recordingPriorSource) QueryDerivativesRange(_ context.Context, scope derivatives.DerivativesScope,
	from, to time.Time,
) ([]*derivatives.DerivativesSnapshot, error) {
	r.calls = append(r.calls, scope)
	r.spans = append(r.spans, bucketSpan{from: from, to: to})
	if r.err != nil {
		return nil, r.err
	}
	return r.rows, nil
}

// TestWriteDerivativesBatch_APartialRewriteDoesNotEraseWhatWasThere — the measured defect:
// write a=1,b=2 → read (1,2); write a=9 to the same bucket → read (9, NULL). ILP has no NULL,
// so omitting is erasing.
//
// The degraded cycle is what produces the partial row: one of the calls did not return, and
// the bucket is rewritten with fewer columns than it already had.
func TestWriteDerivativesBatch_APartialRewriteDoesNotEraseWhatWasThere(t *testing.T) {
	bucket := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)
	prior := &recordingPriorSource{rows: []*derivatives.DerivativesSnapshot{{
		Timestamp:        bucket.UnixMilli(),
		OpenInterestBase: fp(106117.954),
		FundingRate:      fp(0.0001),
		LongShortRatio:   fp(0.9554),
	}}}

	sender := newFakeLineSender()
	store := &Store{ilpSender: sender, tableName: "candles", priors: prior}

	// The degraded cycle: only open interest returned.
	parcial := samplePoints(1)
	parcial[0].Timestamp = bucket
	parcial[0].OpenInterestBase = fp(200000)
	parcial[0].FundingRate = nil
	parcial[0].LongShortRatio = nil

	if err := store.WriteDerivativesBatch(context.Background(), parcial); err != nil {
		t.Fatalf("WriteDerivativesBatch: %v", err)
	}
	if len(sender.lines) != 1 {
		t.Fatalf("%d line(s) emitted, wanted 1", len(sender.lines))
	}
	linha := sender.lines[0]

	// The NEW value wins: it is this cycle's observation.
	if got := linha.floats["open_interest_base"]; got != 200000 {
		t.Fatalf("open_interest_base = %v, wanted 200000 — the value observed now", got)
	}
	// The OLD ones survive the rewrite instead of becoming NULL.
	if got, present := linha.floats["funding_rate"]; !present || got != 0.0001 {
		t.Fatalf("funding_rate = %v (present %v), wanted 0.0001 — the rewrite erased what "+
			"the row already carried", got, present)
	}
	if got, present := linha.floats["long_short_ratio"]; !present || got != 0.9554 {
		t.Fatalf("long_short_ratio = %v (present %v), wanted 0.9554", got, present)
	}

	// The source of truth was queried IN the row's SCOPE, not over the whole table.
	if len(prior.calls) != 1 {
		t.Fatalf("%d query(ies) to the source of truth, wanted 1 per scope", len(prior.calls))
	}
	if prior.calls[0].Symbol != "BTC/USDT" || prior.calls[0].Timeframe != "1d" ||
		prior.calls[0].Source != "coinalyze" {
		t.Fatalf("queried scope = %+v", prior.calls[0])
	}
	if !prior.spans[0].from.Equal(bucket) || !prior.spans[0].to.Equal(bucket) {
		t.Fatalf("queried span = %v..%v, wanted the bucket the batch rewrites (%v)",
			prior.spans[0].from, prior.spans[0].to, bucket)
	}
}

// TestPreserveOmittedColumns_ABucketThatDoesNotExistYetIsWrittenAsIs — there is nothing to
// preserve in a new bucket, and refusing it would swap erosion for an invented hole.
func TestPreserveOmittedColumns_ABucketThatDoesNotExistYetIsWrittenAsIs(t *testing.T) {
	prior := &recordingPriorSource{} // the table answers, and has nothing in that range

	parcial := samplePoints(1)
	parcial[0].FundingRate = nil

	out, outcome := preserveOmittedColumns(context.Background(), prior, parcial)
	if len(out) != 1 {
		t.Fatalf("%d row(s) to write, wanted 1", len(out))
	}
	if out[0].FundingRate != nil {
		t.Fatalf("funding_rate = %v invented in a bucket that did not exist", *out[0].FundingRate)
	}
	if outcome.preserved != 0 || outcome.holes != 0 {
		t.Fatalf("outcome = %+v, wanted nothing preserved and no holes", outcome)
	}
}

// TestPreserveOmittedColumns_AnUnreadableTruthChoosesTheHoleOverTheErosion — when the source
// of truth does not answer, the mechanism does NOT KNOW what the row carried, and writing
// anyway would erase whatever is there.
//
// The choice is declared and counted PER ROW: the old value stays, the new one is lost.
func TestPreserveOmittedColumns_AnUnreadableTruthChoosesTheHoleOverTheErosion(t *testing.T) {
	prior := &recordingPriorSource{err: errors.New("questdb down")}

	pontos := samplePoints(2)
	pontos[0].FundingRate = nil // incomplete: at risk
	// the second carries everything, including 8h AND the unit group — complete, and not at
	// risk
	completar(&pontos[1])

	out, outcome := preserveOmittedColumns(context.Background(), prior, pontos)

	if outcome.holes != 1 {
		t.Fatalf("holes = %d, wanted 1 — the incomplete row had to be discarded instead "+
			"of rewriting the bucket without knowing what was in it", outcome.holes)
	}
	if outcome.preserved != 0 {
		t.Fatalf("preserved = %d without being able to read what was there", outcome.preserved)
	}
	if len(out) != 1 {
		t.Fatalf("%d row(s) to write, wanted 1 — the complete one is not at risk and cannot "+
			"be punished for the read failure", len(out))
	}
	if out[0].FundingRate8h == nil {
		t.Fatal("the row that remained is not the complete one")
	}
}

// TestPreserveOmittedColumns_ACompleteBatchCostsNoRead — the read is targeted, and the
// healthy case does not pay for it. A row carrying the four quantities cannot erode anything.
func TestPreserveOmittedColumns_ACompleteBatchCostsNoRead(t *testing.T) {
	prior := &recordingPriorSource{}

	completos := samplePoints(3)
	for i := range completos {
		completar(&completos[i])
	}

	out, outcome := preserveOmittedColumns(context.Background(), prior, completos)
	if len(prior.calls) != 0 {
		t.Fatalf("%d query(ies) for a batch in which no row omits a column", len(prior.calls))
	}
	if len(out) != 3 || outcome.degraded() {
		t.Fatalf("output = %d rows, outcome = %+v", len(out), outcome)
	}
}

// TestPreserveOmittedColumns_TheObservedValueBeatsThePreviousOne — preservation is for the
// OMITTED column. A present column is this cycle's observation, and keeping it would freeze
// the series at the first value it ever had.
func TestPreserveOmittedColumns_TheObservedValueBeatsThePreviousOne(t *testing.T) {
	bucket := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)
	prior := &recordingPriorSource{rows: []*derivatives.DerivativesSnapshot{{
		Timestamp:        bucket.UnixMilli(),
		OpenInterestBase: fp(1),
		FundingRate:      fp(0.0009),
	}}}

	pontos := samplePoints(1)
	pontos[0].Timestamp = bucket
	pontos[0].FundingRate = fp(0.0002) // observed NOW
	pontos[0].LongShortRatio = nil

	out, outcome := preserveOmittedColumns(context.Background(), prior, pontos)
	if out[0].FundingRate == nil || *out[0].FundingRate != 0.0002 {
		t.Fatalf("funding_rate = %v, wanted the value observed now (0.0002)", out[0].FundingRate)
	}
	if out[0].OpenInterestBase == nil || *out[0].OpenInterestBase == 1 {
		t.Fatalf("open_interest_base = %v — the new row carried it and it was overwritten by the old one",
			out[0].OpenInterestBase)
	}
	if outcome.preserved != 0 {
		t.Fatalf("preserved = %d, wanted 0 — nothing the new row omits existed in the previous one",
			outcome.preserved)
	}
}

// TestPreserveOmittedColumns_TwoScopesAreReadSeparately — the batch can carry more than one
// scope, and one symbol's previous row says nothing about the other's.
func TestPreserveOmittedColumns_TwoScopesAreReadSeparately(t *testing.T) {
	prior := &recordingPriorSource{}

	pontos := samplePoints(2)
	pontos[0].FundingRate = nil
	pontos[1].Symbol = "ETH/USDT"
	pontos[1].FundingRate = nil

	if _, _ = preserveOmittedColumns(context.Background(), prior, pontos); len(prior.calls) != 2 {
		t.Fatalf("%d query(ies) for two scopes at risk, wanted 2", len(prior.calls))
	}
	vistos := map[string]bool{}
	for _, scope := range prior.calls {
		vistos[scope.Symbol] = true
	}
	if !vistos["BTC/USDT"] || !vistos["ETH/USDT"] {
		t.Fatalf("queried scopes = %v", vistos)
	}
}

// TestErosionGuardedColumns_MatchesWhatIsActuallyCarriedForward keeps the declared list from
// becoming prose.
//
// `ErosionGuardedColumns` feeds the boot gate, and a gate fed by a hand-written list protects
// only while someone remembers to update it. Here the list is checked against BEHAVIOR: a
// previous row with every quantity, a new row with none, and the set actually filled must be
// EQUAL to the declared one.
func TestErosionGuardedColumns_MatchesWhatIsActuallyCarriedForward(t *testing.T) {
	prior := &derivatives.DerivativesSnapshot{
		OpenInterestBase:     fp(1),
		FundingRate:          fp(2),
		FundingRate8h:        fp(3),
		LongShortRatio:       fp(4),
		FundingIntervalHours: fp(4),
		NStep:                derivatives.NFromMeasuredSpacing,
		OpenInterest:         fp(5),
		NextFundingRate:      fp(6),
		InterestRate:         fp(7),
		FundingAtAnchor:      bp(true),
		LSLongPct:            fp(48.86),
		LSShortPct:           fp(51.14),
	}
	vazia := derivatives.DerivativesColdPoint{}
	if !fillFromPrior(&vazia, prior) {
		t.Fatal("nothing was preserved in a row that omits everything")
	}

	carregadas := map[string]bool{}
	if vazia.OpenInterestBase != nil {
		carregadas["open_interest_base"] = true
	}
	if vazia.FundingRate != nil {
		carregadas["funding_rate"] = true
	}
	if vazia.FundingRate8h != nil {
		carregadas["funding_rate_8h"] = true
	}
	if vazia.LongShortRatio != nil {
		carregadas["long_short_ratio"] = true
	}
	if vazia.FundingIntervalHours != nil {
		carregadas["funding_interval_hours"] = true
	}
	if vazia.NStep != "" {
		carregadas["n_step"] = true
	}
	if vazia.OpenInterest != nil {
		carregadas["open_interest"] = true
	}
	if vazia.NextFundingRate != nil {
		carregadas["next_funding_rate"] = true
	}
	if vazia.InterestRate != nil {
		carregadas["interest_rate"] = true
	}
	if vazia.FundingAtAnchor != nil {
		carregadas["funding_at_anchor"] = true
	}
	if vazia.LSLongPct != nil {
		carregadas["ls_long_pct"] = true
	}
	if vazia.LSShortPct != nil {
		carregadas["ls_short_pct"] = true
	}

	declaradas := derivatives.ErosionGuardedColumns()
	if len(declaradas) != len(carregadas) {
		t.Fatalf("declared %v, actually carried %v — the list that feeds the gate diverged "+
			"from what the protection does", declaradas, carregadas)
	}
	for _, col := range declaradas {
		if !carregadas[col] {
			t.Errorf("%q is declared protected and is not carried forward", col)
		}
	}
}

// completar fills the row with what it lacks to be COMPLETE in `completeRow`'s sense — the
// four quantities plus the unit group.
//
// It exists because "complete" is not a synonym for "the four old columns": a row with the
// four and without `n_step` rewrites the bucket and erases the rung that was there.
func completar(p *derivatives.DerivativesColdPoint) {
	p.FundingRate8h = fp(0.0003)
	p.FundingIntervalHours = fp(8)
	p.NStep = derivatives.NFromMeasuredSpacing
	// And the SIX additive columns: "complete" grew again, for the same reason as before — a
	// row that omits `ls_long_pct` rewrites the bucket and erases the share that was there.
	p.OpenInterest = fp(5_432_100_000)
	p.NextFundingRate = fp(0.00012)
	p.InterestRate = fp(0.0001)
	p.FundingAtAnchor = bp(true)
	p.LSLongPct = fp(48.86)
	p.LSShortPct = fp(51.14)
}

// bp returns a pointer to a boolean — the predicate's `fp`.
func bp(v bool) *bool { return &v }

// TestFillFromPrior_TheUnitGroupDoesNotTravelWithoutTheNumberItDescribes is the limit of
// preservation, and it is the same pairing the write path requires.
//
// Carrying `n_step` and `funding_interval_hours` forward to a row that does NOT have funding
// would deliver the evidence of a number that row does not carry — a unit without a quantity,
// the mirror image of the defect this code exists to fix.
func TestFillFromPrior_TheUnitGroupDoesNotTravelWithoutTheNumberItDescribes(t *testing.T) {
	prior := &derivatives.DerivativesSnapshot{
		OpenInterestBase:     fp(1),
		FundingIntervalHours: fp(4),
		NStep:                derivatives.NFromMeasuredSpacing,
		// WITHOUT funding_rate: the previous bucket did not have it either.
	}
	nova := derivatives.DerivativesColdPoint{LongShortRatio: fp(0.95)}

	fillFromPrior(&nova, prior)

	if nova.OpenInterestBase == nil {
		t.Fatal("open_interest_base was not preserved — it is a quantity in its own right")
	}
	if nova.NStep != "" {
		t.Fatalf("n_step=%q preserved on a row without funding_rate", nova.NStep)
	}
	if nova.FundingIntervalHours != nil {
		t.Fatalf("funding_interval_hours=%v preserved on a row without funding_rate", *nova.FundingIntervalHours)
	}
}

// TestErosionGuardedColumns_CoversEveryDeclaredDurableColumn is the same comparison the boot
// gate makes, performed over the REAL production values.
//
// The gate is the runtime protection; this case is what makes the order fail in CI instead of
// on the boot of whoever deploys.
func TestErosionGuardedColumns_CoversEveryDeclaredDurableColumn(t *testing.T) {
	protegidas := map[string]bool{}
	for _, col := range derivatives.ErosionGuardedColumns() {
		protegidas[col] = true
	}

	var declaradas int
	for campo, coluna := range redisstore.DerivativesDurableCoverage() {
		if coluna == "" {
			continue
		}
		declaradas++
		if !protegidas[coluna] {
			t.Errorf("%s is declared covered by column %q, which the erosion protection does NOT "+
				"carry forward — dedup erases it on the first degraded cycle", campo, coluna)
		}
	}
	if declaradas == 0 {
		t.Fatal("no column declared covered — the comparison would pass vacuously")
	}
}

// TestFillFromPrior_TheEightHourEquivalentDoesNotTravelUnderARungWithoutEvidence — the
// fourth rung of the ladder applied to the PRESERVATION side, not only to the write side.
//
// THE DEFECT IT KILLS: the row came out with `n_step:inherited` AND `funding_rate_8h:0.0008`
// — an eight-hour equivalent under the rung that declares no evidence FOR THAT BUCKET. It is
// the fabrication `NInheritedOutsideWindow` exists to name, republished through the back door.
//
// THE PATH IS REACHABLE ON EVERY RESTART: the interval measurement window moves forward, so a
// bucket previously labeled `measured` — and already carrying 8h — becomes `inherited` on the
// next seeding. The new row does not bring 8h (the normalizer withholds it under `inherited`),
// the previous one does, and unconditional preservation resurrected it.
//
// THAT DEDUP ERASES THE OLD 8h IS THE RIGHT OUTCOME, not erosion: the protection against
// partial rewrite exists to save MEASUREMENT, and the row itself declares there is no cadence
// evidence here.
func TestFillFromPrior_TheEightHourEquivalentDoesNotTravelUnderARungWithoutEvidence(t *testing.T) {
	// The previous row is always the same: a bucket that already carries the 8h equivalent.
	priorCom8h := func(step derivatives.NDerivationStep) *derivatives.DerivativesSnapshot {
		return &derivatives.DerivativesSnapshot{
			OpenInterestBase:     fp(106117.954),
			FundingRate:          fp(0.0001),
			FundingRate8h:        fp(0.0008),
			FundingIntervalHours: fp(1),
			NStep:                step,
		}
	}

	tests := []struct {
		name string
		nova derivatives.DerivativesColdPoint
		// priorStep is the PREVIOUS row's rung.
		priorStep derivatives.NDerivationStep
		// want8h is the 8h equivalent the row to write must carry; nil = none.
		want8h    *float64
		wantNStep derivatives.NDerivationStep
	}{
		{
			name:      "the inherited rung does not authorize the equivalent the previous row had",
			nova:      derivatives.DerivativesColdPoint{FundingRate: fp(0.0002), FundingIntervalHours: fp(1), NStep: derivatives.NInheritedOutsideWindow},
			priorStep: derivatives.NFromMeasuredSpacing,
			want8h:    nil,
			wantNStep: derivatives.NInheritedOutsideWindow,
		},
		{
			name:      "the unavailable rung does not either",
			nova:      derivatives.DerivativesColdPoint{FundingRate: fp(0.0002), NStep: derivatives.NUnavailable},
			priorStep: derivatives.NFromMeasuredSpacing,
			want8h:    nil,
			wantNStep: derivatives.NUnavailable,
		},
		{
			name:      "the rung OMITTED in the new row is inherited from the previous one, and it decides too",
			nova:      derivatives.DerivativesColdPoint{FundingRate: fp(0.0002)},
			priorStep: derivatives.NInheritedOutsideWindow,
			want8h:    nil,
			wantNStep: derivatives.NInheritedOutsideWindow,
		},
		{
			name:      "measured preserves: it is the rung that carries evidence",
			nova:      derivatives.DerivativesColdPoint{},
			priorStep: derivatives.NFromMeasuredSpacing,
			want8h:    fp(0.0008),
			wantNStep: derivatives.NFromMeasuredSpacing,
		},
		{
			name:      "seed preserves: it normalizes by ladder declaration",
			nova:      derivatives.DerivativesColdPoint{},
			priorStep: derivatives.NFromMetadataSeed,
			want8h:    fp(0.0008),
			wantNStep: derivatives.NFromMetadataSeed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nova := tt.nova
			fillFromPrior(&nova, priorCom8h(tt.priorStep))

			switch {
			case tt.want8h == nil && nova.FundingRate8h != nil:
				t.Fatalf("funding_rate_8h = %v under n_step=%q — an eight-hour equivalent "+
					"under a rung that carries no evidence for this bucket",
					*nova.FundingRate8h, nova.NStep)
			case tt.want8h != nil && nova.FundingRate8h == nil:
				t.Fatalf("funding_rate_8h absent under n_step=%q, wanted %v preserved",
					nova.NStep, *tt.want8h)
			case tt.want8h != nil && *nova.FundingRate8h != *tt.want8h:
				t.Fatalf("funding_rate_8h = %v, wanted %v", *nova.FundingRate8h, *tt.want8h)
			}
			if nova.NStep != tt.wantNStep {
				t.Fatalf("n_step = %q, wanted %q", nova.NStep, tt.wantNStep)
			}

			// THE ROW INVARIANT, asserted in every case: whoever carries the equivalent also
			// carries a rung that authorizes it.
			if nova.FundingRate8h != nil && !nova.NStep.Normalizes() {
				t.Fatalf("incoherent row: funding_rate_8h=%v under n_step=%q",
					*nova.FundingRate8h, nova.NStep)
			}
		})
	}
}
