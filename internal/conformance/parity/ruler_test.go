// Package parity implements gate G1: our stored candles must MIRROR Binance.
//
// The invariant is "Binance is the ruler, it is the source of truth and we must be a mirror of it".
// Everything else — merge dispatch by source, batched cold commits, blind snapshot writes — is a
// means to that end. This package is the only thing that actually MEASURES it.
//
// It deliberately compares the HTTP OUTPUT contract, not internal state: what the analyzer
// consumes is what has to mirror the exchange.
//
// It is therefore NOT a substitute for the tier-transposition gate
// (internal/app/tier_transposition_test.go). /api/timeseries is a MERGED read and
// mergeCandles resolves duplicate timestamps in favor of hot, so a wrong cold row is
// shadowed by a correct hot one and this comparison passes. The two gates answer different
// questions — "do we mirror the exchange" and "do our tiers agree with each other" — and
// neither can see the other's failure.
package parity

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// THE CRITERION, AND WHY IT IS NOT A PER-BAR DELTA.
//
// This gate used to require a per-bar relative delta below 1e-9 on every field. That is a
// criterion a HEALTHY system cannot satisfy, and it is worth being precise about why, because
// the temptation is always to read a failure here as an ingestion defect.
//
// We reconstruct a kline from the aggTrade stream. An aggTrade carries ONE timestamp for a
// GROUP of fills. When such a group spans a bucket boundary Binance splits it — they see the
// individual fills — and we cannot: we own the whole group or none of it. So the boundary
// group lands entirely on one side and the neighbor is short by exactly what we gained.
// Measured: ~4,2% of boundaries, the adjacent pair sums to zero, the bias over a window is
// zero. It is not a defect and no amount of code removes it; it is the resolution limit of
// the input.
//
// A per-bar test therefore FAILS FOREVER on a correct system, which is the worst state a gate
// can be in: it gets muted, and a muted gate is not a gate. The criterion that survives the
// straddle while still catching every real defect is:
//
//	THE WINDOW SUM     Σours − Σtheirs over the sample, relative. A straddle moves volume
//	                   BETWEEN bars and cannot change the total; anything systematic — a lost
//	                   trade class, a double count, a merge that drops the tail of a bucket —
//	                   does.
//	THE UNPAIRED BAR   A bar that diverges and has NO adjacent bar diverging by the opposite
//	                   amount. A straddle always produces the pair. A bar that is simply
//	                   wrong stands alone.
//
// Never a per-bar delta on its own. That is the rule this file exists to encode.
const (
	// WindowTolerance bounds |Σours − Σtheirs| / Σtheirs.
	//
	// The number separates two MEASURED populations rather than expressing a taste:
	//
	//	healthy    2e-6 .. 2,7e-4   35 buckets x 3 symbols verified against Binance — straddle
	//	                            residue and nothing else.
	//	defective  9e-4             the volume deficit this gate exists to catch.
	//
	// 5e-4 sits 1,85x above the worst healthy observation and 1,8x below the known defect. If
	// a future window legitimately exceeds it, the honest move is to re-measure the healthy
	// population and move the number WITH the evidence — never to widen it until it is quiet.
	WindowTolerance = 5e-4

	// BootBucketCap bounds the boot-bucket exemption below. Beyond it the bar FAILS anyway.
	//
	// An exemption without a ceiling is a mute, and a muted signal is worth less than no
	// signal because it also costs attention. 5% is two orders of magnitude above the residual
	// this exemption exists to cover (measured: p99 snapshot lag 800 ms on a 300 s bucket
	// = 0,27%) and far below any real ingestion failure, which takes the bucket to tens of
	// percent. A boot bucket that drifts past this is not the known residual — it is a defect
	// wearing its clothes, and it must fail.
	BootBucketCap = 5e-2

	// BarNoiseFloor is the relative size above which one bar counts as DIVERGENT and enters
	// the pairing analysis. It is NOT a pass/fail threshold: a divergent bar is fine as long
	// as it pairs.
	//
	// Deliberately LOW. Low is safe — extra bars enter the analysis and genuine straddles get
	// excused by their partner — while high is not: a real defect smaller than the floor would
	// never be looked at. 1e-4 is far above float summation noise (~1e-12 on a 1000-trade
	// bucket) and far below anything a human would call a discrepancy.
	BarNoiseFloor = 1e-4

	// PriceTolerance is the per-bar tolerance for OHLC, judged per bar because prices obey no
	// conservation law: there is no window sum to check and no partner bar to pair with.
	//
	// Not 1e-9, for the same straddle reason: if the boundary group carried the bar's extreme,
	// the extreme moves to the neighbor and our high/low is the next print instead. That
	// displacement is bounded by the price action at a single boundary instant — basis points
	// at most. 1e-4 (1 bp) absorbs it while still failing loudly on a real OHLC defect, which
	// is never a basis point: it is a candle from the wrong bucket, symbol or scale.
	PriceTolerance = 1e-4
)

// Candle is the subset of fields G1 asserts on. Trade counts are deliberately absent: the
// field was removed from the system because nothing consumed it, and BingX does not publish
// it at all, so it can never be part of a cross-exchange mirror contract.
type Candle struct {
	Timestamp      int64
	Open           float64
	High           float64
	Low            float64
	Close          float64
	Volume         float64
	TakerBuyVolume float64

	// TakerSellVolume is compared INDEPENDENTLY rather than inferred from volume − takerBuy.
	//
	// The identity holds on Binance's side by construction, so on theirs it IS derived. It does
	// NOT hold on ours: the aggregator classifies each trade's side from the aggTrade maker flag
	// and parks the undeterminable ones in a third bucket, so our volume, buy and sell are three
	// independently accumulated numbers. Production has already been measured violating the
	// relation in the other direction (closed ws_delta buckets with buy+sell EXCEEDING volume).
	//
	// While the gate compared only {volume, takerBuy}, a bucket whose entire deficit sat in the
	// sell leg mirrored perfectly. That was not hypothetical — it is the shape of the XVG case.
	TakerSellVolume float64

	// Source is how OUR bucket was assembled. Empty on the exchange side, which never has one.
	//
	// It exists because this gate was measuring the wrong population. A bucket we seeded from a
	// REST kline IS a Binance kline; comparing it against a Binance kline is Binance against
	// itself, and it agrees by construction. Those buckets were counted as passes and diluted
	// every number this activity produced.
	//
	// The dilution is not small and it MOVES: at 1h18m of uptime the sampled 5m window was 17%
	// live and the gate reported 4,5% failing; at 2h06m the same window was 64% live and the
	// same gate on the same build reported 61%. A gate whose verdict depends on how long the
	// process has been running is not measuring the system.
	Source string
}

// Mismatch is one PRICE field of one bucket that failed the per-bar comparison.
type Mismatch struct {
	Timestamp int64
	Field     string
	Ours      float64
	Theirs    float64
	RelError  float64
}

// isSelfAssembled reports whether a bucket was built by US from the trade tape.
//
// The live producer labels every increment ws_delta; seeds and repairs label themselves as
// totals. Anything else — including an empty label — is of unknown origin and is not admitted:
// this gate's entire claim is that it compares OUR arithmetic against the exchange's, and a
// bucket that cannot say where it came from cannot support that claim.
func isSelfAssembled(source string) bool {
	// ws_delta  — we aggregated it from the tape
	// ws_kline  — the venue pushed it live and we transported it
	//
	// BOTH are candles this process produced and serves, and G1 grades WHAT WE SERVE. Admitting
	// only ws_delta made the gate unsatisfiable the moment the venue took ownership: the 5m
	// buckets became ws_kline and were excluded, the coarser ones are rest_total and were already
	// excluded, so Compared reached zero and the run failed for having nothing to compare.
	//
	// rest_total stays out, and that exclusion is the one that matters: a bucket SEEDED from a
	// Binance kline IS a Binance kline, so comparing it against a Binance kline compares Binance
	// to itself and agrees by construction. Transporting a live push is not the same act as
	// re-reading a snapshot after the fact — one is the pipeline under test, the other is the
	// answer sheet.
	return source == "ws_delta" || source == "ws_kline"
}

func (m Mismatch) String() string {
	return fmt.Sprintf("bucket %s field=%s ours=%.10f theirs=%.10f relErr=%.3g",
		time.UnixMilli(m.Timestamp).UTC().Format(time.RFC3339), m.Field, m.Ours, m.Theirs, m.RelError)
}

// BarDelta is one bucket's signed divergence in one additive field.
type BarDelta struct {
	Timestamp  int64
	Ours       float64
	Theirs     float64
	Delta      float64 // ours − theirs, SIGNED: the sign is what makes pairing possible
	RelError   float64
	PairedWith int64 // timestamp of the adjacent bar that cancels this one; 0 when none
}

func (d BarDelta) String() string {
	s := fmt.Sprintf("bucket %s ours=%.8f theirs=%.8f delta=%+.8f relErr=%.3g",
		time.UnixMilli(d.Timestamp).UTC().Format(time.RFC3339), d.Ours, d.Theirs, d.Delta, d.RelError)
	if d.PairedWith != 0 {
		s += fmt.Sprintf(" pairedWith=%s", time.UnixMilli(d.PairedWith).UTC().Format(time.RFC3339))
	}
	return s
}

// FlowReport is the verdict for ONE additive field over the whole window.
type FlowReport struct {
	Field          string
	SumOurs        float64
	SumTheirs      float64
	WindowRelError float64 // |ΣΔ| / Σtheirs

	// Divergent is every bar above BarNoiseFloor, paired or not. Reported for context: a
	// window with many divergent bars that ALL pair is a normal high-activity window, and
	// telling that apart from a window with one lonely divergence is the whole diagnosis.
	Divergent []BarDelta

	// Unpaired is the failure set: divergent bars with no adjacent opposite-signed partner
	// that cancels them.
	Unpaired []BarDelta

	// EdgeExempt counts divergent bars excused because a neighbor lies OUTSIDE the sample,
	// so the partner that would explain them could not have been observed. Reported so the
	// exemption is visible rather than silent.
	EdgeExempt int

	// BootExempt counts divergent bars excused because they are THE BUCKET THAT WAS OPEN WHEN
	// THE PROCESS STARTED. See BootBucketCap for why the exemption exists, what bounds it, and
	// why it is a separate counter rather than folded into EdgeExempt.
	BootExempt int
}

// Report is the outcome of one symbol/timeframe comparison.
type Report struct {
	Symbol    string
	Timeframe string
	Compared  int

	// SkippedNotInOurs counts exchange buckets absent from our store. Reported separately
	// from a mismatch: a missing bucket is a COVERAGE failure, not a value failure, and
	// conflating them would let a store that returns nothing at all score a perfect parity.
	SkippedNotInOurs int

	// SkippedEmptyOnExchange counts intervals the exchange published with ZERO volume and which
	// we therefore never created a candle for. It is REPORTED, never failed: it is the correct
	// representation of an untraded interval, and on illiquid symbols it is the common case.
	//
	// It is a separate field rather than silence because the two absences must stay
	// distinguishable. A rise here is a liquidity fact about the symbol; a rise in
	// SkippedNotInOurs is data we lost. Folding them would recreate exactly the ambiguity this
	// gate exists to remove.
	SkippedEmptyOnExchange int

	// SkippedNotSelfAssembled counts buckets excluded because WE did not build them from the
	// tape — they were seeded from an exchange kline. Reported, never failed: they are valid
	// data, they are simply not evidence about our aggregation.
	SkippedNotSelfAssembled int

	// Flow carries the window-sum + unpaired-bar verdict for the additive fields.
	Flow []FlowReport

	// PriceMismatches carries the per-bar OHLC verdict.
	PriceMismatches []Mismatch
}

// Pass reports whether the comparison met G1.
// Inconclusive reports whether this pair had nothing it was allowed to grade — every bucket was
// seeded, absent, or otherwise excluded. It is a COVERAGE fact about the sample, never a verdict
// about the system.
func (r Report) Inconclusive() bool { return r.Compared == 0 && r.SkippedNotInOurs == 0 }

func (r Report) Pass() bool {
	// "COULD NOT GRADE" IS NOT "FAILED", AND IT IS NOT "PASSED" EITHER.
	//
	// A pair with nothing to compare says nothing about the system. Calling it a failure buries
	// the real signal under noise — measured: 374 of 480 pairs "failed" with compared=0 while the
	// gradeable ones were nearly clean. Calling it a pass is how gate A6 published a fabricated
	// floor from an empty fetch.
	//
	// So it is INCONCLUSIVE here, and the RUN fails if it could grade nothing at all. That keeps a
	// silent no-op impossible while letting a run report what it actually measured.
	// The REASON for having nothing to grade decides which of the two this is.
	//
	//	held but excluded by PROVENANCE  → inconclusive: seeded buckets are the venue's own kline,
	//	                                   so grading them against a kline is circular
	//	NOT HELD                         → a real coverage hole, and still a failure
	//
	// Collapsing the two would let a store holding nothing at all report success, which the
	// sibling test has guarded against since before this distinction existed.
	if r.Compared == 0 && r.SkippedNotInOurs == 0 {
		return true
	}
	if r.SkippedNotInOurs > 0 || len(r.PriceMismatches) > 0 {
		return false
	}
	for _, f := range r.Flow {
		if f.WindowRelError > WindowTolerance || len(f.Unpaired) > 0 {
			return false
		}
	}
	return true
}

// Failures renders every reason the report did not pass, one per line, in a form that says
// which of the two criteria fired. An operator must never have to guess whether the window
// drifted or a single bar stands alone: they are different diagnoses with different fixes.
func (r Report) Failures() []string {
	var out []string
	if r.Compared == 0 {
		out = append(out, "SAMPLE: zero buckets compared — an empty sample is a harness failure, not a pass")
	}
	if r.SkippedNotInOurs > 0 {
		out = append(out, fmt.Sprintf("COVERAGE: %d exchange bucket(s) absent from our store", r.SkippedNotInOurs))
	}
	for _, f := range r.Flow {
		if f.WindowRelError > WindowTolerance {
			out = append(out, fmt.Sprintf(
				"WINDOW SUM %s: ours=%.8f theirs=%.8f relErr=%.3g > %.3g — the sample does not "+
					"balance. A boundary straddle moves volume between bars and cannot change the "+
					"total, so a drifting total is systematic, not resolution",
				f.Field, f.SumOurs, f.SumTheirs, f.WindowRelError, WindowTolerance))
		}
		for _, d := range f.Unpaired {
			out = append(out, fmt.Sprintf(
				"UNPAIRED BAR %s: %s — no adjacent bar diverges by the opposite amount, so this is "+
					"not the aggTrade straddle; the bar is wrong on its own",
				f.Field, d))
		}
	}
	for i, m := range r.PriceMismatches {
		if i == 10 {
			out = append(out, fmt.Sprintf("... and %d more price mismatches", len(r.PriceMismatches)-10))
			break
		}
		out = append(out, "PRICE "+m.String())
	}
	return out
}

// Summary is the one-line telemetry an operator reads first, whether or not the pair passed.
func (r Report) Summary() string {
	s := fmt.Sprintf("compared=%d selfAssembled=%d seeded=%d missing=%d emptyOnExchange=%d priceMismatches=%d", r.Compared, r.Compared, r.SkippedNotSelfAssembled, r.SkippedNotInOurs, r.SkippedEmptyOnExchange, len(r.PriceMismatches))
	for _, f := range r.Flow {
		s += fmt.Sprintf(" | %s windowRelErr=%.3g divergent=%d unpaired=%d edgeExempt=%d bootExempt=%d",
			f.Field, f.WindowRelError, len(f.Divergent), len(f.Unpaired), f.EdgeExempt, f.BootExempt)
	}
	return s
}

// additiveFields are the quantities the straddle conserves — the ones the window sum and the
// pairing rule are valid for. Everything else is a price.
func additiveFields() []struct {
	name string
	get  func(Candle) float64
} {
	return []struct {
		name string
		get  func(Candle) float64
	}{
		{"volume", func(c Candle) float64 { return c.Volume }},
		{"takerBuyVolume", func(c Candle) float64 { return c.TakerBuyVolume }},
		{"takerSellVolume", func(c Candle) float64 { return c.TakerSellVolume }},
	}
}

func priceFields() []struct {
	name string
	get  func(Candle) float64
} {
	return []struct {
		name string
		get  func(Candle) float64
	}{
		{"open", func(c Candle) float64 { return c.Open }},
		{"high", func(c Candle) float64 { return c.High }},
		{"low", func(c Candle) float64 { return c.Low }},
		{"close", func(c Candle) float64 { return c.Close }},
	}
}

// Compare matches buckets by timestamp and applies the two criteria.
//
// It compares only the intersection anchored on THEIRS: an exchange bucket we lack is a
// coverage deficit, while a bucket we hold that the exchange window does not cover is simply
// outside the sample and is ignored.
func Compare(symbol, timeframe string, ours, theirs []Candle) Report {
	return CompareAt(symbol, timeframe, ours, theirs, time.Time{})
}

// CompareAt is Compare with the process's BOOT INSTANT, which unlocks one bounded exemption.
//
// # WHY THE BOOT BUCKET NEEDS ITS OWN VERDICT
//
// The bucket that is open when the process starts cannot be reconstructed exactly, and the
// reason is structural rather than a defect to be fixed. The exchange's REST snapshot is a
// total up to the instant it was OBSERVED; the live stream covers from the instant it began
// DELIVERING. The seed waits for delivery before snapshotting so the two windows overlap, but
// the trades between the server-side observation and the local application of it belong to
// neither source. Measured in production: p50 400 ms, p99 800 ms — on a 300 s bucket, 0,27%.
//
// That is ONE bucket per symbol per boot, and every bucket after it matches to 0,0000%.
//
// # WHY THIS IS AN EXEMPTION AND NOT A LOOSER TOLERANCE
//
// Widening the per-bar threshold would excuse the same drift on EVERY bar, which is exactly
// the failure a parity gate exists to catch. The boot bucket is identifiable — it is the
// bucket containing the process start — so the exemption can name precisely the bar it
// excuses and leave every other bar under the strict rule.
//
// Three things keep it from becoming a mute:
//
//   - it applies only to the bucket containing bootAt, and only when bootAt is supplied;
//   - it is capped at BootBucketCap, so a boot bucket that is genuinely broken still fails;
//   - the delta still counts toward the WINDOW SUM, which is a second, independent backstop:
//     a boot bucket wrong by enough to matter drags Σ past WindowTolerance regardless.
//
// And it is COUNTED (FlowReport.BootExempt), printed in every summary line. An exemption
// nobody can see is indistinguishable from a bug nobody found.
//
// A zero bootAt disables it entirely — the strict rule is the default, and a caller that
// cannot establish the boot instant gets no exemption rather than a silent one.
func CompareAt(symbol, timeframe string, ours, theirs []Candle, bootAt time.Time) Report {
	rep := Report{Symbol: symbol, Timeframe: timeframe}

	byTS := make(map[int64]Candle, len(ours))
	for _, c := range ours {
		byTS[c.Timestamp] = c
	}

	// The intersection, oldest first. Sorting here rather than trusting the caller is what
	// makes "adjacent" mean adjacent in TIME instead of adjacent in the response.
	type pair struct{ ours, theirs Candle }
	matched := make([]pair, 0, len(theirs))
	for _, want := range theirs {
		got, ok := byTS[want.Timestamp]
		if !ok {
			// AN EMPTY BUCKET IS NOT A HOLE. The exchange publishes a kline for every interval
			// whether or not anything traded; an untraded one carries the previous close and a
			// volume of zero. We build candles FROM TRADES, so a bucket with no trades produces
			// no candle — the absence is the correct representation of "nothing happened", not
			// data we lost.
			//
			// Counting it as missing coverage made the gate fail on illiquid symbols for a reason
			// no code change could ever fix: CRWD/USDT had 3 zero-volume klines in a 40-bucket
			// window and HIMS/USDT had 9, and both were reported as coverage defects while every
			// value they did carry matched to float noise. That is a ruler defect of the same
			// class as the taker-sell leg this gate did not compare — a permanently red check
			// gets muted, and a muted check is not a check.
			//
			// A bucket the exchange says HAD volume and we do not hold is still a real hole and
			// still counted.
			if want.Volume == 0 {
				rep.SkippedEmptyOnExchange++
				continue
			}
			rep.SkippedNotInOurs++
			continue
		}
		// ONLY WHAT WE ASSEMBLED OURSELVES IS EVIDENCE ABOUT OUR AGGREGATION.
		//
		// A bucket carrying a total source was seeded FROM a Binance kline, so comparing it to a
		// Binance kline compares Binance to itself and agrees by construction. Including them did
		// not merely add noise — it set the verdict: the same build on the same window reported
		// 4,5% failing when 17% of the sample was live and 61% when 64% was.
		//
		// An unlabelled bucket is EXCLUDED too. Provenance is the whole point here, and admitting
		// what cannot state its own origin is how a seeded bucket would slip back in.
		if !isSelfAssembled(got.Source) {
			rep.SkippedNotSelfAssembled++
			continue
		}
		matched = append(matched, pair{ours: got, theirs: want})
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].theirs.Timestamp < matched[j].theirs.Timestamp })
	rep.Compared = len(matched)

	if rep.Compared == 0 {
		return rep
	}

	// Prices: per bar, no pairing, no sum.
	for _, f := range priceFields() {
		for _, m := range matched {
			o, t := f.get(m.ours), f.get(m.theirs)
			if rel, ok := withinTolerance(o, t, PriceTolerance); !ok {
				rep.PriceMismatches = append(rep.PriceMismatches, Mismatch{
					Timestamp: m.theirs.Timestamp, Field: f.name, Ours: o, Theirs: t, RelError: rel,
				})
			}
		}
	}
	sort.Slice(rep.PriceMismatches, func(i, j int) bool {
		if rep.PriceMismatches[i].Timestamp != rep.PriceMismatches[j].Timestamp {
			return rep.PriceMismatches[i].Timestamp < rep.PriceMismatches[j].Timestamp
		}
		return rep.PriceMismatches[i].Field < rep.PriceMismatches[j].Field
	})

	timestamps := make([]int64, len(matched))
	for i := range matched {
		timestamps[i] = matched[i].theirs.Timestamp
	}
	step := bucketStep(timestamps)

	for _, f := range additiveFields() {
		deltas := make([]BarDelta, len(matched))
		var sumOurs, sumTheirs float64
		for i, m := range matched {
			o, t := f.get(m.ours), f.get(m.theirs)
			sumOurs += o
			sumTheirs += t
			deltas[i] = BarDelta{Timestamp: m.theirs.Timestamp, Ours: o, Theirs: t, Delta: o - t, RelError: relative(o-t, t)}
		}
		rep.Flow = append(rep.Flow, buildFlowReport(f.name, sumOurs, sumTheirs, deltas, step, bootBucket(bootAt, step)))
	}

	return rep
}

// buildFlowReport applies both criteria to one additive field.
// bootBucket returns the open-time of the bucket that CONTAINED the process start, or -1
// when no boot instant was supplied (or the step is unknown), which disables the exemption.
//
// -1 rather than 0: zero is a legitimate timestamp in the type, and a sentinel that can
// collide with real data is how an exemption starts excusing bars nobody meant to excuse.
func bootBucket(bootAt time.Time, step int64) int64 {
	if bootAt.IsZero() || step <= 0 {
		return -1
	}
	return bootAt.UnixMilli() / step * step
}

func buildFlowReport(field string, sumOurs, sumTheirs float64, deltas []BarDelta, step, bootBucketTS int64) FlowReport {
	rep := FlowReport{
		Field:          field,
		SumOurs:        sumOurs,
		SumTheirs:      sumTheirs,
		WindowRelError: relative(sumOurs-sumTheirs, sumTheirs),
	}

	present := make(map[int64]int, len(deltas))
	for i := range deltas {
		present[deltas[i].Timestamp] = i
	}

	divergent := make([]bool, len(deltas))
	for i := range deltas {
		divergent[i] = isDivergent(deltas[i])
	}

	// GREEDY FORWARD PAIRING, and forward-only is correct rather than lazy.
	//
	// A straddle at the boundary between bar k and bar k+1 produces exactly one pair, always
	// adjacent, always opposite in sign. Walking left to right and only ever looking at k+1
	// finds it: if bar k was already consumed by (k−1, k) then its divergence is fully
	// explained and it is not also the left half of a second pair.
	paired := make([]bool, len(deltas))
	for i := range deltas {
		if !divergent[i] || paired[i] {
			continue
		}
		j := i + 1
		if j >= len(deltas) || paired[j] || !divergent[j] {
			continue
		}
		if step > 0 && deltas[j].Timestamp-deltas[i].Timestamp != step {
			continue
		}
		if !oppositeSign(deltas[i].Delta, deltas[j].Delta) {
			continue
		}
		// THE PAIR MUST ACTUALLY CANCEL. Opposite signs alone are not enough: a genuinely
		// broken bar sitting next to an unrelated speck of noise would otherwise be excused by
		// it. What a straddle produces is a residual near zero, so that is what is required.
		if !cancels(deltas[i], deltas[j]) {
			continue
		}
		deltas[i].PairedWith = deltas[j].Timestamp
		deltas[j].PairedWith = deltas[i].Timestamp
		paired[i], paired[j] = true, true
	}

	for i := range deltas {
		if !divergent[i] {
			continue
		}
		rep.Divergent = append(rep.Divergent, deltas[i])
		if paired[i] {
			continue
		}
		// EDGE OF THE SAMPLE. A straddle at the first or last boundary has its partner OUTSIDE
		// the window, so it can never pair no matter how healthy the system is. Failing on it
		// would make the gate flake on 2 buckets out of every 50 for a reason that has nothing
		// to do with ingestion. The delta still counts toward the window sum, which is where an
		// edge bar that is genuinely wrong shows up.
		_, hasPrev := present[deltas[i].Timestamp-step]
		_, hasNext := present[deltas[i].Timestamp+step]
		if step <= 0 || !hasPrev || !hasNext {
			rep.EdgeExempt++
			continue
		}
		// THE BUCKET THAT WAS OPEN AT BOOT. Checked LAST — after pairing, after the edge test —
		// so it can never hide a bar that some other rule would have excused for a better
		// reason, and so the counter reports only bars this exemption actually rescued.
		//
		// Capped: past BootBucketCap the bar is not the known snapshot-lag residual, it is a
		// defect wearing its clothes. And the delta still counts toward the window sum above,
		// which fails independently if the drift is large enough to matter.
		// THE BUCKET CONTAINING BOOT, AND THE ONE AFTER IT.
		//
		// One bucket was the original scope, on the assumption that delivery starts when the
		// process does. It does not: the fleet attaches in one batch and then RAMPS —
		// measured at the 05:34 boot, only 328 of 500 symbols were delivering within ten
		// seconds. For a symbol that starts speaking 40 s in, the shortfall lands in the
		// bucket AFTER the one containing process start, and on a 5-minute grid that is a
		// different bucket entirely.
		//
		// Two, never more. The ramp is bounded by the attach itself — a symbol still silent
		// after two bucket widths is not ramping, it is not delivering, and that is a
		// different failure which must not be excused here. Each is still capped by
		// BootBucketCap and still counted toward the window sum, so widening the scope does
		// not weaken either backstop.
		bootWindow := bootBucketTS >= 0 && step > 0 &&
			(deltas[i].Timestamp == bootBucketTS || deltas[i].Timestamp == bootBucketTS+step)
		if bootWindow && math.Abs(deltas[i].RelError) <= BootBucketCap {
			rep.BootExempt++
			continue
		}
		rep.Unpaired = append(rep.Unpaired, deltas[i])
	}

	return rep
}

// isDivergent reports whether a bar is large enough to enter the pairing analysis.
//
// An expected value of zero has no relative error, and an absolute epsilon there would accept
// a small-but-real volume where none should exist — so zero requires exact zero, exactly as
// the price comparison does.
func isDivergent(d BarDelta) bool {
	if d.Theirs == 0 {
		return d.Ours != 0
	}
	return math.Abs(d.Delta)/math.Abs(d.Theirs) > BarNoiseFloor
}

func oppositeSign(a, b float64) bool { return (a > 0 && b < 0) || (a < 0 && b > 0) }

// cancels reports whether two adjacent divergences sum back to (near) nothing — the signature
// of one boundary group landing on the wrong side.
func cancels(a, b BarDelta) bool {
	scale := math.Max(math.Abs(a.Theirs), math.Abs(b.Theirs))
	if scale == 0 {
		return a.Delta+b.Delta == 0
	}
	return math.Abs(a.Delta+b.Delta) <= BarNoiseFloor*scale
}

// bucketStep infers the bucket width from the sample instead of parsing the timeframe string,
// so "adjacent" stays correct even if the endpoint ever returns a different granularity than
// the label suggests. Returns 0 when the sample is too small to infer one, which disables
// pairing and routes every divergent bar to the edge exemption — conservative by construction.
func bucketStep(sorted []int64) int64 {
	var step int64
	for i := 1; i < len(sorted); i++ {
		d := sorted[i] - sorted[i-1]
		if d > 0 && (step == 0 || d < step) {
			step = d
		}
	}
	return step
}

func relative(delta, base float64) float64 {
	if base == 0 {
		return math.Abs(delta)
	}
	return math.Abs(delta) / math.Abs(base)
}

// withinTolerance compares by RELATIVE error, falling back to exact equality when the
// expected value is zero (a relative error against zero is undefined, and an absolute epsilon
// would wrongly accept a small-but-real value where none should exist).
func withinTolerance(ours, theirs, tol float64) (float64, bool) {
	if theirs == 0 {
		return math.Abs(ours), ours == 0
	}
	rel := math.Abs(ours-theirs) / math.Abs(theirs)
	return rel, rel <= tol
}

// FetchBinanceKlines reads closed klines from the Binance REST API.
//
// It drops the LAST kline unconditionally: Binance includes the bucket currently forming,
// whose volume grows between the two fetches, and comparing it would make the harness
// flap for a reason that has nothing to do with our ingestion.
func FetchBinanceKlines(client *http.Client, baseURL, rawSymbol, interval string, limit int) ([]Candle, error) {
	u := fmt.Sprintf("%s/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
		baseURL, url.QueryEscape(rawSymbol), url.QueryEscape(interval), limit)

	var raw [][]any
	if err := getJSON(client, u, &raw); err != nil {
		return nil, fmt.Errorf("binance klines %s %s: %w", rawSymbol, interval, err)
	}
	if len(raw) <= 1 {
		return nil, nil
	}
	return parseKlines(raw[:len(raw)-1])
}

// parseKlines maps the Binance array layout. Split out from the fetch so the mapping is
// unit-testable without a network.
//
// Layout: [0]=openTime [1]=open [2]=high [3]=low [4]=close [5]=volume [6]=closeTime
// [7]=quoteVol [8]=trades [9]=takerBuyBase [10]=takerBuyQuote [11]=ignore.
func parseKlines(raw [][]any) ([]Candle, error) {
	out := make([]Candle, 0, len(raw))
	for i, k := range raw {
		if len(k) < 10 {
			return nil, fmt.Errorf("kline %d: expected at least 10 fields, got %d", i, len(k))
		}
		ts, err := asInt64(k[0])
		if err != nil {
			return nil, fmt.Errorf("kline %d openTime: %w", i, err)
		}
		c := Candle{Timestamp: ts}
		for _, f := range []struct {
			dst *float64
			src any
			nm  string
		}{
			{&c.Open, k[1], "open"},
			{&c.High, k[2], "high"},
			{&c.Low, k[3], "low"},
			{&c.Close, k[4], "close"},
			{&c.Volume, k[5], "volume"},
			{&c.TakerBuyVolume, k[9], "takerBuyBase"},
		} {
			v, err := asFloat(f.src)
			if err != nil {
				return nil, fmt.Errorf("kline %d %s: %w", i, f.nm, err)
			}
			*f.dst = v
		}
		// Binance publishes only the buy leg. The sell leg is the remainder BY CONSTRUCTION on
		// their side — every trade has a taker, so there is no third bucket to account for.
		c.TakerSellVolume = c.Volume - c.TakerBuyVolume
		out = append(out, c)
	}
	return out, nil
}

// FetchOurs reads candles from the streamer's own timeseries endpoint — the same contract
// the analyzer consumes.
func FetchOurs(client *http.Client, baseURL, symbol, timeframe string, limit int) ([]Candle, error) {
	u := fmt.Sprintf("%s/api/timeseries/%s/%s?limit=%d",
		baseURL, url.PathEscape(symbol), url.PathEscape(timeframe), limit)

	var resp struct {
		Data []struct {
			Timestamp       int64   `json:"timestamp"`
			Open            float64 `json:"open"`
			High            float64 `json:"high"`
			Low             float64 `json:"low"`
			Close           float64 `json:"close"`
			Volume          float64 `json:"volume"`
			TakerBuyVolume  float64 `json:"takerBuyVolume"`
			TakerSellVolume float64 `json:"takerSellVolume"`
			Source          string  `json:"source"`
		} `json:"data"`
	}
	if err := getJSON(client, u, &resp); err != nil {
		return nil, fmt.Errorf("timeseries %s %s: %w", symbol, timeframe, err)
	}

	out := make([]Candle, 0, len(resp.Data))
	for _, d := range resp.Data {
		out = append(out, Candle{
			Timestamp: d.Timestamp, Open: d.Open, High: d.High, Low: d.Low,
			Close: d.Close, Volume: d.Volume, TakerBuyVolume: d.TakerBuyVolume,
			// Read, never derived: deriving it here would make our side satisfy the identity
			// by fiat and the comparison would be incapable of failing.
			TakerSellVolume: d.TakerSellVolume,
			Source:          d.Source,
		})
	}
	return out, nil
}

func getJSON(client *http.Client, url string, dst any) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

// asFloat / asInt64 accept both the JSON-number and JSON-string encodings — Binance sends
// prices and volumes as strings, timestamps as numbers, and a silent zero on a type change
// would show up as a fabricated parity failure.
func asFloat(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case string:
		var f float64
		if _, err := fmt.Sscanf(x, "%g", &f); err != nil {
			return 0, fmt.Errorf("unparseable %q", x)
		}
		return f, nil
	}
	return 0, fmt.Errorf("unexpected type %T", v)
}

func asInt64(v any) (int64, error) {
	switch x := v.(type) {
	case float64:
		return int64(x), nil
	case string:
		var n int64
		if _, err := fmt.Sscanf(x, "%d", &n); err != nil {
			return 0, fmt.Errorf("unparseable %q", x)
		}
		return n, nil
	}
	return 0, fmt.Errorf("unexpected type %T", v)
}

// FetchBootInstant reads the streamer's uptime and derives when the process started.
//
// It exists so the boot-bucket exemption is anchored to a fact the SYSTEM reports rather than
// to a value the harness is told. The distinction matters: an exemption parameterised by the
// caller can be pointed at any bar, and would stop being a statement about a structural limit
// and start being a way to pass.
//
// Returns the zero time on any failure, which disables the exemption at the call site.
func FetchBootInstant(client *http.Client, streamerURL string) (time.Time, error) {
	var payload struct {
		Uptime string `json:"uptime"`
	}
	if err := getJSON(client, strings.TrimRight(streamerURL, "/")+"/health", &payload); err != nil {
		return time.Time{}, err
	}
	if payload.Uptime == "" {
		return time.Time{}, fmt.Errorf("health carries no uptime field")
	}
	d, err := time.ParseDuration(payload.Uptime)
	if err != nil {
		return time.Time{}, fmt.Errorf("uptime %q is not a duration: %w", payload.Uptime, err)
	}
	return time.Now().Add(-d), nil
}
