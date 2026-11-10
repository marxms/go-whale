package candle

import (
	"testing"
	"time"
)

// TestUnionSeedInto_LoweringMarksTheBucket makes an erased measurement declare itself, so the
// remedy that already exists can fire.
//
// # WHY THE MARK, AND WHY IT IS NOT A SWEEP
//
// The authoritative branch REPLACES. When the incoming record is smaller, a live measurement is
// discarded — legitimately (an over-count settling) or not (a record that arrived short). The
// bucket then closed declaring itself intact, and nothing revisits a bucket that never said it
// was wrong. That is the whole shape of this family of defects: not a dropped trade, an
// overwritten one, with every detector correctly silent.
//
// HasGap already has a consumer — the continuity check enqueues a targeted re-read for any
// closed bucket carrying it. HasGap is a bool and onClose fires once per bucket, so this costs
// at most one extra re-read per bucket. No job, no sweep, no reconciliation pass.
func TestUnionSeedInto_LoweringMarksTheBucket(t *testing.T) {
	t.Parallel()

	const (
		tfMs   = int64(5 * 60 * 1000)
		bucket = int64(1_785_909_600_000)
	)
	closedAt := bucket + tfMs
	settled := closedAt + KlineSettleGraceMs

	stored := Candle{
		Timestamp: bucket, Open: 1, High: 1.1, Low: 0.9, Close: 1.05,
		Volume: 1933.60, TakerBuyVolume: 875.15, TakerSellVolume: 1058.45,
		Source: string(CandleMergeSourceWSDelta),
	}
	lower := Candle{
		Timestamp: bucket, Open: 1, High: 1.1, Low: 0.9, Close: 1.05,
		Volume: 1930.90, TakerBuyVolume: 875.15, TakerSellVolume: 1055.75,
		Source: string(CandleMergeSourceRESTTotal), FetchedAt: settled,
	}

	got := UnionSeedInto(stored, lower, tfMs, settled+1_000)

	// The settlement itself must still happen: a settled record IS authority, and a guard that
	// blocked it would turn the system into a ratchet that only ever goes up.
	if got.Volume != 1930.90 {
		t.Fatalf("volume = %v, want 1930.90 — a SETTLED authoritative record must still settle",
			got.Volume)
	}
	if !got.HasGap {
		t.Error("HasGap = false after a live measurement was replaced by a smaller record — the " +
			"bucket closed claiming to be intact, so the targeted re-read that already exists " +
			"never fires and the substitution is invisible forever")
	}

	// The symmetric case must NOT mark: healing upward discards nothing.
	higher := lower
	higher.Volume = 1940.00
	higher.TakerSellVolume = 1064.85
	if up := UnionSeedInto(stored, higher, tfMs, settled+1_000); up.HasGap {
		t.Error("HasGap = true when the authoritative record was LARGER — nothing was erased, " +
			"and marking every settlement would turn one re-read per erased bucket into one " +
			"re-read per bucket, fleet-wide")
	}
}

// TestUnionSeedInto_GapConfirmationRequiresIndependentCompleteREST pins the confirmation rule:
// a second settled REST read may clear HasGap only when it is genuinely independent — a later
// request that started after the first response arrived — and carries the same measured totals.
func TestUnionSeedInto_GapConfirmationRequiresIndependentCompleteREST(t *testing.T) {
	now := time.Now().UnixMilli()
	ts := BucketTimestamp(now-4*60*60*1000, "15m")
	stored := Candle{Timestamp: ts, Open: 1, High: 2, Low: 1, Close: 2, Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4, Source: string(CandleMergeSourceRESTTotal), FetchedAt: now - 2000, HasGap: true}
	valid := stored
	valid.HasGap, valid.RequestedAt, valid.FetchedAt = false, now-1500, now-1000
	for name, alter := range map[string]func(*Candle){
		"duplicate":    func(c *Candle) { c.FetchedAt = stored.FetchedAt },
		"overlap":      func(c *Candle) { c.RequestedAt = stored.FetchedAt },
		"unstamped":    func(c *Candle) { c.RequestedAt = 0 },
		"no taker":     func(c *Candle) { c.TakerBuyVolume, c.TakerSellVolume = 0, 0 },
		"changed":      func(c *Candle) { c.Close++ },
		"incoming gap": func(c *Candle) { c.HasGap = true },
	} {
		c := valid
		alter(&c)
		if got := UnionSeedInto(stored, c, 15*60*1000, now); !got.HasGap {
			t.Errorf("%s cleared gap without independent identical settled total", name)
		}
	}
	if got := UnionSeedInto(stored, valid, 15*60*1000, now); got.HasGap {
		t.Fatalf("independent identical settled total did not confirm gap: %+v", got)
	}
}
