package candle

import "testing"

// TestIsAuthoritativeClosedTotal_UnsettledKlineMayNotReplace pins the settle-time gate. See
// docs/decisions/0011-settled-kline-authority.md.
//
// The incident: a 5m bucket was fetched 2s after its close, while the exchange was still writing
// it. The short record REPLACED a correct live reconstruction, and every detector stayed silent —
// hasGap was null, the discard counters were zero, and no trade or id was missing. The
// measurement was overwritten, not lost. "The bucket closed" and "the exchange finished writing
// the bucket" are different facts; only a fetch instant separates them.
func TestIsAuthoritativeClosedTotal_UnsettledKlineMayNotReplace(t *testing.T) {
	t.Parallel()

	const (
		tfMs   = int64(5 * 60 * 1000)
		bucket = int64(1_785_909_600_000)
	)
	closedAt := bucket + tfMs

	base := Candle{
		Timestamp:       bucket,
		Volume:          1930.90,
		TakerBuyVolume:  875.15,
		TakerSellVolume: 1055.75,
		Source:          string(CandleMergeSourceRESTTotal),
	}

	for _, tc := range []struct {
		name      string
		fetchedAt int64
		want      bool
		why       string
	}{
		{
			name:      "fetched at a 2s repair delay",
			fetchedAt: closedAt + 2_000,
			want:      false,
			why: "this is the fetch that produced the short bucket: the exchange is still " +
				"writing, so adopting it REPLACES a correct live reconstruction with a short " +
				"one and reports it as a heal",
		},
		{
			name:      "fetched inside the measured convergence window",
			fetchedAt: closedAt + 4_070,
			want:      false,
			why:       "an exchange record measured still divergent at +4.07s has not settled",
		},
		{
			name:      "fetched after the settle grace",
			fetchedAt: closedAt + KlineSettleGraceMs,
			want:      true,
			why: "the exchange has finished writing. Genuine authority MUST survive — a guard " +
				"that never lets a total settle turns the system into a ratchet that can only " +
				"ever go up, which re-elects an inflated number forever",
		},
		{
			name:      "unstamped",
			fetchedAt: 0,
			want:      false,
			why: "a producer added without a stamp must fail SAFE — into the union, which takes " +
				"a maximum and therefore cannot lower a bucket",
		},
		{
			name:      "negative stamp from a zero-value time.Time",
			fetchedAt: -6_795_364_578_871,
			want:      false,
			why: "time.Time{}.UnixMilli() is a large NEGATIVE number, not zero; rejecting it by " +
				"the sign of an overflowed constant would be an accident, so it is rejected " +
				"explicitly",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := base
			c.FetchedAt = tc.fetchedAt
			// `now` is generous throughout: the bucket has long closed in wall-clock terms, so
			// the ONLY thing under test is whether the RECORD had settled when it was fetched.
			got := IsAuthoritativeClosedTotal(c, tfMs, closedAt+3_600_000)
			if got != tc.want {
				t.Errorf("authoritative = %v, want %v — %s", got, tc.want, tc.why)
			}
		})
	}
}

// TestUnionSeedInto_UnsettledKlineCannotLowerTheBucket closes the loop at the level that
// actually damages data: not the predicate, but the merge it gates.
//
// The predicate returning false is only useful if the fallback cannot lower a bucket either.
// It cannot — the union takes a maximum — but that is a property worth pinning, because the
// whole defect was a branch that replaced where everyone assumed it maximized.
func TestUnionSeedInto_UnsettledKlineCannotLowerTheBucket(t *testing.T) {
	t.Parallel()

	const (
		tfMs   = int64(5 * 60 * 1000)
		bucket = int64(1_785_909_600_000)
	)
	closedAt := bucket + tfMs

	// What the live stream correctly assembled, matching the tape digit for digit.
	stored := Candle{
		Timestamp: bucket, Open: 0.9, High: 1.1, Low: 0.85, Close: 1.0,
		Volume: 1933.60, TakerBuyVolume: 875.15, TakerSellVolume: 1058.45,
		Source: string(CandleMergeSourceWSDelta),
	}

	// The short kline our own repair fetched 2s after the close.
	unsettled := Candle{
		Timestamp: bucket, Open: 0.9, High: 1.1, Low: 0.85, Close: 1.0,
		Volume: 1930.90, TakerBuyVolume: 875.15, TakerSellVolume: 1055.75,
		Source: string(CandleMergeSourceRESTTotal), FetchedAt: closedAt + 2_000,
	}

	got := UnionSeedInto(stored, unsettled, tfMs, closedAt+3_600_000)

	if got.Volume != 1933.60 {
		t.Errorf("volume = %v, want 1933.60 — an UNSETTLED kline lowered a bucket the live "+
			"stream had assembled correctly: the tape and the exchange's own kline both say "+
			"1933.60, with hasGap null and every discard counter at zero", got.Volume)
	}
	if got.TakerSellVolume != 1058.45 {
		t.Errorf("takerSell = %v, want 1058.45 — the whole deficit was on the sell side, which "+
			"is what a truncated tail of the bucket looks like when the last trades happen to "+
			"be one-sided", got.TakerSellVolume)
	}
}
