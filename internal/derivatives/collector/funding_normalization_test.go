package collector

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"
)

const hourMs = int64(60 * 60 * 1000)

func TestParseIntervalHours(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		want  float64
		wantK bool
	}{
		{"eight hours", "8h", 8, true},
		{"four hours", "4h", 4, true},
		{"one hour", "1h", 1, true},
		{"uppercase", "8H", 8, true},
		{"padded", " 4h ", 4, true},
		{"empty", "", 0, false},
		{"no unit", "8", 0, false},
		{"minutes are not a settlement cadence", "30m", 0, false},
		{"zero is not an interval", "0h", 0, false},
		{"garbage", "abch", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseIntervalHours(tt.in)
			if ok != tt.wantK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantK)
			}
			if ok && got != tt.want {
				t.Errorf("hours = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMeasureIntervalHours(t *testing.T) {
	steady := func(gapHours int64, n int) []int64 {
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(i) * gapHours * hourMs
		}
		return out
	}

	tests := []struct {
		name  string
		in    []int64
		want  float64
		wantK bool
	}{
		{"eight-hour cadence", steady(8, 10), 8, true},
		{"four-hour cadence", steady(4, 10), 4, true},
		{"one-hour cadence", steady(1, 10), 1, true},
		{"a single settlement yields no gap", []int64{0}, 0, false},
		{"empty history", nil, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := measureIntervalHours(tt.in)
			if ok != tt.wantK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantK)
			}
			if ok && math.Abs(got-tt.want) > 1e-9 {
				t.Errorf("hours = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMeasureIntervalHours_MixtureReturnsThePrevailingCadence covers the reason the
// mode is used instead of the mean. The exchange migrates a symbol between cadences by
// rule, so a sample can straddle a change — and the mean of 8h and 1h gaps is a number
// no contract ever settled on.
func TestMeasureIntervalHours_MixtureReturnsThePrevailingCadence(t *testing.T) {
	// Two settlements at 8h, then a migration to 1h for the rest.
	ts := []int64{0, 8 * hourMs, 16 * hourMs}
	next := int64(16 * hourMs)
	for i := 0; i < 12; i++ {
		next += hourMs
		ts = append(ts, next)
	}

	got, ok := measureIntervalHours(ts)
	if !ok {
		t.Fatal("mixture produced no measurement")
	}
	if got != 1 {
		t.Fatalf("hours = %v, want 1 (the prevailing cadence, not the mean)", got)
	}
	if got == 8 {
		t.Error("returned the superseded cadence")
	}
}

// TestMeasureIntervalHours_TieResolvesToTheShorterCadence pins the DIRECTION of the
// tie-break, which the mode alone does not decide.
//
// A sample that straddles a migration can split evenly, and then two cadences are
// equally prevalent. The declared rule picks the SHORTER one, and the reason is
// asymmetric harm: the 8h-equivalent is rate × 8/N, so choosing 4h over 8h halves the
// published figure while choosing 8h over 4h doubles it. Under-normalizing a
// fast-settling symbol understates a cost the trader can still see in the raw rate;
// over-normalizing FABRICATES an extreme that was never observed and puts the symbol
// at the top of a panel sorted by cost.
//
// The magnitude of getting this backwards is 2× on the 8h-equivalent — the same order
// as the funding defect this ladder exists to prevent.
func TestMeasureIntervalHours_TieResolvesToTheShorterCadence(t *testing.T) {
	// Two 4h gaps then two 8h gaps: neither cadence prevails on count.
	ts := []int64{
		0,
		4 * hourMs,
		8 * hourMs,
		16 * hourMs,
		24 * hourMs,
	}

	got, ok := measureIntervalHours(ts)
	if !ok {
		t.Fatal("an even split produced no measurement")
	}
	if got != 4 {
		t.Fatalf("hours = %v, want 4 — a tie must resolve to the SHORTER cadence; "+
			"returning 8 would publish an 8h-equivalent 2× the true one, fabricating an extreme",
			got)
	}
}

// TestMeasureIntervalHours_JitterSnapsToKnownCadence — recorded settlement instants
// wobble by seconds; exact equality would reject nearly every real gap.
func TestMeasureIntervalHours_JitterSnapsToKnownCadence(t *testing.T) {
	ts := []int64{0}
	for i := 1; i < 10; i++ {
		jitter := int64((i%3)-1) * 3000 // ±3s
		ts = append(ts, int64(i)*4*hourMs+jitter)
	}
	got, ok := measureIntervalHours(ts)
	if !ok {
		t.Fatal("jittered history produced no measurement")
	}
	if got != 4 {
		t.Fatalf("hours = %v, want 4", got)
	}
}

// TestResolveFundingInterval_LadderOrder locks the precedence: what was observed
// outranks what was declared, and neither being available is a declared state rather
// than a default.
func TestResolveFundingInterval_LadderOrder(t *testing.T) {
	declared8h := "8h"

	t.Run("measured outranks declared", func(t *testing.T) {
		got := resolveFundingInterval(measuredCadence{Hours: 1}, true, &declared8h)
		if got.Step != derivatives.NFromMeasuredSpacing {
			t.Errorf("step = %q, want %q", got.Step, derivatives.NFromMeasuredSpacing)
		}
		if got.IntervalHours != 1 {
			t.Errorf("hours = %v, want 1 (the measured value, not the declared 8)", got.IntervalHours)
		}
	})

	t.Run("declared is used when nothing was measured", func(t *testing.T) {
		got := resolveFundingInterval(measuredCadence{}, false, &declared8h)
		if got.Step != derivatives.NFromMetadataSeed {
			t.Errorf("step = %q, want %q", got.Step, derivatives.NFromMetadataSeed)
		}
		if got.IntervalHours != 8 {
			t.Errorf("hours = %v, want 8", got.IntervalHours)
		}
	})

	t.Run("neither rung is an explicit state", func(t *testing.T) {
		got := resolveFundingInterval(measuredCadence{}, false, nil)
		if got.Step != derivatives.NUnavailable {
			t.Errorf("step = %q, want %q", got.Step, derivatives.NUnavailable)
		}
		if got.IntervalHours != 0 {
			t.Errorf("hours = %v, want 0", got.IntervalHours)
		}
	})
}

func TestApplyFundingNormalization(t *testing.T) {
	f := func(v float64) *float64 { return &v }

	t.Run("same raw number costs eight times more at one hour", func(t *testing.T) {
		at8h := &derivatives.DerivativesSnapshot{}
		applyFundingNormalization(at8h, f(0.0001), f(0.0001),
			fundingProfile{IntervalHours: 8, Step: derivatives.NFromMeasuredSpacing})

		at1h := &derivatives.DerivativesSnapshot{}
		applyFundingNormalization(at1h, f(0.0001), f(0.0001),
			fundingProfile{IntervalHours: 1, Step: derivatives.NFromMeasuredSpacing})

		if at8h.FundingRate8h == nil || at1h.FundingRate8h == nil {
			t.Fatal("normalized value not emitted")
		}
		ratio := *at1h.FundingRate8h / *at8h.FundingRate8h
		if math.Abs(ratio-8) > 1e-9 {
			t.Fatalf("ratio = %v, want 8 — the whole point of normalizing", ratio)
		}
	})

	t.Run("no interval means no fabricated equivalent", func(t *testing.T) {
		snap := &derivatives.DerivativesSnapshot{}
		applyFundingNormalization(snap, f(0.0001), f(0.0001),
			fundingProfile{Step: derivatives.NUnavailable})

		if snap.FundingRate8h != nil {
			t.Errorf("emitted %v from an unknown cadence", *snap.FundingRate8h)
		}
		if snap.NStep != derivatives.NUnavailable {
			t.Errorf("step = %q, want %q", snap.NStep, derivatives.NUnavailable)
		}
	})

	t.Run("the step travels with the value", func(t *testing.T) {
		snap := &derivatives.DerivativesSnapshot{}
		applyFundingNormalization(snap, f(0.0001), f(0.0001),
			fundingProfile{IntervalHours: 4, Step: derivatives.NFromMetadataSeed})

		if snap.NStep != derivatives.NFromMetadataSeed {
			t.Errorf("step = %q, want %q", snap.NStep, derivatives.NFromMetadataSeed)
		}
		if snap.FundingIntervalHours == nil || *snap.FundingIntervalHours != 4 {
			t.Error("interval did not travel with the reading")
		}
	})
}

// TestApplyFundingNormalization_AnchorDetection covers the state the panel never had.
//
// The clamp pins funding AT THE INTEREST RATE scaled by the interval, not at zero.
// A pinned value is censored, not measured — printing it with four decimals implies a
// precision that is not there while the premium it censors keeps moving.
func TestApplyFundingNormalization_AnchorDetection(t *testing.T) {
	f := func(v float64) *float64 { return &v }

	tests := []struct {
		name         string
		rate         float64
		interestRate float64
		hours        float64
		wantAnchored bool
	}{
		{"eight-hour contract pinned", 1e-4, 1e-4, 8, true},
		{"four-hour contract pinned at half", 5e-5, 1e-4, 4, true},
		{"one-hour contract pinned at an eighth", 1.25e-5, 1e-4, 1, true},
		{"free reading above the anchor", 3e-4, 1e-4, 8, false},
		{"free reading below the anchor", -2e-4, 1e-4, 8, false},
		{"zero-interest contract pinned at zero", 0, 0, 8, true},
		{"zero-interest contract moving", 4e-4, 0, 8, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := &derivatives.DerivativesSnapshot{}
			applyFundingNormalization(snap, f(tt.rate), f(tt.interestRate),
				fundingProfile{IntervalHours: tt.hours, Step: derivatives.NFromMeasuredSpacing})

			if snap.FundingAtAnchor == nil {
				t.Fatal("anchor state not reported")
			}
			if *snap.FundingAtAnchor != tt.wantAnchored {
				t.Errorf("atAnchor = %v, want %v", *snap.FundingAtAnchor, tt.wantAnchored)
			}
		})
	}
}

// TestApplyFundingNormalization_ZeroInterestIsNotADiscount is the BNBUSDT case, and it
// is why the zero-is-a-discount rule had to become per-symbol.
//
// For a contract whose interest rate is zero, funding of exactly zero is the modal
// neutral state, not evidence of a discount. Measured: 158 of 730 TRADING symbols
// carry a zero interest rate, and BNBUSDT — a first-tier symbol — reports exactly zero
// in 59.5% of its settlements, against 0.0% for BTCUSDT and ETHUSDT. A global rule
// would light up on it permanently.
func TestApplyFundingNormalization_ZeroInterestIsNotADiscount(t *testing.T) {
	f := func(v float64) *float64 { return &v }

	standard := &derivatives.DerivativesSnapshot{}
	applyFundingNormalization(standard, f(0), f(1e-4),
		fundingProfile{IntervalHours: 8, Step: derivatives.NFromMeasuredSpacing})

	zeroInterest := &derivatives.DerivativesSnapshot{}
	applyFundingNormalization(zeroInterest, f(0), f(0),
		fundingProfile{IntervalHours: 8, Step: derivatives.NFromMeasuredSpacing})

	if standard.FundingAtAnchor == nil || zeroInterest.FundingAtAnchor == nil {
		t.Fatal("anchor state not reported")
	}
	// On a standard contract, zero is BELOW the anchor — a discount, not the anchor.
	if *standard.FundingAtAnchor {
		t.Error("zero read as anchored on a contract whose anchor is 0.01%/8h")
	}
	// On a zero-interest contract, zero IS the anchor.
	if !*zeroInterest.FundingAtAnchor {
		t.Error("zero not read as anchored on a zero-interest contract")
	}
	if standard.InterestRate == nil || zeroInterest.InterestRate == nil {
		t.Fatal("interest rate did not travel with the reading — the consumer cannot tell the two apart")
	}
}

// TestFundingProfileCache_ConcurrentAccess — the poll fans out over symbols, so the
// cache is read concurrently with the writes bootstrap performs. Run under -race.
func TestFundingProfileCache_ConcurrentAccess(t *testing.T) {
	cache := NewFundingProfileRegistry()
	sym := SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "SYM"}
	absent := SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "ABSENT"}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			cache.setMeasured(Metrics{}, sym, measuredCadence{Hours: float64(1 + n%8)})
		}(i)
		go func() {
			defer wg.Done()
			_, _ = cache.measuredFor(sym)
		}()
	}
	wg.Wait()

	if _, ok := cache.measuredFor(sym); !ok {
		t.Error("value lost under concurrent access")
	}
	if _, ok := cache.measuredFor(absent); ok {
		t.Error("absent key reported present")
	}
}

// TestFundingProfileCache_RejectsNonPositive — a non-positive interval would divide by
// zero downstream, so it must never enter the cache in the first place.
func TestFundingProfileCache_RejectsNonPositive(t *testing.T) {
	cache := NewFundingProfileRegistry()
	k := SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "K/USDT"}
	cache.setMeasured(Metrics{}, k, measuredCadence{Hours: 0})
	cache.setMeasured(Metrics{}, k, measuredCadence{Hours: -4})
	if _, ok := cache.measuredFor(k); ok {
		t.Error("a non-positive interval was cached")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// PERIODIC REVALIDATION OF THE SETTLEMENT INTERVAL
//
// Measuring the interval exactly ONCE per process, at bootstrap, and trusting it for the
// lifetime of the run goes stale by exchange rule. These tests pin the periodic
// revalidation that keeps the measured rung current.
// ─────────────────────────────────────────────────────────────────────────────

// settlementsEvery builds a funding history whose settlements are `hours` apart.
func settlementsEvery(hours float64, count int) []derivatives.FundingRateHistoryPoint {
	step := int64(hours * 3600 * 1000)
	base := int64(1_700_000_000_000)
	out := make([]derivatives.FundingRateHistoryPoint, 0, count)
	for i := 0; i < count; i++ {
		ts := base + int64(i)*step
		out = append(out, derivatives.FundingRateHistoryPoint{Timestamp: &ts})
	}
	return out
}

// TestRevalidateFundingInterval_HonoursTheDeclaredCadence is the anchor for D2/1.13.
//
// The clock is INJECTED, so the test states the cadence rule directly instead of
// approximating it with a sleep: before the interval has elapsed the measurement is not
// repeated, and after it has, it is. The venue changes cadence between the two
// measurements — 8h to 1h, which is the real exchange behavior when a contract hits
// the funding cap — and the assertion that matters is that the NORMALIZED value follows
// it. Under the previous once-per-process behavior the second snapshot still carries
// N = 8, and the 8h-equivalent it publishes is eight times the truth.
func TestRevalidateFundingInterval_HonoursTheDeclaredCadence(t *testing.T) {
	nowMs := int64(1_800_000_000_000)
	cache := NewFundingProfileRegistry()
	cache.nowMs = func() int64 { return nowMs }

	client := &fakeRestClient{
		funding: derivatives.FundingRate{Rate: fptr(0.0001)},
		fundingHistory: func(call int) []derivatives.FundingRateHistoryPoint {
			if call == 1 {
				return settlementsEvery(8, 6)
			}
			// The venue moved this contract to the 1h cadence.
			return settlementsEvery(1, 6)
		},
	}

	poller := fetchTestPoller()
	poller.fundingProfiles = cache
	scope := SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT"}

	// First poll: nothing has ever been measured, so the interval is measured now.
	first, err := poller.fetchDerivatives(context.Background(), client, scope)
	if err != nil {
		t.Fatalf("fetchDerivatives: %v", err)
	}
	if client.fundingHistoryCalls != 1 {
		t.Fatalf("history calls = %d, want 1 on the first poll", client.fundingHistoryCalls)
	}
	if first.FundingIntervalHours == nil || *first.FundingIntervalHours != 8 {
		t.Fatalf("interval = %v, want 8h measured from the spacing", first.FundingIntervalHours)
	}
	if first.NStep != derivatives.NFromMeasuredSpacing {
		t.Fatalf("nStep = %q, want the measured rung", first.NStep)
	}

	// Still inside the cadence — by one millisecond. Nothing is re-measured.
	nowMs += fundingIntervalRevalidationInterval.Milliseconds() - 1
	if _, err := poller.fetchDerivatives(context.Background(), client, scope); err != nil {
		t.Fatalf("fetchDerivatives: %v", err)
	}
	if client.fundingHistoryCalls != 1 {
		t.Fatalf("history calls = %d one millisecond before the cadence elapsed, want 1 — "+
			"the revalidation is not respecting its own interval", client.fundingHistoryCalls)
	}

	// Past the cadence: measured again, and the new cadence takes effect.
	nowMs++
	second, err := poller.fetchDerivatives(context.Background(), client, scope)
	if err != nil {
		t.Fatalf("fetchDerivatives: %v", err)
	}
	if client.fundingHistoryCalls != 2 {
		t.Fatalf("history calls = %d after the cadence elapsed, want 2 — the interval was "+
			"measured once and never revisited", client.fundingHistoryCalls)
	}
	if second.FundingIntervalHours == nil || *second.FundingIntervalHours != 1 {
		t.Fatalf("interval = %v after the venue switched to 1h, want 1", second.FundingIntervalHours)
	}
	// The consequence, stated as its own assertion: the 8h-equivalent follows N.
	want := 0.0001 * 8 / 1
	if second.FundingRate8h == nil || math.Abs(*second.FundingRate8h-want) > 1e-15 {
		t.Fatalf("fundingRate8h = %v, want %v — the normalized value is still using the stale N",
			second.FundingRate8h, want)
	}
}

// TestRevalidateFundingInterval_FailureDoesNotDemoteTheRung asserts a transient fetch
// failure costs nothing but the attempt: the previously measured interval survives, and
// the next attempt waits one full cadence instead of retrying on every poll.
//
// Both halves matter. Dropping the measured value on a network blip would demote the
// symbol to the exchange's declared metadata — the rung with no evidence behind it — and
// retrying every poll would put one extra REST call per symbol per cycle on a fleet of
// hundreds whenever the endpoint is unhappy.
func TestRevalidateFundingInterval_FailureDoesNotDemoteTheRung(t *testing.T) {
	nowMs := int64(1_800_000_000_000)
	cache := NewFundingProfileRegistry()
	cache.nowMs = func() int64 { return nowMs }

	client := &fakeRestClient{
		funding:        derivatives.FundingRate{Rate: fptr(0.0001)},
		fundingHistory: func(int) []derivatives.FundingRateHistoryPoint { return settlementsEvery(4, 6) },
	}

	poller := fetchTestPoller()
	poller.fundingProfiles = cache
	scope := SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT"}

	if _, err := poller.fetchDerivatives(context.Background(), client, scope); err != nil {
		t.Fatalf("fetchDerivatives: %v", err)
	}

	// The endpoint goes down, and the cadence elapses.
	client.fundingHistoryErr = errors.New("HTTP 503 from the funding history endpoint")
	nowMs += fundingIntervalRevalidationInterval.Milliseconds()

	snapshot, err := poller.fetchDerivatives(context.Background(), client, scope)
	if err != nil {
		t.Fatalf("fetchDerivatives: %v", err)
	}
	if client.fundingHistoryCalls != 2 {
		t.Fatalf("history calls = %d, want 2", client.fundingHistoryCalls)
	}
	if snapshot.FundingIntervalHours == nil || *snapshot.FundingIntervalHours != 4 {
		t.Fatalf("interval = %v after a failed revalidation, want the measured 4h to survive",
			snapshot.FundingIntervalHours)
	}
	if snapshot.NStep != derivatives.NFromMeasuredSpacing {
		t.Fatalf("nStep = %q after a failed revalidation, want the measured rung to survive", snapshot.NStep)
	}

	// The failed attempt still consumed its slot: the next poll does not retry.
	if _, err := poller.fetchDerivatives(context.Background(), client, scope); err != nil {
		t.Fatalf("fetchDerivatives: %v", err)
	}
	if client.fundingHistoryCalls != 2 {
		t.Fatalf("history calls = %d, want 2 — a failed attempt must not retry on every poll",
			client.fundingHistoryCalls)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE KEY IS THE CONTRACT, AND THE REGISTRY OUTLIVES THE POLLER
//
// DUR-1 and DUR-2. Two independent defects, both silent: a ticker-keyed entry merges
// two contracts, and a registry born inside the poller's constructor dies with the
// poller the lifecycle rebuilds.
// ─────────────────────────────────────────────────────────────────────────────

// TestFundingProfileRegistry_TwoVenuesSameTickerAreTwoContracts is DUR-1.
//
// Production runs two exchanges with market_type future, so BTC/USDT names two
// contracts, and their settlement cadences differ. Keyed by ticker, whichever venue
// measured last decides the 8h-equivalent of both — and the error is the ratio between
// the two cadences, published under the rung that claims evidence.
func TestFundingProfileRegistry_TwoVenuesSameTickerAreTwoContracts(t *testing.T) {
	reg := NewFundingProfileRegistry()
	binance := SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT"}
	bybit := SymbolScope{ExchangeID: "bybit", MarketType: "future", Symbol: "BTC/USDT"}
	binanceSwap := SymbolScope{ExchangeID: "binance", MarketType: "swap", Symbol: "BTC/USDT"}

	reg.setMeasured(Metrics{}, binance, measuredCadence{Hours: 8})

	if cadence, ok := reg.measuredFor(bybit); ok {
		t.Fatalf("bybit read %vh measured on binance — the two contracts were merged", cadence.Hours)
	}
	if cadence, ok := reg.measuredFor(binanceSwap); ok {
		t.Fatalf("binance:swap read %vh measured on binance:future — market type is part of the key",
			cadence.Hours)
	}

	// The other direction: measuring the second contract must not overwrite the first.
	reg.setMeasured(Metrics{}, bybit, measuredCadence{Hours: 1})

	cadence, ok := reg.measuredFor(binance)
	if !ok || cadence.Hours != 8 {
		t.Fatalf("binance interval = %v (present %v), want 8 — bybit's 1h cadence overwrote it, "+
			"which publishes an 8h-equivalent 8× the truth", cadence.Hours, ok)
	}
	cadence, ok = reg.measuredFor(bybit)
	if !ok || cadence.Hours != 1 {
		t.Fatalf("bybit interval = %v (present %v), want 1", cadence.Hours, ok)
	}
}

// TestFundingProfileRegistry_AttemptOnOneContractDoesNotSilenceTheOther is the same key
// discipline on the cadence stamp: an attempt recorded under a ticker would make every
// other venue's contract look freshly checked, and none of them would be measured again
// for a full revalidation interval.
func TestFundingProfileRegistry_AttemptOnOneContractDoesNotSilenceTheOther(t *testing.T) {
	nowMs := int64(1_800_000_000_000)
	reg := NewFundingProfileRegistry()
	reg.nowMs = func() int64 { return nowMs }

	binance := SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT"}
	bybit := SymbolScope{ExchangeID: "bybit", MarketType: "future", Symbol: "BTC/USDT"}

	reg.markAttempted(binance)

	if reg.revalidationDue(binance) {
		t.Fatal("binance is due one millisecond after its own attempt")
	}
	if !reg.revalidationDue(bybit) {
		t.Fatal("bybit reported not due after an attempt made on binance — the stamp landed " +
			"on the ticker instead of on the contract")
	}
}

// TestNewDerivativesPoller_RestartKeepsTheMeasuredRung is DUR-2, stated as the
// consequence rather than as the wiring.
//
// The lifecycle rebuilds the poller on a restart. If the registry is rebuilt with it,
// the restarted poller has no measured interval for any symbol, so it re-measures — and
// on the restart that follows an outage, the endpoint it re-measures against is exactly
// the one that is down. The symbol then falls to the exchange's DECLARED cadence, which
// here says 1h against a contract measured at 8h: the published 8h-equivalent is 8×
// the truth, carrying the seed rung.
func TestNewDerivativesPoller_RestartKeepsTheMeasuredRung(t *testing.T) {
	declared1h := "1h"
	client := &fakeRestClient{
		funding: derivatives.FundingRate{Rate: fptr(0.0001), Interval: &declared1h},
		fundingHistory: func(int) []derivatives.FundingRateHistoryPoint {
			return settlementsEvery(8, 6)
		},
	}
	scope := SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT"}
	registry := NewFundingProfileRegistry()

	before := NewDerivativesPoller(t.Context(),
		config.DerivativesConfig{Enabled: true, MaxSeriesSize: 200},
		&recordingStore{}, nil, nil, registry, nil, nil)

	first, err := before.fetchDerivatives(context.Background(), client, scope)
	if err != nil {
		t.Fatalf("fetchDerivatives: %v", err)
	}
	if first.NStep != derivatives.NFromMeasuredSpacing || first.FundingIntervalHours == nil ||
		*first.FundingIntervalHours != 8 {
		t.Fatalf("before the restart: step %q interval %v, want the measured 8h",
			first.NStep, first.FundingIntervalHours)
	}

	// The poller dies and the lifecycle rebuilds it against the SAME registry, while the
	// history endpoint is unreachable — the shape of a restart that follows an outage.
	client.fundingHistoryErr = errors.New("HTTP 503 from the funding history endpoint")
	after := NewDerivativesPoller(t.Context(),
		config.DerivativesConfig{Enabled: true, MaxSeriesSize: 200},
		&recordingStore{}, nil, nil, registry, nil, nil)

	second, err := after.fetchDerivatives(context.Background(), client, scope)
	if err != nil {
		t.Fatalf("fetchDerivatives after restart: %v", err)
	}
	if second.FundingIntervalHours == nil || *second.FundingIntervalHours != 8 {
		t.Fatalf("interval after the restart = %v, want the measured 8h to survive",
			second.FundingIntervalHours)
	}
	if second.NStep != derivatives.NFromMeasuredSpacing {
		t.Fatalf("nStep after the restart = %q, want %q — the restart demoted the symbol to "+
			"the rung with no evidence behind it", second.NStep, derivatives.NFromMeasuredSpacing)
	}
	want := 0.0001 * 8 / 8
	if second.FundingRate8h == nil || math.Abs(*second.FundingRate8h-want) > 1e-15 {
		t.Fatalf("fundingRate8h after the restart = %v, want %v — the declared 1h cadence "+
			"published a value 8× the truth", second.FundingRate8h, want)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE MEASUREMENT HAS A TIME DOMAIN, AND AN UNKNOWN CADENCE IS NOT A CADENCE
//
// A bucket older than the observation inherits instead of claiming evidence, and a gap
// that matches no cadence the venue uses never becomes a measured interval.
// ─────────────────────────────────────────────────────────────────────────────

// TestProfileForBucket_ASeededBucketOutsideTheWindowInherits is DUR-5.
//
// The seeding run writes 2.000 retroactive buckets — in 1d that reaches 2021-03-03 —
// while the measurement covers the last sixteen settlements. Applying today's N there
// and stamping it "measured" claims evidence for an instant nobody looked at.
func TestProfileForBucket_ASeededBucketOutsideTheWindowInherits(t *testing.T) {
	observedFrom := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
	measured := fundingProfile{
		IntervalHours:  8,
		Step:           derivatives.NFromMeasuredSpacing,
		ObservedFromMs: observedFrom,
	}

	tests := []struct {
		name     string
		bucketMs int64
		wantStep derivatives.NDerivationStep
	}{
		{
			name:     "a 2021 bucket is outside anything anybody measured",
			bucketMs: time.Date(2021, 3, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
			wantStep: derivatives.NInheritedOutsideWindow,
		},
		{
			name:     "one millisecond before the observation is still outside it",
			bucketMs: observedFrom - 1,
			wantStep: derivatives.NInheritedOutsideWindow,
		},
		{
			name:     "the oldest settlement the measurement covers is inside it",
			bucketMs: observedFrom,
			wantStep: derivatives.NFromMeasuredSpacing,
		},
		{
			name:     "a bucket after the observation is inside it",
			bucketMs: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
			wantStep: derivatives.NFromMeasuredSpacing,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := profileForBucket(measured, tt.bucketMs)
			if got.Step != tt.wantStep {
				t.Fatalf("step = %q, want %q", got.Step, tt.wantStep)
			}
			// The interval travels either way: it is the best thing known about the
			// contract, and dropping it would turn "inherited" into "unavailable".
			if got.IntervalHours != 8 {
				t.Fatalf("interval = %v, want 8 — the inherited value still travels", got.IntervalHours)
			}
		})
	}
}

// TestProfileForBucket_RungsWithNoWindowAreUntouched — the declared and unavailable
// rungs carry no observation, so there is nothing for a bucket to be outside of.
func TestProfileForBucket_RungsWithNoWindowAreUntouched(t *testing.T) {
	ancient := time.Date(2021, 3, 3, 0, 0, 0, 0, time.UTC).UnixMilli()

	seed := profileForBucket(fundingProfile{IntervalHours: 4, Step: derivatives.NFromMetadataSeed}, ancient)
	if seed.Step != derivatives.NFromMetadataSeed {
		t.Fatalf("seed rung became %q — it declares no observation window to fall outside of", seed.Step)
	}

	none := profileForBucket(fundingProfile{Step: derivatives.NUnavailable}, ancient)
	if none.Step != derivatives.NUnavailable {
		t.Fatalf("unavailable rung became %q", none.Step)
	}
}

// TestApplyFundingNormalization_InheritedBucketCarriesNoEquivalent is the consequence of
// DUR-5 on the record that gets written: the rate and the interval travel, the
// 8h-equivalent does not, and the rung says which of the two happened.
func TestApplyFundingNormalization_InheritedBucketCarriesNoEquivalent(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	observedFrom := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
	profile := fundingProfile{
		IntervalHours:  8,
		Step:           derivatives.NFromMeasuredSpacing,
		ObservedFromMs: observedFrom,
	}
	seeded := time.Date(2021, 3, 3, 0, 0, 0, 0, time.UTC).UnixMilli()

	snap := &derivatives.DerivativesSnapshot{Timestamp: seeded}
	applyFundingNormalization(snap, f(0.0001), f(0.0001), profileForBucket(profile, seeded))

	if snap.NStep != derivatives.NInheritedOutsideWindow {
		t.Fatalf("nStep = %q, want %q", snap.NStep, derivatives.NInheritedOutsideWindow)
	}
	if snap.FundingRate8h != nil {
		t.Fatalf("emitted an 8h-equivalent of %v for a bucket five years older than the "+
			"measurement — that is today's cadence claiming to describe 2021", *snap.FundingRate8h)
	}
	if snap.FundingAtAnchor != nil {
		t.Fatalf("decided the anchor (%v) against an interval that may not have been in force",
			*snap.FundingAtAnchor)
	}
	if snap.FundingIntervalHours == nil || *snap.FundingIntervalHours != 8 {
		t.Fatalf("interval = %v, want 8 to travel — the consumer needs to see WHAT was inherited",
			snap.FundingIntervalHours)
	}

	// The same profile inside the window still normalizes: without this half the test
	// would pass on an implementation that never emits an equivalent at all.
	live := &derivatives.DerivativesSnapshot{Timestamp: observedFrom}
	applyFundingNormalization(live, f(0.0001), f(0.0001), profileForBucket(profile, observedFrom))
	if live.NStep != derivatives.NFromMeasuredSpacing || live.FundingRate8h == nil {
		t.Fatalf("a bucket inside the window came out step %q equivalent %v — the narrowing "+
			"swallowed the case it is not supposed to touch", live.NStep, live.FundingRate8h)
	}
}

// TestMeasureIntervalHours_AGapMatchingNoCadenceDoesNotVote is DUR-39.
//
// A missing settlement widens a gap. Two missing settlements on a 1h contract read as
// 3,0h — a spacing no venue uses — and the previous mode counted it like any other, so on
// a sparse history it could WIN and be published as measured. The 8h-equivalent derived
// from a fabricated N = 3 understates the true one by 2,67×, under the rung that claims
// evidence.
func TestMeasureIntervalHours_AGapMatchingNoCadenceDoesNotVote(t *testing.T) {
	t.Run("the unknown gap loses to the real cadence", func(t *testing.T) {
		// 1h, 1h, then two settlements missing (3h), then 1h.
		ts := []int64{0, hourMs, 2 * hourMs, 5 * hourMs, 6 * hourMs}
		got, ok := measureIntervalHours(ts)
		if !ok {
			t.Fatal("a history with one bad gap produced no measurement at all")
		}
		if got != 1 {
			t.Fatalf("hours = %v, want 1 — the 3,0h gap is missing settlements, not a cadence", got)
		}
	})

	t.Run("a history of nothing but unknown gaps yields no measurement", func(t *testing.T) {
		// Every gap is 3,0h: the mode is 3, and 3 is not a cadence the venue uses.
		ts := []int64{0, 3 * hourMs, 6 * hourMs, 9 * hourMs}
		got, ok := measureIntervalHours(ts)
		if ok {
			t.Fatalf("measured %vh from spacings that match no known cadence — the ladder must "+
				"fall to the declared rung instead of inventing one", got)
		}
	})

	t.Run("the unknown gap does not win a tie either", func(t *testing.T) {
		// Two 3,0h gaps against two 4h gaps: counting the unknown one makes it a tie,
		// and the tie-break toward the shorter cadence would then publish 3.
		ts := []int64{0, 3 * hourMs, 6 * hourMs, 10 * hourMs, 14 * hourMs}
		got, ok := measureIntervalHours(ts)
		if !ok {
			t.Fatal("no measurement from a history that contains two clean 4h gaps")
		}
		if got != 4 {
			t.Fatalf("hours = %v, want 4 — a spacing the venue never uses took the tie", got)
		}
	})
}

// TestObservationStartMs_IsTheOldestSettlement — the window starts where the evidence
// starts, and the exchange returns the series in either direction.
func TestObservationStartMs_IsTheOldestSettlement(t *testing.T) {
	base := int64(1_700_000_000_000)

	ascending := []int64{base, base + hourMs, base + 2*hourMs}
	if got := observationStartMs(ascending); got != base {
		t.Fatalf("ascending: start = %d, want %d", got, base)
	}

	descending := []int64{base + 2*hourMs, base + hourMs, base}
	if got := observationStartMs(descending); got != base {
		t.Fatalf("descending: start = %d, want %d", got, base)
	}

	// No settlement, no window — and a window of zero is what profileForBucket reads as
	// "there is nothing to be outside of".
	if got := observationStartMs(nil); got != 0 {
		t.Fatalf("empty history: start = %d, want 0", got)
	}
}
