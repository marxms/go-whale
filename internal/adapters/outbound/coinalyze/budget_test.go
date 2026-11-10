package coinalyze

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// The per-symbol budget and the wait the provider demanded.
//
// The Coinalyze API publishes NO rate-limit header — measured: a 200 response from
// `/exchanges` carries neither `X-RateLimit-*` nor `RateLimit-*`. The only rate-limit signal
// it emits is the `Retry-After` of a 429, which makes local accounting the only way to
// avoid discovering the ceiling by hitting it.

// budgetHarness builds a budget with a controlled clock and wait, so the wait is MEASURED
// instead of slept.
type budgetHarness struct {
	budget *CoinalyzeBudget
	clock  *fakeClock
	log    *slog.Logger
	sink   *logger.Sink

	mu    sync.Mutex
	waits []time.Duration
	calls int
}

func newBudgetHarness(t *testing.T) *budgetHarness {
	t.Helper()

	h := &budgetHarness{clock: &fakeClock{t: time.Unix(1_785_000_000, 0)}}
	h.log, h.sink = logger.NewCapture(logger.LevelDebug)
	h.budget = NewCoinalyzeBudget(CoinalyzeBudgetOptions{
		Log: h.log,
		// The ceiling is EXPLICIT so the arithmetic of each case is self-contained: what is
		// asserted here is PER-SYMBOL accounting, not the ceiling value.
		CallsPerMinute: CoinalyzeCallsPerMinute,
		Now:            h.clock.Now,
		Wait: func(ctx context.Context, d time.Duration) error {
			h.mu.Lock()
			h.waits = append(h.waits, d)
			h.mu.Unlock()
			// The clock advances by the amount waited: a wait that does not pass time is not
			// a wait, and the bucket refill depends on the clock.
			h.clock.advance(d)
			return ctx.Err()
		},
	})
	return h
}

func (h *budgetHarness) recorded() []time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]time.Duration(nil), h.waits...)
}

func (h *budgetHarness) totalWaited() time.Duration {
	var total time.Duration
	for _, d := range h.recorded() {
		total += d
	}
	return total
}

func (h *budgetHarness) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// ok counts the call and returns success.
func (h *budgetHarness) ok(context.Context) error {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	return nil
}

// failsThenOk returns `err` on the first `times` calls and success afterwards.
func (h *budgetHarness) failsThenOk(err error, times int) func(context.Context) error {
	return func(context.Context) error {
		h.mu.Lock()
		h.calls++
		attempt := h.calls
		h.mu.Unlock()
		if attempt <= times {
			return err
		}
		return nil
	}
}

func symbolList(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("SYM%02d/USDT", i))
	}
	return out
}

// THE BUDGET IS CHARGED PER SYMBOL, NOT PER REQUEST. A multi-symbol request costs one call
// PER SYMBOL against the 40/min ceiling — counting it as one lets the process spend 20x
// what it thinks it spends.
func TestCoinalyzeBudgetChargesPerSymbolNotPerRequest(t *testing.T) {
	h := newBudgetHarness(t)
	// The bucket accumulates up to the BURST CEILING — one full request, 20 calls — and not
	// up to the whole minute. The arithmetic of this case was RE-DERIVED when the ceiling
	// was lowered: it claimed two 20-symbol requests left without waiting, which is exactly
	// the burst the requirement forbids. What the case measures is still the same —
	// PER-SYMBOL accounting — and the discriminating assertion (1.5s for one symbol) stayed
	// intact.
	h.clock.advance(time.Minute)

	ctx := context.Background()
	if err := h.budget.Do(ctx, symbolList(CoinalyzeMaxSymbolsPerRequest), h.ok); err != nil {
		t.Fatalf("the first request should fit the ceiling: %v", err)
	}
	if waited := h.totalWaited(); waited != 0 {
		t.Fatalf("a full request fits the burst ceiling without waiting, waited %v", waited)
	}
	if err := h.budget.Do(ctx, symbolList(CoinalyzeMaxSymbolsPerRequest), h.ok); err != nil {
		t.Fatalf("the second request should fit after the refill: %v", err)
	}

	// The bucket is empty: 2 x 20 = 40 charged calls. The next symbol has to wait for ONE
	// symbol's refill — 60s / 40 = 1.5s. Under per-REQUEST accounting two calls would have
	// been charged, slack would remain, and this wait would be zero.
	antes := h.totalWaited()
	if err := h.budget.Do(ctx, symbolList(1), h.ok); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if esperou := h.totalWaited() - antes; esperou != 1500*time.Millisecond {
		t.Fatalf("expected 1.5s of refill (60s/40), waited %v — the budget charged per request", esperou)
	}
}

// AND THE BUCKET STARTS EMPTY. A process that boots and spends a whole minute at once is
// exactly the boot burst that has already cost this repository 4,602 HTTP 429s on Binance.
func TestCoinalyzeBudgetStartsEmptySoBootCannotBurst(t *testing.T) {
	h := newBudgetHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := h.budget.Do(ctx, symbolList(1), h.ok); err == nil {
		t.Fatal("a freshly created bucket must not fund any call — it started full")
	}
	if h.callCount() != 0 {
		t.Fatalf("the call must not have left, it left %d time(s)", h.callCount())
	}
}

// THE 20-SYMBOL PER-REQUEST CEILING IS ENFORCED, not clamped. Clamping would let 21 symbols
// go to the network having paid for 20 — the worst of both worlds.
func TestCoinalyzeBudgetRefusesMoreSymbolsThanTheCeiling(t *testing.T) {
	h := newBudgetHarness(t)
	h.clock.advance(time.Minute)

	err := h.budget.Do(context.Background(), symbolList(CoinalyzeMaxSymbolsPerRequest+1), h.ok)
	if err == nil {
		t.Fatal("21 symbols in one request must be refused")
	}
	if !errors.Is(err, ErrCoinalyzeTooManySymbols) {
		t.Fatalf("expected ErrCoinalyzeTooManySymbols, got %v", err)
	}
	if h.callCount() != 0 {
		t.Fatalf("the refused request must not have gone to the network, it left %d time(s)", h.callCount())
	}
}

// AND THE REFUSAL CHARGES NOTHING: validating after debiting charges for a request that
// never left.
func TestCoinalyzeBudgetChargesNothingForARefusedRequest(t *testing.T) {
	h := newBudgetHarness(t)
	h.clock.advance(time.Minute) // the bucket accumulates up to the burst ceiling: 20 calls

	_ = h.budget.Do(context.Background(), symbolList(CoinalyzeMaxSymbolsPerRequest+1), h.ok)

	// THE CEILING REMAINS WHOLE: a legal request of 20 passes without waiting. If the
	// refusal had debited before validating, the bucket would be in debt and this request
	// would pay the refill.
	if err := h.budget.Do(context.Background(), symbolList(CoinalyzeMaxSymbolsPerRequest), h.ok); err != nil {
		t.Fatalf("the legal request should fit: %v", err)
	}
	if waited := h.totalWaited(); waited != 0 {
		t.Fatalf("the refusal consumed budget: waited %v", waited)
	}
}

// THE CEILING IS ALSO THE BATCH SIZE: 45 symbols go out as 20, 20 and 5.
func TestCoinalyzeSymbolBatchesCapsAtTwenty(t *testing.T) {
	batches := CoinalyzeSymbolBatches(symbolList(45))

	if len(batches) != 3 {
		t.Fatalf("45 symbols in batches of 20 give 3 batches, gave %d", len(batches))
	}
	want := []int{20, 20, 5}
	for i, batch := range batches {
		if len(batch) != want[i] {
			t.Fatalf("batch %d has %d symbols, expected %d", i, len(batch), want[i])
		}
	}
}

// AND NO SYMBOL IS LOST ALONG THE WAY. An off-by-one in the split silently loses a symbol,
// which is exactly the outcome this batching exists to make visible.
func TestCoinalyzeSymbolBatchesPreservesEverySymbolExactlyOnce(t *testing.T) {
	original := symbolList(45)

	seen := map[string]int{}
	var flat []string
	for _, batch := range CoinalyzeSymbolBatches(original) {
		for _, symbol := range batch {
			seen[symbol]++
			flat = append(flat, symbol)
		}
	}

	if len(flat) != len(original) {
		t.Fatalf("%d symbols came out of %d", len(flat), len(original))
	}
	for i, symbol := range original {
		if seen[symbol] != 1 {
			t.Fatalf("%s appeared %d time(s)", symbol, seen[symbol])
		}
		if flat[i] != symbol {
			t.Fatalf("the order changed at position %d: %s != %s", i, flat[i], symbol)
		}
	}
}

// A 429 WAITS FOR THE PROVIDER'S `Retry-After` BEFORE RESENDING. Resending earlier asks for
// the next 429, and Coinalyze publishes no other rate-limit signal.
func TestCoinalyze429WaitsTheRetryAfterBeforeResending(t *testing.T) {
	h := newBudgetHarness(t)
	h.clock.advance(time.Minute)

	limited := &CoinalyzeError{Status: http.StatusTooManyRequests, Message: "too many requests", RetryAfter: 7 * time.Second}

	if err := h.budget.Do(context.Background(), symbolList(1), h.failsThenOk(limited, 1)); err != nil {
		t.Fatalf("the second attempt should have passed: %v", err)
	}
	if h.callCount() != 2 {
		t.Fatalf("expected 2 attempts, got %d", h.callCount())
	}

	waits := h.recorded()
	if len(waits) != 1 || waits[0] != 7*time.Second {
		t.Fatalf("expected ONE wait of exactly 7s (what the provider demanded), got %v", waits)
	}
}

// WITHOUT A HEADER, THE WAIT IS A DECLARED INTERVAL — never zero. Zero means "resend now",
// and now is when the provider has just said no.
func TestCoinalyze429WithoutRetryAfterFallsBackToADeclaredInterval(t *testing.T) {
	h := newBudgetHarness(t)
	h.clock.advance(time.Minute)

	limited := &CoinalyzeError{Status: http.StatusTooManyRequests, Message: "too many requests"}

	if err := h.budget.Do(context.Background(), symbolList(1), h.failsThenOk(limited, 1)); err != nil {
		t.Fatalf("the second attempt should have passed: %v", err)
	}
	waits := h.recorded()
	if len(waits) != 1 || waits[0] != DefaultCoinalyzeRetryAfter {
		t.Fatalf("expected the declared wait (%v), got %v", DefaultCoinalyzeRetryAfter, waits)
	}
}

// THE RESEND IS BUDGETED TOO. The refused request reached the provider and counts against
// its ceiling; resending it for free is spending without counting.
func TestCoinalyzeResendIsAlsoBudgeted(t *testing.T) {
	h := newBudgetHarness(t)
	h.clock.advance(time.Minute) // 40 available

	limited := &CoinalyzeError{Status: http.StatusTooManyRequests, Message: "too many requests", RetryAfter: time.Second}

	// 20 symbols, one 429, one resend: 40 charged, and the 1s wait restores 40/60 ~ 0.667.
	if err := h.budget.Do(context.Background(), symbolList(CoinalyzeMaxSymbolsPerRequest), h.failsThenOk(limited, 1)); err != nil {
		t.Fatalf("the second attempt should have passed: %v", err)
	}

	// The bucket is in debt: one more symbol no longer fits without refill.
	before := h.totalWaited()
	if err := h.budget.Do(context.Background(), symbolList(1), h.ok); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.totalWaited() == before {
		t.Fatal("the resend was not charged: 20 + 20 should have exhausted the minute's 40")
	}
}

// A PERMANENT ERROR IS NOT RESENT. A 401 remains a 401 on the second attempt; repeating
// only burns budget and delays diagnosis.
func TestCoinalyzeNonRetryableErrorIsNotResent(t *testing.T) {
	h := newBudgetHarness(t)
	h.clock.advance(time.Minute)

	denied := &CoinalyzeError{Status: http.StatusUnauthorized, Message: "Invalid/Missing API key"}

	err := h.budget.Do(context.Background(), symbolList(1), h.failsThenOk(denied, 99))
	if h.callCount() != 1 {
		t.Fatalf("a 401 is not resent, there were %d attempts", h.callCount())
	}
	if waited := h.totalWaited(); waited != 0 {
		t.Fatalf("one does not wait in order not to resend, waited %v", waited)
	}
	var coinErr *CoinalyzeError
	if !errors.As(err, &coinErr) || coinErr.Status != http.StatusUnauthorized {
		t.Fatalf("the provider error must reach the caller, got %v", err)
	}
}

// EXHAUSTED ATTEMPTS RETURN THE PROVIDER ERROR, not a generic "failed": the status is what
// distinguishes a hit ceiling from unavailability.
func TestCoinalyzeExhaustedAttemptsReturnTheProviderError(t *testing.T) {
	h := newBudgetHarness(t)
	h.clock.advance(time.Minute)

	limited := &CoinalyzeError{Status: http.StatusTooManyRequests, Message: "too many requests", RetryAfter: time.Second}

	err := h.budget.Do(context.Background(), symbolList(1), h.failsThenOk(limited, 99))
	if err == nil {
		t.Fatal("a 429 on every attempt must fail")
	}
	var coinErr *CoinalyzeError
	if !errors.As(err, &coinErr) || coinErr.Status != http.StatusTooManyRequests {
		t.Fatalf("the status must survive exhaustion, got %v", err)
	}
	if h.callCount() != DefaultCoinalyzeAttempts {
		t.Fatalf("expected %d attempts, got %d", DefaultCoinalyzeAttempts, h.callCount())
	}
}

// THE WAIT RESPECTS SHUTDOWN. A long `Retry-After` while the process is going down must not
// hold the cycle until the provider releases it.
func TestCoinalyzeRetryWaitHonoursContextCancellation(t *testing.T) {
	h := newBudgetHarness(t)
	h.clock.advance(time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	limited := &CoinalyzeError{Status: http.StatusTooManyRequests, RetryAfter: time.Hour}

	err := h.budget.Do(ctx, symbolList(1), func(context.Context) error {
		h.mu.Lock()
		h.calls++
		h.mu.Unlock()
		cancel() // the process goes down while the response is processed
		return limited
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation must interrupt the wait, got %v", err)
	}
	if h.callCount() != 1 {
		t.Fatalf("there must be no resend after cancellation, there were %d calls", h.callCount())
	}
}

// A 429 BRAKES THE WHOLE PROCESS, NOT ONLY THE REQUEST THAT CAUSED IT.
//
// The resend wait is per REQUEST, but the provider's ceiling is per PROCESS. While the
// refused batch sleeps the minute the provider demanded, the bucket rebuilds tokens and the
// other batches of the cycle keep spending at the full rate — the pressure that caused the
// refusal never falls.
//
// Measured in production (LAN): 82% of resends were refused again, and 38% of the budget
// went to refused requests. The Binance sibling already solved this — `Penalize` suspends
// ALL spending. Here it did not exist.
func TestCoinalyzeBudgetIsDrainedAndSuspendedByA429(t *testing.T) {
	h := newBudgetHarness(t)
	h.clock.advance(time.Minute) // 40 tokens available

	recusa := &CoinalyzeError{
		Status:     http.StatusTooManyRequests,
		Message:    "Too Many Requests",
		RetryAfter: time.Minute,
	}

	// Exhausts the three attempts against a provider that only refuses.
	if err := h.budget.Do(context.Background(), symbolList(2), func(context.Context) error {
		return recusa
	}); err == nil {
		t.Fatal("three consecutive refusals must return an error")
	}

	antes := h.totalWaited()

	// AN INDEPENDENT BATCH, RIGHT AFTER. Without the suspension it leaves without waiting
	// for anything: the bucket rebuilt itself during the resend waits, and the burst returns
	// to the provider that has just said no three times.
	if err := h.budget.Do(context.Background(), symbolList(CoinalyzeMaxSymbolsPerRequest), h.ok); err != nil {
		t.Fatalf("next batch: %v", err)
	}

	// THE WAIT MUST BE THE WINDOW THE PROVIDER DEMANDED, and not merely the refill of what
	// the 429 consumed. Emptying the balance without suspending would already produce a
	// wait — refilling 20 tokens, 30s — and a `> 0` would pass the case with half the fix.
	// The "empties but does not suspend" mutant SURVIVES that weak assertion.
	esperou := h.totalWaited() - antes
	if esperou < recusa.RetryAfter {
		t.Fatalf("a 429 did not suspend the process for the demanded window: waited %v, the provider demanded %v",
			esperou, recusa.RetryAfter)
	}
}

// THE 429 ALSO EMPTIES THE BUCKET, and not only suspends.
//
// THE SHORT WINDOW IS THE POINT. With a 60s `Retry-After` the penalty lasts exactly the
// refill time of a whole minute, so the bucket comes back full either way and "emptying" is
// INDISTINGUISHABLE from "not emptying" — the mutant that removes `b.available = 0`
// SURVIVES the 60s case. This case exists because the independent test team pointed at that
// hole, and the probe confirmed it.
//
// With 5s, the window refill credits 40 x 5/60 = 3.33 tokens. A full batch of 20 has to wait
// for the rest — 16.67 tokens at 40/min = 25s. Without the emptying the previous balance
// survives and the same batch leaves in 5s.
func TestCoinalyzeBudgetDrainsTheBucketAndNotOnlySuspends(t *testing.T) {
	h := newBudgetHarness(t)
	h.clock.advance(time.Minute) // 40 tokens available

	const janelaCurta = 5 * time.Second
	recusa := &CoinalyzeError{
		Status:     http.StatusTooManyRequests,
		Message:    "Too Many Requests",
		RetryAfter: janelaCurta,
	}

	if err := h.budget.Do(context.Background(), symbolList(2), func(context.Context) error {
		return recusa
	}); err == nil {
		t.Fatal("three consecutive refusals must return an error")
	}

	antes := h.totalWaited()

	if err := h.budget.Do(context.Background(), symbolList(CoinalyzeMaxSymbolsPerRequest), h.ok); err != nil {
		t.Fatalf("next batch: %v", err)
	}

	// The floor is well above the window: if the batch leaves near the 5s of the suspension,
	// the previous balance survived the 429 and the bucket was NOT emptied.
	const pisoDeRefil = 20 * time.Second
	if esperou := h.totalWaited() - antes; esperou < pisoDeRefil {
		t.Fatalf("the 429 suspended but did NOT empty the bucket: the next batch waited %v, "+
			"and an emptied bucket would require at least %v of refill", esperou, pisoDeRefil)
	}
}

// TWO IN-FLIGHT REFUSALS DO NOT SHORTEN EACH OTHER'S WAIT.
//
// `Penalize` documents "the longest wins" in prose. The independent test team pointed out
// that prose was all there was: a mutant that replaces the comparison with a direct
// assignment (`b.penaltyUntil = b.now().Add(d)`) SURVIVED the whole suite. Reproduced.
//
// THE CASE MATTERS BECAUSE THE CYCLE IS MULTI-BATCH. When one batch takes a 429 with a 60s
// `Retry-After` and another, shortly after, takes a 5s one, the second penalty must not
// reopen the tap 55s early — which is exactly the defect the suspension exists to prevent.
//
// This is a pattern the repository has already paid for: an invariant in prose is not an
// invariant.
func TestCoinalyzeBudgetPenaltyKeepsTheLongestWindow(t *testing.T) {
	h := newBudgetHarness(t)
	h.clock.advance(time.Minute) // 40 tokens

	h.budget.Penalize(time.Minute)
	// THE SECOND IS SHORTER and arrives later — it must not shorten the first.
	h.budget.Penalize(5 * time.Second)

	antes := h.totalWaited()
	if err := h.budget.Do(context.Background(), symbolList(1), h.ok); err != nil {
		t.Fatalf("batch: %v", err)
	}

	// With "the longest wins" the wait is the whole minute. Without it, the window becomes
	// 5s and the batch leaves almost immediately.
	const piso = 30 * time.Second
	if esperou := h.totalWaited() - antes; esperou < piso {
		t.Fatalf("the short penalty shortened the long one: waited %v, and the longer window "+
			"demanded at least %v", esperou, piso)
	}
}

// AND THE SUSPENSION MUST NOT CORRUPT THE BUCKET UNDER REAL CONCURRENCY.
//
// This case runs the three paths that touch the state — `Do`, `Penalize` and the `acquire`
// inside `Do` — in parallel, so `go test -race` has something to observe.
func TestCoinalyzeBudgetIsRaceFreeUnderConcurrentPenalties(_ *testing.T) {
	// REAL clock and wait, with tiny durations: the harness's fake clock is not safe for
	// concurrent use, and a race case that needs a mutex in the harness itself does not
	// measure the object under test.
	budget := NewCoinalyzeBudget(CoinalyzeBudgetOptions{CallsPerMinute: CoinalyzeCallsPerMinute})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = budget.Do(ctx, symbolList(1), func(context.Context) error { return nil })
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			budget.Penalize(time.Millisecond)
		}()
	}
	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// THE BURST WHEN THE WINDOW REOPENS.
//
// A control probe measured the outcome in production: after waiting the full 39.992 s the
// provider announced, the FIRST following call was refused in 0.18 s, with ZERO successes. A
// bucket that holds the whole minute paces nothing — it accumulates during the wait and
// dumps forty calls the instant it ends.
//
// The two cases below MEASURE the wait with a replaced clock instead of sleeping it. See
// docs/decisions/0031-coinalyze-request-budget.md.
// ─────────────────────────────────────────────────────────────────────────────

// THE WINDOW REOPENS AND THE PROCESS DOES NOT DUMP THE WHOLE MINUTE.
//
// This is the discriminating case: with the bucket holding 40, the second request leaves at
// the SAME instant as the first — zero wait — because the one-minute penalty rebuilt the
// whole minute while it ran.
func TestCoinalyzeBudgetDoesNotDumpTheWholeMinuteWhenTheWindowReopens(t *testing.T) {
	h := newBudgetHarness(t)
	ctx := context.Background()

	// The window the provider demands when it sends no `Retry-After` — the production case.
	h.budget.Penalize(DefaultCoinalyzeRetryAfter)

	// The first batch waits the whole window and leaves at reopening.
	if err := h.budget.Do(ctx, symbolList(CoinalyzeMaxSymbolsPerRequest), h.ok); err != nil {
		t.Fatalf("batch at reopening: %v", err)
	}
	if esperou := h.totalWaited(); esperou != DefaultCoinalyzeRetryAfter {
		t.Fatalf("the batch had to wait the whole window (%v), waited %v",
			DefaultCoinalyzeRetryAfter, esperou)
	}
	antes := h.totalWaited()

	// THE SECOND BATCH, at the SAME instant of reopening.
	if err := h.budget.Do(ctx, symbolList(CoinalyzeMaxSymbolsPerRequest), h.ok); err != nil {
		t.Fatalf("second batch: %v", err)
	}

	// 20 calls at 40/min = 30s. It has to pay that refill; leaving for free is the burst.
	const refilDeUmPedido = 30 * time.Second
	if esperou := h.totalWaited() - antes; esperou != refilDeUmPedido {
		t.Fatalf("the second batch left %v after the first, expected %v — the window reopened "+
			"and the process dumped the accumulated minute", esperou, refilDeUmPedido)
	}
}

// AND THE CALLS SPREAD ACROSS THE CYCLE, instead of leaving in a block when it starts.
//
// This collection cycle is long by design (measured: 1.87x the ticker), so the bucket spends
// minutes idle between batches. A one-minute ceiling turns each idle period into a block of
// forty calls on return; a one-request ceiling makes calls leave at the provider's rate.
func TestCoinalyzeBudgetSpreadsTheCycleInsteadOfBankingIt(t *testing.T) {
	h := newBudgetHarness(t)
	ctx := context.Background()

	// One idle hour: more than any wait of a real cycle.
	h.clock.advance(time.Hour)

	for i := 0; i < 3; i++ {
		if err := h.budget.Do(ctx, symbolList(CoinalyzeMaxSymbolsPerRequest), h.ok); err != nil {
			t.Fatalf("batch %d: %v", i+1, err)
		}
	}

	// Three full requests = 60 calls. The first fits the ceiling; the other two pay 30s of
	// refill each. With the bucket holding the whole minute, the first TWO would leave for
	// free and the total would fall to 30s.
	const esperado = 60 * time.Second
	if esperou := h.totalWaited(); esperou != esperado {
		t.Fatalf("three full requests waited %v in total, expected %v — the idle time "+
			"became balance and the cycle left in a block", esperou, esperado)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE REFUSAL THAT ACCOUNTING ITSELF CANNOT EXPLAIN.
//
// The provider documents 40 calls/min PER KEY. Two processes budgeting 40 each over one key
// of 40 saturate the quota by construction. The process does not fix that — the way out is
// a dedicated key or disabling the laboratory, both operational decisions. What it can stop
// doing is treating the refusal as noise from its own emission. See
// docs/decisions/0031-coinalyze-request-budget.md.
// ─────────────────────────────────────────────────────────────────────────────

// EMITTING BELOW ITS OWN CEILING AND STILL REFUSED: the quota was consumed by another.
func TestCoinalyzeBudgetDeclaresTheExternalConsumerWhenTheRefusalDoesNotAddUp(t *testing.T) {
	h := newBudgetHarness(t)
	// Five idle minutes: this process spent NOTHING in the minute preceding the refusal.
	h.clock.advance(5 * time.Minute)

	recusa := &CoinalyzeError{Status: http.StatusTooManyRequests, Message: "Too Many Requests", RetryAfter: time.Second}

	_ = h.budget.Do(context.Background(), symbolList(1), func(context.Context) error { return recusa })
	records := h.sink.Records()

	tally := h.budget.Tally()
	if tally.ExternalQuotaRefusals != DefaultCoinalyzeAttempts {
		t.Fatalf("counted %d unexplained refusal(s), expected %d — the refusal was treated "+
			"as noise from its own emission", tally.ExternalQuotaRefusals, DefaultCoinalyzeAttempts)
	}

	var linha string
	for _, record := range records {
		if strings.Contains(record.Text(), "not explained by local accounting") {
			linha = record.Text()
			break
		}
	}
	if linha == "" {
		t.Fatalf("the unexplained refusal was not declared: %v", records)
	}
	// THE DECLARATION CARRIES BOTH SIDES OF THE INCOMPATIBILITY, otherwise it is just an
	// opinion.
	for _, want := range []string{"another agent", "ceiling_per_minute=40"} {
		if !strings.Contains(linha, want) {
			t.Fatalf("the declaration does not carry %q: %s", want, linha)
		}
	}
}

// AND SPENDING THE WHOLE CEILING AND BEING REFUSED **DOES NOT** ACCUSE ANYONE.
//
// This is the discriminating case: without the comparison against local accounting, EVERY
// refusal would become "external consumer", and the declaration would stop meaning anything.
func TestCoinalyzeBudgetDoesNotBlameAnyoneWhenItSpentTheWholeCeiling(t *testing.T) {
	h := newBudgetHarness(t)
	ctx := context.Background()

	// Spends the whole minute: two full requests, the second paying the 30s refill.
	for i := 0; i < 2; i++ {
		if err := h.budget.Do(ctx, symbolList(CoinalyzeMaxSymbolsPerRequest), h.ok); err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
	}

	recusa := &CoinalyzeError{Status: http.StatusTooManyRequests, Message: "Too Many Requests", RetryAfter: time.Second}

	_ = h.budget.Do(ctx, symbolList(1), func(context.Context) error { return recusa })
	records := h.sink.Records()

	tally := h.budget.Tally()
	if tally.RateRefusals == 0 {
		t.Fatal("the refusal was not even counted")
	}
	if tally.ExternalQuotaRefusals != 0 {
		t.Fatalf("accused %d external consumer(s) after spending the whole ceiling — "+
			"the refusal IS compatible with local emission", tally.ExternalQuotaRefusals)
	}
	for _, record := range records {
		if strings.Contains(record.Text(), "not explained by local accounting") {
			t.Fatalf("accused an external consumer after spending the ceiling: %s", record.Text())
		}
	}
}

// TestCoinalyzeRefusalBoundary_TheCeilingItselfDoesNotAccuse is the BOUNDARY of the refusal
// classification, and it exists because no case exercised it.
//
// `classifyRefusal` decides by STRICTLY BELOW — *"spending the whole ceiling and being
// refused IS compatible with local emission"* —, and the first independent check measured
// that the `<=` mutant SURVIVED: the whole-ceiling case reaches **41** calls in the window
// (20 + 20 + the symbol that takes the 429), and at 41 both operators answer the same. The
// boundary was never touched.
//
// THE CASE ASSERTS THE SETUP ITSELF, not only the outcome. `SpentLastMinute` is compared with
// `Ceiling` BEFORE the verdict: if the bucket arithmetic changes and the window stops at the
// exact number, this case FAILS instead of silently measuring a point that does not
// discriminate — which is exactly how the gap was born.
//
// THE MUTANTS IT KILLS: `gasto <= b.perMinute` (the ceiling starts accusing) and
// `gasto < b.perMinute-1` (one below the ceiling stops accusing).
func TestCoinalyzeRefusalBoundary_TheCeilingItselfDoesNotAccuse(t *testing.T) {
	cases := []struct {
		name string
		// lotes are SUCCESSFUL requests, in symbols per request: this is how the window
		// reaches the exact number through the production path, not through a hand-written
		// field.
		lotes       []int
		querGasto   int
		querExterno bool
		porque      string
	}{
		{
			name:        "one BELOW the ceiling: the refusal cannot have come from this emission",
			lotes:       []int{CoinalyzeMaxSymbolsPerRequest, CoinalyzeMaxSymbolsPerRequest - 1},
			querGasto:   CoinalyzeCallsPerMinute - 1,
			querExterno: true,
			porque:      "below its own ceiling and refused: the quota was consumed by another agent",
		},
		{
			name:        "the EXACT ceiling: the refusal IS compatible with local emission",
			lotes:       []int{CoinalyzeMaxSymbolsPerRequest, CoinalyzeMaxSymbolsPerRequest},
			querGasto:   CoinalyzeCallsPerMinute,
			querExterno: false,
			porque:      "accusing here would be accusing without evidence",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newBudgetHarness(t)
			ctx := context.Background()
			for i, simbolos := range tc.lotes {
				if err := h.budget.Do(ctx, symbolList(simbolos), h.ok); err != nil {
					t.Fatalf("batch %d (%d symbols): %v", i+1, simbolos, err)
				}
			}

			verdict := h.budget.classifyRefusal()

			if verdict.Ceiling != CoinalyzeCallsPerMinute {
				t.Fatalf("the budget ceiling is %d, expected %d", verdict.Ceiling, CoinalyzeCallsPerMinute)
			}
			// THE SETUP, BEFORE THE VERDICT.
			if verdict.SpentLastMinute != tc.querGasto {
				t.Fatalf("the window stopped at %d call(s) against a ceiling of %d; this case "+
					"only discriminates `<` from `<=` at %d — the boundary was NOT exercised",
					verdict.SpentLastMinute, verdict.Ceiling, tc.querGasto)
			}
			if verdict.ExternalConsumer != tc.querExterno {
				t.Fatalf("with %d of %d spent, ExternalConsumer = %t, expected %t — %s",
					verdict.SpentLastMinute, verdict.Ceiling, verdict.ExternalConsumer, tc.querExterno, tc.porque)
			}
		})
	}
}

// AND THE WINDOW IS THE PROVIDER'S: one minute. What was spent ten minutes ago does not
// explain the current refusal, and counting it would make the accusation vanish precisely
// in the prolonged-saturation regime — which is measured in production.
func TestCoinalyzeBudgetForgetsSpendOlderThanTheProvidersWindow(t *testing.T) {
	h := newBudgetHarness(t)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := h.budget.Do(ctx, symbolList(CoinalyzeMaxSymbolsPerRequest), h.ok); err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
	}
	// Ten minutes later, the earlier spend says nothing about the current window.
	h.clock.advance(10 * time.Minute)

	recusa := &CoinalyzeError{Status: http.StatusTooManyRequests, Message: "Too Many Requests", RetryAfter: time.Second}
	_ = h.budget.Do(ctx, symbolList(1), func(context.Context) error { return recusa })

	if tally := h.budget.Tally(); tally.ExternalQuotaRefusals == 0 {
		t.Fatal("spend from ten minutes ago still explained the current refusal — the window is not the provider's")
	}
}
