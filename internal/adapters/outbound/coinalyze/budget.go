// Package coinalyze adapts the Coinalyze history API to the derivatives source ports.
package coinalyze

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// Coinalyze budget and the wait the provider demanded.
//
// # WHY PER SYMBOL, NOT PER REQUEST
//
// Coinalyze accepts up to 20 symbols in one request and bills ONE CALL PER SYMBOL against
// the 40-per-minute ceiling. Counting the request as one makes the process spend up to 20x
// what it thinks it spends — and it only finds out by hitting the ceiling.
//
// # WHY A LOCAL BUDGET, NOT A HEADER
//
// Measured: a 200 response from this API carries no `X-RateLimit-*`, `RateLimit-*` or any
// other counter. The only rate-limit signal it emits is the `Retry-After` of a 429 — that
// is, it warns only AFTER the fact. Without local accounting, the only way to discover the
// ceiling is to hit it.
const (
	// CoinalyzeCallsPerMinute is the documented calls-per-minute ceiling.
	//
	// Spent in full, without the safety fraction `RestBudget` applies to Binance: there the
	// fraction absorbs what cannot be measured (websocket control plane, retries inside
	// the venue adapter). Here the only other consumer of this host is the markets table, one call every
	// 6 h — 1 in 14,400 of the budget.
	CoinalyzeCallsPerMinute = 40

	// CoinalyzeMaxSymbolsPerRequest is the symbol ceiling per request.
	CoinalyzeMaxSymbolsPerRequest = 20
)

// coinalyzeBurstCeiling is the LARGEST unspent balance the bucket accumulates: ONE full
// request, not the whole minute.
//
// # THE BURST WHEN THE WINDOW REOPENS
//
// A bucket that holds the whole minute paces nothing — it accumulates while the process
// waits and DUMPS forty calls the instant the wait ends. A control probe measured the
// outcome in production: after waiting the full 39.992 s the provider announced, the FIRST
// following call was refused in 0.18 s, with ZERO successes. The window reopens and the
// fleet re-exhausts it at the same instant.
//
// That is why reading the fractional `Retry-After` without this ceiling makes the storm
// WORSE: shorter, correct waits hit the provider sooner.
//
// THE CEILING IS ONE REQUEST BECAUSE THAT IS THE INDIVISIBLE UNIT. The API bills one call
// per symbol and accepts 20 symbols per request, so a full request costs 20 and cannot be
// split — a lower ceiling would lock the caller forever. One request is not a burst; two
// at the same instant are.
//
// THE CEILING DOES NOT CHANGE THE RATE. The refill rate remains the provider's: what
// changes is that calls leave spread across it instead of in minute-sized blocks.
const coinalyzeBurstCeiling = CoinalyzeMaxSymbolsPerRequest

// DefaultCoinalyzeRetryAfter is how long to wait on a 429 that carried NO header.
//
// One minute because the quota window is one minute: waiting less than a full window can
// only land inside the same exhausted window. Zero would mean "resend now", and now is
// exactly when the provider has just said no.
const DefaultCoinalyzeRetryAfter = time.Minute

// DefaultCoinalyzeAttempts is how many times one request is attempted in total.
const DefaultCoinalyzeAttempts = 3

// ErrCoinalyzeTooManySymbols refuses a batch above the ceiling.
//
// REFUSES, does not clamp: clamping would send the 21 symbols to the network having paid
// for 20, which is the worst of both worlds — the request exceeds the provider's ceiling
// AND the local budget believes it did not.
var ErrCoinalyzeTooManySymbols = errors.New("coinalyze: too many symbols in one request")

// CoinalyzeBudgetOptions configures the budget. Every field has a default; tests replace
// the clock and the wait to measure the wait instead of sleeping it.
type CoinalyzeBudgetOptions struct {
	CallsPerMinute int
	Attempts       int
	Now            func() time.Time
	Wait           func(ctx context.Context, d time.Duration) error
	Log            *slog.Logger
}

// CoinalyzeBudget is a token bucket over Coinalyze calls per minute, billed per symbol.
type CoinalyzeBudget struct {
	mu sync.Mutex

	perMinute  int
	attempts   int
	available  float64
	lastRefill time.Time

	// ceiling is the accumulation ceiling. See coinalyzeBurstCeiling.
	ceiling float64

	// spent and refusals are the LOCAL ACCOUNTING: how much this process debited and how
	// many times the provider refused for rate. See CoinalyzeBudgetTally.
	spent    int64
	refusals int64
	// external are the refusals INCOMPATIBLE with local accounting. See classifyRefusal.
	external int64

	// window holds the debits of the last minute, the unit in which the provider counts.
	//
	// THE LIST IS SHORT BY CONSTRUCTION: the bucket does not let this process spend more
	// than `perMinute` in one minute, and each entry costs sixteen bytes. It is pruned on
	// every debit, so there is no path on which it grows.
	window []coinalyzeDebit

	// penaltyUntil suspends ALL spending. Set by `Penalize` when the provider refuses for
	// rate. Having a token does not authorize spending while the provider is still inside
	// the minute it demanded.
	penaltyUntil time.Time

	now  func() time.Time
	wait func(ctx context.Context, d time.Duration) error
	log  *slog.Logger
}

// NewCoinalyzeBudget builds the bucket with defaults filled in.
//
// It starts EMPTY on purpose. A process that boots and spends a whole minute at once is the
// boot burst that has already cost this repository 4,602 HTTP 429s on Binance; the first
// call waiting a few seconds for refill is the price of not repeating that here.
func NewCoinalyzeBudget(opts CoinalyzeBudgetOptions) *CoinalyzeBudget {
	if opts.CallsPerMinute < 1 {
		opts.CallsPerMinute = CoinalyzeCallsPerMinute
	}
	if opts.Attempts < 1 {
		opts.Attempts = DefaultCoinalyzeAttempts
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Wait == nil {
		opts.Wait = coinalyzeSleep
	}

	// THE CEILING NEVER EXCEEDS THE MINUTE: a budget configured below one full request
	// would finance no request at all, and the clamp in `acquire` would be the only thing
	// between the caller and an infinite loop.
	ceiling := float64(coinalyzeBurstCeiling)
	if teto := float64(opts.CallsPerMinute); teto < ceiling {
		ceiling = teto
	}

	return &CoinalyzeBudget{
		perMinute:  opts.CallsPerMinute,
		attempts:   opts.Attempts,
		available:  0,
		lastRefill: opts.Now(),
		ceiling:    ceiling,
		now:        opts.Now,
		wait:       opts.Wait,
		log:        logger.OrDiscard(opts.Log),
	}
}

// Do spends the budget for the symbols, runs the call and, on a 429, waits what the
// provider demanded before resending.
//
// THE RESEND IS BILLED TOO: the refused request reached the provider and counts against its
// ceiling, so resending it for free is spending without counting.
func (b *CoinalyzeBudget) Do(ctx context.Context, symbols []string, call func(context.Context) error) error {
	if len(symbols) > CoinalyzeMaxSymbolsPerRequest {
		// Before any debit: validating after debiting charges for a request that never
		// left.
		return fmt.Errorf("%w: %d symbols requested, ceiling %d", ErrCoinalyzeTooManySymbols, len(symbols), CoinalyzeMaxSymbolsPerRequest)
	}

	var lastErr error
	for attempt := 1; attempt <= b.attempts; attempt++ {
		if err := b.acquire(ctx, len(symbols)); err != nil {
			return err
		}

		err := call(ctx)
		if err == nil {
			return nil
		}
		lastErr = err

		// ONLY A 429 BRAKES THE WHOLE PROCESS. A 5xx is provider unavailability, not a rate
		// refusal: suspending spending there would delay collection without reducing any
		// pressure.
		//
		// THE PENALTY DOES NOT DELAY THIS REQUEST, deliberately: it is set for the SAME
		// duration the loop waits immediately below, so when the next attempt calls
		// `acquire` the window has already expired — punishing the same request twice would
		// only add delay without removing pressure. What it brakes are the OTHER batches of
		// the cycle, which without it keep spending at the full rate against a provider that
		// has just refused; and whatever comes after the terminal failure, where there is no
		// resend wait covering the window. The penalty applies to the CALL, not to the
		// attempt count.
		if coinalyzeIsRateLimited(err) {
			b.declareRefusal(len(symbols))
			b.Penalize(coinalyzeRetryPause(err))
		}

		// Classification belongs to the client, by STATUS — never by the wording of the
		// message.
		if !CoinalyzeIsRetryable(err) {
			return err
		}
		if attempt == b.attempts {
			break
		}

		pause := coinalyzeRetryPause(err)
		logger.OrDiscard(b.log).Warn("coinalyze attempt refused; waiting before resend",
			"attempt", attempt,
			"max_attempts", b.attempts,
			"error", err,
			"wait", pause,
			"symbols", len(symbols))
		if err := b.wait(ctx, pause); err != nil {
			return fmt.Errorf("coinalyze: resend wait interrupted: %w", err)
		}
	}

	return fmt.Errorf("coinalyze: %d attempts for %d symbol(s) and none succeeded: %w", b.attempts, len(symbols), lastErr)
}

// acquire blocks until there is a token for `calls`, or until the context ends.
func (b *CoinalyzeBudget) acquire(ctx context.Context, calls int) error {
	if teto := int(b.ceiling); calls > teto {
		// A batch larger than the ACCUMULATION CEILING would never be fundable — the
		// balance would never reach what it asks for, and the loop below would wait
		// forever. Clamping is the only alternative to locking the caller.
		//
		// THE CLAMP IS BY THE CEILING, NOT BY THE MINUTE: the bucket accumulates one
		// request, not one minute, and a per-minute clamp would fail to protect exactly
		// the range that became unfundable (21 to 40 calls). The refusal in `Do` above 20
		// symbols already makes this unreachable from the production path.
		calls = teto
	}

	for {
		b.mu.Lock()
		b.refillLocked()

		// THE SUSPENSION COMES BEFORE THE BALANCE. Checking the balance first would let
		// through exactly the burst the penalty exists to prevent: during the resend wait
		// the refill rebuilds the whole minute, and the next batch would find a full bucket
		// against a provider that has just refused.
		if penalty := b.penaltyUntil.Sub(b.now()); penalty > 0 {
			b.mu.Unlock()
			if err := b.wait(ctx, penalty); err != nil {
				return fmt.Errorf("coinalyze: penalty wait interrupted: %w", err)
			}
			continue
		}

		var pause time.Duration
		if b.available >= float64(calls) {
			b.available -= float64(calls)
			// COUNTED WHERE THE DEBIT HAPPENS, and by the DEBITED value. Counting at the
			// entry of `Do` would count the request, not the spend: a resend of a 429 is
			// billed again, and a clamped batch costs the ceiling, not what it asked for.
			b.spent += int64(calls)
			b.recordDebitLocked(b.now(), calls)
			b.mu.Unlock()
			return nil
		}
		deficit := float64(calls) - b.available
		pause = time.Duration(deficit / float64(b.perMinute) * float64(time.Minute))
		b.mu.Unlock()

		if pause < 10*time.Millisecond {
			// Floor against a busy loop on a rounding deficit.
			pause = 10 * time.Millisecond
		}
		if err := b.wait(ctx, pause); err != nil {
			return fmt.Errorf("coinalyze: budget wait interrupted: %w", err)
		}
	}
}

// CoinalyzeBudgetTally is the accumulated LOCAL ACCOUNTING of this process.
//
// It exists because the key's real ceiling is not observable any other way: a 200 response
// from this API carries no `X-RateLimit-*` or `RateLimit-*` — measured —, and the only
// signal the provider emits is the `Retry-After` of a 429, that is, afterwards. What can be
// measured is what THIS process spent and how many refusals it took; the rest is inference
// from those two.
//
// BOTH ARE MONOTONIC. A caller that wants one cycle's spend takes the difference between
// two readings — resetting here would make two concurrent readers steal counts from each
// other.
type CoinalyzeBudgetTally struct {
	// CallsSpent are the DEBITED calls, per symbol and per attempt.
	CallsSpent int64
	// RateRefusals are the 429s RECEIVED. A batch that exhausts three attempts counts three.
	RateRefusals int64
	// ExternalQuotaRefusals are the refusals local accounting does NOT explain: the
	// provider refused while this process was emitting below its own ceiling. See
	// classifyRefusal.
	ExternalQuotaRefusals int64
}

// Tally returns the accumulated local accounting.
func (b *CoinalyzeBudget) Tally() CoinalyzeBudgetTally {
	if b == nil {
		return CoinalyzeBudgetTally{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return CoinalyzeBudgetTally{
		CallsSpent:            b.spent,
		RateRefusals:          b.refusals,
		ExternalQuotaRefusals: b.external,
	}
}

// coinalyzeDebit is ONE debit and the instant it left.
type coinalyzeDebit struct {
	at    time.Time
	calls int
}

// CoinalyzeRefusalVerdict is the reading of ONE rate refusal.
type CoinalyzeRefusalVerdict struct {
	// SpentLastMinute is what THIS process debited in the minute preceding the refusal.
	SpentLastMinute int
	// Ceiling is the per-minute ceiling this process budgets — the same one the provider
	// documents per KEY.
	Ceiling int
	// ExternalConsumer says the refusal is NOT explained by local emission.
	ExternalConsumer bool
}

// ─────────────────────────────────────────────────────────────────────────────
// THE REFUSAL THAT ACCOUNTING ITSELF CANNOT EXPLAIN.
//
// # WHAT IS BEING CLAIMED, AND WHAT IS NOT
//
// The claim is about the INCOMPATIBILITY between what this process spent and what it got
// back. It does not identify the other consumer, does not name it and cannot: what local
// accounting can say is "I did not spend that", and that is all it says.
//
// # WHY IT HOLDS
//
// The provider documents 40 calls per minute PER KEY. If this process emitted BELOW its own
// ceiling in the minute preceding the refusal, the refusal cannot have come from its
// emission — the quota was consumed elsewhere. See
// docs/decisions/0031-coinalyze-request-budget.md.
//
// # WHY THE PROCESS DOES NOT FIX IT
//
// Because it is not its to fix: the way out is a dedicated key per environment, or
// disabling collection in the laboratory. Both are operational decisions. What it CAN stop
// doing is lying about the cause — otherwise every refusal becomes "waiting 1m0s", as if it
// were noise from its own emission, and the operator reading the log concludes that the
// fleet is too large.
// ─────────────────────────────────────────────────────────────────────────────

// declareRefusal records ONE 429 received and says, when applicable, that the quota is
// being consumed by another agent.
func (b *CoinalyzeBudget) declareRefusal(symbols int) {
	verdict := b.classifyRefusal()
	if !verdict.ExternalConsumer {
		return
	}
	logger.OrDiscard(b.log).Warn("coinalyze rate refusal is not explained by local accounting; "+
		"another agent with the same key is consuming the quota",
		"spent_last_minute", verdict.SpentLastMinute,
		"ceiling_per_minute", verdict.Ceiling,
		"symbols", symbols)
}

// classifyRefusal counts the refusal and classifies it, all under the same lock: counting
// and classifying in two moves would let the window advance between them.
func (b *CoinalyzeBudget) classifyRefusal() CoinalyzeRefusalVerdict {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.refusals++
	gasto := b.spentInWindowLocked(b.now())
	verdict := CoinalyzeRefusalVerdict{
		SpentLastMinute: gasto,
		Ceiling:         b.perMinute,
		// STRICTLY BELOW. Spending the whole ceiling and being refused IS compatible with
		// local emission, and declaring an external consumer there would be accusing
		// without evidence.
		ExternalConsumer: gasto < b.perMinute,
	}
	if verdict.ExternalConsumer {
		b.external++
	}
	return verdict
}

// recordDebitLocked stores the debit and discards what left the provider window.
func (b *CoinalyzeBudget) recordDebitLocked(now time.Time, calls int) {
	b.window = append(b.window, coinalyzeDebit{at: now, calls: calls})
	b.pruneWindowLocked(now)
}

// pruneWindowLocked discards debits older than one minute.
func (b *CoinalyzeBudget) pruneWindowLocked(now time.Time) {
	fronteira := now.Add(-time.Minute)
	corte := 0
	for corte < len(b.window) && !b.window[corte].at.After(fronteira) {
		corte++
	}
	b.window = b.window[corte:]
}

// spentInWindowLocked sums what was debited in the last minute.
func (b *CoinalyzeBudget) spentInWindowLocked(now time.Time) int {
	b.pruneWindowLocked(now)
	total := 0
	for _, debit := range b.window {
		total += debit.calls
	}
	return total
}

// Penalize suspends ALL spending in the process for `d` and empties the balance.
//
// The resend wait in `Do` is per REQUEST; the provider's ceiling is per PROCESS. Without
// this suspension the other batches of the cycle keep spending at the full rate while the
// refused batch sleeps, and the pressure that caused the 429 never falls — measured on the
// LAN: 82% of resends were refused again, and 38% of the budget went to refused requests.
//
// EMPTY THE BALANCE **AND** SUSPEND: emptying alone is not enough, because the refill
// rebuilds the whole minute inside the resend wait itself, and the next batch would leave
// in a burst.
//
// THE WINDOW IS THE ONE THE PROVIDER DEMANDED, not a number of ours:
// `coinalyzeRetryPause` returns its `Retry-After`, or the declared interval when it asked
// for nothing.
func (b *CoinalyzeBudget) Penalize(d time.Duration) {
	if b == nil || d <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	// THE LONGEST WINS: two in-flight refusals must not shorten each other's wait.
	if until := b.now().Add(d); until.After(b.penaltyUntil) {
		b.penaltyUntil = until
	}
	b.available = 0
	b.lastRefill = b.now()
}

// coinalyzeIsRateLimited reports whether the error is the refusal FOR RATE — 429 — and not
// something else merely retryable. The distinction exists because only that refusal
// justifies braking the whole process.
func coinalyzeIsRateLimited(err error) bool {
	var coinErr *CoinalyzeError
	return errors.As(err, &coinErr) && coinErr.Status == http.StatusTooManyRequests
}

// refillLocked credits the tokens earned since the last credit, with a ceiling of ONE
// REQUEST of balance — banking balance is what a token bucket does, but here what is being
// fought IS the burst, and a whole minute of balance is the burst stored. See
// coinalyzeBurstCeiling.
func (b *CoinalyzeBudget) refillLocked() {
	now := b.now()
	elapsed := now.Sub(b.lastRefill)
	if elapsed <= 0 {
		return
	}
	b.lastRefill = now
	b.available += float64(b.perMinute) * elapsed.Seconds() / 60.0
	if b.available > b.ceiling {
		b.available = b.ceiling
	}
}

// coinalyzeRetryPause returns the wait the provider demanded, or the declared interval when
// it demanded nothing.
//
// THERE IS NO CEILING ON THE PROVIDER'S VALUE: shortening the wait it demanded is resending
// before the provider allows. What limits the wait is the caller's context, which is what
// knows whether the process is shutting down.
func coinalyzeRetryPause(err error) time.Duration {
	var coinErr *CoinalyzeError
	if errors.As(err, &coinErr) && coinErr.RetryAfter > 0 {
		return coinErr.RetryAfter
	}
	return DefaultCoinalyzeRetryAfter
}

// coinalyzeSleep is the production wait: it sleeps, or returns as soon as the context ends.
func coinalyzeSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// CoinalyzeSymbolBatches splits a symbol list into batches of the maximum size accepted per
// request, preserving order.
//
// An off-by-one here silently loses a symbol — and a symbol that vanishes with no one saying
// so is exactly the outcome this batching exists to make visible.
func CoinalyzeSymbolBatches(symbols []string) [][]string {
	if len(symbols) == 0 {
		return nil
	}

	batches := make([][]string, 0, (len(symbols)+CoinalyzeMaxSymbolsPerRequest-1)/CoinalyzeMaxSymbolsPerRequest)
	for start := 0; start < len(symbols); start += CoinalyzeMaxSymbolsPerRequest {
		end := start + CoinalyzeMaxSymbolsPerRequest
		if end > len(symbols) {
			end = len(symbols)
		}
		batches = append(batches, symbols[start:end])
	}
	return batches
}
