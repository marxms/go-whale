package collector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
)

// ─────────────────────────────────────────────────────────────────────────────
// A BATCH EXHAUSTED BY RATE REFUSAL STAYS UNSEEDED.
//
// # THE INCIDENT THIS FILE REPRODUCES
//
// Measured in production: four batches of 20 died on 429 and opened a ROW hole — 40 symbols with
// `4h` full and ZERO rows in `1h`, against `459 + 40 = 499`, the resolved fleet. Marking as seeded
// what the source refused is what makes that hole PERMANENT: the next cycle requests the four-bucket
// tail, and the 2000 points are never requested again.
//
// The case descends to the REAL BUDGET — `coinalyze.CoinalyzeBudget` with clock and wait replaced —
// instead of stubbing the spend. It is the only way to exercise the whole path the incident
// walked: provider 429, three attempts, exhaustion, batch error.
// ─────────────────────────────────────────────────────────────────────────────

// relogioDeTeste is the budget's manual clock: the wait is MEASURED and time advances, instead of
// the case sleeping the minutes the provider asked for.
type relogioDeTeste struct {
	mu sync.Mutex
	t  time.Time
}

func (c *relogioDeTeste) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *relogioDeTeste) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// resolvedorFixo is the symbol join without a trip to the markets table: `SYM07/USDT` becomes
// `SYM07_PERP.A`.
type resolvedorFixo struct{}

func (resolvedorFixo) Resolve(_ context.Context, symbol string) (coinalyze.CoinalyzeMarket, error) {
	base, _, ok := strings.Cut(symbol, "/")
	if !ok {
		return coinalyze.CoinalyzeMarket{}, fmt.Errorf("%w: %s", coinalyze.ErrCoinalyzeSymbolNotFound, symbol)
	}
	return coinalyze.CoinalyzeMarket{Symbol: base + "_PERP.A", DenominatedIn: "BASE_ASSET"}, nil
}

// sourceThatRejectsOneBatch answers 429 to every request carrying the marked symbol, and empty history
// to the others.
type sourceThatRejectsOneBatch struct {
	mu        sync.Mutex
	recusando string
	pedidos   []url.Values
}

func (f *sourceThatRejectsOneBatch) reject(symbol string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recusando = symbol
}

func (f *sourceThatRejectsOneBatch) recorded() []url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]url.Values(nil), f.pedidos...)
}

func (f *sourceThatRejectsOneBatch) servidor(t *testing.T) *coinalyze.CoinalyzeClient {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()

		f.mu.Lock()
		if r.URL.Path == coinalyzeOpenInterestPath {
			f.pedidos = append(f.pedidos, query)
		}
		recusando := f.recusando
		f.mu.Unlock()

		if recusando != "" && strings.Contains(query.Get("symbols"), recusando) {
			// THE FRACTIONAL SPELLING MEASURED IN PRODUCTION, so the whole path — parser,
			// penalty, exhaustion — is the real one.
			w.Header().Set("Retry-After", "0.500")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(server.Close)
	return coinalyze.NewCoinalyzeClient("test-key", coinalyze.WithCoinalyzeBaseURL(server.URL))
}

// frotaDeTeste returns the configured symbols and the corresponding provider ids.
func frotaDeTeste(n int) (configurados, ids []string) {
	for i := 0; i < n; i++ {
		configurados = append(configurados, fmt.Sprintf("SYM%02d/USDT", i))
		ids = append(ids, fmt.Sprintf("SYM%02d_PERP.A", i))
	}
	return configurados, ids
}

// A BATCH THAT EXHAUSTS ITS ATTEMPTS ON 429 IS NOT MARKED, AND THE NEXT CYCLE RETAKES IT — while
// the batches that passed stay on the tail.
//
// The fleet has 25 symbols: two batches, 20 and 5. The second is refused.
func TestSeed_ABatchExhaustedByRateRefusalStaysUnseededAndIsRetaken(t *testing.T) {
	const frota = 25
	configurados, ids := frotaDeTeste(frota)
	recusado := ids[frota-1] // it is in the second batch, the one of 5

	fonte := &sourceThatRejectsOneBatch{}
	client := fonte.servidor(t)
	relogio := &relogioDeTeste{t: time.Unix(1787465684, 0).UTC()}

	poller := NewCoinalyzePoller(CoinalyzePollerOptions{
		Client:     client,
		Catalog:    resolvedorFixo{},
		Store:      &recordingCoinalyzeStore{},
		Exchange:   "binance",
		MarketType: "future",
		Symbols:    configurados,
		Timeframes: []string{"1d"},
		Lookback:   72 * time.Hour,
		Now:        relogio.Now,
		// THE REAL BUDGET, with the wait measured instead of slept.
		Budget: coinalyze.NewCoinalyzeBudget(coinalyze.CoinalyzeBudgetOptions{
			CallsPerMinute: coinalyze.CoinalyzeCallsPerMinute,
			Now:            relogio.Now,
			Wait: func(ctx context.Context, d time.Duration) error {
				relogio.advance(d)
				return ctx.Err()
			},
		}),
	})
	poller.newPacer = unpacedTicks

	// Cycle 1: the provider refuses the second batch and serves the first.
	fonte.reject(recusado)
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("a cycle with one refused batch had to degrade, not abort: %v", err)
	}

	// Cycle 2: the provider returns in full. The refused batch is in BACKOFF — it did not return
	// to the deep window at the same pace — and that is what this cycle asserts.
	fonte.reject("")
	before := len(fonte.recorded())
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 2: %v", err)
	}

	const semeadura = int64(2000 * 86400)
	cauda := int64(poller.windowFor("1d", false) / time.Second)

	for _, q := range fonte.recorded()[before:] {
		from, _ := strconv.ParseInt(q.Get("from"), 10, 64)
		to, _ := strconv.ParseInt(q.Get("to"), 10, 64)
		if to-from >= semeadura {
			t.Fatalf("the cycle after the refusal re-seeded at the same pace (%d s): the batch refusal "+
				"must reduce pressure before requesting the deep window again", to-from)
		}
	}

	// Cycle 3: after the wait, the refused batch is retaken in the deep window — it was never
	// marked, and this half keeps backoff from becoming abandonment of deep history.
	before = len(fonte.recorded())
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 3: %v", err)
	}

	var pedidoFundo, pedidoDeCauda url.Values
	for _, q := range fonte.recorded()[before:] {
		from, _ := strconv.ParseInt(q.Get("from"), 10, 64)
		to, _ := strconv.ParseInt(q.Get("to"), 10, 64)
		switch to - from {
		case semeadura:
			pedidoFundo = q
		case cauda:
			pedidoDeCauda = q
		default:
			t.Fatalf("cycle 3 requested a window of %d s, which is neither seeding (%d) nor tail (%d)",
				to-from, semeadura, cauda)
		}
	}

	if pedidoFundo == nil {
		t.Fatal("after the backoff wait, the refused batch did not return to the deep window: it was " +
			"taken as seeded without ever having been collected")
	}
	if pedidoDeCauda == nil {
		t.Fatal("cycle 3 re-seeded the whole fleet: the batches that passed lost their mark")
	}

	// THE FIVE REFUSED ONES, AND ONLY THEM, RETURN TO THE DEEP WINDOW.
	refeitos := strings.Split(pedidoFundo.Get("symbols"), ",")
	if len(refeitos) != frota-coinalyze.CoinalyzeMaxSymbolsPerRequest {
		t.Fatalf("the re-seeding covered %d symbol(s), expected the %d of the refused batch: %v",
			len(refeitos), frota-coinalyze.CoinalyzeMaxSymbolsPerRequest, refeitos)
	}
	for _, id := range ids[coinalyze.CoinalyzeMaxSymbolsPerRequest:] {
		if !strings.Contains(pedidoFundo.Get("symbols"), id) {
			t.Fatalf("%s was refused in cycle 1 and did not return to the deep window: %v", id, refeitos)
		}
	}
	for _, id := range ids[:coinalyze.CoinalyzeMaxSymbolsPerRequest] {
		if strings.Contains(pedidoFundo.Get("symbols"), id) {
			t.Fatalf("%s was collected in cycle 1 and was re-seeded anyway: %v", id, refeitos)
		}
	}
}

// AND THE RATE REFUSAL REACHES THE CALLER AS A RATE REFUSAL.
//
// Classification is what separates "the batch was not collected" from "the batch does not exist":
// a 429 arriving as a generic error would make exhaustion look like unavailability, and the
// decision to retake would become guesswork.
func TestSeed_TheRateRefusalSurvivesTheAttemptsAsARateRefusal(t *testing.T) {
	configurados, ids := frotaDeTeste(1)

	fonte := &sourceThatRejectsOneBatch{}
	client := fonte.servidor(t)
	fonte.reject(ids[0])
	relogio := &relogioDeTeste{t: time.Unix(1787465684, 0).UTC()}

	budget := coinalyze.NewCoinalyzeBudget(coinalyze.CoinalyzeBudgetOptions{
		CallsPerMinute: coinalyze.CoinalyzeCallsPerMinute,
		Now:            relogio.Now,
		Wait: func(ctx context.Context, d time.Duration) error {
			relogio.advance(d)
			return ctx.Err()
		},
	})

	poller := NewCoinalyzePoller(CoinalyzePollerOptions{
		Client:     client,
		Catalog:    resolvedorFixo{},
		Budget:     budget,
		Store:      &recordingCoinalyzeStore{},
		Exchange:   "binance",
		MarketType: "future",
		Symbols:    configurados,
		Timeframes: []string{"1d"},
		Lookback:   72 * time.Hour,
		Now:        relogio.Now,
	})
	poller.newPacer = unpacedTicks

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("the cycle had to degrade: %v", err)
	}

	// THREE ATTEMPTS, and not one more: exhaustion is what returns the batch to the next cycle,
	// and resending forever would spend the client's quota against a provider that already said no.
	if pedidos := len(fonte.recorded()); pedidos != coinalyze.DefaultCoinalyzeAttempts {
		t.Fatalf("the refused batch was requested %d time(s), expected %d attempts",
			pedidos, coinalyze.DefaultCoinalyzeAttempts)
	}

	// AND THE SYMBOL REMAINS AWAITING SEEDING.
	porSemear, jaSemeados := poller.splitBySeeding(context.Background(), nil, ids, relogio.Now())
	if len(jaSemeados) != 0 {
		t.Fatalf("the symbol refused by rate was marked as seeded: %v", jaSemeados)
	}
	if len(porSemear) != 1 {
		t.Fatalf("the refused symbol left the seeding queue: %v", porSemear)
	}
}
