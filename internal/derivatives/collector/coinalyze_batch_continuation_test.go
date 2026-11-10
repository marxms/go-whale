package collector

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// The two symbols and the three responses the batch brings. Write order is the SORTED one
// (`coinalyzePointSet.symbols`), so `BTCUSDT_PERP.A` is written first — which is why the case fails
// exactly that one: with a `return` on the first failure, ETH never reached storage, and that absence
// is what discriminates.
const (
	coinalyzeDoisMercadosBody = `[
		{"symbol":"BTCUSDT_PERP.A","exchange":"A","symbol_on_exchange":"BTCUSDT","oi_lq_vol_denominated_in":"BASE_ASSET"},
		{"symbol":"ETHUSDT_PERP.A","exchange":"A","symbol_on_exchange":"ETHUSDT","oi_lq_vol_denominated_in":"BASE_ASSET"}
	]`

	coinalyzeDoisOpenInterestBody = `[
		{"symbol":"BTCUSDT_PERP.A","history":[{"t":1787270400,"o":107640.019,"h":111039.103,"l":105678.351,"c":106117.954}]},
		{"symbol":"ETHUSDT_PERP.A","history":[{"t":1787270400,"o":2510.412,"h":2588.900,"l":2470.115,"c":2536.771}]}
	]`

	coinalyzeDoisFundingBody = `[
		{"symbol":"BTCUSDT_PERP.A","history":[{"t":1787270400,"o":0.004964,"h":0.01,"l":0.004964,"c":0.01}]},
		{"symbol":"ETHUSDT_PERP.A","history":[{"t":1787270400,"o":0.006,"h":0.012,"l":0.006,"c":0.012}]}
	]`

	coinalyzeDoisLongShortBody = `[
		{"symbol":"BTCUSDT_PERP.A","history":[{"t":1787270400,"r":0.9554,"l":48.86,"s":51.14}]},
		{"symbol":"ETHUSDT_PERP.A","history":[{"t":1787270400,"r":1.0421,"l":51.03,"s":48.97}]}
	]`
)

// storeQueFalhaPorSimbolo refuses ONE symbol's write and records those that passed.
//
// The file's common double (`recordingCoinalyzeStore`) records the write BEFORE returning the error,
// so it does not separate "reached storage" from "was written" — exactly this case's distinction.
type storeQueFalhaPorSimbolo struct {
	mu       sync.Mutex
	falhaEm  string
	err      error
	gravados []string
}

func (s *storeQueFalhaPorSimbolo) WriteBatch(_ context.Context, batch derivatives.DerivativesBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, snapshot := range batch.Snapshots {
		if snapshot.Symbol == s.falhaEm {
			return s.err
		}
	}
	for _, snapshot := range batch.Snapshots {
		s.gravados = append(s.gravados, snapshot.Symbol)
	}
	return nil
}

func (s *storeQueFalhaPorSimbolo) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.gravados...)
}

// THE FIRST WRITE FAILURE DOES NOT DISCARD THE REST OF THE BATCH.
//
// `write` must not `return` on the first failing `WriteBatch`. The other symbols in the `pointSet` —
// up to 19 in a batch — had already been PAID to the call budget and had already come from the
// network; they simply were not written in that cycle. The next hourly cycle pays it all again to
// harvest the same data.
//
// The criterion is to continue and AGGREGATE: the others write, and the returned error names only
// what failed — an error naming the whole batch would send the operator looking across 20 symbols
// for what happened in 1.
func TestCoinalyzePoller_AFailedWriteDoesNotDiscardTheRestOfThePaidBatch(t *testing.T) {
	poller, _, _, _, sink := newCoinalyzeHarness(t, []string{"1d"}, map[string]string{
		"/future-markets":           coinalyzeDoisMercadosBody,
		"/open-interest-history":    coinalyzeDoisOpenInterestBody,
		"/funding-rate-history":     coinalyzeDoisFundingBody,
		"/long-short-ratio-history": coinalyzeDoisLongShortBody,
	})
	poller.symbolsFunc = func(context.Context) []string { return []string{"BTC/USDT", "ETH/USDT"} }

	tierFora := errors.New("durable tier unavailable")
	store := &storeQueFalhaPorSimbolo{falhaEm: "BTC/USDT", err: tierFora}
	poller.store = store

	err := poller.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("a partially failing batch degrades, it does not abort the cycle: %v", err)
	}

	// THE FOLLOWING SYMBOL WAS WRITTEN IN THE SAME CYCLE. It is the assertion that kills the
	// `return`.
	gravados := store.all()
	if len(gravados) != 1 || gravados[0] != "ETH/USDT" {
		t.Fatalf("written = %v, expected [ETH/USDT] — the first symbol's failure took the rest of the already-paid batch with it", gravados)
	}

	// THE AGGREGATED ERROR NAMES ONLY THE FAILURE. The cycle logs it at WARN.
	linha, achou := warnLineContaining(sink, "BTC/USDT")
	if !achou {
		t.Fatalf("no WARN names the symbol that failed; output: %v", sink.Records())
	}
	if !strings.Contains(linha, tierFora.Error()) {
		t.Fatalf("the WARN does not carry the failure cause: %q", linha)
	}
	if strings.Contains(linha, "ETH/USDT") {
		t.Fatalf("the aggregated error accuses ETH/USDT, which wrote without a problem: %q", linha)
	}
}

// warnLineContaining returns the first WARN-level line containing `substr`.
func warnLineContaining(sink *logger.Sink, substr string) (string, bool) {
	for _, r := range sink.Records() {
		if r.Level == logger.LevelWarn && strings.Contains(r.Text(), substr) {
			return r.Text(), true
		}
	}
	return "", false
}

// AND WHEN NO SYMBOL FAILS, THERE IS NO ERROR. `errors.Join` of an empty slice returns nil, and
// this case pins that: an aggregation returning a non-nil error with zero failures would make every
// healthy cycle look degraded.
func TestCoinalyzePoller_ACleanBatchAggregatesNoError(t *testing.T) {
	poller, _, _, _, sink := newCoinalyzeHarness(t, []string{"1d"}, map[string]string{
		"/future-markets":           coinalyzeDoisMercadosBody,
		"/open-interest-history":    coinalyzeDoisOpenInterestBody,
		"/funding-rate-history":     coinalyzeDoisFundingBody,
		"/long-short-ratio-history": coinalyzeDoisLongShortBody,
	})
	poller.symbolsFunc = func(context.Context) []string { return []string{"BTC/USDT", "ETH/USDT"} }

	store := &storeQueFalhaPorSimbolo{falhaEm: "NONE", err: errors.New("never happens")}
	poller.store = store

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	gravados := store.all()
	if len(gravados) != 2 {
		t.Fatalf("written = %v, expected both symbols", gravados)
	}
	if _, achou := warnLineContaining(sink, "falhou"); achou {
		t.Fatalf("a cycle with no failure at all logged a failure WARN; output: %v", sink.Records())
	}
}
