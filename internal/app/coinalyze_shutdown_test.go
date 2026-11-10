package app

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
	"github.com/marxms/go-whale/internal/derivatives/collector"
)

// THE FIRST CYCLE DOES NOT HAPPEN AFTER SHUTDOWN.
//
// `awaitFleet` returns as soon as the context falls — that is what TestAwaitFleet_HonoursCancellation
// already asserts. The `PollOnce` right below then runs against an ALREADY CANCELED context: symbol
// resolution, the catalog and the three history requests all fail, and shutdown leaves covered in
// warnings that describe the shutdown itself. Noise at that moment is expensive: it is exactly when
// someone is reading the log to understand why the process died.
//
// The signal is the CATALOG: `PollOnce` resolves one symbol at a time before touching the network,
// so zero resolutions is the proof that the cycle did not start.
func TestRunCoinalyzeCycles_ACancelledContextSkipsTheFirstCycle(t *testing.T) {
	reg := &fakeFleetRegistry{}
	reg.set("binance:future", trintaSimbolos(30))

	app := appComFrota(reg)
	app.fleetSettle = time.Millisecond
	app.fleetWait = time.Second

	catalogo := &contandoResolver{}
	poller := collector.NewCoinalyzePoller(collector.CoinalyzePollerOptions{
		Client:      coinalyze.NewCoinalyzeClient("chave-de-teste"),
		Catalog:     catalogo,
		Budget:      recusandoOrcamento{},
		Store:       descartandoStore{},
		Exchange:    "binance",
		MarketType:  "future",
		Symbols:     []string{"BTC/USDT"},
		SymbolsFunc: app.coinalyzeFleet,
		Timeframes:  []string{"1d"},
	})

	// The context has already fallen before the cycle goroutine starts — shutdown during the
	// wait for the fleet, the window in which this actually happens.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pronto := make(chan struct{})
	// NULL meter: what this case measures is shutdown, and the meter's nil receiver is a declared
	// contract — see CoinalyzeSpendMeter.Cycle.
	go func() { app.runCoinalyzeCycles(ctx, poller, nil); close(pronto) }()

	select {
	case <-pronto:
	case <-time.After(3 * time.Second):
		t.Fatal("the loop did not return with the context canceled")
	}

	if visto := catalogo.noPrimeiroCiclo(); visto != 0 {
		t.Fatalf("the first cycle resolved %d symbol(s) with the context already canceled — the cycle ran fully against a dead context", visto)
	}
}
