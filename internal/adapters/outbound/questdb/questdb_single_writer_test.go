package questdb

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/questdb/go-questdb-client/v3"

	"github.com/marxms/go-whale/internal/candle"
)

// THE SINGLE-WRITER INVARIANT, MEASURED INSTEAD OF WRITTEN IN PROSE.
//
// `questdb_store.go` once declared, just above `WriteBatches`: "the QuestDB ILP LineSender is
// NOT goroutine-safe. ONLY the Pipeline flush loop may call WriteBatches". The prose did not
// hold — a SECOND writer (`DerivativesTieredWriter`, on its own cycle goroutine) was attached
// to the SAME `*Store` the pipeline already used.
//
// Measured with two goroutines over the same store: `go test -race` reports a DATA RACE in
// `questdb.(*buffer).Table()`, and WITHOUT `-race` three of three runs end in
// `panic: bytes.Buffer: truncation out of range`. Neither production goroutine has `recover()`,
// so the outcome is the ENTIRE PROCESS going down: streamer down, WebSocket down, pipeline
// down. And swallowing by design does not cover that, because a panic never becomes an `error`
// to be swallowed.
//
// The suite missed it because `cold_singlewriter_test.go` only exercises concurrent producers
// THROUGH the pipeline, against a fake store that implements only `WriteBatches`. No case put
// `WriteBatches` and `WriteDerivativesBatch` on the SAME `*Store` at the same time — which is
// exactly the wiring production does.

// senderOnlyStore assembles a `*Store` with a real ILP sender against a local server, and
// WITHOUT a PG pool: the two paths exercised here touch only the sender.
func senderOnlyStore(t *testing.T) *Store {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = readAllAndDiscard(r)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	conf := "http::addr=" + srv.Listener.Addr().String() + ";"
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sender, err := questdb.LineSenderFromConf(ctx, conf)
	if err != nil {
		t.Fatalf("sender: %v", err)
	}
	t.Cleanup(func() { _ = sender.Close(context.Background()) })

	return &Store{
		ilpSender: sender, ilpConfStr: conf, tableName: "candles",
		ctx: ctx, cancel: cancel, lastFlush: time.Now(),
		// No pool: the erosion protection's source of truth is declared as an empty table.
		// What this case measures is the sender's buffer, not preservation.
		priors: emptyPriorSource{},
	}
}

func readAllAndDiscard(r *http.Request) (int, error) {
	buf := make([]byte, 4096)
	total := 0
	for {
		n, err := r.Body.Read(buf)
		total += n
		if err != nil {
			return total, nil
		}
	}
}

// TestILPSender_TheTwoWritersDoNotShareTheBuffer is the case production demanded.
//
// Run with `-race`: without the lock, it reports a race in `questdb.(*buffer)`. Without
// `-race`, it ends in a panic. With the lock, it passes.
func TestILPSender_TheTwoWritersDoNotShareTheBuffer(t *testing.T) {
	store := senderOnlyStore(t)

	velas := map[candle.SeriesKey][]candle.Candle{
		mustSeriesKey("binance", "future", "BTC/USDT", "1m"): bars(40, time.Now().UnixMilli(), 60_000),
	}
	pontos := samplePoints(40)

	var wg sync.WaitGroup
	// Two goroutines, as production wires them: the pipeline flush loop on one side, the
	// Coinalyze cycle on the other.
	for i := 0; i < 12; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _ = store.WriteBatches(context.Background(), velas) }()
		go func() { defer wg.Done(); _ = store.WriteDerivativesBatch(context.Background(), pontos) }()
	}
	wg.Wait()
}

// AND RECONNECT DOES NOT SWAP THE SENDER UNDER WHOEVER IS WRITING — closing the sender the
// other path has in flight discards its batch and reopens the same race.
func TestILPSender_ReconnectDoesNotRaceTheWriters(t *testing.T) {
	store := senderOnlyStore(t)
	pontos := samplePoints(20)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _ = store.WriteDerivativesBatch(context.Background(), pontos) }()
		go func() { defer wg.Done(); _ = store.reconnectILP(t.Context()) }()
	}
	wg.Wait()
}
