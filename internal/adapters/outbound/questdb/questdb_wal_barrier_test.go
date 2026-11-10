package questdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/questdb/go-questdb-client/v3"

	"github.com/marxms/go-whale/internal/candle"
)

func TestWALBarrier_FixedTargetAllowsSequencerToKeepAdvancing(t *testing.T) {
	reads := 0
	q := &Store{walPosition: func(context.Context) (coldWALPosition, error) {
		reads++
		return coldWALPosition{writer: 7, sequencer: int64(99 + reads)}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := q.WaitApplied(ctx, 7); err != nil || reads != 1 {
		t.Fatalf("waited for moving sequencer: reads=%d err=%v", reads, err)
	}
}

func TestWALBarrier_SuspendedInvalidAndCancelledDoNotSucceed(t *testing.T) {
	tests := []struct {
		name     string
		position coldWALPosition
		readErr  error
		cancel   bool
	}{
		{name: "suspended", position: coldWALPosition{writer: 5, sequencer: 5, suspended: true}},
		{name: "invalid", position: coldWALPosition{writer: 6, sequencer: 5}},
		{name: "negative", position: coldWALPosition{writer: -1, sequencer: 5}},
		{name: "unavailable", readErr: errors.New("no metadata")},
		{name: "canceled", cancel: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &Store{walPosition: func(context.Context) (coldWALPosition, error) { return tt.position, tt.readErr }}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if tt.cancel {
				cancel()
			}
			if err := q.WaitApplied(ctx, 5); err == nil {
				t.Fatal("invalid/unavailable apply was accepted")
			}
		})
	}
}

func TestWALBarrier_DelayedWriterAndCancellation(t *testing.T) {
	calls := 0
	q := &Store{walPosition: func(context.Context) (coldWALPosition, error) {
		calls++
		writer := int64(3)
		if calls > 1 {
			writer = 4
		}
		return coldWALPosition{writer: writer, sequencer: 9}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := q.WaitApplied(ctx, 4); err != nil || calls != 2 {
		t.Fatalf("did not wait for persisted writer: calls=%d err=%v", calls, err)
	}
	q.walPosition = func(context.Context) (coldWALPosition, error) { return coldWALPosition{writer: 3, sequencer: 4}, nil }
	canceled, cancelWait := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelWait()
	if err := q.WaitApplied(canceled, 4); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancel not propagated: %v", err)
	}
}

func TestWALBarrier_WorkerTurnDrainsPredecessorWithoutAdvancingOwnDay(t *testing.T) {
	sender := newFakeLineSender()
	q := &Store{ilpSender: sender, tableName: "candles", ctx: context.Background(), walPosition: func(context.Context) (coldWALPosition, error) {
		return coldWALPosition{writer: 9, sequencer: 9}, nil
	}}
	entries := scriptedEntries()
	old := &coldDayPlan{days: []*coldDayWrite{
		{entries: entries[:1], acknowledged: true, targetKnown: true, target: 7},
		{entries: entries[1:2], acknowledged: true, targetKnown: true, target: 8},
	}}
	q.pendingPlan = old
	p := NewPipeline(q, 16, time.Hour)
	for i := 1; i <= 2; i++ {
		var input []candleEntry
		if i == 1 {
			input = entries[3:]
		}
		if !p.flushWorkerTurn(input) {
			t.Fatal("confirmed predecessor did not permit the next turn")
		}
		if old.next != i || p.Stats().Flushed != 0 || p.Stats().RetryLen != 1 || p.pendingDays[0].acknowledged || sender.flushes != 0 {
			t.Fatalf("predecessor advanced pipeline DAY: old=%d stats=%+v", old.next, p.Stats())
		}
		if i == 1 && q.pendingPlan != old {
			t.Fatal("predecessor tail was discarded")
		}
	}
	if q.pendingPlan != nil {
		t.Fatal("fully confirmed predecessor was not released")
	}
	if !p.flushWorkerTurn(nil) || sender.flushes != 1 || len(sender.lines) != 1 || sender.lines[0].ts.UnixMilli() != entries[3].Candle.Timestamp || p.Stats().Flushed != 1 || p.Stats().RetryLen != 0 {
		t.Fatalf("own DAY was not submitted exactly once after predecessor: flushes=%d lines=%v stats=%+v", sender.flushes, sender.lines, p.Stats())
	}
}

func TestPipelinePartition_RealILPHTTPWaitsBetweenDays(t *testing.T) {
	var mu sync.Mutex
	bodies := []string{}
	acknowledged := int64(0)
	applied := int64(0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		if acknowledged != applied {
			t.Errorf("next DAY sent before writer applied prior: ack=%d writer=%d", acknowledged, applied)
		}
		bodies = append(bodies, string(body))
		acknowledged++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conf := candleILPConfig(srv.Listener.Addr().String())
	sender, err := questdb.LineSenderFromConf(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sender.Close(context.Background()); err != nil {
			t.Errorf("close ILP sender: %v", err)
		}
	}()
	q := &Store{ilpSender: sender, ilpConfStr: conf, tableName: "candles", ctx: ctx, cancel: cancel, walPosition: func(context.Context) (coldWALPosition, error) {
		mu.Lock()
		defer mu.Unlock()
		applied = acknowledged
		return coldWALPosition{writer: applied, sequencer: acknowledged}, nil
	}}
	p := NewPipeline(q, 16, time.Hour)
	p.flushBatches(scriptedEntries())
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 3 || p.Stats().Flushed != 4 {
		t.Fatalf("real HTTP framing not DAY-bound: bodies=%v stats=%+v", bodies, p.Stats())
	}
	if !strings.Contains(bodies[1], "close=2") || !strings.Contains(bodies[1], "close=3") || strings.Index(bodies[1], "close=2") > strings.Index(bodies[1], "close=3") {
		t.Fatalf("HTTP lost/reordered revisions: %q", bodies[1])
	}
	for i, body := range bodies {
		if n := strings.Count(strings.TrimSpace(body), "\n") + 1; n != []int{1, 2, 1}[i] {
			t.Fatalf("HTTP batch %d rowcount=%d", i, n)
		}
	}
}

func TestWALBarrier_FlushLostACKDoesNotRetrySnapshot(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		if err := conn.Close(); err != nil {
			t.Errorf("close lost-ACK connection: %v", err)
		}
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conf := candleILPConfig(srv.Listener.Addr().String())
	sender, err := questdb.LineSenderFromConf(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sender.Close(context.Background()); err != nil {
			t.Errorf("close ILP sender: %v", err)
		}
	}()
	q := &Store{ilpSender: sender, ilpConfStr: conf, tableName: "candles", ctx: ctx, cancel: cancel}
	groups := map[candle.SeriesKey][]candle.Candle{mustSeriesKey("binance", "future", "BTC/USDT", "5m"): {{Timestamp: 1, Close: 1}}}
	err = q.WriteBatches(ctx, groups)
	var uncertain *candle.ColdWriteUncertainError
	if !errors.As(err, &uncertain) || requests.Load() != 1 {
		t.Fatalf("lost ACK retried or classified unsent: requests=%d err=%v", requests.Load(), err)
	}
}

func TestWALBarrier_AtErrorDoesNotBecomeSentUncertainty(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := newFakeLineSender()
	sender.atErr = io.EOF
	q := &Store{ilpSender: sender, ilpConfStr: candleILPConfig("127.0.0.1:1"), tableName: "candles", ctx: ctx, cancel: cancel}
	groups := map[candle.SeriesKey][]candle.Candle{mustSeriesKey("binance", "future", "BTC/USDT", "5m"): {{Timestamp: 1, Close: 1}}}
	err := q.WriteBatches(ctx, groups)
	var uncertain *candle.ColdWriteUncertainError
	if err == nil || errors.As(err, &uncertain) || sender.flushes != 0 {
		t.Fatalf("pre-send At failure misclassified/transmitted: flushes=%d err=%v", sender.flushes, err)
	}
	if err := q.ilpSender.Close(context.Background()); err != nil {
		t.Errorf("close reset ILP sender: %v", err)
	}
}

// TestWALBarrier_IsolatedQuestDB823 is opt-in: its only writes are in the explicitly
// authorized private database. It verifies real SDK/PG compatibility, not production SLO.
func TestWALBarrier_IsolatedQuestDB823(t *testing.T) {
	if os.Getenv("WAL_TEST_ISOLATED") != "yes" {
		t.Skip("requires an explicitly isolated QuestDB 8.2.3")
	}
	ilpAddress, pgAddress := os.Getenv("WAL_TEST_ILP"), os.Getenv("WAL_TEST_PG")
	if ilpAddress == "" || pgAddress == "" {
		t.Fatal("isolated HTTP and PG addresses required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, "postgres://admin:quest@"+pgAddress+"/qdb?sslmode=disable&pool_max_conns=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var version string
	if err := pool.QueryRow(ctx, "SELECT build").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(version, "8.2.3") {
		t.Fatalf("isolated database version is not 8.2.3: %q", version)
	}
	table := fmt.Sprintf("wal_arch_%d", time.Now().UnixNano())
	ddl := "CREATE TABLE " + table + " (timestamp TIMESTAMP,exchange SYMBOL,market_type SYMBOL,symbol SYMBOL,timeframe SYMBOL,open DOUBLE,high DOUBLE,low DOUBLE,close DOUBLE,volume DOUBLE,taker_buy_volume DOUBLE,taker_sell_volume DOUBLE) TIMESTAMP(timestamp) PARTITION BY DAY WAL DEDUP UPSERT KEYS(timestamp,exchange,market_type,symbol,timeframe)"
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	var observationsMu sync.Mutex
	actualRequests := 0
	var dropNextACK atomic.Bool
	requestDays := []int64{}
	observedPositions := []coldWALPosition{}
	q := &Store{pgPool: pool, tableName: table, ctx: ctx, cancel: cancel}
	// A transparent proxy records the actual Go sender's request boundaries. Metadata
	// and apply barriers below are the production PG implementation, without a fake.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			t.Error(readErr)
			http.Error(w, "read", 500)
			return
		}
		var day int64
		for i, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			var timestamp int64
			if _, err := fmt.Sscanf(line[strings.LastIndex(line, " ")+1:], "%d", &timestamp); err != nil {
				t.Error(err)
				http.Error(w, "timestamp", 500)
				return
			}
			current := time.Unix(0, timestamp).UTC().Truncate(24 * time.Hour).UnixMilli()
			if i > 0 && current != day {
				t.Errorf("real request touches multiple DAYs: %d vs %d", current, day)
			}
			day = current
		}
		position, positionErr := q.readColdWALPosition(ctx)
		if positionErr != nil {
			t.Error(positionErr)
			http.Error(w, "metadata", 500)
			return
		}
		observationsMu.Lock()
		observedPositions = append(observedPositions, position)
		observationsMu.Unlock()
		if position.writer < position.sequencer {
			t.Errorf("next HTTP request entered while previous target unapplied: %+v", position)
		}
		target := url.URL{Scheme: "http", Host: ilpAddress, Path: r.URL.Path, RawQuery: r.URL.RawQuery}
		forwarded, err := http.NewRequestWithContext(ctx, r.Method, target.String(), strings.NewReader(string(body)))
		if err != nil {
			t.Error(err)
			http.Error(w, "request", 500)
			return
		}
		forwarded.Header = r.Header.Clone()
		response, err := http.DefaultClient.Do(forwarded) //nolint:gosec // G704: request targets a server created by this test.
		if err != nil {
			t.Error(err)
			http.Error(w, "forward", 500)
			return
		}
		defer func() {
			if err := response.Body.Close(); err != nil {
				t.Errorf("close forwarded response: %v", err)
			}
		}()
		observationsMu.Lock()
		actualRequests++
		requestDays = append(requestDays, day)
		observationsMu.Unlock()
		if response.StatusCode == http.StatusNoContent && dropNextACK.Swap(false) {
			// The real QuestDB ACK has arrived; only the Go client's ACK is lost.
			_, _ = io.Copy(io.Discard, response.Body)
			connection, _, hijackErr := w.(http.Hijacker).Hijack()
			if hijackErr != nil {
				t.Error(hijackErr)
				return
			}
			if err := connection.Close(); err != nil {
				t.Errorf("close dropped-ACK connection: %v", err)
			}
			return
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	}))
	defer srv.Close()
	conf := candleILPConfig(srv.Listener.Addr().String())
	sender, err := questdb.LineSenderFromConf(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sender.Close(context.Background()); err != nil {
			t.Errorf("close ILP sender: %v", err)
		}
	}()
	q.ilpSender = sender
	q.ilpConfStr = conf
	p := NewPipeline(q, 16, time.Hour)
	// Modern dates avoid server timestamp-range restrictions; equal identities still
	// carry an original close and its later revision through the real sender.
	entries := scriptedEntries()
	base := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
	for i := range entries {
		entries[i].Candle.Timestamp += base
	}
	p.flushBatches(entries)
	observationsMu.Lock()
	requestCount := actualRequests
	observationsMu.Unlock()
	if requestCount != 3 || p.Stats().Flushed != 4 || p.Stats().Blocked {
		t.Fatalf("real integration failed: requests=%d stats=%+v", requestCount, p.Stats())
	}
	var count int64
	if err := pool.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("dedup/content count=%d want3", count)
	}
	// Read every persisted identity/value. QuestDB 8.2.3 PgWire does not match
	// the timestamp predicate with pgx's time.Time bind; compare the complete
	// result instead of inferring a missing financial row.
	rows, err := pool.Query(ctx, "SELECT cast(timestamp as long), close FROM "+table+" ORDER BY timestamp")
	if err != nil {
		t.Fatal(err)
	}
	expectedClose := []float64{1, 3, 4}
	observed := 0
	for rows.Next() {
		var timestamp int64
		var closeValue float64
		if err := rows.Scan(&timestamp, &closeValue); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if observed >= len(expectedClose) || timestamp != (base+int64(observed)*int64(24*time.Hour/time.Millisecond))*1000 || closeValue != expectedClose[observed] {
			rows.Close()
			t.Fatalf("persisted row%d=(%d,%v), expected exact timestamp/value sequence", observed, timestamp, closeValue)
		}
		observed++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if observed != 3 {
		t.Fatalf("persisted identities=%d want3", observed)
	}
	revised := expectedClose[1]

	observationsMu.Lock()
	t.Logf("QuestDB823 real SDK+PG integration: requests=%d request_days=%v positions_before_requests=%+v rows=%d revision=%v", actualRequests, requestDays, observedPositions, count, revised)
	observationsMu.Unlock()

	// A second real pipeline loses its first ACK after the database accepted it.
	// It must fence, retain its tail and never resend that snapshot or send DAY2.
	dropNextACK.Store(true)
	uncertainPipeline := NewPipeline(q, 16, time.Hour)
	uncertainEntries := scriptedEntries()
	for i := range uncertainEntries {
		uncertainEntries[i].Candle.Timestamp += base + 3*int64(24*time.Hour/time.Millisecond)
	}
	uncertainPipeline.flushBatches(uncertainEntries)
	uncertainPipeline.flushBatches(nil)
	uncertainPipeline.Stop(t.Context())
	observationsMu.Lock()
	requestCount = actualRequests
	observationsMu.Unlock()
	if requestCount != 4 || uncertainPipeline.Stats().BlockedPhase != "UNCERTAIN" || uncertainPipeline.Stats().Flushed != 0 || uncertainPipeline.Stats().RetryLen != 4 {
		t.Fatalf("real lost ACK replay/advance: requests=%d stats=%+v", requestCount, uncertainPipeline.Stats())
	}
	// This external observation demonstrates the uncertain first DAY did persist;
	// it deliberately does not resolve the pipeline's uncertainty by inference.
	target, err := q.CaptureApplyTarget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.WaitApplied(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("real ACK-loss content: got%d rows want4 (first DAY only)", count)
	}
	t.Logf("QuestDB823 real ACK lost after server204: requests=%d rows=%d phase=%s retained=%d; no replay or DAY2", requestCount, count, uncertainPipeline.Stats().BlockedPhase, uncertainPipeline.Stats().RetryLen)
}
