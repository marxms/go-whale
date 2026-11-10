package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/distribution"
)

// Transport-side benchmarks for the WebSocket edge. The isolated actor benchmarks
// (fan-out, conflation slot, producer) live in internal/distribution; these price the
// REAL pumps, channel hand-offs and JSON the process pays per connection.
//
// Run: go test -run '^$' -bench . -benchmem -count=6 -timeout 30m ./internal/adapters/inbound/realtime/
// (-race is deliberately NOT used: the race detector distorts timings by ~5-10x.)

// benchConn is a wsConn that counts writes and retains nothing. fakeConn (hub_test.go)
// appends every frame to a slice, which would grow to gigabytes over a benchmark's
// millions of iterations and turn the measurement into an allocator benchmark.
type benchConn struct {
	writes   atomic.Int64
	incoming chan []byte
	closed   chan struct{}
	once     sync.Once
}

func newBenchConn() *benchConn {
	return &benchConn{incoming: make(chan []byte, 128), closed: make(chan struct{})}
}

// ReadMessage blocks until a control frame arrives or the connection closes; returning
// immediately would spin readPump into a hot error loop that steals CPU from the path
// under measurement.
func (b *benchConn) ReadMessage() (int, []byte, error) {
	select {
	case msg := <-b.incoming:
		return websocket.TextMessage, msg, nil
	case <-b.closed:
		return 0, nil, errFakeClosed
	}
}

func (b *benchConn) WriteMessage(int, []byte) error {
	b.writes.Add(1)
	return nil
}

func (b *benchConn) WriteControl(int, []byte, time.Time) error { return nil }
func (b *benchConn) SetReadLimit(int64)                        {}
func (b *benchConn) SetReadDeadline(time.Time) error           { return nil }
func (b *benchConn) SetWriteDeadline(time.Time) error          { return nil }
func (b *benchConn) SetPongHandler(func(string) error)         {}
func (b *benchConn) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

// benchScope spreads load over distinct symbols so the actor's maps are exercised with
// real key variety (one hot key would be an unrealistically cache-friendly best case).
func benchScope(i int) Scope {
	return Scope{
		Exchange:   "binance",
		MarketType: "future",
		Symbol:     fmt.Sprintf("SYM%03dUSDT", i),
		Timeframe:  "4h",
	}
}

// The lane depth is kept modest ON PURPOSE. An earlier 1<<14 version made the 100-client
// registry hold ~220 MB of live channel buffers, and the resulting GC pressure leaked
// into the NEXT benchmark in the same binary. Small lanes keep each sub-benchmark
// independent.
const benchEventsBuf = 2048 // per-client lossless lane depth used by the benchmarks

// BenchmarkEnvelopeMarshal measures the per-frame JSON cost each connection's writePump
// pays. This is the SOCKET half: unlike fanout it is parallel across connections.
func BenchmarkEnvelopeMarshal(b *testing.B) {
	scope := benchScope(0)
	cnd := closedCandle(1_700_000_000_000, 42)

	// mustCarry proves ONCE, before the timed loop, that the bytes being priced are the
	// real wire frame. Checking only `err == nil` would price a Marshal that returned
	// `{}` — a full-frame cost claim has to be anchored to a full frame.
	mustCarry := func(b *testing.B, data []byte, wantType string) {
		b.Helper()
		var got map[string]json.RawMessage
		if err := json.Unmarshal(data, &got); err != nil {
			b.Fatalf("frame is not JSON: %v", err)
		}
		for _, field := range []string{"v", "type", "scope"} {
			if _, ok := got[field]; !ok {
				b.Fatalf("frame missing %q: %s", field, data)
			}
		}
		if string(got["type"]) != `"`+wantType+`"` {
			b.Fatalf("frame type = %s, want %q", got["type"], wantType)
		}
		if !bytes.Contains(data, []byte(scope.Symbol)) {
			b.Fatalf("frame does not carry the scope symbol %q: %s", scope.Symbol, data)
		}
	}

	b.Run("close", func(b *testing.B) {
		env := NewEnvelope(EventCandleClose, scope, cnd, 7, 1)
		first, err := env.Marshal()
		if err != nil {
			b.Fatal(err)
		}
		mustCarry(b, first, string(EventCandleClose))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := env.Marshal(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("open_update", func(b *testing.B) {
		env := NewEnvelope(EventCandleOpenUpdate, scope, cnd, 0, 1)
		first, err := env.Marshal()
		if err != nil {
			b.Fatal(err)
		}
		mustCarry(b, first, string(EventCandleOpenUpdate))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := env.Marshal(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("snapshot", func(b *testing.B) {
		// frame.frame() is the test-only conversion from the historical fixture
		// vocabulary to distribution.Frame. Hoist it out of the timed loop: building a
		// Snapshot there would allocate per iteration and price the fixture, not
		// marshalEventFrame (the production path takes a Frame that the lane already
		// carries). The T05 baseline measured the same production marshal.
		frame := outFrame{snap: &snapshotPayload{scope: scope, baseline: 12, open: &cnd}}.frame()
		first, err := marshalEventFrame(frame)
		if err != nil {
			b.Fatal(err)
		}
		mustCarry(b, first, "snapshot")
		b.ReportAllocs()
		for b.Loop() {
			if _, err := marshalEventFrame(frame); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkOpenTickEndToEnd measures a WHOLE open_update tick: poll, actor conflation and
// every subscriber's writePump flush (map swap, one Envelope.Marshal and one socket write
// per pending scope). Divided by PollInterval it yields the per-client open_update rate.
func BenchmarkOpenTickEndToEnd(b *testing.B) {
	for _, nClients := range []int{1, 10, 100} {
		for _, nScopes := range []int{1, 10, 100} {
			b.Run(fmt.Sprintf("clients=%d/scopes=%d", nClients, nScopes), func(b *testing.B) {
				opens := newFakeOpens()
				h := NewHub(opens, func() int64 { return 1 }, WithEventsBufSize(benchEventsBuf))
				h.Start()
				defer func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					h.Stop(ctx)
				}()

				conns := make([]*benchConn, 0, nClients)
				for i := 0; i < nClients; i++ {
					conn := newBenchConn()
					conns = append(conns, conn)
					c := newClient(conn, benchEventsBuf)
					h.add(c)
					for s := 0; s < nScopes; s++ {
						h.Subscribe(c, benchScope(s))
					}
				}
				awaitWrites(b, conns, int64(nScopes))

				b.ReportAllocs()
				iter := 0
				for b.Loop() {
					iter++
					for s := 0; s < nScopes; s++ {
						opens.set(benchScope(s), candle.Candle{
							Timestamp:  1_700_000_000_000,
							LastUpdate: int64(iter),
							Close:      1,
						})
					}
					h.PollOnce()
					// +1 accounts for the snapshot frame each scope produced at subscribe.
					awaitWrites(b, conns, int64(nScopes*(iter+1)))
				}
				b.StopTimer()

				wantWrites := int64(nScopes * (b.N + 1))
				for i, cn := range conns {
					if got := cn.writes.Load(); got != wantWrites {
						b.Fatalf("conn %d: wrote %d open frames, want %d", i, got, wantWrites)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*nScopes*nClients), "ns/openframe")
			})
		}
	}
}

// BenchmarkPollPeekRealEngine measures the poller's read side against the REAL
// CandleEngine (RLock + map lookup + O(1) ring peek), not a stub.
func BenchmarkPollPeekRealEngine(b *testing.B) {
	for _, nScopes := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("scopes=%d", nScopes), func(b *testing.B) {
			ce := engine.NewCandleEngine(map[string]int{"default": 500})
			scopes := make([]Scope, nScopes)
			for i := range scopes {
				scopes[i] = benchScope(i)
				ce.UpdateCandle(scopes[i].Exchange, scopes[i].MarketType, scopes[i].Symbol,
					scopes[i].Timeframe, closedCandle(1_700_000_000_000, 1))
			}
			var src OpenCandleSource = ce

			for _, s := range scopes {
				got, ok := src.PeekLatestCandle(s.Exchange, s.MarketType, s.Symbol, s.Timeframe)
				if !ok {
					b.Fatalf("missing candle for %v", s)
				}
				if got.Timestamp != int64(1_700_000_000_000) || got.Close != 1 {
					b.Fatalf("peek for %v returned %+v, want the candle that was ingested", s, got)
				}
			}

			b.ReportAllocs()
			for b.Loop() {
				for _, s := range scopes {
					if _, ok := src.PeekLatestCandle(s.Exchange, s.MarketType, s.Symbol, s.Timeframe); !ok {
						b.Fatalf("missing candle for %v", s)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*nScopes), "ns/peek")
		})
	}
}

// BenchmarkBoundaryBurstEndToEnd is the closest local analog of the 00:00 UTC
// boundary: N buckets close in the same instant and the LIVE hub — real actor, real two
// pumps per connection, real channel hand-offs and scheduler — has to absorb them all.
func BenchmarkBoundaryBurstEndToEnd(b *testing.B) {
	const burstScopes = 18
	for _, nClients := range []int{1, 10} {
		b.Run(fmt.Sprintf("clients=%d/burst=%d", nClients, burstScopes), func(b *testing.B) {
			h := NewHub(newFakeOpens(), func() int64 { return 1 }, WithEventsBufSize(benchEventsBuf))
			h.Start()
			conns := make([]*benchConn, 0, nClients)
			for i := 0; i < nClients; i++ {
				conn := newBenchConn()
				conns = append(conns, conn)
				h.add(newClient(conn, benchEventsBuf))
				for s := 0; s < burstScopes; s++ {
					req, err := json.Marshal(clientRequest{Action: "subscribe", Scope: benchScope(s)})
					if err != nil {
						b.Fatal(err)
					}
					conn.incoming <- req
				}
			}
			awaitWrites(b, conns, int64(burstScopes))

			cnd := closedCandle(1_700_000_000_000, 42)
			base := int64(burstScopes)
			bursts := 0

			b.ReportAllocs()
			for b.Loop() {
				cnd.Timestamp++
				for s := 0; s < burstScopes; s++ {
					h.IngestClose(benchScope(s), cnd)
				}
				bursts++
				awaitWrites(b, conns, base+int64(bursts*burstScopes))
			}
			b.StopTimer()

			wantTotal := base + int64(b.N*burstScopes)
			for i, cn := range conns {
				if got := cn.writes.Load(); got != wantTotal {
					b.Fatalf("conn %d: %d frames written, want exactly %d (snapshots + one per close)", i, got, wantTotal)
				}
			}
			if got := h.SnapshotStats().Clients; got != nClients {
				b.Fatalf("hub holds %d clients, want %d (eviction during the burst)", got, nClients)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*burstScopes*nClients), "ns/delivery")

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			h.Stop(ctx)
		})
	}
}

// awaitWrites blocks until every conn has written at least want frames. Predicate
// polling, never a fixed sleep: a sleep would either flake or pad the measurement.
//
// runtime.Gosched() on every failed pass is load-bearing: this spins INSIDE the timed
// region while waiting on the actor and on 2×N pump goroutines.
func awaitWrites(b *testing.B, conns []*benchConn, want int64) {
	b.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		done := true
		for _, c := range conns {
			if c.writes.Load() < want {
				done = false
				break
			}
		}
		if done {
			return
		}
		if time.Now().After(deadline) {
			b.Fatalf("burst not drained: want >=%d writes per conn", want)
		}
		runtime.Gosched()
	}
}

// compile-time proof that the real engine satisfies the hub's read port.
var _ distribution.OpenCandleSource = (*engine.CandleEngine)(nil)
