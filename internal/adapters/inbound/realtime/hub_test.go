package realtime

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/distribution"
)

// topic is the historical name the actor tests use for a fan-out key; the actor itself
// now lives in distribution and calls it Scope.
type topic = Scope

func TestMain(m *testing.M) {
	flag.Bool("update", false, "rewrite the wire-contract goldens")
	goleak.VerifyTestMain(m)
}

// ---- test doubles -----------------------------------------------------------

var errFakeClosed = errors.New("fake conn closed")

// fakeConn is a deterministic wsConn: ReadMessage pulls injected client frames (or
// errors on close), WriteMessage can be gated (to force a slow consumer) and counts
// concurrent writers (to assert the single-writer invariant).
type fakeConn struct {
	incoming  chan []byte
	closed    chan struct{}
	closeOnce sync.Once
	gate      chan struct{} // if non-nil, WriteMessage blocks until closed

	// hold parks the writePump INSIDE WriteMessage while it is set. Unlike gate (installed at
	// construction and blocking forever) it is installed and RELEASED mid-test, which is what
	// lets a test freeze one connection's writer, drive the actor through a couple of turns, and
	// inspect what the hub did to a frame that is still PENDING — the only state in which
	// delete-on-close is observable.
	holdMu sync.Mutex
	hold   chan struct{}

	mu       sync.Mutex
	writes   [][]byte
	controls []wsControl

	inWrite  int32
	maxWrite int32
}

// wsControl is a recorded control frame. Control frames are the ONLY way the server tells a
// client why it is going away, so they must be observable: without recording them, deleting
// the WriteControl from Hub.reject or from writePump.writeClose leaves every test green
// while real browsers sit on a dead socket until their own read deadline expires.
type wsControl struct {
	typ  int
	data []byte
}

func newFakeConn() *fakeConn {
	return &fakeConn{incoming: make(chan []byte, 64), closed: make(chan struct{})}
}

func (f *fakeConn) ReadMessage() (int, []byte, error) {
	select {
	case msg := <-f.incoming:
		return websocket.TextMessage, msg, nil
	case <-f.closed:
		return 0, nil, errFakeClosed
	}
}

func (f *fakeConn) WriteMessage(_ int, data []byte) error {
	n := atomic.AddInt32(&f.inWrite, 1)
	for {
		m := atomic.LoadInt32(&f.maxWrite)
		if n <= m || atomic.CompareAndSwapInt32(&f.maxWrite, m, n) {
			break
		}
	}
	defer atomic.AddInt32(&f.inWrite, -1)

	if f.gate != nil {
		select {
		case <-f.gate:
		case <-f.closed:
			return errFakeClosed
		}
	}
	f.holdMu.Lock()
	hold := f.hold
	f.holdMu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-f.closed: // a forgotten release must not turn into a goroutine leak
			return errFakeClosed
		}
	}
	select {
	case <-f.closed:
		return errFakeClosed
	default:
	}
	f.mu.Lock()
	f.writes = append(f.writes, append([]byte(nil), data...))
	f.mu.Unlock()
	return nil
}

func (f *fakeConn) WriteControl(messageType int, data []byte, _ time.Time) error {
	f.mu.Lock()
	f.controls = append(f.controls, wsControl{typ: messageType, data: append([]byte(nil), data...)})
	f.mu.Unlock()
	return nil
}

// closeCodes returns the WebSocket close codes the client was sent, in order. gorilla's
// FormatCloseMessage encodes the code as the first two bytes, big-endian.
func (f *fakeConn) closeCodes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int, 0, len(f.controls))
	for _, c := range f.controls {
		if c.typ == websocket.CloseMessage && len(c.data) >= 2 {
			out = append(out, int(binary.BigEndian.Uint16(c.data[:2])))
		}
	}
	return out
}

func (f *fakeConn) SetReadLimit(int64)                {}
func (f *fakeConn) SetReadDeadline(time.Time) error   { return nil }
func (f *fakeConn) SetWriteDeadline(time.Time) error  { return nil }
func (f *fakeConn) SetPongHandler(func(string) error) {}
func (f *fakeConn) Close() error {
	f.closeOnce.Do(func() { close(f.closed) })
	return nil
}

// holdWrites parks the writePump on its next WriteMessage. The returned release func is
// idempotent so a test can defer it and still call it explicitly at the interesting moment.
func (f *fakeConn) holdWrites() func() {
	h := make(chan struct{})
	f.holdMu.Lock()
	f.hold = h
	f.holdMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			f.holdMu.Lock()
			f.hold = nil
			f.holdMu.Unlock()
			close(h)
		})
	}
}

// pendingWrites is the number of WriteMessage calls currently in flight. A test uses it to
// confirm the writePump really is parked before it starts driving the actor.
func (f *fakeConn) pendingWrites() int32 { return atomic.LoadInt32(&f.inWrite) }

func (f *fakeConn) writeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.writes)
}

func (f *fakeConn) framesOfType(typ string) [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out [][]byte
	for _, w := range f.writes {
		if frameType(w) == typ {
			out = append(out, append([]byte(nil), w...))
		}
	}
	return out
}

func frameType(data []byte) string {
	var m struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(data, &m)
	return m.Type
}

type fakeOpens struct {
	mu   sync.Mutex
	data map[Scope]candle.Candle
}

func newFakeOpens() *fakeOpens { return &fakeOpens{data: map[Scope]candle.Candle{}} }

func (f *fakeOpens) set(s Scope, c candle.Candle) {
	f.mu.Lock()
	f.data[s] = c
	f.mu.Unlock()
}

func (f *fakeOpens) PeekLatestCandle(ex, mt, sym, tf string) (candle.Candle, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.data[Scope{Exchange: ex, MarketType: mt, Symbol: sym, Timeframe: tf}]
	return c, ok
}

// statsForTest queries actor-observed counts; polling on it synchronizes async command
// processing deterministically. The actor lives in distribution; this is the wrapper's
// only test-side seam.
func (h *Hub) statsForTest() distribution.Stats { return h.SnapshotStats() }

// pollOnce drives one open-candle tick deterministically, without waiting out the
// production 2s ticker.
func (h *Hub) pollOnce() { h.PollOnce() }

func startHub(t *testing.T, opens OpenCandleSource) *Hub {
	t.Helper()
	h := NewHub(opens, func() int64 { return 1 })
	h.Start()
	return h
}

func stopHub(t *testing.T, h *Hub) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.Stop(ctx)
	select {
	case <-h.Finished():
	default:
		t.Fatal("hub did not finish within Stop deadline")
	}
}

func subscribe(t *testing.T, fc *fakeConn, scope Scope) {
	t.Helper()
	req, err := json.Marshal(clientRequest{Action: "subscribe", Scope: scope})
	require.NoError(t, err)
	fc.incoming <- req
}

func testScope(sym string) Scope {
	return Scope{Exchange: "binance", MarketType: "futures", Symbol: sym, Timeframe: "4h"}
}

func closedCandle(ts int64, closePrice float64) candle.Candle {
	return candle.Candle{Timestamp: ts, LastUpdate: ts, Open: 1, High: 2, Low: 0.5, Close: closePrice, Volume: 10}
}

const (
	eventuallyWait = 3 * time.Second
	eventuallyTick = time.Millisecond
)

// ---- tests ------------------------------------------------------------------

func TestHubStartStopNoLeak(t *testing.T) {
	h := startHub(t, newFakeOpens())
	stopHub(t, h)
}

func TestConnDisconnectCleansRegistry(t *testing.T) {
	h := startHub(t, newFakeOpens())
	defer stopHub(t, h)

	fc := newFakeConn()
	h.add(newClient(fc, h.eventsBufSize))
	subscribe(t, fc, testScope("BTCUSDT"))

	require.Eventually(t, func() bool { return h.statsForTest().Scopes == 1 }, eventuallyWait, eventuallyTick)
	require.Eventually(t, func() bool { return fc.writeCount() >= 1 }, eventuallyWait, eventuallyTick, "snapshot delivered")

	_ = fc.Close() // client disconnects
	require.Eventually(t, func() bool {
		s := h.statsForTest()
		return s.Clients == 0 && s.Scopes == 0
	}, eventuallyWait, eventuallyTick, "registry cleaned + scope GC")
}

func TestEvictSlowClientIsolatesFast(t *testing.T) {
	// A small lossless buffer makes eviction deterministic and fast under -race.
	const buf = 4
	h := NewHub(newFakeOpens(), func() int64 { return 1 }, WithEventsBufSize(buf))
	h.Start()
	scope := testScope("BTCUSDT")

	slow := newFakeConn()
	slow.gate = make(chan struct{}) // writes block => this client cannot drain
	h.add(newClient(slow, h.eventsBufSize))
	subscribe(t, slow, scope)
	// Wait until the slow client is actually SUBSCRIBED (in the index), not merely
	// registered — otherwise the flood could fan out to an empty scope.
	require.Eventually(t, func() bool { return h.statsForTest().Scopes == 1 }, eventuallyWait, eventuallyTick, "slow subscribed")

	fast := newFakeConn()
	h.add(newClient(fast, h.eventsBufSize))
	subscribe(t, fast, scope)
	// The fast client's snapshot write proves it is subscribed (its writes are ungated).
	require.Eventually(t, func() bool { return fast.writeCount() >= 1 }, eventuallyWait, eventuallyTick, "fast subscribed")

	// Send closes ONE AT A TIME, letting the fast client drain each (so its buffer never
	// fills). The gated slow client cannot drain, so its tiny buffer overflows and it is
	// evicted — while the fast client stays connected (head-of-line isolation).
	for i := 0; i < buf+5 && h.statsForTest().Clients == 2; i++ {
		before := fast.writeCount()
		h.IngestClose(scope, closedCandle(int64(1000+i), float64(i)))
		require.Eventually(t, func() bool {
			return fast.writeCount() > before || h.statsForTest().Clients < 2
		}, eventuallyWait, eventuallyTick)
	}

	require.Eventually(t, func() bool { return h.statsForTest().Clients == 1 }, eventuallyWait, eventuallyTick, "slow evicted, fast survives")
	require.Greater(t, fast.writeCount(), buf, "fast received more than a frozen buffer holds")

	close(slow.gate) // let the evicted client's writePump drain and exit
	stopHub(t, h)
}

func TestSnapshotThenContiguousDeltas(t *testing.T) {
	h := startHub(t, newFakeOpens())
	defer stopHub(t, h)
	scope := testScope("BTCUSDT")

	fc := newFakeConn()
	h.add(newClient(fc, h.eventsBufSize))
	subscribe(t, fc, scope)
	require.Eventually(t, func() bool { return h.statsForTest().Scopes == 1 }, eventuallyWait, eventuallyTick)

	for i := 0; i < 3; i++ {
		h.IngestClose(scope, closedCandle(int64(2000+i), float64(i)))
	}
	require.Eventually(t, func() bool { return fc.writeCount() >= 4 }, eventuallyWait, eventuallyTick)

	fc.mu.Lock()
	first := fc.writes[0]
	fc.mu.Unlock()
	require.Equal(t, "snapshot", frameType(first), "first frame is the snapshot")

	// Close deltas carry a contiguous per-scope sequence 1,2,3 after baseline 0.
	var seqs []uint64
	for _, w := range fc.framesOfType(string(EventCandleClose)) {
		var e Envelope
		require.NoError(t, json.Unmarshal(w, &e))
		seqs = append(seqs, e.Seq)
	}
	require.Equal(t, []uint64{1, 2, 3}, seqs)
}

func TestNoOpenAfterCloseRegression(t *testing.T) {
	opens := newFakeOpens()
	h := startHub(t, opens)
	defer stopHub(t, h)
	scope := testScope("BTCUSDT")

	fc := newFakeConn()
	h.add(newClient(fc, h.eventsBufSize))
	subscribe(t, fc, scope)
	require.Eventually(t, func() bool { return h.statsForTest().Scopes == 1 }, eventuallyWait, eventuallyTick)

	// Close bucket at T=3000.
	h.IngestClose(scope, closedCandle(3000, 1))
	require.Eventually(t, func() bool { return len(fc.framesOfType(string(EventCandleClose))) == 1 }, eventuallyWait, eventuallyTick)

	// A stale open at the SAME bucket (Timestamp==lastClosedTs) must be dropped.
	opens.set(scope, candle.Candle{Timestamp: 3000, LastUpdate: 3001, Close: 9})
	h.pollOnce()
	// A fresh open at a LATER bucket must pass (positive control).
	opens.set(scope, candle.Candle{Timestamp: 4000, LastUpdate: 4001, Close: 2})
	h.pollOnce()

	require.Eventually(t, func() bool { return len(fc.framesOfType(string(EventCandleOpenUpdate))) == 1 }, eventuallyWait, eventuallyTick)
	openFrames := fc.framesOfType(string(EventCandleOpenUpdate))
	var e Envelope
	require.NoError(t, json.Unmarshal(openFrames[0], &e))
	require.Equal(t, int64(4000), e.Candle.Timestamp, "only the post-close open is delivered")
}

func TestSingleWriterInvariant(t *testing.T) {
	h := startHub(t, newFakeOpens())
	scope := testScope("BTCUSDT")

	var conns []*fakeConn
	for i := 0; i < 4; i++ {
		fc := newFakeConn()
		conns = append(conns, fc)
		h.add(newClient(fc, h.eventsBufSize))
		subscribe(t, fc, scope)
	}
	require.Eventually(t, func() bool { return h.statsForTest().Clients == 4 }, eventuallyWait, eventuallyTick)

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			h.IngestClose(scope, closedCandle(int64(5000+n), float64(n)))
		}(i)
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		for _, fc := range conns {
			if fc.writeCount() < 10 {
				return false
			}
		}
		return true
	}, eventuallyWait, eventuallyTick)

	stopHub(t, h)
	for _, fc := range conns {
		require.LessOrEqual(t, atomic.LoadInt32(&fc.maxWrite), int32(1), "at most one concurrent WriteMessage per conn")
	}
}

// ---- heartbeat cadence ---------------------------------------------

// clientSilenceTimeoutMirror mirrors `DEFAULT_SILENCE_TIMEOUT_MS` from
// `apps/web/src/services/chartStreamClient.ts` (120 s). Go does not read the TypeScript
// constant, so the coupling is mirrored here and named on both sides: `hub.go` points to the
// client watchdog in the `pingPeriod` declaration, and the client watchdog points back to
// `pingPeriod`. If one of the two clocks changes without the other, this test lights up.
const clientSilenceTimeoutMirror = 120 * time.Second

// TestPingPeriodCadence pins WHEN the tick happens, not that a per-connection heartbeat leaves
// on each tick — `client_test.go` already measures that.
//
// `pingPeriod` is the only heartbeat clock, and the heartbeat cases inject `WithPingPeriod` to
// observe the cadence without waiting 54 s for a tick. That leaves the default VALUE free, so
// this case pins it: a 300 s period is destructive for two independent reasons, and each
// subtest below pins one.
func TestPingPeriodCadence(t *testing.T) {
	t.Parallel()

	t.Run("emits every 54s", func(t *testing.T) {
		t.Parallel()
		// The requirement: `GoWhale` SHALL emit an application heartbeat frame every 54 s per
		// connection. The outcome is a value, and this is where it is pinned.
		require.Equal(t, 54*time.Second, pingPeriod,
			"the heartbeat cadence is 54 s; changing it changes the liveness contract the client watchdog consumes")
	})

	t.Run("stays strictly below pongWait", func(t *testing.T) {
		t.Parallel()
		// A period >= pongWait makes the server's read hit the deadline between two pings and
		// tears down healthy connections.
		require.Less(t, pingPeriod, pongWait,
			"pingPeriod (%s) must be strictly less than pongWait (%s), otherwise SetReadDeadline expires before the next ping",
			pingPeriod, pongWait)
	})

	t.Run("two periods fit under the client silence threshold", func(t *testing.T) {
		t.Parallel()
		// This is the clause that prevents the destructive regression, and its reason: the
		// browser watchdog tears the wire down after `clientSilenceTimeoutMirror` without a RAW
		// ARRIVAL. If one heartbeat period exceeds that threshold, every healthy wire in a quiet
		// market is torn down every 120 s — the self-inflicted defect with the fix becoming the
		// disease. Two periods, not one, because the client tolerates the isolated loss of one
		// heartbeat without declaring death.
		require.Less(t, 2*pingPeriod, clientSilenceTimeoutMirror,
			"two heartbeat periods (%s) must fit under the %s of silence tolerated by the client; above that the watchdog tears down a healthy wire in a quiet market",
			2*pingPeriod, clientSilenceTimeoutMirror)
	})
}
