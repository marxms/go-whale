package distribution

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

const testWait = 3 * time.Second

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

// fakeMember is the distribution-level connection double. It records what the actor
// does to it — lane frames, conflated opens, wake signals — without a socket or a pump.
type fakeMember struct {
	lane *Lane

	mu       sync.Mutex
	open     map[Scope]candle.Candle
	wake     chan struct{}
	started  bool
	rejected bool
}

func newFakeMember(size int) *fakeMember {
	return &fakeMember{
		lane: NewLane(size),
		open: make(map[Scope]candle.Candle),
		wake: make(chan struct{}, 1),
	}
}

func (m *fakeMember) Lane() *Lane { return m.lane }

func (m *fakeMember) SetOpen(scope Scope, c candle.Candle) {
	m.mu.Lock()
	m.open[scope] = c
	m.mu.Unlock()
}

func (m *fakeMember) DeleteOpen(scope Scope) {
	m.mu.Lock()
	delete(m.open, scope)
	m.mu.Unlock()
}

func (m *fakeMember) SignalOpen() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *fakeMember) Start(*Hub) {
	m.mu.Lock()
	m.started = true
	m.mu.Unlock()
}

func (m *fakeMember) Reject() {
	m.mu.Lock()
	m.rejected = true
	m.mu.Unlock()
}

func testScope(symbol string) Scope {
	return Scope{Exchange: "binance", MarketType: "future", Symbol: symbol, Timeframe: "5m"}
}

func startTestHub(t *testing.T, opens OpenCandleSource) *Hub {
	t.Helper()
	h := NewHub(opens, func() int64 { return 1 })
	h.Start()
	return h
}

func stopTestHub(t *testing.T, h *Hub) {
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

func registerAndSubscribe(t *testing.T, h *Hub, m *fakeMember, scope Scope) Frame {
	t.Helper()
	h.Register(m)
	h.Subscribe(m, scope)
	frame, ok := recvFrame(t, m.lane, testWait)
	if !ok {
		t.Fatal("no snapshot frame arrived: the actor never applied the subscribe")
	}
	if frame.Snapshot == nil || frame.Snapshot.Scope != scope {
		t.Fatalf("first frame = %+v, want the scope snapshot", frame)
	}
	return frame
}

func recvFrame(t *testing.T, lane *Lane, timeout time.Duration) (Frame, bool) {
	t.Helper()
	select {
	case f, ok := <-lane.Recv():
		return f, ok
	case <-time.After(timeout):
		return Frame{}, false
	}
}

func waitFor(t *testing.T, condition func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(testWait)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

// TestLaneRefusesNewestWhenFullAndClosesIdempotently pins the bounded lossless lane:
// full means the NEW event is refused, never an old one silently displaced, and Close
// is safe more than once and turns later pushes into a refusal instead of a panic.
func TestLaneRefusesNewestWhenFullAndClosesIdempotently(t *testing.T) {
	lane := NewLane(2)
	if got := lane.Cap(); got != 2 {
		t.Fatalf("Cap = %d, want 2", got)
	}
	if !lane.Push(Frame{Envelope: Envelope{Seq: 1}}) || !lane.Push(Frame{Envelope: Envelope{Seq: 2}}) {
		t.Fatal("the first two pushes must fit")
	}
	if lane.Push(Frame{Envelope: Envelope{Seq: 3}}) {
		t.Fatal("a full lane accepted the newest frame: drop-newest is the backpressure contract")
	}
	lane.Close()
	lane.Close()
	if lane.Push(Frame{Envelope: Envelope{Seq: 4}}) {
		t.Fatal("a closed lane accepted a frame")
	}

	var seqs []uint64
	for f := range lane.Recv() {
		seqs = append(seqs, f.Envelope.Seq)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("drained seqs = %v, want [1 2] (the oldest events survive a full lane)", seqs)
	}
}

// TestHubSnapshotPrecedesContiguousDeltas pins the subscription total order:
// [snapshot@B, delta@B+1, ...] with no gap, duplicate or reorder.
func TestHubSnapshotPrecedesContiguousDeltas(t *testing.T) {
	h := startTestHub(t, newFakeOpens())
	defer stopTestHub(t, h)
	scope := testScope("BTC/USDT")

	m := newFakeMember(16)
	snap := registerAndSubscribe(t, h, m, scope)
	if snap.Snapshot.Baseline != 0 {
		t.Fatalf("baseline = %d, want 0 on a fresh scope", snap.Snapshot.Baseline)
	}

	for i := 1; i <= 3; i++ {
		h.IngestClose(scope, closedCandle(int64(1000*i), 1))
	}
	var seqs []uint64
	for i := 0; i < 3; i++ {
		frame, ok := recvFrame(t, m.lane, testWait)
		if !ok {
			t.Fatalf("missing delta %d", i+1)
		}
		if frame.Envelope.Type != EventCandleClose {
			t.Fatalf("delta %d type = %s, want candle.close", i+1, frame.Envelope.Type)
		}
		seqs = append(seqs, frame.Envelope.Seq)
	}
	want := []uint64{1, 2, 3}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("sequences = %v, want %v (per-scope monotonic)", seqs, want)
		}
	}
}

// TestHubEvictsFullLaneWithoutBlockingOthers pins head-of-line isolation: one frozen
// member is evicted while a healthy member still receives every delta.
func TestHubEvictsFullLaneWithoutBlockingOthers(t *testing.T) {
	h := startTestHub(t, newFakeOpens())
	defer stopTestHub(t, h)
	scope := testScope("BTC/USDT")

	slow := newFakeMember(2)
	fast := newFakeMember(32)
	registerAndSubscribe(t, h, slow, scope)
	registerAndSubscribe(t, h, fast, scope)

	for i := 0; i < 6; i++ {
		h.IngestClose(scope, closedCandle(int64(2000+i), 1))
	}
	waitFor(t, func() bool { return h.SnapshotStats().Clients == 1 }, "the full slow member must be evicted")
	if got := h.SnapshotStats().Clients; got != 1 {
		t.Fatalf("clients = %d, want 1", got)
	}

	// The healthy member is lossless: 6 deltas after its snapshot.
	frames := 0
	for frames < 6 {
		if _, ok := recvFrame(t, fast.lane, testWait); !ok {
			t.Fatalf("healthy member received %d/6 deltas", frames)
		}
		frames++
	}
}

// TestHubShutdownClosesEveryLaneAndRejectsLateRegister pins the drain contract: Stop
// closes every lane and waits, and a registration that loses the shutdown race is
// rejected instead of being owned by nobody.
func TestHubShutdownClosesEveryLaneAndRejectsLateRegister(t *testing.T) {
	h := startTestHub(t, newFakeOpens())
	m := newFakeMember(4)
	registerAndSubscribe(t, h, m, testScope("BTC/USDT"))

	stopTestHub(t, h)
	if _, ok := recvFrame(t, m.lane, time.Second); ok {
		t.Fatal("a lane still had a frame after shutdown drain")
	}

	late := newFakeMember(4)
	h.Register(late)
	late.mu.Lock()
	rejected := late.rejected
	started := late.started
	late.mu.Unlock()
	if !rejected || started {
		t.Fatalf("late register rejected=%v started=%v, want rejected and never started", rejected, started)
	}
}

// TestHubRevisionSharesSequenceAndKeepsAnchorMonotonic pins two properties the client
// depends on: a revision consumes the same per-scope sequence, and it never drags the
// seam anchor backwards.
func TestHubRevisionSharesSequenceAndKeepsAnchorMonotonic(t *testing.T) {
	h := startTestHub(t, newFakeOpens())
	defer stopTestHub(t, h)
	scope := testScope("BTC/USDT")

	m := newFakeMember(16)
	registerAndSubscribe(t, h, m, scope)
	h.IngestClose(scope, closedCandle(10_000, 1))
	h.IngestRevised(scope, closedCandle(9_000, 2))

	if _, ok := recvFrame(t, m.lane, testWait); !ok {
		t.Fatal("missing close")
	}
	if _, ok := recvFrame(t, m.lane, testWait); !ok {
		t.Fatal("missing revision")
	}

	second := newFakeMember(4)
	snap := registerAndSubscribe(t, h, second, scope)
	if snap.Snapshot.Baseline != 2 {
		t.Fatalf("baseline = %d, want 2 (the revision consumes the same counter)", snap.Snapshot.Baseline)
	}
	if snap.Snapshot.BaselineTime != 10_000 {
		t.Fatalf("baselineTime = %d, want 10000 (a revision of an older bucket must not move the anchor back)", snap.Snapshot.BaselineTime)
	}
}

// TestHubOpenConflationAndDeleteOnClose pins the conflation slot: an open is overwritten
// latest-wins, then retracted when its own bucket closes.
func TestHubOpenConflationAndDeleteOnClose(t *testing.T) {
	opens := newFakeOpens()
	h := startTestHub(t, opens)
	defer stopTestHub(t, h)
	scope := testScope("BTC/USDT")

	m := newFakeMember(8)
	registerAndSubscribe(t, h, m, scope)

	opens.set(scope, candle.Candle{Timestamp: 1000, LastUpdate: 1, Close: 1})
	h.PollOnce()
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.open[scope].LastUpdate == 1
	}, "the first open was never conflated")

	opens.set(scope, candle.Candle{Timestamp: 1000, LastUpdate: 2, Close: 2})
	h.PollOnce()
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.open[scope].LastUpdate == 2
	}, "the newer open must overwrite the pending slot (latest-wins)")

	h.IngestClose(scope, closedCandle(1000, 1))
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		_, stillPending := m.open[scope]
		return !stillPending
	}, "delete-on-close must retract the pending open for the bucket that just closed")
}
