package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/marxms/go-whale/internal/candle"
)

// scale/concurrency stress for the hub actor.
//
// hub_test.go proves each invariant in isolation with 1-4 connections and one scope. That
// leaves the interesting failure class untested: the actor is a SINGLE goroutine that owns
// every registry map, so the bugs that survive a unit test are the ones that only appear
// when many connections mutate the control plane WHILE fanout iterates the very index
// those mutations rewrite (map mutated during range, victim removed mid-iteration,
// cross-scope leakage from a stale index entry, a scope map that is never garbage
// collected, a pump left blocked at shutdown). Every test below drives the REAL pumps and
// the REAL actor; the only double is the wsConn.
//
// Synchronization rule used throughout: NEVER sleep. Subscriptions are asynchronous
// (readPump -> cmds -> actor), so before measuring anything the tests wait on a barrier
// that PROVES the actor already applied the subscribe — either the client's own snapshot
// frame, or a per-client unique "barrier scope" appearing in the actor-reported index.
// Both work because cmds is a single FIFO channel per readPump: if the barrier was
// applied, everything the same pump sent before it was too.

// ---- stress-specific helpers ------------------------------------------------

// newFakeConnBuf is newFakeConn with a deeper inbound queue: the stress tests push
// hundreds of subscribe/unsubscribe frames and must never block the producing goroutine on
// the fake's queue (a blocked test goroutine would be indistinguishable from a hub stall).
func newFakeConnBuf(inbound int) *fakeConn {
	return &fakeConn{incoming: make(chan []byte, inbound), closed: make(chan struct{})}
}

func (f *fakeConn) allWrites() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, 0, len(f.writes))
	for _, w := range f.writes {
		out = append(out, append([]byte(nil), w...))
	}
	return out
}

// recvFrame is the decoded shape shared by every frame kind the client can receive
// (snapshot and the three envelope types all carry "type" and "scope").
type recvFrame struct {
	Typ          string `json:"type"`
	Scope        Scope  `json:"scope"`
	Seq          uint64 `json:"seq"`
	Baseline     uint64 `json:"baseline"`     // snapshot frames only
	BaselineTime int64  `json:"baselineTime"` // snapshot frames only; the seam anchor
	Candle       struct {
		Timestamp int64 `json:"timestamp"`
	} `json:"candle"`
}

// decodeFrames is the STRICT decoder and must only be called from the test goroutine: it
// asserts that every byte string handed to a client is valid JSON.
func decodeFrames(t *testing.T, raw [][]byte) []recvFrame {
	t.Helper()
	out := make([]recvFrame, 0, len(raw))
	for _, w := range raw {
		var f recvFrame
		require.NoError(t, json.Unmarshal(w, &f), "every frame written to a client must be valid JSON")
		out = append(out, f)
	}
	return out
}

// decodeFramesLenient skips undecodable frames instead of failing. Counting helpers used
// inside require.Eventually MUST go through it: testify runs the condition on its own
// goroutine, and require.NoError there calls t.FailNow off the test goroutine, which is
// undefined behavior (it would surface as a hang or a panic rather than a clean failure).
// The "every frame is valid JSON" property is still asserted, by decodeFrames, on the test
// goroutine at the end of each test.
func decodeFramesLenient(raw [][]byte) []recvFrame {
	out := make([]recvFrame, 0, len(raw))
	for _, w := range raw {
		var f recvFrame
		if json.Unmarshal(w, &f) != nil {
			continue
		}
		out = append(out, f)
	}
	return out
}

// countByTypeScope counts delivered frames of a given type on a given scope. Eventually-safe.
func countByTypeScope(fc *fakeConn, typ string, scope Scope) int {
	n := 0
	for _, f := range decodeFramesLenient(fc.allWrites()) {
		if f.Typ == typ && f.Scope == scope {
			n++
		}
	}
	return n
}

// framesFor returns, in delivery order, the frames of a type delivered on a scope. STRICT
// (test-goroutine only) because callers inspect field values, not just counts.
func framesFor(t *testing.T, fc *fakeConn, typ string, scope Scope) []recvFrame {
	t.Helper()
	out := make([]recvFrame, 0, 4)
	for _, f := range decodeFrames(t, fc.allWrites()) {
		if f.Typ == typ && f.Scope == scope {
			out = append(out, f)
		}
	}
	return out
}

// scopesForTest asks the actor for the live fan-out index keys. Polling on it (instead of
// sleeping) is how these tests synchronize with asynchronous command processing.
func (h *Hub) scopesForTest() map[topic]struct{} {
	list := h.ActiveScopes()
	set := make(map[topic]struct{}, len(list))
	for _, s := range list {
		set[s] = struct{}{}
	}
	return set
}

func stressScopes(n int) []Scope {
	out := make([]Scope, n)
	for i := range out {
		out[i] = testScope(fmt.Sprintf("SYM%03d", i))
	}
	return out
}

// ctlFrames pre-marshals the subscribe/unsubscribe bytes for a scope set. Pre-marshaling
// keeps require.* out of the churn goroutines (a FailNow off the test goroutine is
// undefined behavior, not a test failure).
type ctlFrames struct {
	sub   map[Scope][]byte
	unsub map[Scope][]byte
}

func newCtlFrames(t *testing.T, scopes []Scope) ctlFrames {
	t.Helper()
	c := ctlFrames{sub: map[Scope][]byte{}, unsub: map[Scope][]byte{}}
	for _, s := range scopes {
		sub, err := json.Marshal(clientRequest{Action: "subscribe", Scope: s})
		require.NoError(t, err)
		unsub, err := json.Marshal(clientRequest{Action: "unsubscribe", Scope: s})
		require.NoError(t, err)
		c.sub[s] = sub
		c.unsub[s] = unsub
	}
	return c
}

func subReq(t *testing.T, action string, scope Scope) []byte {
	t.Helper()
	req, err := json.Marshal(clientRequest{Action: action, Scope: scope})
	require.NoError(t, err)
	return req
}

// sendCtl pushes a control frame, giving up if the connection is already gone, so a churn
// goroutine can never outlive its connection and trip goleak.
func sendCtl(fc *fakeConn, data []byte) bool {
	select {
	case fc.incoming <- data:
		return true
	case <-fc.closed:
		return false
	}
}

func sortedScopeList(set map[Scope]struct{}) []Scope {
	out := make([]Scope, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out
}

func contiguous(n int) []uint64 {
	out := make([]uint64, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, uint64(i))
	}
	return out
}

// ---- (a) scale / concurrency ------------------------------------------------

// TestConcurrentScopeChurnNeverDeliversUnsubscribedScope pins the isolation property that
// makes the shared index safe: a client may only ever see frames for scopes IT asked for.
//
// The failure this impedes is CROSS-SCOPE misrouting under concurrent index rewrites:
// fanout ranges h.index[scope] while 60 readPumps rewrite that same index, so a hub that
// indexed under the wrong key, or that let a mid-range mutation shift the iteration onto a
// neighboring bucket, would start pushing another symbol's candles at a client — silent
// cross-symbol corruption on the chart, which no single-client test can observe. Plus the
// leak side: once every connection is gone, the registry and all five per-scope maps must be
// empty again.
//
// What this test deliberately does NOT prove: that an unsubscribe has any EFFECT. It only
// compares against each client's own allowed set, which a no-op unsubscribe also satisfies
// (verified by mutation). That property is pinned deterministically in
// TestUnsubscribeStopsDeliveryAndResubscribeRebaselines below.
func TestConcurrentScopeChurnNeverDeliversUnsubscribedScope(t *testing.T) {
	const (
		numClients      = 60
		numScopes       = 20
		scopesPerClient = 4
		churnRounds     = 40
		fanoutRounds    = 15
		witnessCloses   = 3
	)

	// A deep lossless lane keeps eviction OUT of this test: the property under test is
	// index isolation, and an evicted client would silently make it vacuous.
	h := NewHub(newFakeOpens(), func() int64 { return 1 }, WithEventsBufSize(4096))
	h.Start()

	scopes := stressScopes(numScopes)
	witness := scopes[0]

	type peer struct {
		conn    *fakeConn
		allowed map[Scope]struct{}
		frames  ctlFrames
		barrier Scope
	}
	peers := make([]*peer, numClients)
	for i := range peers {
		allowed := make(map[Scope]struct{}, scopesPerClient)
		for j := 0; j < scopesPerClient; j++ {
			allowed[scopes[(i*scopesPerClient+j)%numScopes]] = struct{}{}
		}
		list := sortedScopeList(allowed)
		p := &peer{
			conn:    newFakeConnBuf(churnRounds + 8),
			allowed: allowed,
			frames:  newCtlFrames(t, list),
			// Unique per client: its presence in the actor's index proves THIS client's
			// earlier subscribes were applied (single FIFO cmds channel per readPump).
			barrier: testScope(fmt.Sprintf("BARRIER%03d", i)),
		}
		peers[i] = p
		h.add(newClient(p.conn, h.eventsBufSize))
	}
	require.Eventually(t, func() bool { return h.statsForTest().Clients == numClients },
		eventuallyWait, eventuallyTick, "all connections registered")

	var wg sync.WaitGroup
	// Each client churns ONLY inside its own allowed set. Any frame for a scope outside
	// that set therefore proves the hub leaked a foreign subscription, never the test.
	for i, p := range peers {
		wg.Add(1)
		go func(seed int, p *peer) {
			defer wg.Done()
			list := sortedScopeList(p.allowed)
			rng := rand.New(rand.NewSource(int64(seed))) //nolint:gosec // deterministic churn, not crypto
			for r := 0; r < churnRounds; r++ {
				s := list[rng.Intn(len(list))]
				data := p.frames.sub[s]
				if rng.Intn(2) == 0 {
					data = p.frames.unsub[s]
				}
				if !sendCtl(p.conn, data) {
					return
				}
			}
		}(i, p)
	}
	// Fanout runs CONCURRENTLY with the churn: the actor must interleave index mutation
	// and index iteration in the same turn sequence.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := 0; r < fanoutRounds; r++ {
			for si, s := range scopes {
				h.IngestClose(s, closedCandle(int64(10_000+r*100+si), float64(r)))
			}
		}
	}()
	wg.Wait()

	// Deterministic non-vacuity: put every client back on a known scope, then wait on the
	// per-client barrier so the fanout below cannot race an unapplied subscribe.
	for _, p := range peers {
		require.True(t, sendCtl(p.conn, subReq(t, "subscribe", witness)))
		require.True(t, sendCtl(p.conn, subReq(t, "subscribe", p.barrier)))
	}
	require.Eventually(t, func() bool {
		live := h.scopesForTest()
		for _, p := range peers {
			if _, ok := live[p.barrier]; !ok {
				return false
			}
		}
		return true
	}, eventuallyWait, eventuallyTick, "every client's witness subscribe was applied by the actor")

	for r := 0; r < witnessCloses; r++ {
		h.IngestClose(witness, closedCandle(int64(90_000+r), float64(r)))
	}
	require.Eventually(t, func() bool {
		for _, p := range peers {
			if countByTypeScope(p.conn, string(EventCandleClose), witness) < witnessCloses {
				return false
			}
		}
		return true
	}, eventuallyWait, eventuallyTick, "every client receives the witness deltas")

	for i, p := range peers {
		allowed := p.allowed
		allowed[witness] = struct{}{}
		allowed[p.barrier] = struct{}{}
		for _, f := range decodeFrames(t, p.conn.allWrites()) {
			_, ok := allowed[f.Scope]
			require.Truef(t, ok, "client %d received frame %q for scope %+v it never subscribed to", i, f.Typ, f.Scope)
		}
	}

	// The registry must return to EMPTY once every connection is gone: a surviving *Client
	// or a scope map that outlives its last subscriber is an unbounded leak in a process
	// that runs for weeks.
	for _, p := range peers {
		_ = p.conn.Close()
	}
	require.Eventually(t, func() bool {
		s := h.statsForTest()
		return s.Clients == 0 && s.Scopes == 0
	}, eventuallyWait, eventuallyTick, "registry and per-scope maps fully garbage collected")

	stopHub(t, h)
}

// TestUnsubscribeStopsDeliveryAndResubscribeRebaselines closes the hole the churn test above
// leaves open. Nothing in this package asserted the EFFECT of an unsubscribe: replacing the
// whole body of doUnsubscribe with an unconditional `return` leaves all 160 tests green, and
// so does deleting the duplicate-subscribe guard in doSubscribe. The churn test sends
// thousands of unsubscribe frames but only compares against each client's own allowed set,
// which a no-op unsubscribe satisfies too.
//
// Three properties, each with a concrete production consequence:
// 1. an unsubscribe really detaches the client from the fan-out index — otherwise a
// dashboard keeps being pushed (and keeps rendering) the symbol/timeframe the user
// navigated away from, and a socket that lives for hours only ever accumulates load;
// 2. a duplicate subscribe is idempotent — a second snapshot would hand the client a fresh
// baseline it has already passed, and it would discard live deltas as "already seen";
// 3. a RESUBSCRIBE re-baselines to the sequence the stream reached while the client was
// away, which is the only way the client can tell "I missed 3..5" instead of believing
// the next delta is contiguous with the last one it saw.
//
// Fully deterministic: every wait is a barrier that PROVES the actor already applied the
// step, never a sleep.
func TestUnsubscribeStopsDeliveryAndResubscribeRebaselines(t *testing.T) {
	const (
		beforeCloses = 2
		awayCloses   = 3
	)

	h := NewHub(newFakeOpens(), func() int64 { return 1 })
	h.Start()
	scope := testScope("BTCUSDT")

	// The keeper never leaves. It has two jobs: it holds `scope` so its per-scope sequence is
	// NOT garbage collected when the churner unsubscribes (with no subscriber left the counter
	// legitimately resets, which would hide the re-baselining property), and it proves the
	// closes below were actually fanned out.
	keeper := newFakeConnBuf(8)
	h.add(newClient(keeper, h.eventsBufSize))
	require.True(t, sendCtl(keeper, subReq(t, "subscribe", scope)))

	churner := newFakeConnBuf(8)
	h.add(newClient(churner, h.eventsBufSize))
	require.True(t, sendCtl(churner, subReq(t, "subscribe", scope)))
	// A duplicate subscribe on the SAME scope, immediately followed by a unique barrier scope.
	// cmds is one FIFO channel per readPump, and c.events is FIFO per client, so once the
	// barrier's snapshot has been WRITTEN every frame the duplicate might have produced was
	// already written too.
	require.True(t, sendCtl(churner, subReq(t, "subscribe", scope)))
	dupBarrier := testScope("DUPBARRIER")
	require.True(t, sendCtl(churner, subReq(t, "subscribe", dupBarrier)))
	require.Eventually(t, func() bool {
		return countByTypeScope(churner, "snapshot", dupBarrier) == 1
	}, eventuallyWait, eventuallyTick, "the churner's duplicate subscribe has been processed")

	snaps := framesFor(t, churner, "snapshot", scope)
	require.Len(t, snaps, 1,
		"a duplicate subscribe must not inject a second snapshot: it would rewind the client's baseline")
	require.Equal(t, uint64(0), snaps[0].Baseline,
		"a first subscribe baselines at 0 — the client starts counting from the next delta")

	for i := 1; i <= beforeCloses; i++ {
		h.IngestClose(scope, closedCandle(int64(10_000+i), float64(i)))
	}
	require.Eventually(t, func() bool {
		return countByTypeScope(churner, string(EventCandleClose), scope) == beforeCloses
	}, eventuallyWait, eventuallyTick, "the churner received both deltas while subscribed")

	// Unsubscribe, then a second unique barrier on the same readPump: its arrival in the
	// actor's index proves the unsubscribe was applied BEFORE anything below is ingested.
	require.True(t, sendCtl(churner, subReq(t, "unsubscribe", scope)))
	awayBarrier := testScope("AWAYBARRIER")
	require.True(t, sendCtl(churner, subReq(t, "subscribe", awayBarrier)))
	require.Eventually(t, func() bool {
		_, ok := h.scopesForTest()[awayBarrier]
		return ok
	}, eventuallyWait, eventuallyTick, "the churner's unsubscribe has been applied by the actor")

	for i := 0; i < awayCloses; i++ {
		h.IngestClose(scope, closedCandle(int64(20_000+i), float64(i)))
	}
	// Two barriers are needed to make the "received nothing" check non-vacuous. The keeper
	// proves the closes really were fanned out (a hub that dropped them entirely would
	// otherwise pass), and the churner's own delta on awayBarrier proves its writePump has
	// already drained past anything wrongly enqueued for `scope`.
	require.Eventually(t, func() bool {
		return countByTypeScope(keeper, string(EventCandleClose), scope) == beforeCloses+awayCloses
	}, eventuallyWait, eventuallyTick, "the keeper stayed subscribed and saw every close")
	h.IngestClose(awayBarrier, closedCandle(30_000, 1))
	require.Eventually(t, func() bool {
		return countByTypeScope(churner, string(EventCandleClose), awayBarrier) == 1
	}, eventuallyWait, eventuallyTick, "the churner's data plane is alive on the scope it still holds")

	require.Len(t, framesFor(t, churner, string(EventCandleClose), scope), beforeCloses,
		"an unsubscribed client must stop receiving the scope entirely")

	// The counter kept advancing for the subscriber that stayed: 1..5 with no reset.
	keeperSeqs := make([]uint64, 0, beforeCloses+awayCloses)
	for _, f := range framesFor(t, keeper, string(EventCandleClose), scope) {
		keeperSeqs = append(keeperSeqs, f.Seq)
	}
	require.Equal(t, contiguous(beforeCloses+awayCloses), keeperSeqs,
		"one client leaving must not disturb the per-scope sequence seen by the others")

	require.True(t, sendCtl(churner, subReq(t, "subscribe", scope)))
	require.Eventually(t, func() bool {
		return countByTypeScope(churner, "snapshot", scope) == 2
	}, eventuallyWait, eventuallyTick, "the resubscribe produced a fresh snapshot")
	snaps = framesFor(t, churner, "snapshot", scope)
	require.Equal(t, uint64(beforeCloses+awayCloses), snaps[1].Baseline,
		"the resubscribe snapshot must re-baseline to the sequence reached while the client was away, so the client detects the gap instead of assuming contiguity")

	h.IngestClose(scope, closedCandle(40_000, 1))
	require.Eventually(t, func() bool {
		return countByTypeScope(churner, string(EventCandleClose), scope) == beforeCloses+1
	}, eventuallyWait, eventuallyTick, "the churner is receiving the scope again")
	after := framesFor(t, churner, string(EventCandleClose), scope)
	require.Equal(t, uint64(beforeCloses+awayCloses+1), after[len(after)-1].Seq,
		"the first delta after a resubscribe must be baseline+1 — that is what makes the client's gap detector work across a resubscribe")

	_ = churner.Close()
	_ = keeper.Close()
	stopHub(t, h)
}

// TestBoundaryBurstKeepsPerScopeSequenceContiguous simulates the 00:00 UTC boundary, where
// every timeframe of every symbol closes at once — the worst-case fan-in the ingest channel
// is sized for.
//
// The failure this impedes is a per-scope sequence hole or reorder under fan-in. Sequence
// is the client's ONLY gap detector (the transport is at-most-once), so a hole makes the
// web client rehydrate needlessly and a reorder makes it accept a stale bar as newer. The
// counter is bumped and the frame enqueued in the same actor turn; if those two steps were
// ever split (e.g. a lock-based hub, or stamping outside fanout), the delivered order would
// diverge from the stamped order exactly under this burst.
func TestBoundaryBurstKeepsPerScopeSequenceContiguous(t *testing.T) {
	const (
		numClients     = 20
		numScopes      = 10
		closesPerScope = 25
	)

	h := NewHub(newFakeOpens(), func() int64 { return 1 }, WithEventsBufSize(4096))
	h.Start()
	scopes := stressScopes(numScopes)

	conns := make([]*fakeConn, numClients)
	for i := range conns {
		conns[i] = newFakeConnBuf(numScopes + 4)
		h.add(newClient(conns[i], h.eventsBufSize))
		for _, s := range scopes {
			require.True(t, sendCtl(conns[i], subReq(t, "subscribe", s)))
		}
	}
	// One snapshot per scope per client is the barrier: numScopes writes prove every
	// subscribe of that client was applied, so no close below can miss a subscriber.
	require.Eventually(t, func() bool {
		for _, fc := range conns {
			if fc.writeCount() < numScopes {
				return false
			}
		}
		return true
	}, eventuallyWait, eventuallyTick, "every client has its per-scope snapshot")

	// One goroutine per scope, all firing at once: maximal fan-in on the single ingest
	// channel. Timestamps are intentionally NOT globally ordered — the contract is
	// per-scope sequence integrity, not cross-scope ordering.
	var wg sync.WaitGroup
	for si, s := range scopes {
		wg.Add(1)
		go func(si int, s Scope) {
			defer wg.Done()
			for r := 0; r < closesPerScope; r++ {
				h.IngestClose(s, closedCandle(int64(1_000_000+r*1_000+si), float64(r)))
			}
		}(si, s)
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		for _, fc := range conns {
			if fc.writeCount() < numScopes+numScopes*closesPerScope {
				return false
			}
		}
		return true
	}, eventuallyWait, eventuallyTick, "every client drained the whole burst")

	want := contiguous(closesPerScope)
	for i, fc := range conns {
		perScope := make(map[Scope][]uint64, numScopes)
		for _, f := range decodeFrames(t, fc.allWrites()) {
			if f.Typ == string(EventCandleClose) {
				perScope[f.Scope] = append(perScope[f.Scope], f.Seq)
			}
		}
		require.Lenf(t, perScope, numScopes, "client %d saw deltas on every scope", i)
		for s, seqs := range perScope {
			// Equality (not merely "sorted") pins BOTH properties at once: strictly
			// increasing delivery order AND no missing sequence.
			require.Equalf(t, want, seqs,
				"client %d scope %s: per-scope sequence must be contiguous and in order", i, s.Symbol)
		}
	}

	for _, fc := range conns {
		_ = fc.Close()
	}
	stopHub(t, h)
}

// TestEvictionUnderMultiScopePressureKeepsHealthyClientsLossless pins victim isolation at
// scale: several stuck connections are evicted while every healthy connection still
// receives EVERY close, on EVERY scope, with no hole.
//
// The failure this impedes is head-of-line coupling in the actor. fanout collects victims
// and removes them only after the range; if it removed a victim while ranging (or blocked
// on a full buffer instead of evicting), one stuck TCP socket would stall or corrupt the
// stream of every other subscriber of that scope — the exact outage mode a push
// architecture must not have.
func TestEvictionUnderMultiScopePressureKeepsHealthyClientsLossless(t *testing.T) {
	const (
		numScopes      = 4
		numHealthy     = 6
		numVictims     = 3
		closesPerScope = 10
	)

	h := NewHub(newFakeOpens(), func() int64 { return 1 })
	h.Start()
	scopes := stressScopes(numScopes)

	// Victim buffers hold exactly their snapshots (numScopes + barrier), so the first close
	// after subscribe overflows them deterministically. Healthy buffers are deep, so the
	// same flood can never evict them — that asymmetry is what makes isolation measurable.
	victims := make([]*fakeConn, numVictims)
	victimBarriers := make([]Scope, numVictims)
	for i := range victims {
		fc := newFakeConnBuf(numScopes + 4)
		fc.gate = make(chan struct{}) // writes block forever => cannot drain
		victims[i] = fc
		victimBarriers[i] = testScope(fmt.Sprintf("VBARRIER%02d", i))
		h.add(newClient(fc, numScopes+1))
	}
	healthy := make([]*fakeConn, numHealthy)
	for i := range healthy {
		fc := newFakeConnBuf(numScopes + 4)
		healthy[i] = fc
		h.add(newClient(fc, 4096))
	}
	for _, fc := range append(append([]*fakeConn{}, victims...), healthy...) {
		for _, s := range scopes {
			require.True(t, sendCtl(fc, subReq(t, "subscribe", s)))
		}
	}
	// A gated victim writes nothing, so its snapshots cannot serve as a barrier; its unique
	// barrier scope appearing in the index proves its subscribes were applied. Without this
	// the flood could be fully consumed BEFORE a victim was ever indexed, and the victim
	// would never be pressured at all (a silently vacuous test).
	for i, fc := range victims {
		require.True(t, sendCtl(fc, subReq(t, "subscribe", victimBarriers[i])))
	}
	require.Eventually(t, func() bool {
		live := h.scopesForTest()
		for _, b := range victimBarriers {
			if _, ok := live[b]; !ok {
				return false
			}
		}
		return true
	}, eventuallyWait, eventuallyTick, "every stuck client is indexed on every scope")
	require.Eventually(t, func() bool {
		for _, fc := range healthy {
			if fc.writeCount() < numScopes {
				return false
			}
		}
		return true
	}, eventuallyWait, eventuallyTick, "healthy clients subscribed to every scope")

	for r := 0; r < closesPerScope; r++ {
		for si, s := range scopes {
			h.IngestClose(s, closedCandle(int64(2_000_000+r*1_000+si), float64(r)))
		}
	}

	require.Eventually(t, func() bool { return h.statsForTest().Clients == numHealthy },
		eventuallyWait, eventuallyTick, "every stuck client evicted, every healthy client retained")

	require.Eventually(t, func() bool {
		for _, fc := range healthy {
			if fc.writeCount() < numScopes+numScopes*closesPerScope {
				return false
			}
		}
		return true
	}, eventuallyWait, eventuallyTick, "healthy clients received the full flood")

	want := contiguous(closesPerScope)
	for i, fc := range healthy {
		perScope := make(map[Scope][]uint64, numScopes)
		for _, f := range decodeFrames(t, fc.allWrites()) {
			if f.Typ == string(EventCandleClose) {
				perScope[f.Scope] = append(perScope[f.Scope], f.Seq)
			}
		}
		require.Lenf(t, perScope, numScopes, "healthy client %d kept every scope", i)
		for s, seqs := range perScope {
			require.Equalf(t, want, seqs,
				"healthy client %d scope %s stayed lossless while victims were evicted", i, s.Symbol)
		}
	}

	for _, fc := range victims {
		close(fc.gate) // release the blocked writePumps so shutdown can complete
	}
	for _, fc := range healthy {
		_ = fc.Close()
	}
	stopHub(t, h)
}

// TestShutdownUnderLoadClosesEveryConnectionWithoutLeak stops the hub while subscribes and
// fanout are still in flight.
//
// The failures this impedes are the classic shutdown bugs of this design: a pump left
// blocked on a send to the actor (goroutine leak — caught by goleak in TestMain), a
// send-on-closed-channel panic if anything but the actor closed c.events, an FD never
// closed because the drain skipped a client, and Stop() hanging past its deadline. It also
// covers connect-during-shutdown: an h.add that loses the race must reject the connection
// AND close it, never spawn a pump (which would race connWg.Wait).
func TestShutdownUnderLoadClosesEveryConnectionWithoutLeak(t *testing.T) {
	const (
		numClients   = 40
		numScopes    = 5
		churnRounds  = 20
		fanoutRounds = 200
	)

	h := NewHub(newFakeOpens(), func() int64 { return 1 })
	h.Start()
	scopes := stressScopes(numScopes)
	frames := newCtlFrames(t, scopes)

	conns := make([]*fakeConn, numClients)
	for i := range conns {
		conns[i] = newFakeConnBuf(numScopes*churnRounds + 8)
		h.add(newClient(conns[i], h.eventsBufSize))
	}

	var wg sync.WaitGroup
	for _, fc := range conns {
		wg.Add(1)
		go func(fc *fakeConn) {
			defer wg.Done()
			for r := 0; r < churnRounds; r++ {
				for _, s := range scopes {
					data := frames.sub[s]
					if r%3 == 2 {
						data = frames.unsub[s]
					}
					if !sendCtl(fc, data) {
						return
					}
				}
			}
		}(fc)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := 0; r < fanoutRounds; r++ {
			for si, s := range scopes {
				h.IngestClose(s, closedCandle(int64(3_000_000+r*1_000+si), float64(r)))
			}
		}
	}()
	// Stop must land MID-FLIGHT. Without this gate the test could stop an already-idle hub
	// and prove nothing about draining under load.
	require.Eventually(t, func() bool {
		return len(h.scopesForTest()) > 0 && conns[0].writeCount() > 0
	}, eventuallyWait, eventuallyTick, "traffic is flowing before Stop")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h.Stop(ctx)
	wg.Wait()

	select {
	case <-h.Finished():
	default:
		t.Fatal("hub did not finish draining within the Stop deadline")
	}

	// The writePump is the SOLE closer of the FD; if the drain missed a client, its socket
	// would stay open for the life of the process. And a clean drain owes every client a
	// GOING_AWAY, otherwise 40 browsers wait out a 60s read deadline before reconnecting to
	// the replacement instance instead of reconnecting immediately.
	for i, fc := range conns {
		select {
		case <-fc.closed:
		default:
			t.Fatalf("connection %d was not closed by shutdown", i)
		}
		require.Equalf(t, []int{websocket.CloseGoingAway}, fc.closeCodes(),
			"connection %d must be told GOING_AWAY exactly once by the drain", i)
	}

	// The engine hook calls IngestClose from its own goroutine and knows nothing about
	// shutdown. If the post-shutdown send had no escape it would block forever, wedging the
	// candle engine mid-shutdown; asserted off the test goroutine so a regression reports as a
	// failure instead of a 10-minute test timeout.
	ingested := make(chan struct{})
	go func() {
		h.IngestClose(scopes[0], closedCandle(9_000_000, 1))
		h.pollOnce()
		close(ingested)
	}()
	require.Eventually(t, func() bool {
		select {
		case <-ingested:
			return true
		default:
			return false
		}
	}, eventuallyWait, eventuallyTick, "ingest and poll must not block after the hub has stopped")

	// Stop is documented as safe to call repeatedly; a second close(done) would panic.
	h.Stop(ctx)
}

// TestUpgradeAfterShutdownIsRejectedAndClosed covers the connect-during-shutdown path,
// which is REACHABLE IN PRODUCTION rather than theoretical: App.Shutdown calls hub.Stop
// before httpServer.Shutdown, so /api/ws keeps upgrading for seconds after the hub's
// `done` is closed.
//
// The failure this impedes: `add` racing a BUFFERED cmds send against `<-done` in a single
// select — Go picks uniformly among ready cases — so about half of the late connections have
// their register parked in a buffer run() no longer reads. Nobody owns the connection: no
// pump, no drain, no Close, no GOING_AWAY. The client sits on a dead socket until its own
// read deadline instead of reconnecting to the replacement instance.
func TestUpgradeAfterShutdownIsRejectedAndClosed(t *testing.T) {
	h := NewHub(newFakeOpens(), func() int64 { return 1 })
	h.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.Stop(ctx)

	// Repeated because the bug was probabilistic: a single attempt passed ~50% of the time.
	for i := 0; i < 32; i++ {
		late := newFakeConnBuf(4)
		h.add(newClient(late, h.eventsBufSize))
		select {
		case <-late.closed:
		default:
			t.Fatalf("late upgrade %d was neither registered nor closed — its FD is owned by nobody", i)
		}
		// Closing the FD is only half of the contract: the client must be TOLD to go away, or
		// it sits on a socket that is gone until its own read deadline expires instead of
		// reconnecting to the replacement instance. Nothing else in the package pins this —
		// deleting the WriteControl from reject() is otherwise invisible.
		require.Equalf(t, []int{websocket.CloseGoingAway}, late.closeCodes(),
			"late upgrade %d must be told GOING_AWAY exactly once", i)
		// And it must never be served a data frame: spawning a pump after connWg.Wait would be
		// a use-after-shutdown.
		require.Zerof(t, late.writeCount(), "late upgrade %d must not be served data frames", i)
	}
}

// TestSingleWriterHoldsAcrossAllThreeWritePaths extends the check in hub_test.go,
// which only exercises the close-delta path. A connection has THREE distinct write sites
// (snapshot, close delta, conflated open flush) and the open flush is the dangerous one: it
// is driven by c.wake and reads c.open, so a design that flushed opens from the actor (or
// from a third goroutine) would produce two concurrent writers on one socket — interleaved
// frames on the wire, which gorilla explicitly does not tolerate.
func TestSingleWriterHoldsAcrossAllThreeWritePaths(t *testing.T) {
	const (
		numClients = 12
		rounds     = 150
	)

	opens := newFakeOpens()
	h := NewHub(opens, func() int64 { return 1 }, WithEventsBufSize(4096))
	h.Start()

	// Distinct scopes for closes and opens: an open at/behind its own close is legitimately
	// suppressed by, so mixing them here could starve the open path and make the
	// concurrency claim vacuous.
	closeScope := testScope("CLOSES")
	openScope := testScope("OPENS")

	conns := make([]*fakeConn, numClients)
	for i := range conns {
		conns[i] = newFakeConnBuf(64)
		h.add(newClient(conns[i], h.eventsBufSize))
		require.True(t, sendCtl(conns[i], subReq(t, "subscribe", closeScope)))
		require.True(t, sendCtl(conns[i], subReq(t, "subscribe", openScope)))
	}
	// Two snapshots per client is the barrier: both subscribes are applied, so the traffic
	// below reaches every connection.
	require.Eventually(t, func() bool {
		for _, fc := range conns {
			if fc.writeCount() < 2 {
				return false
			}
		}
		return true
	}, eventuallyWait, eventuallyTick, "every client subscribed to both scopes")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for r := 0; r < rounds; r++ {
			h.IngestClose(closeScope, closedCandle(int64(4_000_000+r), float64(r)))
		}
	}()
	go func() {
		defer wg.Done()
		for r := 0; r < rounds; r++ {
			// A fresh LastUpdate every tick defeats the dedupe, so the open lane really
			// wakes the writePump instead of being conflated away.
			opens.set(openScope, candle.Candle{Timestamp: 5_000_000, LastUpdate: int64(6_000_000 + r), Close: float64(r)})
			h.pollOnce()
		}
	}()
	wg.Wait()

	require.Eventually(t, func() bool {
		for _, fc := range conns {
			var snap, closes, openUpdates int
			for _, f := range decodeFramesLenient(fc.allWrites()) {
				switch f.Typ {
				case "snapshot":
					snap++
				case string(EventCandleClose):
					closes++
				case string(EventCandleOpenUpdate):
					openUpdates++
				}
			}
			if snap < 2 || closes < 1 || openUpdates < 1 {
				return false
			}
		}
		return true
	}, eventuallyWait, eventuallyTick, "all three write paths were exercised on every connection")

	for _, fc := range conns {
		_ = fc.Close()
	}
	stopHub(t, h)

	for i, fc := range conns {
		require.LessOrEqualf(t, atomic.LoadInt32(&fc.maxWrite), int32(1),
			"connection %d had concurrent WriteMessage callers (sole-writer invariant violated)", i)
		// Strict decode on the test goroutine: interleaved writers corrupt frames rather than
		// merely racing a counter, so byte-level validity is part of the same property.
		require.NotEmptyf(t, decodeFrames(t, fc.allWrites()), "connection %d wrote decodable frames", i)
	}
}

// TestOpenNeverDeliveredAtOrBehindItsCloseUnderRace is the racing counterpart of
// TestNoOpenAfterCloseRegression in hub_test.go, which drives poll and close SEQUENTIALLY.
// It pins the SAFETY half of no open bar is ever handed to a client at or behind a
// bucket the same client has already seen close, which on the chart would mean a live bar
// overwriting the bar that just finalized.
//
// The pacing is load-bearing, and both of the naive shapes are blind:
// - two free-running loops: `IngestClose` is a single send on a 4096-deep buffered channel
// while `pollOnce` pays two blocking actor round-trips, so the closer outruns the poller
// by orders of magnitude, lastClosedTs is permanently ahead, and EVERY raced open is
// rejected. Measured: ZERO opens delivered during the race across 5 runs, leaving the
// assertion below evaluated only against the trivially-safe positive control.
// - handing the bucket to the closer only AFTER pollOnce: now every open the poller ships
// is strictly ahead of every close, so nothing stale is ever offered to the guard.
//
// So this test runs BOTH pacings at once, on two scopes, concurrently:
// - LEAD hands off after pollOnce => the open reliably reaches the actor first and sits
// PENDING in c.open while its close arrives (and, empirically, 30-55 opens per run are
// delivered, which is what keeps the assertion below non-vacuous);
// - LAG hands off before pollOnce => pollOnce and IngestClose race for the same bucket,
// so the batch usually lands STALE and the lastClosedTs admission guard is what has to
// reject it (mutation-verified: deleting that guard fails this test).
//
// Note on what this does NOT prove: whether a pending open is RETRACTED by delete-on-close is
// decided here by which of c.events/c.wake the writePump happens to pick, so a hub with
// delete-on-close removed still passes most runs. That mechanism is pinned deterministically
// by TestPendingOpenIsRetractedWhenItsBucketCloses below.
func TestOpenNeverDeliveredAtOrBehindItsCloseUnderRace(t *testing.T) {
	const steps = 300

	opens := newFakeOpens()
	h := NewHub(opens, func() int64 { return 1 }, WithEventsBufSize(4096))
	h.Start()
	leadScope := testScope("LEADOPEN")
	lagScope := testScope("LAGOPEN")

	fc := newFakeConnBuf(8)
	h.add(newClient(fc, h.eventsBufSize))
	for _, s := range []Scope{leadScope, lagScope} {
		require.True(t, sendCtl(fc, subReq(t, "subscribe", s)))
	}
	// Two snapshots => both subscribes applied, so no close below can miss the subscriber.
	require.Eventually(t, func() bool { return fc.writeCount() >= 2 },
		eventuallyWait, eventuallyTick, "both subscribes applied")

	var wg sync.WaitGroup
	// Opens and closes walk the SAME timestamp grid per scope, so "open at exactly its own
	// close" and "open behind the last close" are both reachable.
	race := func(scope Scope, handoffAfterPoll bool) {
		closable := make(chan int64) // unbuffered: keeps the two producers interleaved
		wg.Add(2)
		go func() {
			defer wg.Done()
			defer close(closable)
			for i := 1; i <= steps; i++ {
				ts := int64(1_000 * i)
				opens.set(scope, candle.Candle{Timestamp: ts, LastUpdate: int64(7_000_000 + i), Close: float64(i)})
				if handoffAfterPoll {
					h.pollOnce()
					closable <- ts
					continue
				}
				closable <- ts
				h.pollOnce()
			}
		}()
		go func() {
			defer wg.Done()
			for ts := range closable {
				h.IngestClose(scope, closedCandle(ts, float64(ts)))
			}
		}()
	}
	race(leadScope, true)
	race(lagScope, false)
	wg.Wait()

	require.Eventually(t, func() bool {
		return countByTypeScope(fc, string(EventCandleClose), leadScope) == steps &&
			countByTypeScope(fc, string(EventCandleClose), lagScope) == steps
	}, eventuallyWait, eventuallyTick, "all closes delivered on both scopes")

	// Positive control on the LAG scope, whose opens are almost all legitimately rejected: an
	// open strictly ahead of every close MUST still get through, so a hub that simply stopped
	// serving that scope's open lane cannot pass. Matched by its OWN timestamp — waiting for
	// "any open frame" would be satisfied by a leftover from the race and would stop being a
	// control. One pollOnce suffices (lastClosedTs is 1_000*steps, so admission is
	// deterministic) and keeps pollOnce off testify's condition goroutine.
	const controlTs = int64(1_000 * (steps + 10))
	opens.set(lagScope, candle.Candle{Timestamp: controlTs, LastUpdate: 9_999_999, Close: 1})
	h.pollOnce()
	require.Eventually(t, func() bool {
		for _, f := range decodeFramesLenient(fc.allWrites()) {
			if f.Typ == string(EventCandleOpenUpdate) && f.Scope == lagScope && f.Candle.Timestamp == controlTs {
				return true
			}
		}
		return false
	}, eventuallyWait, eventuallyTick, "a strictly-ahead open is delivered")

	const lastRacedTs = int64(1_000 * steps)
	maxClosedTs := map[Scope]int64{}
	racedOpens := map[Scope]int{}
	for _, f := range decodeFrames(t, fc.allWrites()) {
		switch f.Typ {
		case string(EventCandleClose), string(EventCandleRevised):
			if f.Candle.Timestamp > maxClosedTs[f.Scope] {
				maxClosedTs[f.Scope] = f.Candle.Timestamp
			}
		case string(EventCandleOpenUpdate):
			if f.Candle.Timestamp <= lastRacedTs {
				racedOpens[f.Scope]++
			}
			// Per scope: the sequence of closes on one symbol says nothing about another's.
			require.Greaterf(t, f.Candle.Timestamp, maxClosedTs[f.Scope],
				"%s: open bar at %d was delivered after its bucket closed at %d",
				f.Scope.Symbol, f.Candle.Timestamp, maxClosedTs[f.Scope])
		}
	}
	// ANTI-VACUITY: the positive control alone would satisfy the check above, so the race must
	// have delivered opens of its own. If this ever hits zero the test has stopped exercising
	// the interleaving and must be re-paced rather than left green.
	require.Positivef(t, racedOpens[leadScope],
		"no open was delivered during the race (only the positive control at ts>%d): the delete-on-close assertion above proved nothing", lastRacedTs)

	_ = fc.Close()
	stopHub(t, h)
}

// TestPendingOpenIsRetractedWhenItsBucketCloses pins delete-on-close — the second, and
// weaker-tested, half of: an open bar that has already been conflated into a client's
// pending slot must be RETRACTED when its own bucket closes, never flushed afterwards.
//
// Why it needs its own test: in the raced test above, whether a surviving pending open becomes
// visible depends on which of c.events / c.wake the writePump happens to pick, so removing
// delete-on-close from fanout stays green on most runs (verified with -count=3). Here the
// client's writePump is PARKED inside WriteMessage while the actor is driven through the two
// relevant turns, which forces exactly the state the invariant is about and makes the outcome
// independent of select order.
//
// The assertion is white-box on purpose: the pending slot IS the invariant ("no open survives
// its bucket's close"), and reading it is deterministic where reading the wire is not.
func TestPendingOpenIsRetractedWhenItsBucketCloses(t *testing.T) {
	const (
		fillerTs = int64(1_000)
		bucketTs = int64(2_000)
	)

	opens := newFakeOpens()
	h := NewHub(opens, func() int64 { return 1 }, WithEventsBufSize(64))
	h.Start()
	scope := testScope("BTCUSDT")

	// The keeper is never parked. It is the barrier that proves the actor really did process
	// the open batch and then the close while the other client's writer was frozen.
	keeper := newFakeConnBuf(8)
	h.add(newClient(keeper, h.eventsBufSize))
	require.True(t, sendCtl(keeper, subReq(t, "subscribe", scope)))

	frozenConn := newFakeConnBuf(8)
	frozen := newClient(frozenConn, h.eventsBufSize)
	h.add(frozen)
	require.True(t, sendCtl(frozenConn, subReq(t, "subscribe", scope)))
	require.Eventually(t, func() bool { return frozenConn.writeCount() >= 1 && keeper.writeCount() >= 1 },
		eventuallyWait, eventuallyTick, "both clients have their snapshot")

	// Park the frozen client's writer: it dequeues the filler delta and blocks inside
	// WriteMessage, so nothing handed to it afterwards can reach the wire until we release it.
	// The deferred release guarantees the pump can always exit, so a failure earlier in this
	// test reports as a failure and not as a goleak timeout.
	release := frozenConn.holdWrites()
	defer release()
	h.IngestClose(scope, closedCandle(fillerTs, 1))
	require.Eventually(t, func() bool { return frozenConn.pendingWrites() == 1 },
		eventuallyWait, eventuallyTick, "the frozen client's writePump is parked mid-write")

	// The open for bucketTs is admitted (bucketTs > fillerTs) and conflated into every
	// subscriber's pending slot. The keeper's delivery proves the actor turn completed; the slot
	// check is the POSITIVE CONTROL that this test actually reached the interesting state
	// instead of asserting the absence of something that was never there.
	opens.set(scope, candle.Candle{Timestamp: bucketTs, LastUpdate: 42, Close: 9})
	h.pollOnce()
	require.Eventually(t, func() bool {
		return countByTypeScope(keeper, string(EventCandleOpenUpdate), scope) == 1
	}, eventuallyWait, eventuallyTick, "the actor admitted the open (observed via the unparked keeper)")
	frozen.mu.Lock()
	pending, hadPending := frozen.open[scope]
	frozen.mu.Unlock()
	require.True(t, hadPending, "the open must be pending on the parked client — otherwise this test proves nothing")
	require.Equal(t, bucketTs, pending.Timestamp, "the pending open is the one whose bucket is about to close")

	// Close that very bucket while the open is still pending.
	h.IngestClose(scope, closedCandle(bucketTs, 2))
	require.Eventually(t, func() bool {
		return countByTypeScope(keeper, string(EventCandleClose), scope) == 2
	}, eventuallyWait, eventuallyTick, "the close was fanned out while the other writer was parked")

	frozen.mu.Lock()
	_, stillPending := frozen.open[scope]
	frozen.mu.Unlock()
	require.False(t, stillPending,
		"the open for a bucket that just closed is still queued for delivery: it would repaint the bar the chart already finalized (delete-on-close)")

	release()
	// Consequence on the wire: the client gets both deltas and never an open for that bucket.
	require.Eventually(t, func() bool {
		return countByTypeScope(frozenConn, string(EventCandleClose), scope) == 2
	}, eventuallyWait, eventuallyTick, "the released writer drained its backlog")
	require.Zero(t, countByTypeScope(frozenConn, string(EventCandleOpenUpdate), scope),
		"a retracted open must never reach the wire")

	_ = frozenConn.Close()
	_ = keeper.Close()
	stopHub(t, h)
}

// TestRevisedSharesTheClientSequenceWithCloses covers the hub's gap-recovery fan-out path,
// which nothing in the package touched: every existing revised-event test is about the wire
// shape (wire_contract_test.go) or the producer's Redis sequence (producer_test.go), never
// about Hub.fanout. Both stress files above ingest closes only.
//
// The property that matters to a client is that a revision belongs to the SAME per-scope
// stream as the closes: it consumes the next sequence number instead of a parallel counter, so
// the browser's gap detector keeps working across a gap-recovery correction rather than
// reporting a phantom hole and rehydrating the whole chart every time one arrives.
func TestRevisedSharesTheClientSequenceWithCloses(t *testing.T) {
	h := NewHub(newFakeOpens(), func() int64 { return 1 })
	h.Start()
	scope := testScope("BTCUSDT")

	fc := newFakeConnBuf(8)
	h.add(newClient(fc, h.eventsBufSize))
	require.True(t, sendCtl(fc, subReq(t, "subscribe", scope)))
	require.Eventually(t, func() bool { return fc.writeCount() >= 1 },
		eventuallyWait, eventuallyTick, "snapshot delivered => subscribe applied")

	// The realistic shape: recovery corrects a bucket that is OLDER than the newest close.
	h.IngestClose(scope, closedCandle(10_000, 1))
	h.IngestRevised(scope, closedCandle(9_000, 2))
	h.IngestClose(scope, closedCandle(11_000, 3))

	require.Eventually(t, func() bool {
		return countByTypeScope(fc, string(EventCandleClose), scope) == 2 &&
			countByTypeScope(fc, string(EventCandleRevised), scope) == 1
	}, eventuallyWait, eventuallyTick, "a revision is fanned out to subscribers just like a close")

	var seqs []uint64
	for _, f := range decodeFrames(t, fc.allWrites()) {
		if f.Typ == string(EventCandleClose) || f.Typ == string(EventCandleRevised) {
			seqs = append(seqs, f.Seq)
		}
	}
	require.Equal(t, []uint64{1, 2, 3}, seqs,
		"a revision must consume the next number in the client's per-scope sequence, not a parallel counter — otherwise the browser sees a phantom gap and rehydrates the whole chart")

	// The watermark is pinned in TestRevisedNeverMovesTheSeamAnchorBackwards below.
	// IngestRevised has a production caller: internal/app/candle_sinks.go calls it.

	_ = fc.Close()
	stopHub(t, h)
}

// lastClosedTs is the seam anchor a subscribing client compares its history window against. A
// revision corrects an OLDER bucket, so an unconditional assignment walks the anchor backwards —
// and a client whose window is perfectly good then fails the seam check and refetches, forever,
// fixing nothing.
//
// The anchor is actor-private state with no query API, so it is asserted the way a client sees it:
// through a fresh subscription's snapshot.
func TestRevisedNeverMovesTheSeamAnchorBackwards(t *testing.T) {
	h := NewHub(newFakeOpens(), func() int64 { return 1 })
	h.Start()
	scope := testScope("BTCUSDT")

	fc := newFakeConnBuf(8)
	h.add(newClient(fc, h.eventsBufSize))
	require.True(t, sendCtl(fc, subReq(t, "subscribe", scope)))
	require.Eventually(t, func() bool { return fc.writeCount() >= 1 },
		eventuallyWait, eventuallyTick, "snapshot delivered => subscribe applied")

	// Newest close first, then a correction to an older bucket — the shape gap recovery produces.
	h.IngestClose(scope, closedCandle(10_000, 1))
	h.IngestRevised(scope, closedCandle(9_000, 2))
	require.Eventually(t, func() bool {
		return countByTypeScope(fc, string(EventCandleClose), scope) == 1 &&
			countByTypeScope(fc, string(EventCandleRevised), scope) == 1
	}, eventuallyWait, eventuallyTick, "both frames fanned out")

	// A second subscriber reads the anchor out of its own snapshot.
	fc2 := newFakeConnBuf(8)
	h.add(newClient(fc2, h.eventsBufSize))
	require.True(t, sendCtl(fc2, subReq(t, "subscribe", scope)))
	require.Eventually(t, func() bool { return fc2.writeCount() >= 1 },
		eventuallyWait, eventuallyTick, "second snapshot delivered")

	var snap *recvFrame
	for _, f := range decodeFrames(t, fc2.allWrites()) {
		if f.Typ == "snapshot" {
			captured := f
			snap = &captured
			break
		}
	}
	require.NotNil(t, snap, "the second subscriber must receive a snapshot")

	require.Equal(t, int64(10_000), snap.BaselineTime,
		"the seam anchor must hold at the NEWEST closed bucket; a revision of an older one must not drag it back, or a valid history window reads as stale forever")
	require.Equal(t, uint64(2), snap.Baseline,
		"the event counter still counts the revision — anchor and cursor are different quantities, which is the whole reason M13 exists")

	_ = fc.Close()
	_ = fc2.Close()
	stopHub(t, h)
}
