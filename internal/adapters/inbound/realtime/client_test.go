package realtime

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// Liveness on the WebSocket edge.
//
// The protocol ping the writePump already sent is answered by a browser's network stack and
// NEVER surfaces to JS, so a web client cannot tell "connected and alive" from "connected and
// mute": its chart freezes while the status still reads `open`. The heartbeat is that same
// liveness expressed as a data frame the consumer can observe. These tests assert the three
// properties that make it safe to build a client watchdog on top of it: it is per CONNECTION,
// it does not introduce a second writer, and a failed heartbeat write dies like any other.

// heartbeatCounts samples the ping controls and the heartbeat frames under ONE lock, so the
// two numbers describe the same instant. Sampling them separately would allow a tick to slip
// between the reads and turn a correct implementation into a flake.
func (f *fakeConn) heartbeatCounts() (pings, beats int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.controls {
		if c.typ == websocket.PingMessage {
			pings++
		}
	}
	for _, w := range f.writes {
		if frameType(w) == string(EventHeartbeat) {
			beats++
		}
	}
	return pings, beats
}

// TestHeartbeatIsPerConnectionNotPerScope pins: one heartbeat per tick per CONNECTION.
//
// The scope count is 3 on purpose. A heartbeat emitted from the conflation flush (or anywhere
// else that iterates subscriptions) would produce three frames per tick, which `beats <= pings`
// catches — and it would also make a quiet market look like a dead wire, inverting the fix.
func TestHeartbeatIsPerConnectionNotPerScope(t *testing.T) {
	h := NewHub(newFakeOpens(), func() int64 { return 1 }, WithPingPeriod(2*time.Millisecond))
	h.Start()
	defer stopHub(t, h)

	fc := newFakeConn()
	h.add(newClient(fc, h.eventsBufSize))
	for _, sym := range []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"} {
		subscribe(t, fc, testScope(sym))
	}
	require.Eventually(t, func() bool { return h.statsForTest().Scopes == 3 }, eventuallyWait, eventuallyTick,
		"three scopes subscribed on a single connection")

	require.Eventually(t, func() bool {
		_, beats := fc.heartbeatCounts()
		return beats >= 3
	}, eventuallyWait, eventuallyTick, "the heartbeat flows on the ping tick")

	pings, beats := fc.heartbeatCounts()
	require.LessOrEqual(t, beats, pings,
		"one heartbeat per tick at most: %d beats for %d pings means the frame is being emitted per SCOPE", beats, pings)
	require.LessOrEqual(t, pings-beats, 1,
		"every ping carries a heartbeat; at most one tick can be caught mid-flight (pings=%d beats=%d)", pings, beats)
}

// TestHeartbeatKeepsWritePumpTheSoleWriter pins against the new frame: while the
// writePump is parked inside a write, NOTHING else may put a heartbeat on the socket.
//
// The invariant is asserted, not presumed: with the sole writer frozen the ticker keeps
// firing, so a heartbeat emitted from the actor, from the poller, or from a goroutine of its
// own would show up as a write that happened while the pump was blocked.
func TestHeartbeatKeepsWritePumpTheSoleWriter(t *testing.T) {
	h := NewHub(newFakeOpens(), func() int64 { return 1 }, WithPingPeriod(time.Millisecond))
	h.Start()
	defer stopHub(t, h)

	fc := newFakeConn()
	release := fc.holdWrites() // park the writePump inside its first WriteMessage
	defer release()
	h.add(newClient(fc, h.eventsBufSize))
	subscribe(t, fc, testScope("BTCUSDT"))

	require.Eventually(t, func() bool { return fc.pendingWrites() == 1 }, eventuallyWait, eventuallyTick,
		"the writePump is parked mid-write (the snapshot)")

	// ~50 ticks pass with the sole writer frozen.
	time.Sleep(50 * time.Millisecond)
	require.Zero(t, fc.writeCount(),
		"a frame landed while the SOLE writer was parked — the heartbeat introduced a second writer")

	release()
	require.Eventually(t, func() bool {
		_, beats := fc.heartbeatCounts()
		return beats >= 2
	}, eventuallyWait, eventuallyTick, "heartbeats resume once the pump is released")
	require.LessOrEqual(t, atomic.LoadInt32(&fc.maxWrite), int32(1),
		"at most one concurrent WriteMessage per conn")
}

// heartbeatFailingConn fails ONLY the heartbeat write. Failing every write would park the
// test on the snapshot and prove nothing about the new frame; failing just this one isolates
// the branch under test.
type heartbeatFailingConn struct {
	*fakeConn
	failed atomic.Bool
}

func (f *heartbeatFailingConn) WriteMessage(messageType int, data []byte) error {
	if frameType(data) == string(EventHeartbeat) {
		f.failed.Store(true)
		return errFakeClosed
	}
	return f.fakeConn.WriteMessage(messageType, data)
}

// TestHeartbeatWriteFailureEndsThePumpLikeAnyOtherFrame pins the third Done-when criterion:
// the heartbeat has no error path of its own.
//
// A failed write returns from the pump, which closes the FD (it is the sole closer) and lets
// the readPump unregister the client — and it does NOT send a close control frame, exactly
// like the events lane. Asserting the ABSENCE of that frame is what distinguishes "took the
// existing path" from "grew a bespoke one that happens to also disconnect".
func TestHeartbeatWriteFailureEndsThePumpLikeAnyOtherFrame(t *testing.T) {
	h := NewHub(newFakeOpens(), func() int64 { return 1 }, WithPingPeriod(2*time.Millisecond))
	h.Start()
	defer stopHub(t, h)

	fc := &heartbeatFailingConn{fakeConn: newFakeConn()}
	h.add(newClient(fc, h.eventsBufSize))
	subscribe(t, fc.fakeConn, testScope("BTCUSDT")) // same inbound channel; the wrapper only fails writes
	// The production ping period here is 2 ms on purpose; under the full -race suite the
	// scheduler can starve a 2 ms ticker past the shared 3 s deadline. The deadline is
	// longer, the condition is not weaker: the write must still be attempted and fail.
	wait := 10 * time.Second
	require.Eventually(t, func() bool { return h.statsForTest().Scopes == 1 }, wait, eventuallyTick)

	require.Eventually(t, func() bool { return fc.failed.Load() }, wait, eventuallyTick,
		"the heartbeat write was attempted and failed")
	require.Eventually(t, func() bool { return h.statsForTest().Clients == 0 }, wait, eventuallyTick,
		"the failed heartbeat write ended the pump, closed the FD and unregistered the client")

	select {
	case <-fc.closed:
	default:
		t.Fatal("the connection was not closed by the pump that failed to write the heartbeat")
	}
	require.Empty(t, fc.closeCodes(),
		"a heartbeat write failure must return like any other frame's, not send a close frame of its own")
}

// guard: the doubles must satisfy the surface the pumps drive.
var (
	_ wsConn = (*fakeConn)(nil)
	_ wsConn = (*heartbeatFailingConn)(nil)
)
