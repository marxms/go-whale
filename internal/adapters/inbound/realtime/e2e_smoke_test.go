//go:build e2e

// e2e_smoke_test.go is the self-verification of the gate-6.8 chart harness. It boots the
// SAME e2eHarness the gate runs (on an ephemeral port, so it never collides with 3100),
// drives it exclusively through the public HTTP surface, and connects a REAL gorilla
// WebSocket client — proving the six properties the browser-side gate depends on:
//
//	(1) invalid ticket => 401
//	(2) unlisted Origin => 403
//	(3) subscribe => snapshot frame
//	(4) POST /drive/close => candle.close deltas with a CONTIGUOUS per-scope seq
//	(5) POST /drive/open => candle.open_update frame (no 2s poll wait)
//	(6) POST /drive/dropall => the socket dies and the hub reaps the client
//
// + a reconnect with a fresh ticket gets a new snapshot
package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/distribution"
)

const (
	smokeSymbol    = "BTCUSDT"
	smokeTimeframe = "4h"
	smokeReadWait  = 5 * time.Second
	// openReadWait is deliberately FAR below pollInterval: after /drive/open answers, the open
	// frame is already in the client's lane, so anything close to 2s would silently accept a
	// frame produced by the background ticker instead of by the forced pollOnce.
	openReadWait = 500 * time.Millisecond
)

type smokeRig struct {
	t       *testing.T
	h       *e2eHarness
	base    string
	wsBase  string
	client  *http.Client
	origin  string
	secret  []byte
	served  chan error
	scopeID Scope
}

func newSmokeRig(t *testing.T) *smokeRig {
	t.Helper()
	origins := e2eOriginsFromEnv()
	secret := e2eSecretFromEnv()
	h, err := newE2EHarness(e2eHarnessConfig{
		addr:    "127.0.0.1:0",
		secret:  secret,
		origins: origins,
		logger:  log.New(os.Stdout, "[e2e-smoke] ", log.LstdFlags|log.Lmicroseconds),
	})
	require.NoError(t, err, "harness must bind")

	rig := &smokeRig{
		t:       t,
		h:       h,
		base:    "http://" + h.addr,
		wsBase:  "ws://" + h.addr,
		client:  &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{}},
		origin:  origins[0],
		secret:  secret,
		served:  make(chan error, 1),
		scopeID: Scope{Exchange: e2eDefaultExchange, MarketType: e2eDefaultMarket, Symbol: smokeSymbol, Timeframe: smokeTimeframe},
	}
	go func() { rig.served <- h.serve(60 * time.Second) }()
	t.Cleanup(func() {
		h.requestShutdown()
		select {
		case err := <-rig.served:
			require.NoError(t, err, "harness must shut down cleanly")
		case <-time.After(15 * time.Second):
			t.Error("harness did not shut down within 15s")
		}
		rig.client.CloseIdleConnections()
	})
	return rig
}

// ---- HTTP helpers ------------------------------------------------------------

func (r *smokeRig) get(path string) (int, map[string]any) {
	r.t.Helper()
	req, err := http.NewRequest(http.MethodGet, r.base+path, nil)
	require.NoError(r.t, err)
	req.Header.Set("Origin", r.origin)
	return r.do(req)
}

func (r *smokeRig) post(path string, payload any) (int, map[string]any) {
	r.t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(r.t, err)
	req, err := http.NewRequest(http.MethodPost, r.base+path, bytes.NewReader(raw))
	require.NoError(r.t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", r.origin)
	return r.do(req)
}

// postStatusNoAssert is the goroutine-safe POST: it never calls require/t.FailNow, because
// those are illegal outside the test goroutine. Transport errors surface as status 0.
func (r *smokeRig) postStatusNoAssert(path string, payload any) int {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0
	}
	req, err := http.NewRequest(http.MethodPost, r.base+path, bytes.NewReader(raw))
	if err != nil {
		return 0
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return 0
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	return resp.StatusCode
}

func (r *smokeRig) do(req *http.Request) (int, map[string]any) {
	r.t.Helper()
	resp, err := r.client.Do(req)
	require.NoError(r.t, err, "%s %s", req.Method, req.URL.Path)
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil && err != io.EOF {
		require.NoError(r.t, err, "decode %s", req.URL.Path)
	}
	return resp.StatusCode, body
}

func (r *smokeRig) mintTicket() string {
	r.t.Helper()
	status, body := r.get("/api/ws-ticket")
	require.Equal(r.t, http.StatusOK, status, "ticket mint")
	ticket, _ := body["ticket"].(string)
	require.NotEmpty(r.t, ticket, "ticket must be non-empty")
	require.NotEqual(r.t, "noauth", ticket, "the gate must exercise the REAL ticket path, not the escape hatch")
	require.NoError(r.t, ValidateTicket(r.secret, ticket, time.Now().UnixMilli()), "minted ticket must validate")
	return ticket
}

func (r *smokeRig) stats() (clients int, scopes int) {
	r.t.Helper()
	status, body := r.get("/drive/stats")
	require.Equal(r.t, http.StatusOK, status)
	c, _ := body["clients"].(float64)
	s, _ := body["scopes"].(float64)
	return int(c), int(s)
}

func (r *smokeRig) candleBody(ts, lastUpdate int64, closePx float64) map[string]any {
	return map[string]any{
		"symbol":    smokeSymbol,
		"timeframe": smokeTimeframe,
		"candle": map[string]any{
			"timestamp":  ts,
			"open":       100.0,
			"high":       110.0,
			"low":        90.0,
			"close":      closePx,
			"volume":     42.0,
			"lastUpdate": lastUpdate,
		},
	}
}

// ---- WS helpers --------------------------------------------------------------

func (r *smokeRig) dial(ticket, origin string) (*websocket.Conn, *http.Response, error) {
	url := r.wsBase + "/api/ws?ticket=" + ticket
	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
	}
	return websocket.DefaultDialer.Dial(url, header)
}

func (r *smokeRig) connect() *websocket.Conn {
	r.t.Helper()
	conn, resp, err := r.dial(r.mintTicket(), r.origin)
	require.NoError(r.t, err, "handshake with a valid ticket + listed Origin must succeed")
	require.Equal(r.t, http.StatusSwitchingProtocols, resp.StatusCode)
	r.t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func (r *smokeRig) subscribe(conn *websocket.Conn) {
	r.t.Helper()
	raw, err := json.Marshal(clientRequest{Action: "subscribe", Scope: r.scopeID})
	require.NoError(r.t, err)
	require.NoError(r.t, conn.WriteMessage(websocket.TextMessage, raw))
}

// waitSubscribed polls the actor-observed registry (not a sleep) until the scope is indexed,
// so a subsequent /drive/close cannot fan out to an empty scope.
func (r *smokeRig) waitSubscribed(wantScopes int) {
	r.t.Helper()
	require.Eventually(r.t, func() bool {
		_, scopes := r.stats()
		return scopes == wantScopes
	}, 5*time.Second, 5*time.Millisecond, "scope indexed by the hub actor")
}

func readFrame(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(smokeReadWait)))
	_, data, err := conn.ReadMessage()
	require.NoError(t, err, "expected a frame within %s", smokeReadWait)
	var frame map[string]any
	require.NoError(t, json.Unmarshal(data, &frame), "frame must be JSON: %s", string(data))
	return frame
}

func frameTypeOf(frame map[string]any) string {
	s, _ := frame["type"].(string)
	return s
}

func candleTimestamp(t *testing.T, frame map[string]any) int64 {
	t.Helper()
	candle, ok := frame["candle"].(map[string]any)
	require.True(t, ok, "frame carries a candle: %v", frame)
	ts, _ := candle["timestamp"].(float64)
	return int64(ts)
}

// ---- the smoke test ----------------------------------------------------------

func TestE2EGateHarnessSmoke(t *testing.T) {
	rig := newSmokeRig(t)

	t.Run("health", func(t *testing.T) {
		status, body := rig.get("/drive/health")
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, true, body["ok"])
	})

	t.Run("cors_allows_the_cra_origin_on_the_ticket_endpoint", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, rig.base+"/api/ws-ticket", nil)
		require.NoError(t, err)
		req.Header.Set("Origin", rig.origin)
		resp, err := rig.client.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, rig.origin, resp.Header.Get("Access-Control-Allow-Origin"),
			"the browser fetches the ticket cross-origin (web:3006 -> hub:3100)")
	})

	t.Run("1_invalid_ticket_is_401", func(t *testing.T) {
		conn, resp, err := rig.dial("bogus.ticket", rig.origin)
		require.Error(t, err)
		require.Nil(t, conn)
		require.NotNil(t, resp)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("2_unlisted_origin_is_403", func(t *testing.T) {
		conn, resp, err := rig.dial(rig.mintTicket(), "http://evil.example")
		require.Error(t, err)
		require.Nil(t, conn)
		require.NotNil(t, resp)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusForbidden, resp.StatusCode)
	})

	conn := rig.connect()

	t.Run("3_subscribe_yields_a_snapshot", func(t *testing.T) {
		rig.subscribe(conn)
		frame := readFrame(t, conn)
		require.Equal(t, "snapshot", frameTypeOf(frame), "first frame is the snapshot: %v", frame)
		baseline, _ := frame["baseline"].(float64)
		require.Equal(t, float64(0), baseline, "fresh scope starts at baseline 0")
		rig.waitSubscribed(1)
	})

	t.Run("4_close_yields_contiguous_deltas", func(t *testing.T) {
		for i, ts := range []int64{1000, 2000} {
			status, body := rig.post("/drive/close", rig.candleBody(ts, ts, float64(100+i)))
			require.Equal(t, http.StatusOK, status, "drive/close body=%v", body)
			require.Equal(t, true, body["ok"])
		}
		var seqs []uint64
		for i := 0; i < 2; i++ {
			frame := readFrame(t, conn)
			require.Equal(t, string(EventCandleClose), frameTypeOf(frame), "frame %d: %v", i, frame)
			seq, _ := frame["seq"].(float64)
			seqs = append(seqs, uint64(seq))
		}
		require.Equal(t, []uint64{1, 2}, seqs, "per-scope sequence must be contiguous after baseline 0")
	})

	t.Run("4b_revised_continues_the_same_sequence", func(t *testing.T) {
		status, body := rig.post("/drive/revised", rig.candleBody(2000, 2001, 123.5))
		require.Equal(t, http.StatusOK, status, "drive/revised body=%v", body)
		frame := readFrame(t, conn)
		require.Equal(t, string(EventCandleRevised), frameTypeOf(frame), "%v", frame)
		seq, _ := frame["seq"].(float64)
		require.Equal(t, float64(3), seq)
	})

	t.Run("5_open_yields_an_open_update_without_waiting_for_the_ticker", func(t *testing.T) {
		// DISCRIMINATOR (do not weaken): `oneShot` makes the harness REMOVE the candle from
		// the open source right after the barrier, so the background 2s pollLoop can never
		// peek it. The frame below therefore can only have come from the FORCED pollOnce.
		//
		// A wall-clock assertion cannot do this job: the ticker reads the same source, so
		// "elapsed < pollInterval" is satisfied ~75% of the time even with the forced
		// pollOnce deleted (verified by mutation). Nor can the test read the hub's
		// lastOpenLU/closeSeq directly — that is actor-owned state and racing with the
		// actor's later turns (removeClient) is a real, -race-reported data race.
		body := rig.candleBody(3000, 3001, 105.5)
		body["oneShot"] = true
		started := time.Now()
		status, resp := rig.post("/drive/open", body)
		postElapsed := time.Since(started)
		require.Equal(t, http.StatusOK, status, "drive/open body=%v", resp)
		require.Equal(t, true, resp["subscribed"], "the open must have reached a live subscriber: %v", resp)

		// The barrier already applied the open, so the frame is queued in the client's lane; a
		// deadline far below pollInterval is safe (measured ~0.3ms) and fails loudly if not.
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(openReadWait)))
		_, data, err := conn.ReadMessage()
		require.NoError(t, err, "open frame must already be in flight (<%s)", openReadWait)
		var frame map[string]any
		require.NoError(t, json.Unmarshal(data, &frame))
		require.Equal(t, string(EventCandleOpenUpdate), frameTypeOf(frame), "%v", frame)
		require.Equal(t, int64(3000), candleTimestamp(t, frame))
		require.Less(t, postElapsed, distribution.PollInterval,
			"POST /drive/open itself must not wait out the %s poll ticker", distribution.PollInterval)
	})

	t.Run("stats_reflect_one_client_and_one_scope", func(t *testing.T) {
		clients, scopes := rig.stats()
		require.Equal(t, 1, clients)
		require.Equal(t, 1, scopes)
	})

	// Keeps the `subscribed` flag honest: a field that is always true is worse than no field,
	// because a runner would read "delivered" from a push that reached nobody.
	t.Run("a_push_to_an_unsubscribed_scope_reports_subscribed_false", func(t *testing.T) {
		body := rig.candleBody(4000, 4000, 1)
		body["symbol"] = "ETHUSDT" // BTCUSDT is subscribed, this scope is not
		status, resp := rig.post("/drive/close", body)
		require.Equal(t, http.StatusOK, status, "the push is still ACCEPTED: %v", resp)
		require.Equal(t, false, resp["subscribed"], "it must not claim delivery: %v", resp)
	})

	t.Run("6_dropall_kills_the_socket_and_the_hub_reaps_the_client", func(t *testing.T) {
		status, body := rig.post("/drive/dropall", map[string]any{})
		require.Equal(t, http.StatusOK, status, "drive/dropall body=%v", body)
		dropped, _ := body["dropped"].(float64)
		require.GreaterOrEqual(t, dropped, float64(1), "at least the chart socket must be dropped")

		require.NoError(t, conn.SetReadDeadline(time.Now().Add(smokeReadWait)))
		_, _, err := conn.ReadMessage()
		require.Error(t, err, "the socket must be dead after a TCP-level drop")
		require.False(t, websocket.IsCloseError(err, websocket.CloseNormalClosure),
			"a network drop is NOT a graceful close: %v", err)

		require.Eventually(t, func() bool {
			clients, scopes := rig.stats()
			return clients == 0 && scopes == 0
		}, 5*time.Second, 5*time.Millisecond, "hub must reap the dropped client and GC its scope")
	})

	t.Run("6b_reconnect_with_a_fresh_ticket_gets_a_new_snapshot", func(t *testing.T) {
		again := rig.connect()
		rig.subscribe(again)
		frame := readFrame(t, again)
		require.Equal(t, "snapshot", frameTypeOf(frame), "%v", frame)
		rig.waitSubscribed(1)
		_ = again.Close()
		require.Eventually(t, func() bool {
			clients, _ := rig.stats()
			return clients == 0
		}, 5*time.Second, 5*time.Millisecond, "registry cleaned after an explicit close")
	})

	t.Run("bad_request_and_method_guards", func(t *testing.T) {
		status, body := rig.post("/drive/close", map[string]any{"timeframe": smokeTimeframe})
		require.Equal(t, http.StatusBadRequest, status)
		require.Contains(t, fmt.Sprint(body["error"]), "required")

		status, _ = rig.get("/drive/close")
		require.Equal(t, http.StatusMethodNotAllowed, status)
	})

	t.Run("shutdown_endpoint_stops_the_harness", func(t *testing.T) {
		status, body := rig.post("/drive/shutdown", map[string]any{})
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, true, body["ok"])
		select {
		case err := <-rig.served:
			require.NoError(t, err)
			rig.served <- nil // hand a value back so the rig cleanup does not block
		case <-time.After(15 * time.Second):
			t.Fatal("POST /drive/shutdown did not stop the harness")
		}
	})
}

// TestE2EGateHarnessDriveBlocksOnAParkedActor is the DETERMINISTIC proof that a /drive push
// answers only AFTER the hub actor has drained it — the whole reason the browser-side gate
// may assert "after this call, the effect happened" instead of sleeping.
//
// Black-box timing cannot prove this: on a healthy machine the actor wins the race against
// the HTTP response every single time, so deleting the barrier still passes every frame
// order/sequence assertion (verified by mutation). Two devices remove the luck:
//
// - PARK: replyScopes answers with `reply <- scopes`, so handing the actor an UNBUFFERED
// reply channel nobody reads freezes it inside that turn. A push must not answer then.
// - BACKLOG + len(ingest): 500 queued closes make the actor's drain long enough to be
// observable. `len` on a channel is safe to call concurrently, so asserting an empty
// ingest lane at response time is a race-free way to say "the fanout is done". Without
// the barrier the handler only needs ONE actor rendezvous, which the actor grants after a
// couple of iterations, leaving hundreds of events still queued.
//
// No production code is touched: park and backlog both go through existing channels.
func TestE2EGateHarnessDriveBlocksOnAParkedActor(t *testing.T) {
	rig := newSmokeRig(t)

	parkCtx, parkCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer parkCancel()
	resume, err := rig.h.hub.ParkActor(parkCtx)
	if err != nil {
		t.Fatalf("hub actor never accepted the park request: %v", err)
	}

	// No WS client on purpose: fanout still costs an actor turn per event, and with no
	// subscriber the 500-event backlog cannot trip the per-client buffer cap.
	const backlog = 500
	for i := 0; i < backlog; i++ {
		rig.h.hub.IngestClose(rig.scopeID, candle.Candle{Timestamp: int64(i + 1)})
	}
	require.Equal(t, backlog, rig.h.hub.PendingIngest(), "the parked actor must not have drained anything yet")

	answered := make(chan int, 1)
	go func() { answered <- rig.postStatusNoAssert("/drive/close", rig.candleBody(9_000, 9_000, 1)) }()

	select {
	case status := <-answered:
		t.Fatalf("POST /drive/close answered %d while the hub actor was parked: it is NOT synchronizing", status)
	case <-time.After(250 * time.Millisecond):
	}

	resume() // release the actor: the parked turn completes, then the backlog drains

	select {
	case status := <-answered:
		require.Equal(t, http.StatusOK, status, "the call must complete once the actor is free")
	case <-time.After(10 * time.Second):
		t.Fatal("POST /drive/close never completed after the actor was released")
	}
	require.Zero(t, rig.h.hub.PendingIngest(),
		"the ingest lane must be fully drained when a /drive push answers (barrier missing?)")
}

// TestE2EGateHarnessBarrierIsNotASleep proves the determinism contract of the /drive
// endpoints: by the time the handler responds, the hub actor has FINISHED the fanout, so a
// caller may assert on the effect immediately instead of polling with a timeout.
//
// Read it together with TestE2EGateHarnessDriveBlocksOnAParkedActor: frame ORDER alone
// proves nothing about the barrier (it comes from the FIFO of the ingest channel and
// survives the barrier's removal — verified by mutation), so the parked-actor test is what
// discriminates. What THIS test adds is that 20 back-to-back synchronous pushes neither
// reorder, drop, nor duplicate, and that each response reports a live subscriber.
func TestE2EGateHarnessBarrierIsNotASleep(t *testing.T) {
	rig := newSmokeRig(t)
	conn := rig.connect()
	rig.subscribe(conn)
	require.Equal(t, "snapshot", frameTypeOf(readFrame(t, conn)))
	rig.waitSubscribed(1)

	// 20 closes back to back, nothing reading the socket in between: every response is issued
	// only after the actor finished the corresponding fanout.
	const n = 20
	for i := 0; i < n; i++ {
		ts := int64(10_000 + i*1_000)
		status, body := rig.post("/drive/close", rig.candleBody(ts, ts, float64(i)))
		require.Equal(t, http.StatusOK, status, "body=%v", body)
		require.Equal(t, true, body["subscribed"], "push %d must have reached a live subscriber: %v", i, body)
	}
	for i := 0; i < n; i++ {
		frame := readFrame(t, conn)
		require.Equal(t, string(EventCandleClose), frameTypeOf(frame))
		seq, _ := frame["seq"].(float64)
		require.Equal(t, float64(i+1), seq, "frame %d has a contiguous seq", i)
		require.Equal(t, int64(10_000+i*1_000), candleTimestamp(t, frame), "frames stay in order")
	}
}
