//go:build e2e

// e2e_server_test.go hosts the "gate 6.8" chart-stream harness: a long-lived HTTP server
// that exposes the REAL production WebSocket edge (NewHub + NewServer + ServeWS +
// MintTicket + pollOnce + IngestClose/IngestRevised) plus a small /drive control plane the
// browser-side gate uses to drive it deterministically.
//
// WHY IT LIVES IN A _test.go FILE UNDER A BUILD TAG:
// - hub.add, newClient, pollOnce, statsReq and the hub's internal channels are PRIVATE to
// package realtime, so the harness must be compiled inside this package.
// - test files never take part in `go build ./...`.
// - the `e2e` build tag keeps `go test ./...` (no -tags) byte-for-byte unchanged.
//
// A `cmd/` main package would NOT work: a main package whose only file carries a build tag
// breaks `go build ./...` with "build constraints exclude all Go files".
//
// NOTHING in this file mutates production code or production behavior: authDisabled is
// FALSE, so the gate exercises the real Origin allow-list and the real HMAC ticket path.
package realtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

const (
	e2eDefaultPort     = "3100"
	e2eDefaultSecret   = "e2e-gate-secret"
	e2eDefaultExchange = "binance"
	e2eDefaultMarket   = "future"
	e2eTicketTTL       = 30 * time.Second
	e2eDefaultLifetime = 10 * time.Minute
	e2eBarrierTimeout  = 5 * time.Second
	e2eStopTimeout     = 5 * time.Second
	// e2eDeadlineMargin is the head-room left for a clean teardown (hub drain + HTTP shutdown
	// + goleak) before `go test -timeout` would fire; e2eMinLifetime keeps a very short
	// -timeout from producing a zero/negative lifetime.
	e2eDeadlineMargin = 30 * time.Second
	e2eMinLifetime    = 5 * time.Second
)

// e2eDefaultOrigins are the CRA dev-server origins the gate runs the web app on (3006 is
// used instead of 3005 so the harness never collides with a developer's own dev server).
var e2eDefaultOrigins = []string{"http://localhost:3006", "http://127.0.0.1:3006"}

// ---- controllable open-candle source ----------------------------------------

// e2eOpenSource is the OpenCandleSource the hub polls. POST /drive/open writes into it and
// then forces an immediate pollOnce, so the gate never waits out the 2s poll ticker.
type e2eOpenSource struct {
	mu   sync.Mutex
	data map[Scope]candle.Candle
}

func newE2EOpenSource() *e2eOpenSource {
	return &e2eOpenSource{data: make(map[Scope]candle.Candle)}
}

func (s *e2eOpenSource) set(scope Scope, c candle.Candle) {
	s.mu.Lock()
	s.data[scope] = c
	s.mu.Unlock()
}

// unset removes a scope's open candle so the background poll ticker can never re-deliver it.
// See the `oneShot` flag on POST /drive/open.
func (s *e2eOpenSource) unset(scope Scope) {
	s.mu.Lock()
	delete(s.data, scope)
	s.mu.Unlock()
}

func (s *e2eOpenSource) PeekLatestCandle(ex, mt, sym, tf string) (candle.Candle, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.data[Scope{Exchange: ex, MarketType: mt, Symbol: sym, Timeframe: tf}]
	return c, ok
}

// ---- connection-tracking listener -------------------------------------------

// e2eConn is an accepted connection the listener can later close from the outside. It is
// how POST /drive/dropall simulates a NETWORK failure: the socket dies at the TCP level,
// with no cooperation from (and no reach into) the hub's internal state — reaching into
// h.clients from off-actor would be a data race.
type e2eConn struct {
	net.Conn
	owner *e2eListener
}

func (c *e2eConn) Close() error {
	c.owner.forget(c)
	return c.Conn.Close()
}

// e2eListener tracks every accepted connection and remembers which ones were hijacked into
// a WebSocket, so /drive/dropall can kill the streams without killing the driver's own
// (plain HTTP, keep-alive) control connection.
type e2eListener struct {
	net.Listener
	mu    sync.Mutex
	conns map[net.Conn]bool // value: true once the conn was hijacked into a WebSocket
}

func newE2EListener(inner net.Listener) *e2eListener {
	return &e2eListener{Listener: inner, conns: make(map[net.Conn]bool)}
}

func (l *e2eListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tracked := &e2eConn{Conn: c, owner: l}
	l.mu.Lock()
	l.conns[tracked] = false
	l.mu.Unlock()
	return tracked, nil
}

func (l *e2eListener) forget(c net.Conn) {
	l.mu.Lock()
	delete(l.conns, c)
	l.mu.Unlock()
}

// markWS flags a connection as an upgraded WebSocket. It is called from the Hijack spy, so
// only handshakes that actually SUCCEEDED are flagged (a 401/403 leaves the conn a plain
// keep-alive HTTP conn, which dropall must not touch).
func (l *e2eListener) markWS(c net.Conn) {
	if c == nil {
		return
	}
	l.mu.Lock()
	if _, ok := l.conns[c]; ok {
		l.conns[c] = true
	}
	l.mu.Unlock()
}

// dropConns closes tracked connections at the TCP level and returns how many died.
// wsOnly=true restricts the kill to upgraded WebSockets.
func (l *e2eListener) dropConns(wsOnly bool) int {
	l.mu.Lock()
	victims := make([]net.Conn, 0, len(l.conns))
	for c, isWS := range l.conns {
		if wsOnly && !isWS {
			continue
		}
		victims = append(victims, c)
		delete(l.conns, c) // deleted first, so victim.Close() -> forget() is a no-op (no re-entrant lock)
	}
	l.mu.Unlock()
	for _, c := range victims {
		_ = c.Close()
	}
	return len(victims)
}

// ---- hijack spy --------------------------------------------------------------

type e2eConnKey struct{}

func e2eConnFrom(ctx context.Context) net.Conn {
	c, _ := ctx.Value(e2eConnKey{}).(net.Conn)
	return c
}

// e2eHijackSpy observes the ResponseWriter hijack that gorilla performs on a successful
// upgrade. It is a pure observer: every call is forwarded untouched.
type e2eHijackSpy struct {
	http.ResponseWriter
	onHijack func()
}

func (s *e2eHijackSpy) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := s.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("e2e: response writer is not a http.Hijacker")
	}
	conn, rw, err := hj.Hijack()
	if err == nil && s.onHijack != nil {
		s.onHijack()
	}
	return conn, rw, err
}

func (s *e2eHijackSpy) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ---- harness -----------------------------------------------------------------

type e2eHarness struct {
	hub   *Hub
	edge  *Server
	opens *e2eOpenSource
	ln    *e2eListener
	http  *http.Server
	addr  string
	lg    *log.Logger

	shutdown chan struct{}
	shutOnce sync.Once
	stopOnce sync.Once
}

type e2eHarnessConfig struct {
	addr    string
	secret  []byte
	origins []string
	logger  *log.Logger
}

// newE2EHarness binds the listener, builds the REAL hub + WS edge, and wires the /drive
// control plane. It does not serve yet — call serve.
func newE2EHarness(cfg e2eHarnessConfig) (*e2eHarness, error) {
	inner, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return nil, fmt.Errorf("e2e: listen %q: %w", cfg.addr, err)
	}
	lg := cfg.logger
	if lg == nil {
		lg = log.New(os.Stdout, "[e2e-ws] ", log.LstdFlags|log.Lmicroseconds)
	}
	opens := newE2EOpenSource()
	hub := NewHub(opens, func() int64 { return time.Now().UnixMilli() })
	// authDisabled=false on purpose: the gate must exercise the REAL ticket + Origin path.
	edge := NewServer(hub, cfg.secret, cfg.origins, e2eTicketTTL, func() int64 { return time.Now().UnixMilli() }, false)

	h := &e2eHarness{
		hub:      hub,
		edge:     edge,
		opens:    opens,
		ln:       newE2EListener(inner),
		addr:     inner.Addr().String(),
		lg:       lg,
		shutdown: make(chan struct{}),
	}
	h.http = &http.Server{
		Handler:           h.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return context.WithValue(ctx, e2eConnKey{}, c)
		},
	}
	hub.Start()
	return h, nil
}

func (h *e2eHarness) routes() http.Handler {
	mux := http.NewServeMux()

	// REAL production handlers.
	mux.Handle("/api/ws-ticket", h.withCORS(http.HandlerFunc(h.edge.MintTicket)))
	// No CORS wrapper on the upgrade path: a WS handshake is not a CORS request, and the
	// Origin allow-list inside ServeWS is the real gate being exercised.
	mux.HandleFunc("/api/ws", func(w http.ResponseWriter, r *http.Request) {
		conn := e2eConnFrom(r.Context())
		spy := &e2eHijackSpy{ResponseWriter: w, onHijack: func() { h.ln.markWS(conn) }}
		h.edge.ServeWS(spy, r)
	})

	// /drive control plane.
	drive := http.NewServeMux()
	drive.HandleFunc("/drive/health", h.method(http.MethodGet, h.handleHealth))
	drive.HandleFunc("/drive/stats", h.method(http.MethodGet, h.handleStats))
	drive.HandleFunc("/drive/close", h.method(http.MethodPost, h.handleIngest(EventCandleClose)))
	drive.HandleFunc("/drive/revised", h.method(http.MethodPost, h.handleIngest(EventCandleRevised)))
	drive.HandleFunc("/drive/open", h.method(http.MethodPost, h.handleOpen))
	drive.HandleFunc("/drive/dropall", h.method(http.MethodPost, h.handleDropAll))
	drive.HandleFunc("/drive/shutdown", h.method(http.MethodPost, h.handleShutdown))
	mux.Handle("/drive/", h.withCORS(drive))

	return mux
}

func (h *e2eHarness) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			if _, ok := h.edge.allowedOrigins[origin]; ok {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Credentials", "true")
				w.Header().Add("Vary", "Origin")
			}
		}
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *e2eHarness) method(verb string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != verb {
			w.Header().Set("Allow", verb)
			h.writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "error": "method not allowed"})
			return
		}
		next(w, r)
	}
}

// serve blocks until POST /drive/shutdown arrives or maxLifetime elapses (safety valve so a
// forgotten harness cannot outlive the gate run), then tears everything down cleanly.
func (h *e2eHarness) serve(maxLifetime time.Duration) error {
	h.lg.Printf("listening on %s (origins=%v)", h.addr, e2eOriginList(h.edge))
	errCh := make(chan error, 1)
	go func() { errCh <- h.http.Serve(h.ln) }()

	var lifetime <-chan time.Time
	if maxLifetime > 0 {
		t := time.NewTimer(maxLifetime)
		defer t.Stop()
		lifetime = t.C
	}

	var serveErr error
	select {
	case serveErr = <-errCh:
		h.lg.Printf("serve returned early: %v", serveErr)
		h.stop()
		if errors.Is(serveErr, http.ErrServerClosed) {
			return nil
		}
		return serveErr
	case <-h.shutdown:
		h.lg.Printf("shutdown requested")
	case <-lifetime:
		h.lg.Printf("max lifetime %s elapsed; shutting down", maxLifetime)
	}
	h.stop()
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (h *e2eHarness) requestShutdown() {
	h.shutOnce.Do(func() { close(h.shutdown) })
}

// stop tears down in dependency order: the hub first (its pumps own and close the WS FDs
// and send GOING_AWAY), then the HTTP server, then anything still tracked.
func (h *e2eHarness) stop() {
	h.stopOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), e2eStopTimeout)
		defer cancel()
		h.hub.Stop(ctx)
		_ = h.http.Shutdown(ctx)
		h.ln.dropConns(false)
		_ = h.http.Close()
		h.lg.Printf("stopped")
	})
}

// ---- /drive handlers ---------------------------------------------------------

type e2eCandleBody struct {
	Exchange   string    `json:"exchange"`
	MarketType string    `json:"marketType"`
	Symbol     string    `json:"symbol"`
	Timeframe  string    `json:"timeframe"`
	Candle     CandleDTO `json:"candle"`
	// OneShot (POST /drive/open only) drops the candle from the open source after the forced
	// pollOnce, so the background 2s ticker can never re-deliver it. Opt-in, because with it a
	// client that subscribes AFTER the push gets the open only through the hub's own snapshot
	// state; without it the ticker keeps re-offering the candle (deduped by lastUpdate).
	OneShot bool `json:"oneShot"`
}

func (b e2eCandleBody) scope() (Scope, error) {
	symbol := strings.TrimSpace(b.Symbol)
	timeframe := strings.TrimSpace(b.Timeframe)
	if symbol == "" || timeframe == "" {
		return Scope{}, errors.New(`"symbol" and "timeframe" are required`)
	}
	exchange := strings.TrimSpace(b.Exchange)
	if exchange == "" {
		exchange = e2eDefaultExchange
	}
	marketType := strings.TrimSpace(b.MarketType)
	if marketType == "" {
		marketType = e2eDefaultMarket
	}
	return Scope{Exchange: exchange, MarketType: marketType, Symbol: symbol, Timeframe: timeframe}, nil
}

func (h *e2eHarness) handleHealth(w http.ResponseWriter, _ *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (h *e2eHarness) handleStats(w http.ResponseWriter, _ *http.Request) {
	// statsForTest round-trips the hub's statsReq channel: the only race-free way to read
	// actor-owned registry state.
	s := h.hub.statsForTest()
	h.writeJSON(w, http.StatusOK, map[string]any{"clients": s.Clients, "scopes": s.Scopes})
}

func (h *e2eHarness) handleIngest(typ EventType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, scope, ok := h.decodeCandle(w, r)
		if !ok {
			return
		}
		cnd := body.Candle.ToCandle()
		switch typ {
		case EventCandleClose:
			h.hub.IngestClose(scope, cnd)
		case EventCandleRevised:
			h.hub.IngestRevised(scope, cnd)
		default:
			h.writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": "unsupported event type"})
			return
		}
		if err := h.barrier(); err != nil {
			h.lg.Printf("%s %s ts=%d BARRIER FAILED: %v", typ, e2eScopeKey(scope), cnd.Timestamp, err)
			h.writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		subscribed := h.scopeSubscribed(scope)
		h.lg.Printf("%s %s ts=%d close=%v (fanned out, subscribed=%t)",
			typ, e2eScopeKey(scope), cnd.Timestamp, cnd.Close, subscribed)
		h.writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "type": string(typ), "scope": scope,
			"timestamp": cnd.Timestamp, "subscribed": subscribed,
		})
	}
}

func (h *e2eHarness) handleOpen(w http.ResponseWriter, r *http.Request) {
	body, scope, ok := h.decodeCandle(w, r)
	if !ok {
		return
	}
	// The hub dedupes an open by LastUpdate, so a driver that resends the SAME lastUpdate
	// will see its second push silently discarded. That is deliberate production behavior,
	// so the body carries lastUpdate verbatim and the caller varies it.
	cnd := body.Candle.ToCandle()
	h.opens.set(scope, cnd)
	h.hub.PollOnce() // immediate push: never wait out the 2s poll ticker
	if err := h.barrier(); err != nil {
		h.lg.Printf("open %s ts=%d BARRIER FAILED: %v", e2eScopeKey(scope), cnd.Timestamp, err)
		h.writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.OneShot {
		// Safe only AFTER the barrier: pollOnce has already peeked the candle and the actor has
		// already applied the batch, so dropping it cannot cancel THIS push — it only stops the
		// background ticker from ever re-offering it.
		h.opens.unset(scope)
	}
	subscribed := h.scopeSubscribed(scope)
	h.lg.Printf("open %s ts=%d lastUpdate=%d close=%v (polled, subscribed=%t, oneShot=%t)",
		e2eScopeKey(scope), cnd.Timestamp, cnd.LastUpdate, cnd.Close, subscribed, body.OneShot)
	h.writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "type": string(EventCandleOpenUpdate), "scope": scope,
		"timestamp": cnd.Timestamp, "lastUpdate": cnd.LastUpdate,
		"subscribed": subscribed, "oneShot": body.OneShot,
	})
}

// scopeSubscribed reports whether the scope currently has at least one subscriber, so a
// /drive push can answer "delivered" instead of merely "accepted". A 200 with
// subscribed=false is the signature of a push that fanned out to NOBODY — precisely the
// false green a browser-side gate would otherwise read as success.
//
// It goes through the hub's scopeReq channel because the index is actor-owned: reading it
// from here would be a real data race (the actor keeps mutating it on every
// subscribe/unsubscribe/removeClient, and -race reports exactly that).
func (h *e2eHarness) scopeSubscribed(scope Scope) bool {
	for _, s := range h.hub.ActiveScopes() {
		if s == scope {
			return true
		}
	}
	return false
}

func (h *e2eHarness) handleDropAll(w http.ResponseWriter, r *http.Request) {
	// Excluding the caller's own connection is why only HIJACKED conns are dropped: the
	// driver's control channel must survive to read this very response.
	dropped := h.ln.dropConns(true)
	h.lg.Printf("dropall: %d websocket connection(s) killed at TCP level", dropped)
	h.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "dropped": dropped})
}

func (h *e2eHarness) handleShutdown(w http.ResponseWriter, _ *http.Request) {
	h.lg.Printf("shutdown endpoint hit")
	w.Header().Set("Connection", "close")
	h.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "shutdown": true})
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	// Safe to signal synchronously: this handler returns immediately, so the Shutdown()
	// that follows in serve() has nothing left to wait for.
	h.requestShutdown()
}

func (h *e2eHarness) decodeCandle(w http.ResponseWriter, r *http.Request) (e2eCandleBody, Scope, bool) {
	var body e2eCandleBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		h.writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid json: " + err.Error()})
		return body, Scope{}, false
	}
	scope, err := body.scope()
	if err != nil {
		h.writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return body, Scope{}, false
	}
	return body, scope, true
}

func (h *e2eHarness) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// barrier blocks until the hub's actor has FINISHED processing everything the driver just
// enqueued, which is what makes every /drive endpoint an assertable "after this call, the
// effect happened" boundary instead of a sleep.
//
// Two facts make it exact, without touching production code:
// 1. len(h.ingest) == 0 proves every ingest message has been DEQUEUED by the actor.
// 2. the actor is single-threaded, so it can only dequeue the next message after the
// previous handler RETURNED. openBatch has capacity 1, so a second nil send cannot
// complete until the first nil was dequeued — and that completion is the proof that the
// preceding fanout/handleOpenBatch already returned.
//
// A nil batch is a genuine no-op: handleOpenBatch ranges over zero elements.
func (h *e2eHarness) barrier() error {
	deadline := time.Now().Add(e2eBarrierTimeout)
	for h.hub.PendingIngest() > 0 {
		if time.Now().After(deadline) {
			return errors.New("barrier: ingest lane not drained")
		}
		runtime.Gosched()
	}
	for i := 0; i < 2; i++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return errors.New("barrier: deadline exceeded before the actor turn completed")
		}
		ctx, cancel := context.WithTimeout(context.Background(), remaining)
		err := h.hub.NoopOpenBatch(ctx)
		cancel()
		if err != nil {
			return errors.New("barrier: " + err.Error())
		}
	}
	return nil
}

// ---- env plumbing ------------------------------------------------------------

func e2eEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func e2eSecretFromEnv() []byte {
	return []byte(e2eEnv("E2E_TICKET_SECRET", e2eDefaultSecret))
}

// e2eOriginsFromEnv returns the fixed CRA-on-3006 origins plus anything E2E_WS_ORIGINS adds.
func e2eOriginsFromEnv() []string {
	origins := append([]string(nil), e2eDefaultOrigins...)
	for _, o := range strings.Split(os.Getenv("E2E_WS_ORIGINS"), ",") {
		if o = strings.TrimSpace(o); o != "" {
			origins = append(origins, o)
		}
	}
	return origins
}

func e2eLifetimeFromEnv() time.Duration {
	raw := e2eEnv("E2E_WS_MAX_LIFETIME", e2eDefaultLifetime.String())
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return e2eDefaultLifetime
	}
	return d
}

func e2eOriginList(s *Server) []string {
	out := make([]string, 0, len(s.allowedOrigins))
	for o := range s.allowedOrigins {
		out = append(out, o)
	}
	return out
}

func e2eScopeKey(s Scope) string {
	return s.Exchange + "|" + s.MarketType + "|" + s.Symbol + "|" + s.Timeframe
}

// ---- entry point -------------------------------------------------------------

// TestE2EGateServer is the harness entry point, not an assertion: it serves the real chart
// WebSocket edge on E2E_WS_PORT (default 3100) and BLOCKS until POST /drive/shutdown or the
// E2E_WS_MAX_LIFETIME safety valve (default 10m).
//
// It skips unless explicitly asked for, so `go test -tags e2e ./...` never hangs.
func TestE2EGateServer(t *testing.T) {
	if os.Getenv("E2E_WS_PORT") == "" && os.Getenv("E2E_WS_SERVE") == "" {
		t.Skip("set E2E_WS_PORT (or E2E_WS_SERVE=1) to run the long-lived gate harness")
	}
	addr := e2eEnv("E2E_WS_HOST", "") + ":" + e2eEnv("E2E_WS_PORT", e2eDefaultPort)
	h, err := newE2EHarness(e2eHarnessConfig{
		addr:    addr,
		secret:  e2eSecretFromEnv(),
		origins: e2eOriginsFromEnv(),
	})
	if err != nil {
		t.Fatalf("harness bind failed: %v", err)
	}
	t.Cleanup(h.stop)
	t.Logf("e2e chart hub harness ready on %s", h.addr)
	if err := h.serve(e2eLifetime(t)); err != nil {
		t.Fatalf("harness serve failed: %v", err)
	}
}

// e2eLifetime clamps the harness lifetime to just under `go test -timeout`, so the harness
// always dies by its OWN clean path (hub drained, goleak honest, exit 0) instead of being
// killed by the test binary's watchdog.
//
// Without this, the defaults collide head-on: `go test` times out at 10m and
// E2E_WS_MAX_LIFETIME is also 10m, so the run is a coin flip between a clean stop and a
// "test timed out" panic with a full goroutine dump and a non-zero exit — a gate failure
// with no relation to what the gate measured. It also removed the requirement that every
// caller remember `-timeout 0`.
func e2eLifetime(t *testing.T) time.Duration {
	lifetime := e2eLifetimeFromEnv()
	deadline, ok := t.Deadline()
	if !ok {
		return lifetime // -timeout 0: no watchdog to lose to
	}
	budget := time.Until(deadline) - e2eDeadlineMargin
	if budget < e2eMinLifetime {
		budget = e2eMinLifetime
	}
	if budget < lifetime {
		t.Logf("clamping harness lifetime %s -> %s to stay inside `go test -timeout` (deadline %s)",
			lifetime, budget, deadline.Format(time.RFC3339))
		return budget
	}
	return lifetime
}
