package realtime

// STREAM AUTHENTICATION HARDENING (the chart WebSocket path).
//
// `ticket_test.go` and `server_test.go` already cover the happy path plus tamper/expiry/origin.
// This suite pins the properties that the *threat model* rests on and that a refactor could
// silently break:
//
// - the ticket is STATELESS (replayable inside its TTL) and carries NO scope binding — those
// are deliberate, and the ONLY thing bounding them is the TTL + the connection's max age, so
// the tests below assert the bound rather than the wish;
// - expiry is evaluated against the VALIDATOR's clock with zero leeway, so the >=/> boundary and
// clock skew must behave exactly as written;
// - every malformed shape (truncated/short/oversized signature, non-numeric or absurd expiry,
// non-URL-safe base64) collapses onto ONE generic error, so nothing leaks which check failed;
// - the Origin allow-list is an EXACT match, so lookalike hosts, case and port variants are all
// rejected — the whole CSWSH defense is that comparison.
//
// Threat model for the severity calls: this stream carries public market data (OHLCV of exchange
// symbols). There is no per-user payload, so ticket replay costs a leaked read-only feed, not
// account access. What still matters is that (a) unauthenticated third parties cannot mount a
// free fan-out load on the hub, and (b) a hostile page cannot open a stream from a victim's
// browser (CSWSH).

import (
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// baseNowMs is a fixed, realistic epoch so expiry arithmetic in these tests reads like production
// (ms since epoch) instead of the toy 1000/2000 values used by ticket_test.go.
const baseNowMs int64 = 1_700_000_000_000

// hardeningServer is the real WS edge wired to a MUTABLE clock: a test can move the validator's
// notion of "now" across a ticket's expiry without sleeping, which is what makes the
// ticket-lifetime assertions deterministic.
type hardeningServer struct {
	hub   *Hub
	srv   *Server
	wsURL string
	nowMs *atomic.Int64
}

func newHardeningServer(t *testing.T, secret []byte, origins []string, authDisabled bool, hubOpts ...HubOption) *hardeningServer {
	t.Helper()
	now := new(atomic.Int64)
	now.Store(baseNowMs)
	h := NewHub(newFakeOpens(), func() int64 { return 1 }, hubOpts...)
	h.Start()
	t.Cleanup(func() { stopHub(t, h) })
	s := NewServer(h, secret, origins, 30*time.Second, func() int64 { return now.Load() }, authDisabled)
	ts := httptest.NewServer(http.HandlerFunc(s.ServeWS))
	t.Cleanup(ts.Close) // LIFO: the HTTP server closes before the hub stops
	return &hardeningServer{hub: h, srv: s, wsURL: "ws" + strings.TrimPrefix(ts.URL, "http"), nowMs: now}
}

// dial opens the WS handshake with an explicit header set (nil = send no Origin at all, which is
// what a non-browser client does).
func (s *hardeningServer) dial(ticket string, hdr http.Header) (*websocket.Conn, *http.Response, error) {
	target := s.wsURL
	if ticket != "" {
		target += "?ticket=" + ticket
	}
	return websocket.DefaultDialer.Dial(target, hdr)
}

func allowedOriginHeader() http.Header { return http.Header{"Origin": {allowedOrigin}} }

// ---- 1. REPLAY: the ticket is stateless, so it is reusable inside its TTL --------------------

// A stateless HMAC ticket has no server-side "used" mark, so the SAME ticket authorizes as many
// handshakes as fit in its TTL. This is accepted by design (there is no per-user data behind the
// stream and the mint endpoint sits behind the gateway's auth_request), but it must be an
// ASSERTED property, not an assumption: if someone later claims one-shot semantics, this test
// says otherwise. The bound on the exposure is the TTL, pinned by the tests further down.
func TestSameTicketAuthorizesMultipleConcurrentConnections(t *testing.T) {
	s := newHardeningServer(t, testSecret(), []string{allowedOrigin}, false)

	ticket := MintTicket(testSecret(), baseNowMs+30_000)
	const replays = 3
	for i := 0; i < replays; i++ {
		conn, resp, err := s.dial(ticket, allowedOriginHeader())
		require.NoErrorf(t, err, "replay #%d rejected — ticket is NOT stateless anymore; revisit the "+
			"client's fetch-ticket-then-connect flow (it re-mints per connect, but reconnect storms reuse)", i)
		require.NotNil(t, resp)
		_ = resp.Body.Close()
		defer func() { _ = conn.Close() }()
	}

	require.Eventually(t, func() bool { return s.hub.statsForTest().Clients == replays },
		eventuallyWait, eventuallyTick, "all replayed connections registered")
}

// The ticket is checked ONLY at the handshake, so an established connection keeps streaming after
// its ticket expires — the connection's max age is the only thing that ends it. This test pins
// both halves at once: with the clock moved past expiry, the LIVE connection survives while a NEW
// handshake with the same (now stale) ticket is refused. If the halves ever swap, either the
// stream drops mid-session or a stale credential buys an unbounded session.
func TestExpiredTicketStopsNewHandshakesButNotAnEstablishedConnection(t *testing.T) {
	s := newHardeningServer(t, testSecret(), []string{allowedOrigin}, false)

	expiry := baseNowMs + 30_000
	ticket := MintTicket(testSecret(), expiry)
	live, liveResp, err := s.dial(ticket, allowedOriginHeader())
	require.NoError(t, err)
	require.NotNil(t, liveResp)
	_ = liveResp.Body.Close()
	defer func() { _ = live.Close() }()
	require.Eventually(t, func() bool { return s.hub.statsForTest().Clients == 1 },
		eventuallyWait, eventuallyTick, "live connection registered")

	s.nowMs.Store(expiry + 1) // one ms past the ticket's window

	_, resp, err := s.dial(ticket, allowedOriginHeader())
	require.Error(t, err)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode, "stale ticket must not open a NEW stream")
	_ = resp.Body.Close()

	// The established connection is still usable: it accepts a control frame and stays registered.
	require.NoError(t, live.WriteJSON(clientRequest{Action: "subscribe", Scope: testScope("BTCUSDT")}))
	require.Eventually(t, func() bool {
		st := s.hub.statsForTest()
		return st.Clients == 1 && st.Scopes == 1
	}, eventuallyWait, eventuallyTick, "established connection keeps serving past ticket expiry")
}

// The mitigation for replay + no-revalidation is the bounded connection lifetime: the server hangs
// up so the client must re-mint. Without this the 30s TTL would be decorative — one ticket would
// buy a forever-stream.
func TestConnMaxAgeClosesTheStreamSoTheClientMustReMint(t *testing.T) {
	s := newHardeningServer(t, testSecret(), []string{allowedOrigin}, false, WithConnMaxAge(80*time.Millisecond))

	conn, resp, err := s.dial(MintTicket(testSecret(), baseNowMs+30_000), allowedOriginHeader())
	require.NoError(t, err)
	require.NotNil(t, resp)
	_ = resp.Body.Close()
	defer func() { _ = conn.Close() }()

	require.Eventually(t, func() bool { return s.hub.statsForTest().Clients == 0 },
		eventuallyWait, eventuallyTick, "server closed the aged connection")
	_ = conn.SetReadDeadline(time.Now().Add(eventuallyWait))
	_, _, readErr := conn.ReadMessage()
	require.Error(t, readErr, "client observes the close, so it can re-mint and reconnect")
}

// ---- 2. CROSS-SCOPE: the ticket authorizes the STREAM, not any particular scope --------------

// The signed payload is EXACTLY the expiry — no symbol, timeframe, client id or subject. So there
// is nothing a cross-scope check could even be based on. Asserting the payload shape (rather than
// just "it validates") is what makes the absence of a binding visible in review.
func TestTicketPayloadCarriesOnlyExpiryAndNoScopeBinding(t *testing.T) {
	expiry := baseNowMs + 30_000
	ticket := MintTicket(testSecret(), expiry)

	payload, _, ok := splitTicket(ticket)
	require.True(t, ok)
	require.Equal(t, strconv.FormatInt(expiry, 10), payload,
		"payload must be the bare expiry; any extra field here would need a matching check in ValidateTicket")
}

// One handshake ticket lets the client subscribe to ANY scope it asks for over the socket. That is
// intentional for a public market-data feed, but it means the ticket can never be used as an
// authorization boundary for a future per-user or premium-symbol feed — this test is the tripwire
// for that assumption.
func TestOneTicketAuthorizesSubscriptionToAnyScope(t *testing.T) {
	s := newHardeningServer(t, testSecret(), []string{allowedOrigin}, false)

	conn, resp, err := s.dial(MintTicket(testSecret(), baseNowMs+30_000), allowedOriginHeader())
	require.NoError(t, err)
	require.NotNil(t, resp)
	_ = resp.Body.Close()
	defer func() { _ = conn.Close() }()

	require.NoError(t, conn.WriteJSON(clientRequest{Action: "subscribe", Scope: testScope("BTCUSDT")}))
	require.NoError(t, conn.WriteJSON(clientRequest{Action: "subscribe", Scope: testScope("ETHUSDT")}))

	require.Eventually(t, func() bool { return s.hub.statsForTest().Scopes == 2 },
		eventuallyWait, eventuallyTick, "unscoped ticket admits every requested scope")
}

// ---- 3. EXPIRY BOUNDARY AND CLOCK SKEW ------------------------------------------------------

// The comparison is `nowMs > expiryMs`, i.e. expiry is INCLUSIVE. Pinning the three points around
// the boundary catches an accidental flip to `>=` (which would shorten every ticket by 1ms — the
// kind of change that only shows up as rare 401s in production).
func TestExpiryBoundaryIsInclusiveOfExpiresAt(t *testing.T) {
	expiry := baseNowMs + 30_000
	ticket := MintTicket(testSecret(), expiry)

	cases := []struct {
		name    string
		nowMs   int64
		wantErr bool
	}{
		{"one ms before expiry", expiry - 1, false},
		{"exactly at expiry", expiry, false},
		{"one ms after expiry", expiry + 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTicket(testSecret(), ticket, tc.nowMs)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrInvalidTicket)
				return
			}
			require.NoError(t, err)
		})
	}
}

// Expiry is judged purely by the validator's clock with NO leeway constant in either direction, and
// the drift MAGNITUDES are the point of this test. A tolerance window ("allow a few seconds of clock
// skew") is the classic patch applied when mint and validation drift apart, and it silently extends
// every ticket: only a rejection measured 1ms PAST the signed expiry fails when such a constant is
// introduced, which is why the rows are anchored on `expiry` rather than on a round drift from now.
// Downward drift is unbounded by design — the payload has no not-before claim — so a validator
// running arbitrarily behind must still accept.
func TestExpiryHasNoGraceLeewayAndNoNotBeforeClaim(t *testing.T) {
	expiry := baseNowMs + 30_000
	ticket := MintTicket(testSecret(), expiry)

	const oneYearMs int64 = 365 * 24 * 3600 * 1000
	for _, behind := range []int64{1, 60_000, oneYearMs} {
		require.NoErrorf(t, ValidateTicket(testSecret(), ticket, expiry-behind),
			"a validator %dms behind the expiry must accept: there is no not-before claim to violate", behind)
	}
	for _, ahead := range []int64{1, 1_000, 60_000} {
		require.ErrorIsf(t, ValidateTicket(testSecret(), ticket, expiry+ahead), ErrInvalidTicket,
			"a validator %dms past the expiry must reject: any leeway constant would widen EVERY ticket", ahead)
	}
}

// ---- 4. TICKET SHAPE ------------------------------------------------------------------------

// Every malformed shape must land on ErrInvalidTicket. The signature-length cases matter most:
// hmac.Equal handles unequal lengths by returning false, so a truncated or padded signature must
// NOT panic or produce a different error (which would be an oracle for the signature length).
func TestMalformedTicketShapesAreRejected(t *testing.T) {
	valid := MintTicket(testSecret(), baseNowMs+30_000)
	dot := strings.IndexByte(valid, '.')
	payloadSeg, sigSeg := valid[:dot], valid[dot+1:]

	b64 := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	sigOfLen := func(n int) string {
		return payloadSeg + "." + base64.RawURLEncoding.EncodeToString(make([]byte, n))
	}

	cases := []struct {
		name   string
		ticket string
	}{
		{"empty string", ""},
		{"signature truncated by 8 base64 chars", payloadSeg + "." + sigSeg[:len(sigSeg)-8]},
		{"signature truncated to one char", payloadSeg + "." + sigSeg[:1]},
		{"signature absent (trailing dot)", payloadSeg + "."},
		{"signature one byte short of sha256", sigOfLen(31)},
		{"signature one byte longer than sha256", sigOfLen(33)},
		{"signature doubled in length", sigOfLen(64)},
		{"valid signature moved onto a different payload", b64("9999999999999") + "." + sigSeg},
		{"standard-base64 padding is not URL-safe", payloadSeg + "==." + sigSeg},
		{"standard-base64 alphabet (+ and /) rejected", "MTcwMDAw+/8.a" + sigSeg[1:]},
		{"whitespace inside the segments", payloadSeg + " ." + sigSeg},
		{"percent-encoded separator", strings.Replace(valid, ".", "%2E", 1)},
		{"three segments", valid + "." + sigSeg},
		{"payload and signature swapped", sigSeg + "." + payloadSeg},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, ValidateTicket(testSecret(), tc.ticket, baseNowMs), ErrInvalidTicket)
		})
	}
}

// A signature can be valid while the payload is not a number — that is exactly what a buggy or
// rogue minter would emit. The ParseInt guard must reject it instead of treating an unparseable
// expiry as "no expiry".
func TestSignedButNonNumericOrNonPositiveExpiryIsRejected(t *testing.T) {
	cases := []struct {
		name    string
		payload string
	}{
		{"empty payload", ""},
		{"alphabetic expiry", "not-a-number"},
		{"float expiry", "1700000000000.5"},
		{"hex expiry", "0x18BC4F0"},
		{"int64 overflow", "99999999999999999999"},
		{"zero expiry", "0"},
		{"negative expiry", "-1700000000000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Sign the hostile payload with the REAL secret: the signature is genuine, so only the
			// expiry parsing/comparison can reject it.
			ticket := base64.RawURLEncoding.EncodeToString([]byte(tc.payload)) + "." +
				base64.RawURLEncoding.EncodeToString(sign(testSecret(), tc.payload))
			require.ErrorIs(t, ValidateTicket(testSecret(), ticket, baseNowMs), ErrInvalidTicket)
		})
	}
}

// strconv.ParseInt accepts an explicit sign, so a `+`-prefixed expiry denotes the SAME instant and
// is accepted. Harmless (the value is identical) but pinned because it is a leniency the legacy path
// JSON payload does not share — a future shared-format refactor must not assume the two parsers
// agree on surface syntax.
func TestExplicitlySignedPositiveExpiryDenotesTheSameInstant(t *testing.T) {
	payload := "+" + strconv.FormatInt(baseNowMs+30_000, 10)
	ticket := base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." +
		base64.RawURLEncoding.EncodeToString(sign(testSecret(), payload))

	require.NoError(t, ValidateTicket(testSecret(), ticket, baseNowMs))
	require.ErrorIs(t, ValidateTicket(testSecret(), ticket, baseNowMs+30_001), ErrInvalidTicket,
		"the signed form still expires at the same instant")
}

// There is NO upper cap on a signed expiry: a ticket stamped for the year 9999 validates forever.
// Only the streamer mints (expiry = clock + configured TTL), so this is not reachable from
// outside — but it means the TTL config is the ONLY thing bounding a ticket's life, and a secret
// leak yields a permanent credential rather than a 30s one. Asserted so the property is explicit
// instead of accidental.
func TestSignedExpiryHasNoUpperCap(t *testing.T) {
	const year9999Ms int64 = 253_402_300_799_000
	ticket := MintTicket(testSecret(), year9999Ms)

	require.NoError(t, ValidateTicket(testSecret(), ticket, baseNowMs))
	require.NoError(t, ValidateTicket(testSecret(), ticket, year9999Ms-1),
		"no cap: the ticket is still valid ~8000 years after minting")
}

// An oversized ticket must be rejected without panicking or hanging. The HMAC is necessarily
// computed over the attacker-controlled payload BEFORE the compare, so validation cost is linear
// in the query string; the practical bound is net/http's request-line/header limit, not this
// function. This test pins the "no panic, still generic error" half.
func TestOversizedTicketIsRejectedWithoutPanic(t *testing.T) {
	huge := strings.Repeat("A", 1<<20) // 1 MiB of valid base64url chars
	require.ErrorIs(t, ValidateTicket(testSecret(), huge+"."+huge, baseNowMs), ErrInvalidTicket)
}

// ---- 5. CONSTANT-TIME COMPARISON AND ERROR INDISTINGUISHABILITY -----------------------------

// hmac.Equal (subtle.ConstantTimeCompare) is length-safe in Go — unlike Node's timingSafeEqual,
// which THROWS on unequal lengths. This test locks that in: a wrong-length signature must produce
// the identical error value as a wrong-value signature, never a panic and never a distinct error.
func TestSignatureLengthMismatchYieldsTheSameGenericError(t *testing.T) {
	payload := strconv.FormatInt(baseNowMs+30_000, 10)
	payloadSeg := base64.RawURLEncoding.EncodeToString([]byte(payload))
	correct := sign(testSecret(), payload)

	wrongValue := append([]byte(nil), correct...)
	wrongValue[0] ^= 0xFF
	baseline := ValidateTicket(testSecret(),
		payloadSeg+"."+base64.RawURLEncoding.EncodeToString(wrongValue), baseNowMs)
	require.ErrorIs(t, baseline, ErrInvalidTicket)

	for _, n := range []int{0, 1, 16, 31, 33, 64, 512} {
		sig := base64.RawURLEncoding.EncodeToString(make([]byte, n))
		err := ValidateTicket(testSecret(), payloadSeg+"."+sig, baseNowMs)
		// Require the error FIRST: an accepted all-zero signature returns nil, and calling
		// .Error() on it would blow up as a panic instead of a readable failure.
		require.ErrorIsf(t, err, ErrInvalidTicket, "all-zero signature of length %d was accepted", n)
		require.Equal(t, baseline.Error(), err.Error(),
			"signature length %d must be indistinguishable from a wrong signature value", n)
	}
}

// The error is deliberately generic. A caller (or an attacker probing the endpoint) must not be
// able to tell "expired" from "bad signature" — otherwise a forged ticket's signature could be
// confirmed independently of its expiry.
func TestExpiredAndForgedTicketsAreIndistinguishable(t *testing.T) {
	expired := MintTicket(testSecret(), baseNowMs-1)
	forged := MintTicket([]byte("some-other-secret-000000000000000"), baseNowMs+30_000)

	expiredErr := ValidateTicket(testSecret(), expired, baseNowMs)
	forgedErr := ValidateTicket(testSecret(), forged, baseNowMs)
	require.Error(t, expiredErr)
	require.Error(t, forgedErr)
	require.Equal(t, expiredErr.Error(), forgedErr.Error(), "error text must not classify the failure")
	require.True(t, errors.Is(expiredErr, ErrInvalidTicket) && errors.Is(forgedErr, ErrInvalidTicket))
}

// The 401 body is likewise uniform at the HTTP edge: an expired ticket and a forged one must look
// the same to a browser (or a scanner) probing the handshake.
func TestHandshakeRejectionDoesNotClassifyTheTicketFailure(t *testing.T) {
	s := newHardeningServer(t, testSecret(), []string{allowedOrigin}, false)

	tickets := map[string]string{
		"expired": MintTicket(testSecret(), baseNowMs-1),
		"forged":  MintTicket([]byte("some-other-secret-000000000000000"), baseNowMs+30_000),
		"garbage": "not.aticket",
	}
	seen := map[string]struct{}{}
	for name, ticket := range tickets {
		_, resp, err := s.dial(ticket, allowedOriginHeader())
		require.Errorf(t, err, "%s ticket must not upgrade", name)
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode, name)
		// io.ReadAll, not a single Read: a short read is legal and would collapse every case onto
		// the empty string, making the "one body" assertion below pass without comparing anything.
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, readErr, name)
		require.NotEmpty(t, body, "%s: the 401 body must actually be read for this comparison to mean anything", name)
		seen[string(body)] = struct{}{}
	}
	require.Len(t, seen, 1, "all ticket failures must share one response body")
}

// ---- 6. SECRET HANDLING ---------------------------------------------------------------------

// Rotating the secret must invalidate outstanding tickets in BOTH directions — otherwise a
// rotation would not actually revoke anything.
func TestTicketsAreNotTransferableAcrossSecrets(t *testing.T) {
	secretA := []byte("secret-A-0000000000000000000000000")
	secretB := []byte("secret-B-0000000000000000000000000")

	require.ErrorIs(t, ValidateTicket(secretB, MintTicket(secretA, baseNowMs+30_000), baseNowMs), ErrInvalidTicket)
	require.ErrorIs(t, ValidateTicket(secretA, MintTicket(secretB, baseNowMs+30_000), baseNowMs), ErrInvalidTicket)
}

// An empty secret is a DEGENERATE HMAC key, not a bypass: a ticket minted with the real secret is
// still rejected. But note the asymmetry the second assertion documents — ValidateTicket itself
// does not fail closed on an empty key (it happily verifies a ticket signed with that same empty
// key, which anyone could forge). The fail-closed decision therefore lives entirely in the CALLER,
// which is why the next test exercises it at the HTTP edge.
func TestEmptySecretIsADegenerateKeyNotAWildcard(t *testing.T) {
	require.ErrorIs(t, ValidateTicket(nil, MintTicket(testSecret(), baseNowMs+30_000), baseNowMs), ErrInvalidTicket)
	require.NoError(t, ValidateTicket(nil, MintTicket(nil, baseNowMs+30_000), baseNowMs),
		"an empty key still verifies its own signature — callers MUST reject an unconfigured secret themselves")
}

// The edge is where fail-closed is enforced: with no secret configured and the escape hatch OFF,
// the handshake answers 503 and never upgrades — even for a ticket that is arithmetically valid
// under the empty key (i.e. one an attacker could mint unaided).
func TestHandshakeFailsClosedWhenNoSecretIsConfigured(t *testing.T) {
	s := newHardeningServer(t, nil, []string{allowedOrigin}, false)

	forgeable := MintTicket(nil, baseNowMs+30_000) // anyone can produce this with an empty key
	_, resp, err := s.dial(forgeable, allowedOriginHeader())
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"unconfigured secret must be 'not available', never 'open'")
	_ = resp.Body.Close()
	require.Equal(t, 0, s.hub.statsForTest().Clients)
}

// The MINT half of the same fail-closed decision, which nothing asserted: server_test.go covers the
// placeholder (hatch ON) and the real ticket (secret set), leaving the misconfigured production case
// — hatch OFF, secret missing — unpinned. It matters more than the handshake half: minting under an
// empty key would hand out a credential ANYONE can reproduce, so the endpoint must emit no ticket
// field at all rather than a technically-well-formed one.
func TestMintTicketFailsClosedWhenNoSecretIsConfigured(t *testing.T) {
	s := newHardeningServer(t, nil, []string{allowedOrigin}, false)

	rec := httptest.NewRecorder()
	s.srv.MintTicket(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/ws-ticket", nil))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.NotContains(t, rec.Body.String(), `"ticket"`,
		"a misconfigured edge must not emit a ticket field — an empty-key ticket is forgeable by anyone")
}

// ---- 7. ORIGIN ALLOW-LIST (CSWSH) -----------------------------------------------------------

// The allow-list is an EXACT string match on the Origin header, and it runs BEFORE the ticket
// check. Every variant below is a real-world CSWSH attempt shape: a missing header (non-browser
// client), a `null` origin (sandboxed iframe / file://), suffix and prefix lookalikes, and
// case/port/scheme drift. All must 403 with a VALID ticket in hand — the ticket is not the
// defense here, the allow-list is.
func TestOriginAllowListRejectsLookalikesAndVariants(t *testing.T) {
	s := newHardeningServer(t, testSecret(), []string{allowedOrigin}, false)
	ticket := MintTicket(testSecret(), baseNowMs+30_000)

	cases := []struct {
		name   string
		header http.Header
	}{
		{"no Origin header at all", nil},
		{"empty Origin value", http.Header{"Origin": {""}}},
		{"null origin (sandboxed iframe / file://)", http.Header{"Origin": {"null"}}},
		{"suffix lookalike", http.Header{"Origin": {allowedOrigin + ".evil.test"}}},
		{"prefix lookalike", http.Header{"Origin": {"https://evil-app.test"}}},
		{"hyphen sibling of the host", http.Header{"Origin": {"https://app-test.evil"}}},
		{"subdomain of the allowed host", http.Header{"Origin": {"https://sub.app.test"}}},
		{"uppercase host", http.Header{"Origin": {"https://APP.TEST"}}},
		{"explicit default port", http.Header{"Origin": {allowedOrigin + ":443"}}},
		{"other port", http.Header{"Origin": {allowedOrigin + ":8443"}}},
		{"downgraded scheme", http.Header{"Origin": {"http://app.test"}}},
		{"trailing slash", http.Header{"Origin": {allowedOrigin + "/"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, resp, err := s.dial(ticket, tc.header)
			require.Error(t, err)
			require.Nil(t, conn)
			require.NotNil(t, resp)
			require.Equal(t, http.StatusForbidden, resp.StatusCode)
			_ = resp.Body.Close()
		})
	}
	require.Equal(t, 0, s.hub.statsForTest().Clients, "no rejected origin leaked a registered client")
}

// The origin check must precede the ticket check, so a hostile page gets 403 (not 401) and learns
// nothing about ticket validity. Ordering is also what keeps HMAC work off unauthenticated
// cross-origin requests.
func TestOriginIsCheckedBeforeTheTicket(t *testing.T) {
	s := newHardeningServer(t, testSecret(), []string{allowedOrigin}, false)

	_, resp, err := s.dial("", http.Header{"Origin": {"https://evil.test"}}) // no ticket AND bad origin
	require.Error(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode, "origin verdict wins over the missing ticket")
	_ = resp.Body.Close()
}

// An empty allow-list rejects everything (fail closed) — a misconfigured
// GOWHALE_REALTIME_WS_ALLOWED_ORIGINS must not degrade into "any origin".
func TestEmptyOriginAllowListRejectsEveryOrigin(t *testing.T) {
	s := newHardeningServer(t, testSecret(), nil, false)

	_, resp, err := s.dial(MintTicket(testSecret(), baseNowMs+30_000), allowedOriginHeader())
	require.Error(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	_ = resp.Body.Close()
}

// GOWHALE_REALTIME_WS_AUTH_DISABLED drops the CSWSH defense too, not just the ticket: any Origin is accepted.
// server_test.go covers "no ticket, no Origin"; this pins the sharper edge — an EXPLICITLY hostile
// origin is admitted — so the LAN-only constraint in the flag's docs is load-bearing, not advisory.
func TestAuthDisabledAcceptsAHostileOrigin(t *testing.T) {
	s := newHardeningServer(t, nil, nil, true)

	conn, resp, err := s.dial("", http.Header{"Origin": {"https://evil.test"}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	_ = resp.Body.Close()
	defer func() { _ = conn.Close() }()
	require.Eventually(t, func() bool { return s.hub.statsForTest().Clients == 1 },
		eventuallyWait, eventuallyTick, "escape hatch admits any origin — LAN only")
}

// The hatch OVERRIDES a fully-configured deployment: with a real secret AND a real allow-list in
// place, a garbage ticket from a hostile origin still upgrades. This is the misconfiguration that
// actually happens — "the secret is set, so we are protected" is the natural (and wrong) reading of
// the config, since GOWHALE_REALTIME_WS_AUTH_DISABLED does not merely fill in for missing auth, it disables the
// auth that IS there. The sibling test above uses an unconfigured server, so it cannot show this.
func TestAuthDisabledOverridesAConfiguredSecretAndAllowList(t *testing.T) {
	s := newHardeningServer(t, testSecret(), []string{allowedOrigin}, true)

	conn, resp, err := s.dial("garbage.ticket", http.Header{"Origin": {"https://evil.test"}})
	require.NoError(t, err, "a configured secret does NOT re-arm the checks the hatch turned off")
	require.NotNil(t, resp)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	_ = resp.Body.Close()
	defer func() { _ = conn.Close() }()
	require.Eventually(t, func() bool { return s.hub.statsForTest().Clients == 1 },
		eventuallyWait, eventuallyTick, "hostile origin + invalid ticket still reached the hub")
}
