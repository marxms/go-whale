package realtime

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

const allowedOrigin = "https://app.test"

func newTestServer(t *testing.T) (*Server, *Hub, string) {
	t.Helper()
	h := startHub(t, newFakeOpens())
	s := NewServer(h, testSecret(), []string{allowedOrigin}, 30*time.Second, func() int64 { return 1000 }, false)
	ts := httptest.NewServer(http.HandlerFunc(s.ServeWS))
	t.Cleanup(ts.Close)
	return s, h, "ws" + strings.TrimPrefix(ts.URL, "http")
}

// newAuthDisabledServer builds the LOCAL/INTRANET escape-hatch server: no secret, no origins, and
// authDisabled=true — the handshake must still upgrade without a ticket or an Origin header.
func newAuthDisabledServer(t *testing.T) (*Server, *Hub, string) {
	t.Helper()
	h := startHub(t, newFakeOpens())
	s := NewServer(h, nil, nil, 30*time.Second, func() int64 { return 1000 }, true)
	ts := httptest.NewServer(http.HandlerFunc(s.ServeWS))
	t.Cleanup(ts.Close)
	return s, h, "ws" + strings.TrimPrefix(ts.URL, "http")
}

func TestServeWSRejectsBadOrigin(t *testing.T) {
	_, h, wsURL := newTestServer(t)
	defer stopHub(t, h)

	ticket := MintTicket(testSecret(), 31000)
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL+"?ticket="+ticket, http.Header{"Origin": {"https://evil.test"}})
	require.Error(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	_ = resp.Body.Close()
	require.Nil(t, conn)
}

func TestServeWSRejectsInvalidTicket(t *testing.T) {
	_, h, wsURL := newTestServer(t)
	defer stopHub(t, h)

	conn, resp, err := websocket.DefaultDialer.Dial(wsURL+"?ticket=bogus.ticket", http.Header{"Origin": {allowedOrigin}})
	require.Error(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	_ = resp.Body.Close()
	require.Nil(t, conn)
}

func TestServeWSAcceptsValidTicketAndOrigin(t *testing.T) {
	_, h, wsURL := newTestServer(t)
	defer stopHub(t, h)

	ticket := MintTicket(testSecret(), 31000)
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL+"?ticket="+ticket, http.Header{"Origin": {allowedOrigin}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	_ = resp.Body.Close()

	require.Eventually(t, func() bool { return h.statsForTest().Clients == 1 }, eventuallyWait, eventuallyTick, "client registered")

	_ = conn.Close()
	require.Eventually(t, func() bool { return h.statsForTest().Clients == 0 }, eventuallyWait, eventuallyTick, "client cleaned up on disconnect")
}

func TestServeWSAuthDisabledAcceptsWithoutTicketOrOrigin(t *testing.T) {
	_, h, wsURL := newAuthDisabledServer(t)
	defer stopHub(t, h)

	// No ticket, no Origin header — the local escape hatch must still upgrade (network trust).
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	_ = resp.Body.Close()
	require.Eventually(t, func() bool { return h.statsForTest().Clients == 1 }, eventuallyWait, eventuallyTick, "client registered")
	_ = conn.Close()
}

func TestMintTicketAuthDisabledReturnsPlaceholder(t *testing.T) {
	s, h, _ := newAuthDisabledServer(t)
	defer stopHub(t, h)

	rec := httptest.NewRecorder()
	s.MintTicket(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/ws-ticket", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Ticket    string `json:"ticket"`
		ExpiresAt int64  `json:"expiresAt"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "noauth", body.Ticket) // placeholder — ServeWS ignores it in this mode
}

func TestMintTicketReturnsValidTicket(t *testing.T) {
	s, h, _ := newTestServer(t)
	defer stopHub(t, h)

	rec := httptest.NewRecorder()
	s.MintTicket(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/ws-ticket", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Ticket    string `json:"ticket"`
		ExpiresAt int64  `json:"expiresAt"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, int64(31000), body.ExpiresAt) // clock 1000 + TTL 30s
	require.NoError(t, ValidateTicket(testSecret(), body.Ticket, 1000))
}
