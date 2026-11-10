package realtime

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Server exposes the chart WebSocket edge and the ticket-mint endpoint. It enforces an
// Origin allow-list (defense against cross-site WebSocket hijacking) and validates a
// short-lived HMAC ticket on the handshake, because a browser cannot send an
// Authorization header on the WS upgrade.
type Server struct {
	hub            *Hub
	ticketSecret   []byte
	mintToken      []byte
	allowedOrigins map[string]struct{}
	ticketTTL      time.Duration
	clock          func() int64
	authDisabled   bool
	upgrader       websocket.Upgrader
}

// NewServer builds the WS edge. allowedOrigins is the exact set of browser origins
// permitted (e.g. "https://app.example.com"); an empty set rejects everything (fail
// closed). ticketSecret signs/validates connection tickets. mintToken is the
// server-to-gateway secret required by MintTicket: the authenticating gateway injects
// X-Gowhale-Mint-Token after it has authenticated the user.
//
// authDisabled is the LOCAL/INTRANET escape hatch: when true the handshake skips BOTH the ticket
// check and the Origin allow-list (network trust replaces app auth on a trusted LAN). It must be an
// explicit, deliberate opt-in — never inferred from an empty secret — so production stays fail-closed.
func NewServer(hub *Hub, ticketSecret, mintToken []byte, allowedOrigins []string, ticketTTL time.Duration, clock func() int64, authDisabled bool) *Server {
	origins := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		if o != "" {
			origins[o] = struct{}{}
		}
	}
	s := &Server{
		hub:            hub,
		ticketSecret:   ticketSecret,
		mintToken:      mintToken,
		allowedOrigins: origins,
		ticketTTL:      ticketTTL,
		clock:          clock,
		authDisabled:   authDisabled,
	}
	s.upgrader = websocket.Upgrader{
		CheckOrigin:     s.originAllowed, // backstop; ServeWS pre-checks for a clean 403
		ReadBufferSize:  1024,
		WriteBufferSize: 4096,
	}
	return s
}

// originAllowed enforces the Origin allow-list. A missing Origin is rejected (a browser
// always sends one; rejecting protects against non-browser clients that omit it). With
// authDisabled (local/intranet) every Origin is accepted — network trust replaces the CSWSH check.
func (s *Server) originAllowed(r *http.Request) bool {
	if s.authDisabled {
		return true
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	_, ok := s.allowedOrigins[origin]
	return ok
}

// ServeWS authenticates (origin + ticket), upgrades, and registers the client.
func (s *Server) ServeWS(w http.ResponseWriter, r *http.Request) {
	if !s.originAllowed(r) {
		http.Error(w, "forbidden origin", http.StatusForbidden)
		return
	}
	// LOCAL/INTRANET: skip ticket validation entirely. The `authDisabled` gate is an explicit,
	// deliberate opt-in (never an empty-secret fallback), so production keeps the checks below.
	if !s.authDisabled {
		if len(s.ticketSecret) == 0 {
			http.Error(w, "stream auth not configured", http.StatusServiceUnavailable)
			return
		}
		if err := ValidateTicket(s.ticketSecret, r.URL.Query().Get("ticket"), s.clock()); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote the failure response
	}
	s.hub.add(newClient(conn, s.hub.eventsBufSize))
}

// MintTicket issues a short-lived ticket. The authenticating gateway MUST verify the
// user and inject X-Gowhale-Mint-Token before proxying this request; the handler itself
// is not a user-auth boundary. The browser then opens the WS with ?ticket=<value>.
func (s *Server) MintTicket(w http.ResponseWriter, r *http.Request) {
	expiry := s.clock() + s.ticketTTL.Milliseconds()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	// LOCAL/INTRANET: return a placeholder ticket (200) so the browser's unchanged fetch-then-connect
	// flow works; ServeWS ignores the ticket in this mode.
	if s.authDisabled {
		_ = json.NewEncoder(w).Encode(struct {
			Ticket    string `json:"ticket"`
			ExpiresAt int64  `json:"expiresAt"`
		}{Ticket: "noauth", ExpiresAt: expiry})
		return
	}
	if len(s.ticketSecret) == 0 || len(s.mintToken) == 0 {
		http.Error(w, "stream auth not configured", http.StatusServiceUnavailable)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Gowhale-Mint-Token")), s.mintToken) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	_ = json.NewEncoder(w).Encode(struct {
		Ticket    string `json:"ticket"`
		ExpiresAt int64  `json:"expiresAt"`
	}{Ticket: MintTicket(s.ticketSecret, expiry), ExpiresAt: expiry})
}
