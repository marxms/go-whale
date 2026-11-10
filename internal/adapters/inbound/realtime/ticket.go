package realtime

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
)

// ErrInvalidTicket is returned for any malformed, tampered, or expired ticket. It is
// deliberately generic (no detail about which check failed) so a caller cannot probe
// the difference between "bad signature" and "expired".
var ErrInvalidTicket = errors.New("invalid ws ticket")

// MintTicket creates a short-lived, HMAC-SHA256-signed ticket that authorizes a chart
// WebSocket connection. Browsers cannot send an Authorization header on the WS
// handshake, so an authenticated REST call (behind auth_request) mints this ticket and
// the client passes it in the WS URL. The ticket is STATELESS: a valid, unexpired
// ticket proves an authenticated caller minted it recently. For a read-only market-data
// stream this is sufficient — there is no per-user scope, and the short TTL bounds the
// replay window of a leaked ticket.
func MintTicket(secret []byte, expiryMs int64) string {
	payload := strconv.FormatInt(expiryMs, 10)
	sig := sign(secret, payload)
	return base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." +
		base64.RawURLEncoding.EncodeToString(sig)
}

// ValidateTicket verifies the signature in constant time and checks expiry. nowMs is
// the current time in ms epoch.
func ValidateTicket(secret []byte, ticket string, nowMs int64) error {
	payload, sig, ok := splitTicket(ticket)
	if !ok {
		return ErrInvalidTicket
	}
	if !hmac.Equal(sig, sign(secret, payload)) { // constant-time compare
		return ErrInvalidTicket
	}
	expiryMs, err := strconv.ParseInt(payload, 10, 64)
	if err != nil {
		return ErrInvalidTicket
	}
	if nowMs > expiryMs {
		return ErrInvalidTicket
	}
	return nil
}

func splitTicket(ticket string) (payload string, sig []byte, ok bool) {
	parts := strings.SplitN(ticket, ".", 2)
	if len(parts) != 2 {
		return "", nil, false
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", nil, false
	}
	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", nil, false
	}
	return string(payloadBytes), sigBytes, true
}

func sign(secret []byte, payload string) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(payload))
	return m.Sum(nil)
}
