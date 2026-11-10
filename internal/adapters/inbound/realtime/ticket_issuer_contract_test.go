package realtime

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
)

// THE CONTRACT THE TWO TICKET ISSUERS DO NOT SHARE.
//
// The live chart presented this WS with the ticket issued by the ANALYZER
// (`/analyzer/api/indicators/stream-ticket`) and got 401 on every attempt. Both services issue
// tickets with the same general shape — `base64url(payload).base64url(HMAC)` — which makes them
// easy to confuse by eye. They are not interchangeable, for two independent reasons:
//
// 1. WHAT IS SIGNED differs. Here the HMAC message is the DECODED payload (the raw decimal);
// in the analyzer it is the base64url TEXT. Different messages, different signatures, same
// secret.
// 2. WHAT THE PAYLOAD CARRIES differs. Here it is a decimal integer; there it is a JSON object
// `{"exp":<ms>}`.
//
// The cases below pin this with the SAME SECRET on both sides, deliberately: that isolates the
// FORMAT, not the key. Aligning secrets in production would not make the wrong ticket pass. The
// only possible fix is for the client to ask the right issuer.

// mintAnalyzerFormatTicket reproduces
// the legacy analyzer ticket format byte for byte. It lives here
// and not in a fixture because a frozen fixture would not flag a future change in the analyzer
// issuer; this function is its executable specification.
func mintAnalyzerFormatTicket(secret []byte, expiryMs int64) string {
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, expiryMs)))
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(payload)) // signs the base64 TEXT, not the decoded bytes
	return payload + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

func TestAnalyzerIssuedTicketIsRejectedEvenWithTheSameSecret(t *testing.T) {
	const nowMs = 1_786_553_000_000
	// A perfectly valid ticket — for the analyzer. Not expired, signed with THIS secret.
	tk := mintAnalyzerFormatTicket(testSecret(), nowMs+60_000)
	if err := ValidateTicket(testSecret(), tk, nowMs); !errors.Is(err, ErrInvalidTicket) {
		t.Fatalf("analyzer-issued ticket was accepted by the streamer WS: %v", err)
	}
}

func TestStreamerIssuedTicketIsRejectedByTheAnalyzerFormat(t *testing.T) {
	// The reciprocal, to prove the incompatibility is of the FORMAT and not an asymmetry: this
	// service's ticket is also not what the analyzer would sign for the same instant.
	const expiryMs = 1_786_553_060_000
	if MintTicket(testSecret(), expiryMs) == mintAnalyzerFormatTicket(testSecret(), expiryMs) {
		t.Fatal("the two issuers produced the SAME ticket — the contract stopped discriminating the issuer")
	}
}

// THE CANONICAL UNIT OF `exp` IS MILLISECONDS.
//
// `MintTicket`/`ValidateTicket` speak milliseconds end to end, and what feeds them in
// production is `time.Now().UnixMilli()` (`internal/app/app.go`). An issuer that swapped
// milliseconds for seconds would not break any signature — the ticket would remain
// well-formed — but it would be born expired ~55 years in the past. That is a silent unit
// failure, so it needs its own sensor.
func TestExpiryUnitIsMillisecondsNotSeconds(t *testing.T) {
	const nowMs = 1_786_553_000_000
	const validForMs = 60_000

	if err := ValidateTicket(testSecret(), MintTicket(testSecret(), nowMs+validForMs), nowMs); err != nil {
		t.Fatalf("millisecond ticket rejected: %v", err)
	}
	// Same window, expressed in seconds: well-formed, correct signature, and still invalid.
	inSeconds := int64(nowMs+validForMs) / 1000
	if err := ValidateTicket(testSecret(), MintTicket(testSecret(), inSeconds), nowMs); !errors.Is(err, ErrInvalidTicket) {
		t.Fatal("ticket with expiry in SECONDS was accepted — the unit stopped being milliseconds")
	}
}
