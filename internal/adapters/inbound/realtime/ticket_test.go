package realtime

import (
	"errors"
	"strings"
	"testing"
)

func testSecret() []byte {
	return []byte("a-strong-32-byte-secret-key-000000")
}

func TestValidTicketPasses(t *testing.T) {
	tk := MintTicket(testSecret(), 2000)
	if err := ValidateTicket(testSecret(), tk, 1000); err != nil {
		t.Fatalf("valid ticket rejected: %v", err)
	}
}

func TestExpiredTicketFails(t *testing.T) {
	tk := MintTicket(testSecret(), 1000)
	if err := ValidateTicket(testSecret(), tk, 1001); !errors.Is(err, ErrInvalidTicket) {
		t.Fatalf("expired ticket accepted or wrong error: %v", err)
	}
}

func TestTicketAtExactExpiryPasses(t *testing.T) {
	tk := MintTicket(testSecret(), 1000)
	if err := ValidateTicket(testSecret(), tk, 1000); err != nil {
		t.Fatalf("ticket at exact expiry rejected: %v", err)
	}
}

func TestWrongSecretFails(t *testing.T) {
	tk := MintTicket(testSecret(), 2000)
	if err := ValidateTicket([]byte("different-secret-key-0000000000000"), tk, 1000); !errors.Is(err, ErrInvalidTicket) {
		t.Fatalf("ticket accepted under wrong secret")
	}
}

func TestTamperedSignatureFails(t *testing.T) {
	tk := MintTicket(testSecret(), 2000)
	// Flip the FIRST char of the signature segment — its 6 bits are significant (they
	// carry the top of the first signature byte), so the decoded bytes actually change.
	// (Flipping the LAST char can be a no-op: a 32-byte HMAC's final base64 char has
	// unused low bits.)
	i := strings.LastIndexByte(tk, '.')
	sig := []byte(tk[i+1:])
	if sig[0] == 'A' {
		sig[0] = 'B'
	} else {
		sig[0] = 'A'
	}
	tampered := tk[:i+1] + string(sig)
	if err := ValidateTicket(testSecret(), tampered, 1000); !errors.Is(err, ErrInvalidTicket) {
		t.Fatalf("tampered signature accepted")
	}
}

func TestTamperedPayloadFails(t *testing.T) {
	// Forge a payload with a far-future expiry but keep the original signature.
	tk := MintTicket(testSecret(), 2000)
	i := strings.IndexByte(tk, '.')
	forged := "OTk5OTk5OTk5OTk=" + tk[i:] // base64 of a big number, original sig
	if err := ValidateTicket(testSecret(), forged, 1000); !errors.Is(err, ErrInvalidTicket) {
		t.Fatalf("forged payload accepted")
	}
}

func TestMalformedTicketsFail(t *testing.T) {
	cases := []string{"", "no-dot", "only.", ".only", "!!!.###", "a.b.c"}
	for _, tk := range cases {
		if err := ValidateTicket(testSecret(), tk, 1000); !errors.Is(err, ErrInvalidTicket) {
			t.Errorf("malformed ticket %q accepted", tk)
		}
	}
}
