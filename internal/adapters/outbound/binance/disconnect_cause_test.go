package binancesdk

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/gorilla/websocket"
)

// TestClassifyDisconnect pins the SRE/kernel/app cause taxonomy. The field
// signature `close 1006 (abnormal closure): unexpected EOF` is covered, but it
// is NOT a venue reason: 1006 is generated locally when no close frame arrived.
func TestClassifyDisconnect(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "none"},
		{"close 1006 abnormal (no close frame)", &websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"}, "transport_unattributed"},
		{"close 1000 normal", &websocket.CloseError{Code: websocket.CloseNormalClosure}, "close_1000"},
		{"close 1012 service restart", &websocket.CloseError{Code: 1012}, "close_1012"},
		{"econnreset wrapped (field #3)", &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}, "tcp_reset"},
		{"connection reset string", errors.New("read tcp 192.0.2.10:44776->203.0.113.10:443: read: connection reset by peer"), "tcp_reset"},
		{"broken pipe", &net.OpError{Op: "write", Err: syscall.EPIPE}, "broken_pipe"},
		{"dial timeout", &net.OpError{Op: "dial", Err: &timeoutError{}}, "io_timeout"},
		{"timeout string", errors.New("dial tcp 108.139.95.44:443: i/o timeout"), "io_timeout"},
		{"unexpected eof", io.ErrUnexpectedEOF, "eof"},
		{"plain eof", io.EOF, "eof"},
		{"net closed", net.ErrClosed, "closed"},
		{"use of closed string", errors.New("use of closed network connection"), "closed"},
		{"unknown", errors.New("something we have never seen"), "other"},
		{"wrapped close 1006", fmt.Errorf("read: %w", &websocket.CloseError{Code: 1006}), "transport_unattributed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyDisconnect(tt.err); got != tt.want {
				t.Errorf("classifyDisconnect(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

// TestClose1006WithoutFrameIsTransportUnattributed is the named SPC-13 proof: 1006 is the
// local stack reporting that NO close frame arrived (RFC 6455 §7.4.1 reserves it and forbids
// sending it). The label must not blame the venue, and it must stay distinct from a real close
// frame carrying a documented code.
func TestClose1006WithoutFrameIsTransportUnattributed(t *testing.T) {
	noFrame := &websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"}
	if got := classifyDisconnect(noFrame); got != "transport_unattributed" {
		t.Fatalf("classifyDisconnect(1006) = %q, want transport_unattributed", got)
	}
	// A close frame that DID arrive keeps its code: the rule is about absence, not about 1xxx.
	if got := classifyDisconnect(&websocket.CloseError{Code: websocket.CloseNormalClosure}); got != "close_1000" {
		t.Fatalf("classifyDisconnect(close frame 1000) = %q, want close_1000", got)
	}
	// The same absence expressed as a transport sentinel is never attributed to the venue.
	for _, err := range []error{io.EOF, io.ErrUnexpectedEOF, syscall.ECONNRESET} {
		if got := classifyDisconnect(err); strings.HasPrefix(got, "close_") {
			t.Fatalf("classifyDisconnect(%v) = %q, want a transport cause", err, got)
		}
	}
}

// timeoutError is a minimal net.Error whose Timeout() reports true.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
