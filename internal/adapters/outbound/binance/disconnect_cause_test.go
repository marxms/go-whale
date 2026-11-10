package binancesdk

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/gorilla/websocket"
)

// TestClassifyDisconnect pins the SRE/kernel/app cause taxonomy. The two field
// signatures we actually observed in production are explicitly covered: the
// `close 1006 (abnormal closure): unexpected EOF` (Binance-side recycle) and the
// `read tcp ...: read: connection reset by peer` (kernel RST from the peer).
func TestClassifyDisconnect(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "none"},
		{"close 1006 abnormal (field #2)", &websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"}, "close_1006"},
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
		{"wrapped close 1006", fmt.Errorf("read: %w", &websocket.CloseError{Code: 1006}), "close_1006"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyDisconnect(tt.err); got != tt.want {
				t.Errorf("classifyDisconnect(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

// timeoutError is a minimal net.Error whose Timeout() reports true.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
