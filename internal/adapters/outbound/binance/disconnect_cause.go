package binancesdk

import (
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"

	"github.com/gorilla/websocket"
)

// classifyDisconnect maps a WebSocket read error into a low-cardinality cause
// label. The categories split a reconnect's proximate cause across the layers the
// SRE troubleshooting needs:
//
//   - close_<code>: the server sent a WebSocket close frame (Binance-side). 1006
//     (abnormal closure) is what Binance uses for un-announced recycling/load-shed.
//   - tcp_reset / eof / io_timeout: kernel/transport drops — an RST from the peer,
//     a truncated stream, or a dial/read timeout (a timeout often co-occurs across
//     exchanges, i.e. a local-network event; the cross-exchange correlator labels
//     that blast radius separately).
//   - closed: our own intentional close (use of closed network connection); the SDK
//     filters these from ErrorChan, so it should be rare here — kept for safety.
//   - other: anything unclassified, so the metric never silently drops a case.
//
// The cause set is bounded (a handful of close codes + a fixed list), keeping the
// metric label cardinality low. Pure function — unit-tested in isolation.
func classifyDisconnect(err error) string {
	if err == nil {
		return "none"
	}
	// A WebSocket close frame carries the most specific signal — check it first.
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return "close_" + strconv.Itoa(closeErr.Code)
	}
	// Kernel/transport sentinels (traverse wrapped *net.OpError/*os.SyscallError).
	switch {
	case errors.Is(err, syscall.ECONNRESET):
		return "tcp_reset"
	case errors.Is(err, syscall.EPIPE):
		return "broken_pipe"
	case errors.Is(err, net.ErrClosed):
		return "closed"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "io_timeout"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "eof"
	}
	// String fallback for errors that carry only a message (some net/tls layers).
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "connection reset"):
		return "tcp_reset"
	case strings.Contains(msg, "broken pipe"):
		return "broken_pipe"
	case strings.Contains(msg, "i/o timeout"), strings.Contains(msg, "timeout"):
		return "io_timeout"
	case strings.Contains(msg, "unexpected eof"), strings.Contains(msg, "eof"):
		return "eof"
	case strings.Contains(msg, "use of closed"):
		return "closed"
	}
	return "other"
}
