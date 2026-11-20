package binancews

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Production spent hours unable to attribute 1006s: the code, the peer's reason and the initiator
// were discarded when the socket closed. This is the evidence layer that makes the next
// disconnect attributable instead of a guess.
func TestCloseEvidenceRetainsTheVenueCloseCode(t *testing.T) {
	server := newWSServer(t, func(conn *websocket.Conn, _ *http.Request) {
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1008, "policy"))
	})

	c := NewConnection(3, wsURL(server))
	if err := c.Connect(context.Background(), map[string]Handler{}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("connection did not stop after the peer's close frame")
	}

	ev := c.CloseEvidence()
	if ev.Code != 1008 {
		t.Fatalf("close code = %d, want 1008: the peer's code must survive the close", ev.Code)
	}
	if ev.Initiator != InitiatorRemote {
		t.Fatalf("initiator = %q, want %q", ev.Initiator, InitiatorRemote)
	}
}

func TestCloseEvidenceMarksOurOwnShutdown(t *testing.T) {
	server := newWSServer(t, func(_ *websocket.Conn, _ *http.Request) {
		<-time.After(2 * time.Second)
	})

	c := NewConnection(4, wsURL(server))
	if err := c.Connect(context.Background(), map[string]Handler{}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if ev := c.CloseEvidence(); ev.Initiator != InitiatorLocalShutdown {
		t.Fatalf("initiator = %q, want %q — our own shutdown must never be counted as a venue fault",
			ev.Initiator, InitiatorLocalShutdown)
	}
}

// TestCloseEvidenceCarriesDialAgeAndStreams is the named T24 proof: the evidence retained at
// close says how long the socket lived, how many streams it carried and, when the owner knows
// it, how fresh the sister partition was.
func TestCloseEvidenceCarriesDialAgeAndStreams(t *testing.T) {
	server := newWSServer(t, func(_ *websocket.Conn, _ *http.Request) {
		<-time.After(2 * time.Second)
	})
	c := NewConnection(9, wsURL(server))
	handlers := map[string]Handler{
		"btcusdt@kline_1m": func([]byte) {},
		"ethusdt@kline_1m": func([]byte) {},
		"solusdt@kline_1m": func([]byte) {},
	}
	if err := c.Connect(context.Background(), handlers); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	c.SetSisterLastFrameAge(1500 * time.Millisecond)
	evidence := c.CloseEvidence()
	if evidence.DialAge < 50*time.Millisecond {
		t.Fatalf("dial age = %v, want the time the socket lived", evidence.DialAge)
	}
	if evidence.Streams != len(handlers) {
		t.Fatalf("streams = %d, want %d", evidence.Streams, len(handlers))
	}
	if evidence.SisterLastFrameAge != 1500*time.Millisecond {
		t.Fatalf("sister age = %v, want the owner-stamped value", evidence.SisterLastFrameAge)
	}
}

// The watcher channel holds ONE error. A second one used to disappear in silence, leaving a
// reconnect with no cause; the retained copy is what the log and the recovery path read.
func TestTerminalErrorSurvivesAFullWatcherChannel(t *testing.T) {
	c := NewConnection(5, "ws://127.0.0.1:1")
	c.report(errors.New("first"))
	c.report(errors.New("second"))

	if got := c.DroppedReports(); got != 1 {
		t.Fatalf("dropped reports = %d, want 1", got)
	}
	if err := c.TerminalError(); err == nil || err.Error() != "second" {
		t.Fatalf("terminal error = %v, want the latest one", err)
	}
}
