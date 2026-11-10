package binancews

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func newWSServer(t *testing.T, handle func(*websocket.Conn, *http.Request)) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		handle(conn, r)
	}))
	t.Cleanup(server.Close)
	return server
}

func wsURL(server *httptest.Server) string {
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func writeFrame(t *testing.T, conn *websocket.Conn, stream, data string) {
	t.Helper()
	payload := `{"stream":"` + stream + `","data":` + data + `}`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
		t.Errorf("write frame: %v", err)
	}
}

func TestConnectInstallsHandlersBeforeFirstFrame(t *testing.T) {
	frames := make(chan string, 2)
	server := newWSServer(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrame(t, conn, "btcusdt@aggTrade", `{"a":1}`)
		writeFrame(t, conn, "btcusdt@aggTrade", `{"a":2}`)
		<-time.After(200 * time.Millisecond)
	})

	conn := NewConnection(0, wsURL(server))
	if err := conn.Connect(context.Background(), map[string]Handler{
		"btcusdt@aggTrade": func(data []byte) { frames <- string(data) },
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, want := range []string{`{"a":1}`, `{"a":2}`} {
		select {
		case got := <-frames:
			if got != want {
				t.Fatalf("frame = %s, want %s", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("frame %s was not delivered", want)
		}
	}
}

func TestSubscribeRegistersBeforeControlFrame(t *testing.T) {
	control := make(chan string, 1)
	server := newWSServer(t, func(conn *websocket.Conn, _ *http.Request) {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Errorf("read control: %v", err)
			return
		}
		control <- string(payload)
		writeFrame(t, conn, "ethusdt@aggTrade", `{"a":7}`)
		<-time.After(200 * time.Millisecond)
	})

	conn := NewConnection(0, wsURL(server))
	if err := conn.Connect(context.Background(), nil); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	received := make(chan string, 1)
	if err := conn.Subscribe(context.Background(), "ethusdt@aggTrade", func(data []byte) {
		received <- string(data)
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case got := <-control:
		if !strings.Contains(got, `"method":"SUBSCRIBE"`) || !strings.Contains(got, `ethusdt@aggTrade`) {
			t.Fatalf("control frame = %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("SUBSCRIBE was not sent")
	}
	select {
	case got := <-received:
		if got != `{"a":7}` {
			t.Fatalf("frame = %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("subscribed frame was not delivered")
	}
}

func TestRawFrameIsRoutedBySymbolAndEvent(t *testing.T) {
	server := newWSServer(t, func(conn *websocket.Conn, _ *http.Request) {
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Errorf("read control: %v", err)
			return
		}
		raw := `{"e":"aggTrade","s":"ETHUSDT","a":7,"p":"100"}`
		if err := conn.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
			t.Errorf("write raw frame: %v", err)
		}
		<-time.After(200 * time.Millisecond)
	})

	conn := NewConnection(0, wsURL(server))
	if err := conn.Connect(context.Background(), nil); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	received := make(chan string, 1)
	if err := conn.Subscribe(context.Background(), "ethusdt@aggTrade", func(data []byte) {
		received <- string(data)
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case got := <-received:
		if !strings.Contains(got, `"e":"aggTrade"`) || !strings.Contains(got, `"s":"ETHUSDT"`) {
			t.Fatalf("raw frame = %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("raw frame was not routed to its handler")
	}
}

func TestMalformedFrameReportsAndReaderContinues(t *testing.T) {
	received := make(chan string, 1)
	server := newWSServer(t, func(conn *websocket.Conn, _ *http.Request) {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"stream":`))
		writeFrame(t, conn, "btcusdt@aggTrade", `{"a":9}`)
		<-time.After(200 * time.Millisecond)
	})

	conn := NewConnection(0, wsURL(server))
	if err := conn.Connect(context.Background(), map[string]Handler{
		"btcusdt@aggTrade": func(data []byte) { received <- string(data) },
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	select {
	case err := <-conn.Errors():
		if !strings.Contains(err.Error(), "malformed frame") {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("malformed frame was not reported")
	}
	select {
	case got := <-received:
		if got != `{"a":9}` {
			t.Fatalf("frame = %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("reader stopped after malformed frame")
	}
}

func TestCloseIsIdempotentAndDoesNotBlock(t *testing.T) {
	server := newWSServer(t, func(_ *websocket.Conn, _ *http.Request) {
		<-time.After(time.Second)
	})
	conn := NewConnection(0, wsURL(server))
	if err := conn.Connect(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if conn.Healthy() {
		t.Fatal("connection healthy after Close")
	}
	select {
	case <-conn.Done():
	default:
		t.Fatal("Done not closed after Close")
	}
}

func TestScheduledRenewalReportsAndCloses(t *testing.T) {
	server := newWSServer(t, func(_ *websocket.Conn, _ *http.Request) {
		<-time.After(time.Second)
	})
	conn := NewConnection(0, wsURL(server))
	conn.RenewalInterval = 30 * time.Millisecond
	if err := conn.Connect(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	select {
	case <-conn.Done():
	case <-time.After(time.Second):
		t.Fatal("renewal did not close the connection")
	}
	select {
	case err := <-conn.Errors():
		if !strings.Contains(err.Error(), "renewal") {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("renewal did not report an error")
	}
}

func TestBoundedBufferConstants(t *testing.T) {
	if ReadBufferSize != 256*1024 || WriteBufferSize != 256*1024 {
		t.Fatalf("buffers = %d/%d, want 256 KiB", ReadBufferSize, WriteBufferSize)
	}
}

// handshakeBodySpy proves the response body is actually released. gorilla returns a live
// *http.Response on success AND on ErrBadHandshake; the body must be closed on both.
type handshakeBodySpy struct{ closed bool }

func (s *handshakeBodySpy) Read([]byte) (int, error) { return 0, io.EOF }
func (s *handshakeBodySpy) Close() error             { s.closed = true; return nil }

func TestCloseHandshakeResponseClosesBody(t *testing.T) {
	body := &handshakeBodySpy{}
	closeHandshakeResponse(&http.Response{Body: body})
	if !body.closed {
		t.Fatal("handshake response body was not closed")
	}

	// Both nil shapes reach the close helper on the error path of a failed dial.
	closeHandshakeResponse(nil)
	closeHandshakeResponse(&http.Response{})
}
