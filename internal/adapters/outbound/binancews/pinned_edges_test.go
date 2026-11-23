package binancews

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func wsTestServer(t *testing.T) (*httptest.Server, <-chan string) {
	t.Helper()
	paths := make(chan string, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		<-time.After(100 * time.Millisecond)
	}))
	t.Cleanup(server.Close)
	return server, paths
}

// TestConnectionUsesPinnedEdge proves the TCP dial goes to the pinned host:port
// even when the URL names a different host; the URL still supplies the Host and
// the stream path.
func TestConnectionUsesPinnedEdge(t *testing.T) {
	server, paths := wsTestServer(t)
	pinned := strings.TrimPrefix(server.URL, "http://")
	conn := NewConnection(0, "ws://edge.invalid:443", WithPinnedEdges([]string{pinned}))
	if err := conn.Connect(context.Background(), map[string]Handler{
		"btcusdt@trade": func([]byte) {},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	select {
	case path := <-paths:
		if path != "/stream" {
			t.Fatalf("path = %q, want /stream", path)
		}
	case <-time.After(time.Second):
		t.Fatal("the handshake never reached the pinned edge")
	}
}

// TestConnectionFallsBackToURLHost proves a dead pinned edge does not disable
// the connection: the dialer falls back to the URL host.
func TestConnectionFallsBackToURLHost(t *testing.T) {
	server, paths := wsTestServer(t)
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	conn := NewConnection(0, url, WithPinnedEdges([]string{"127.0.0.1:1"}))
	if err := conn.Connect(context.Background(), map[string]Handler{
		"btcusdt@trade": func([]byte) {},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	select {
	case path := <-paths:
		if path != "/stream" {
			t.Fatalf("path = %q, want /stream", path)
		}
	case <-time.After(time.Second):
		t.Fatal("the fallback never reached the URL host")
	}
}
