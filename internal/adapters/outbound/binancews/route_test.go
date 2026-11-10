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

func TestConnectionURLUsesRoute(t *testing.T) {
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
	defer server.Close()

	conn := NewConnection(0, "ws"+strings.TrimPrefix(server.URL, "http"), WithRoute("/market"))
	if err := conn.Connect(context.Background(), map[string]Handler{
		"btcusdt@kline_5m": func([]byte) {},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	select {
	case path := <-paths:
		if path != "/market/stream" {
			t.Fatalf("path = %q, want /market/stream", path)
		}
	case <-time.After(time.Second):
		t.Fatal("the handshake never reached the server")
	}
}

func TestRouteForStreamClass(t *testing.T) {
	cases := []struct {
		route string
		path  string
	}{
		{"", "/stream"},
		{"/public", "/public/stream"},
		{"/market", "/market/stream"},
	}
	for _, tc := range cases {
		conn := NewConnection(0, "wss://fstream.binance.com", WithRoute(tc.route))
		url, err := conn.urlFor(map[string]Handler{"btcusdt@kline_5m": func([]byte) {}})
		if err != nil {
			t.Fatalf("urlFor(%q): %v", tc.route, err)
		}
		want := "wss://fstream.binance.com" + tc.path + "?streams=btcusdt@kline_5m"
		if url != want {
			t.Errorf("urlFor(%q) = %q, want %q", tc.route, url, want)
		}
	}
}

func TestWithReadIdleApplies(t *testing.T) {
	conn := NewConnection(0, "wss://example.invalid", WithReadIdle(4*time.Minute))
	if conn.readIdle != 4*time.Minute {
		t.Fatalf("readIdle = %v, want 4m", conn.readIdle)
	}
	plain := NewConnection(0, "wss://example.invalid")
	if plain.readIdle != 0 {
		t.Fatalf("default readIdle = %v, want zero (no documented deadline)", plain.readIdle)
	}
}
