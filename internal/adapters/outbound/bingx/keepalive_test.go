package bingx

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func dialPongServer(t *testing.T) (*websocket.Conn, <-chan []byte, func()) {
	t.Helper()
	received := make(chan []byte, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, msg, err := conn.ReadMessage(); err == nil {
			received <- msg
		}
	}))
	client, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if response != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		server.Close()
		t.Fatalf("dial: %v", err)
	}
	return client, received, server.Close
}

func TestBingXKeepaliveEncoding(t *testing.T) {
	t.Run("swap answers text Ping with Pong", func(t *testing.T) {
		conn, received, closeServer := dialPongServer(t)
		defer closeServer()
		defer func() { _ = conn.Close() }()

		client := &BingXClient{connected: true, conn: conn}
		client.handleMessage([]byte("Ping"))

		select {
		case msg := <-received:
			if string(msg) != "Pong" {
				t.Fatalf("pong = %q, want Pong", msg)
			}
		case <-time.After(time.Second):
			t.Fatal("no text pong was written")
		}
	})

	t.Run("spot answers JSON ping with JSON pong", func(t *testing.T) {
		conn, received, closeServer := dialPongServer(t)
		defer closeServer()
		defer func() { _ = conn.Close() }()

		client := &BingXClient{connected: true, conn: conn}
		client.handleMessage([]byte(`{"ping":"abc","time":"123"}`))

		select {
		case msg := <-received:
			var pong map[string]any
			if err := json.Unmarshal(msg, &pong); err != nil {
				t.Fatalf("pong is not JSON: %v (%s)", err, msg)
			}
			if pong["pong"] != "abc" || pong["time"] != "123" {
				t.Fatalf("pong = %v, want pong=abc time=123", pong)
			}
		case <-time.After(time.Second):
			t.Fatal("no JSON pong was written")
		}
	})
}
