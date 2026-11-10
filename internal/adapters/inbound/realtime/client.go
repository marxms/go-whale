package realtime

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/distribution"
)

// maxClientMessageSize bounds an inbound client control message (subscribe requests
// are tiny); it protects the read pump from a hostile large frame.
const maxClientMessageSize = 4096

// wsConn is the minimal websocket surface the pumps use. *websocket.Conn satisfies it
// in production; a fake satisfies it in tests (deterministic eviction / single-writer
// assertions without a real socket).
type wsConn interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteMessage(messageType int, data []byte) error
	WriteControl(messageType int, data []byte, deadline time.Time) error
	SetReadLimit(limit int64)
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	SetPongHandler(h func(appData string) error)
	Close() error
}

// clientRequest is the wire shape a client sends to (un)subscribe a scope.
type clientRequest struct {
	Action string `json:"action"` // "subscribe" | "unsubscribe"
	Scope  Scope  `json:"scope"`
}

// snapshotWire is the JSON the client receives for a snapshot frame.
type snapshotWire struct {
	Version  int    `json:"v"`
	Type     string `json:"type"`
	Scope    Scope  `json:"scope"`
	Baseline uint64 `json:"baseline"`
	// BaselineTime is the seam anchor, in the same unit as Candle.Timestamp so the client can
	// compare it directly against the bars in its history window. Omitted when Baseline is 0: no
	// close has been counted, so there is no seam to verify and an anchor of 0 would be a lie.
	BaselineTime int64      `json:"baselineTime,omitempty"`
	Open         *CandleDTO `json:"open,omitempty"`
}

// heartbeatWire is the JSON a client receives for a liveness frame. It rides the same
// versioned, type-discriminated envelope as every other frame so an existing consumer can
// route it by `type`, and it carries NOTHING else: no scope, no candle, no sequence. A
// client with N subscribed scopes receives ONE heartbeat per tick, because the frame is a
// property of the connection, not of any subscription.
type heartbeatWire struct {
	Version int    `json:"v"`
	Type    string `json:"type"`
}

// buildHeartbeatFrame serializes the liveness frame. The frame has no per-connection or
// per-scope state, so each connection builds it once; the struct above stays the single source
// of these bytes (wire_contract_test.go pins them).
func buildHeartbeatFrame() []byte {
	data, err := json.Marshal(heartbeatWire{Version: SchemaVersion, Type: string(EventHeartbeat)})
	if err != nil {
		panic("realtime: heartbeat wire is not marshalable: " + err.Error())
	}
	return data
}

// Client is one WebSocket connection. It implements distribution.Member: the actor owns
// membership and subscriptions; this type owns the socket, the bounded lossless lane
// (shared with the actor) and the conflated-open slot (guarded by mu).
type Client struct {
	conn wsConn

	lane *distribution.Lane // lossless: the hub is the only sender AND closer

	mu   sync.Mutex
	open map[Scope]candle.Candle // latest-wins per scope; guarded by mu
	wake chan struct{}           // buf 1; the actor signals non-blockingly; never closed

	// Transport lifetime knobs copied from the Hub wrapper at registration.
	pingPeriod time.Duration
	connMaxAge time.Duration
	// heartbeat is the serialized liveness frame, built once for this connection.
	heartbeat []byte
}

func newClient(conn wsConn, eventsBufSize int) *Client {
	return &Client{
		conn:       conn,
		lane:       distribution.NewLane(eventsBufSize),
		open:       make(map[Scope]candle.Candle),
		wake:       make(chan struct{}, 1),
		pingPeriod: pingPeriod,
		heartbeat:  buildHeartbeatFrame(),
	}
}

// distribution.Member implementation.

// Lane returns the bounded lossless queue the hub pushes to.
func (c *Client) Lane() *distribution.Lane { return c.lane }

// SetOpen conflates the latest open candle for a scope.
func (c *Client) SetOpen(scope Scope, cnd candle.Candle) {
	c.mu.Lock()
	c.open[scope] = cnd
	c.mu.Unlock()
}

// DeleteOpen retracts a pending open whose bucket closed.
func (c *Client) DeleteOpen(scope Scope) {
	c.mu.Lock()
	delete(c.open, scope)
	c.mu.Unlock()
}

// SignalOpen wakes the writePump to flush conflated opens. Non-blocking.
func (c *Client) SignalOpen() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Start launches the connection's two pumps inside the actor's registration turn.
func (c *Client) Start(h *distribution.Hub) {
	h.Go(func() { c.readPump(h) })
	h.Go(func() { c.writePump(h) })
}

// Reject closes a connection the hub will never own, telling the client to go away so it
// reconnects instead of waiting out its read deadline.
func (c *Client) Reject() {
	_ = c.conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseGoingAway, ""),
		time.Now().Add(writeWait))
	_ = c.conn.Close()
}

// readPump is the SOLE reader. It parses (un)subscribe requests and forwards them to
// the actor (with a shutdown escape). Any read error unregisters the client. It never
// closes the connection — that is the writePump's job (single-owner FD close).
func (c *Client) readPump(h *distribution.Hub) {
	defer h.Unregister(c)
	c.conn.SetReadLimit(maxClientMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var req clientRequest
		if json.Unmarshal(data, &req) != nil {
			continue // ignore malformed client control frames
		}
		switch req.Action {
		case "subscribe":
			h.Subscribe(c, req.Scope)
		case "unsubscribe":
			h.Unsubscribe(c, req.Scope)
		}
	}
}

// writePump is the SOLE writer of data frames. It drains the lossless lane one
// frame per select iteration (so a large backlog never starves the ping), flushes the
// conflated opens on wake, and emits periodic pings. It is the SOLE closer of the FD.
func (c *Client) writePump(h *distribution.Hub) {
	ping := time.NewTicker(c.pingPeriod)
	var maxAge <-chan time.Time
	if c.connMaxAge > 0 {
		t := time.NewTimer(c.connMaxAge)
		defer t.Stop()
		maxAge = t.C // a nil channel (age disabled) blocks forever in select
	}
	defer func() {
		ping.Stop()
		_ = c.conn.Close()
	}()
	for {
		select {
		case frame, ok := <-c.lane.Recv():
			if !ok {
				c.writeClose()
				return
			}
			data, err := marshalEventFrame(frame)
			if err != nil {
				continue
			}
			if c.writeText(data) != nil {
				return
			}
		case <-c.wake:
			if c.flushOpen(h) != nil {
				return
			}
		case <-ping.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)) != nil {
				return
			}
			// Application-level liveness. The control ping above never reaches
			// a browser's JS: the network stack answers the pong for it, so a web client cannot
			// distinguish "connected and alive" from "connected and mute" and its chart freezes
			// while the status still reads open. This data frame is that same liveness, made
			// observable. It rides the ping tick deliberately — one clock, one goroutine, and
			// writePump stays the SOLE writer of frames. A failed write ends the pump
			// exactly like any other frame's, with no path of its own.
			if c.writeText(c.heartbeat) != nil {
				return
			}
		case <-maxAge:
			// Bounded lifetime: close so the client reconnects with a fresh ticket.
			c.writeClose()
			return
		}
	}
}

// flushOpen swaps out the pending conflated opens under mu (no I/O under the
// lock), then marshals and writes each. Bounded by the number of subscribed scopes.
func (c *Client) flushOpen(h *distribution.Hub) error {
	c.mu.Lock()
	batch := c.open
	c.open = make(map[Scope]candle.Candle)
	c.mu.Unlock()
	for scope, cnd := range batch {
		env := NewEnvelope(EventCandleOpenUpdate, scope, cnd, 0, h.Now()) // Seq unused for opens
		data, err := env.Marshal()
		if err != nil {
			continue
		}
		if err := c.writeText(data); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) writeText(data []byte) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return c.conn.WriteMessage(websocket.TextMessage, data)
}

func (c *Client) writeClose() {
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	_ = c.conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseGoingAway, ""),
		time.Now().Add(writeWait))
}

func marshalEventFrame(f distribution.Frame) ([]byte, error) {
	if f.Snapshot != nil {
		var open *CandleDTO
		if f.Snapshot.Open != nil {
			dto := NewCandleDTO(*f.Snapshot.Open)
			open = &dto
		}
		return json.Marshal(snapshotWire{
			Version:      SchemaVersion,
			Type:         "snapshot",
			Scope:        f.Snapshot.Scope,
			Baseline:     f.Snapshot.Baseline,
			BaselineTime: f.Snapshot.BaselineTime,
			Open:         open,
		})
	}
	return f.Envelope.Marshal()
}

var _ distribution.Member = (*Client)(nil)
