// Package binancews owns the Binance WebSocket transport used by GoWhale.
//
// The upstream connector hardcodes 16 MiB read and write buffers per socket and
// dispatches every callback on a new goroutine. Both properties were measured as
// production defects. This connection keeps bounded buffers and a single reader,
// so frames reach the adapter in arrival order.
package binancews

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ReadBufferSize and WriteBufferSize size the WebSocket framing buffers; readLimit caps
// one inbound frame.
const (
	ReadBufferSize  = 256 * 1024
	WriteBufferSize = 256 * 1024
	readLimit       = 64 * 1024 * 1024
	handshakeWait   = 45 * time.Second
	writeWait       = 10 * time.Second
)

const defaultRenewalInterval = 23 * time.Hour

// ErrClosed, ErrNotConnected and ErrHandlerRequired name connection-state failures.
var (
	ErrClosed          = errors.New("binancews: connection closed")
	ErrNotConnected    = errors.New("binancews: connection not established")
	ErrHandlerRequired = errors.New("binancews: handler required")
)

// Handler receives the `data` object of one combined-stream frame. It runs on
// the connection's single reader goroutine; handlers must not block on another
// frame or call Close on their own connection.
type Handler func(data []byte)

type envelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
	ID     *int64          `json:"id,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	} `json:"error,omitempty"`
}

// Connection is one Binance WebSocket connection. Reconnection is owned by the
// caller, which receives the terminal error through Errors and decides when to
// dial a replacement.
type Connection struct {
	ID      int
	BaseURL string

	// route is the venue's stream-class prefix: /public, /market or /private.
	// Empty keeps the legacy unrouted path, which Binance USD-M no longer serves
	// for market-class streams.
	route string

	// readIdle bounds one silent read. Zero means the venue documents no idle
	// deadline; the transport must not invent one.
	readIdle time.Duration

	// RenewalInterval closes this connection after the configured lifetime. A
	// non-positive value disables scheduled renewal.
	RenewalInterval time.Duration

	dialer *websocket.Dialer

	ctx    context.Context
	cancel context.CancelFunc

	connMu    sync.RWMutex
	conn      *websocket.Conn
	connected atomic.Bool
	closed    atomic.Bool
	closeOnce sync.Once

	writeMu sync.Mutex
	nextID  atomic.Int64

	handlersMu sync.RWMutex
	handlers   map[string]Handler

	lastMessageAt atomic.Int64

	done  chan struct{}
	errCh chan error
	wg    sync.WaitGroup
}

// Option adjusts a connection before it dials.
type Option func(*Connection)

// WithReadIdle sets the documented idle read deadline. Zero disables it.
func WithReadIdle(idle time.Duration) Option {
	return func(c *Connection) { c.readIdle = idle }
}

// WithRoute selects the venue's stream-class prefix, such as /market for kline
// streams. An empty route keeps the legacy path.
func WithRoute(route string) Option {
	return func(c *Connection) {
		route = strings.TrimSpace(route)
		route = strings.TrimSuffix(route, "/")
		if route != "" && !strings.HasPrefix(route, "/") {
			route = "/" + route
		}
		c.route = route
	}
}

// NewConnection creates one combined-stream connection; Connect dials it and installs handlers.
func NewConnection(id int, baseURL string, opts ...Option) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &Connection{
		ID:              id,
		BaseURL:         baseURL,
		RenewalInterval: defaultRenewalInterval,
		dialer: &websocket.Dialer{
			ReadBufferSize:   ReadBufferSize,
			WriteBufferSize:  WriteBufferSize,
			HandshakeTimeout: handshakeWait,
		},
		ctx:      ctx,
		cancel:   cancel,
		handlers: make(map[string]Handler),
		done:     make(chan struct{}),
		errCh:    make(chan error, 1),
	}
	for _, opt := range opts {
		opt(conn)
	}
	return conn
}

// extendReadDeadline pushes the idle deadline forward when one is configured.
func (c *Connection) extendReadDeadline(conn *websocket.Conn) {
	if c.readIdle <= 0 {
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(c.readIdle))
}

// closeHandshakeResponse releases the HTTP response that gorilla returns with every
// websocket handshake. It is non-nil on success and on ErrBadHandshake, and the library
// never closes it, so both paths own a body that must be released.
func closeHandshakeResponse(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}

// Connect dials a combined-stream URL and installs every handler before the
// reader starts. Installing handlers first closes the window in which the venue
// could deliver a frame that has no route yet.
func (c *Connection) Connect(ctx context.Context, handlers map[string]Handler) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if c.connected.Load() {
		return nil
	}

	c.handlersMu.Lock()
	for stream, handler := range handlers {
		if handler == nil {
			c.handlersMu.Unlock()
			return fmt.Errorf("%w for stream %s", ErrHandlerRequired, stream)
		}
		c.handlers[stream] = handler
	}
	c.handlersMu.Unlock()

	target, err := c.urlFor(handlers)
	if err != nil {
		return err
	}
	conn, resp, err := c.dialer.DialContext(ctx, target, nil)
	closeHandshakeResponse(resp)
	if err != nil {
		return fmt.Errorf("binancews: dial %s: %w", target, err)
	}

	conn.SetReadLimit(readLimit)
	c.extendReadDeadline(conn)
	conn.SetPingHandler(func(payload string) error {
		c.extendReadDeadline(conn)
		if err := conn.WriteControl(websocket.PongMessage, []byte(payload), time.Now().Add(writeWait)); err != nil {
			return err
		}
		return nil
	})

	c.connMu.Lock()
	if c.closed.Load() {
		c.connMu.Unlock()
		_ = conn.Close()
		return ErrClosed
	}
	c.conn = conn
	c.connected.Store(true)
	c.lastMessageAt.Store(time.Now().UnixMilli())
	c.connMu.Unlock()

	c.wg.Add(1)
	go c.readLoop()

	if c.RenewalInterval > 0 {
		c.wg.Add(1)
		go c.renewalLoop(c.RenewalInterval)
	}
	return nil
}

func (c *Connection) urlFor(handlers map[string]Handler) (string, error) {
	if c.BaseURL == "" {
		return "", errors.New("binancews: empty base URL")
	}
	base := c.BaseURL + c.route
	if len(handlers) == 0 {
		return base + "/ws", nil
	}
	streams := make([]string, 0, len(handlers))
	for stream := range handlers {
		streams = append(streams, stream)
	}
	// Map iteration is deliberately not part of the protocol contract; the
	// adapter supplies stream names that are order-independent to the venue.
	return base + "/stream?streams=" + joinStreams(streams), nil
}

func joinStreams(streams []string) string {
	out := ""
	for i, stream := range streams {
		if i > 0 {
			out += "/"
		}
		out += stream
	}
	return out
}

// Subscribe registers a handler before asking the venue to add the stream, then
// updates the subscription on the live socket.
func (c *Connection) Subscribe(ctx context.Context, stream string, handler Handler) error {
	if handler == nil {
		return ErrHandlerRequired
	}
	if c.closed.Load() || !c.connected.Load() {
		return ErrNotConnected
	}

	c.handlersMu.Lock()
	c.handlers[stream] = handler
	c.handlersMu.Unlock()

	if err := c.send(ctx, "SUBSCRIBE", stream); err != nil {
		c.handlersMu.Lock()
		delete(c.handlers, stream)
		c.handlersMu.Unlock()
		return err
	}
	return nil
}

// Unsubscribe removes a stream handler without sending a SUBSCRIBE frame.
func (c *Connection) Unsubscribe(ctx context.Context, stream string) error {
	c.handlersMu.Lock()
	delete(c.handlers, stream)
	c.handlersMu.Unlock()
	if c.closed.Load() || !c.connected.Load() {
		return nil
	}
	return c.send(ctx, "UNSUBSCRIBE", stream)
}

func (c *Connection) send(ctx context.Context, method, stream string) error {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()
	if conn == nil {
		return ErrNotConnected
	}
	request := struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
		ID     int64    `json:"id"`
	}{
		Method: method,
		Params: []string{stream},
		ID:     c.nextID.Add(1),
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
		done <- conn.WriteMessage(websocket.TextMessage, payload)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ctx.Done():
		return ErrClosed
	case err := <-done:
		return err
	}
}

func (c *Connection) readLoop() {
	defer c.wg.Done()
	for {
		_, payload, err := c.conn.ReadMessage()
		if err != nil {
			c.stop(fmt.Errorf("binancews: read: %w", err))
			return
		}
		c.extendReadDeadline(c.conn)
		c.lastMessageAt.Store(time.Now().UnixMilli())
		c.dispatch(payload)
	}
}

// rawEvent is the minimal shape needed to route a frame from a socket that was
// dialed without a combined-stream URL and subscribed later. Binance sends
// those frames unwrapped: there is no `stream` field to route by.
type rawEvent struct {
	Event  string `json:"e"`
	Symbol string `json:"s"`
	Kline  *struct {
		Interval string `json:"i"`
	} `json:"k"`
}

func (c *Connection) dispatch(payload []byte) {
	var frame envelope
	if err := json.Unmarshal(payload, &frame); err != nil {
		c.report(fmt.Errorf("binancews: malformed frame: %w", err))
		return
	}
	if frame.Stream == "" {
		if frame.Error != nil {
			c.report(fmt.Errorf("binancews: venue error %d: %s", frame.Error.Code, frame.Error.Msg))
			return
		}
		c.dispatchRaw(payload)
		return
	}

	c.dispatchTo(frame.Stream, frame.Data)
}

// dispatchRaw routes an unwrapped event to the handler registered for its
// stream name. The dynamic-subscribe path opens a plain `/ws` socket, so every
// event arrives as a raw payload; dropping it because it lacks `stream` makes
// the connection look healthy while no trade ever reaches the aggregator.
func (c *Connection) dispatchRaw(payload []byte) {
	var event rawEvent
	if err := json.Unmarshal(payload, &event); err != nil || event.Symbol == "" {
		return
	}

	var stream string
	switch event.Event {
	case "aggTrade":
		stream = strings.ToLower(event.Symbol) + "@aggTrade"
	case "trade":
		stream = strings.ToLower(event.Symbol) + "@trade"
	case "kline":
		if event.Kline == nil {
			return
		}
		stream = strings.ToLower(event.Symbol) + "@kline_" + event.Kline.Interval
	default:
		return
	}
	c.dispatchTo(stream, payload)
}

func (c *Connection) dispatchTo(stream string, data []byte) {
	c.handlersMu.RLock()
	handler := c.handlers[stream]
	c.handlersMu.RUnlock()
	if handler == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			c.report(fmt.Errorf("binancews: handler panic on %s: %v", stream, recovered))
		}
	}()
	handler(data)
}

func (c *Connection) renewalLoop(interval time.Duration) {
	defer c.wg.Done()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-c.ctx.Done():
		return
	case <-timer.C:
		c.stop(errors.New("binancews: scheduled connection renewal"))
	}
}

func (c *Connection) report(err error) {
	select {
	case c.errCh <- err:
	default:
	}
}

func (c *Connection) stop(err error) {
	c.closeOnce.Do(func() {
		c.connected.Store(false)
		c.closed.Store(true)
		c.cancel()
		c.connMu.Lock()
		conn := c.conn
		c.connMu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		if err != nil {
			c.report(err)
		}
		close(c.done)
	})
}

// Close stops the reader and waits for every connection goroutine to exit.
func (c *Connection) Close() error {
	c.stop(nil)
	c.wg.Wait()
	return nil
}

// Done is closed when the connection stops for any reason.
func (c *Connection) Done() <-chan struct{} {
	return c.done
}

// Errors carries terminal connection errors to the owner.
func (c *Connection) Errors() <-chan error {
	return c.errCh
}

// Healthy reports whether the socket is connected and not closed.
func (c *Connection) Healthy() bool {
	return c.connected.Load() && !c.closed.Load()
}

// LastMessageAt returns when the last frame arrived, or the zero time.
func (c *Connection) LastMessageAt() time.Time {
	ms := c.lastMessageAt.Load()
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}
