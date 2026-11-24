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
	"net"
	"net/http"
	"net/url"
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
	// dialWait bounds the TCP connect of a socket without pinned edges. Gorilla's default
	// dialer is net.Dial with NO timeout, so a blackholed edge can stall the dial for the
	// kernel's SYN retries (~2 min) — measured 2026-11-24: the kline watchdog was delayed
	// 89 s behind a dial like this. The pinned path already bounds each edge.
	dialWait = 10 * time.Second
)

const defaultRenewalInterval = 23 * time.Hour

// ErrClosed, ErrNotConnected and ErrHandlerRequired name connection-state failures.
var (
	ErrClosed          = errors.New("binancews: connection closed")
	ErrNotConnected    = errors.New("binancews: connection not established")
	ErrHandlerRequired = errors.New("binancews: handler required")
)

// WHO closed the socket decides what the close MEANS. A renewal or an explicit shutdown is ours;
// counting either as an exchange fault is how "the exchange failed" gets asserted without proof.
const (
	InitiatorRemote        = "remote"
	InitiatorLocalShutdown = "local_shutdown"
	InitiatorLocalRenewal  = "local_renewal"
)

// CloseEvidence is what the transport retained about why a connection ended. Code is zero when the
// peer never sent a close frame — which is itself the fact worth recording: 1006 is synthesized
// locally and proves nothing about the venue.
type CloseEvidence struct {
	Code      int
	Reason    string
	Initiator string
	// DialAge is how long the connection had been established when it ended. It separates a
	// venue that drops a long-lived socket from a network that cannot keep one.
	DialAge time.Duration
	// Streams is how many streams the closed connection carried. The failure rate the owner
	// needs is per stream, not per connection.
	Streams int
	// SisterLastFrameAge is stamped by the partitioned owner: the age of the freshest sibling
	// partition's last frame. Zero means no sister (or none observed).
	SisterLastFrameAge time.Duration
	// Component names the owner of this socket ("trade", "kline", "spot"). A close is only
	// attributable when the log says which transport lost it.
	Component string
	// RemoteAddr is the peer address the socket was actually dialed to (IP:port), so a close
	// can be assigned to an edge without a packet capture.
	RemoteAddr string
	// LastFrameAge is how long THIS socket had been silent when it ended. It separates a peer
	// close on a live stream from a half-open socket the read deadline caught.
	LastFrameAge time.Duration
	// Pings and PongErrors count venue pings answered and pong writes that failed. A silent
	// socket with pong errors names the write path; a silent socket with zero pings names the
	// read path.
	Pings      int64
	PongErrors int64
}

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

	// pinnedEdges are optional host:port endpoints tried before the venue's
	// DNS. A resolver answer that lands on a flapping edge would otherwise pin
	// every reconnect to it. Empty means plain DNS.
	pinnedEdges []string
	edgeCursor  atomic.Uint64

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
	// dialedAtMs is when the socket was established; the evidence reports its age at close.
	dialedAtMs atomic.Int64
	// streams is the number of streams the connection carried, fixed at Connect.
	streams atomic.Int32

	// component names the owner of this socket; it is retained in the close evidence.
	component string
	// remoteAddr is the peer actually dialed (IP:port), retained at Connect.
	remoteAddr atomic.Value // string
	// pings/pongErrors count venue pings and failed pong writes for the close evidence.
	pings      atomic.Int64
	pongErrors atomic.Int64

	done  chan struct{}
	errCh chan error
	wg    sync.WaitGroup

	// The evidence survives the close: nothing retained it before, and the terminal error was
	// dropped when the channel was full, so a 1006 could not be attributed.
	evidenceMu sync.Mutex
	evidence   CloseEvidence
	lastErrMu  sync.Mutex
	lastErr    error
	dropped    atomic.Int64
}

// Option adjusts a connection before it dials.
type Option func(*Connection)

// WithReadIdle sets the documented idle read deadline. Zero disables it.
func WithReadIdle(idle time.Duration) Option {
	return func(c *Connection) { c.readIdle = idle }
}

// WithComponent labels the socket's owner so a close is attributable by transport.
func WithComponent(name string) Option {
	return func(c *Connection) { c.component = strings.TrimSpace(name) }
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

// WithPinnedEdges pins the TCP dial to these host:port endpoints, in rotation,
// before falling back to the URL host. The TLS SNI and Host header remain the
// URL's, so the venue's certificate still matches.
func WithPinnedEdges(edges []string) Option {
	return func(c *Connection) {
		c.pinnedEdges = c.pinnedEdges[:0]
		for _, edge := range edges {
			if edge = strings.TrimSpace(edge); edge != "" {
				c.pinnedEdges = append(c.pinnedEdges, edge)
			}
		}
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
	if len(conn.pinnedEdges) > 0 {
		conn.dialer.NetDialContext = conn.pinnedDialContext
	} else {
		conn.dialer.NetDialContext = (&net.Dialer{
			Timeout:   dialWait,
			KeepAlive: 15 * time.Second,
		}).DialContext
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

// pinnedDialContext tries the pinned edges in rotation, then falls back to the
// URL host. The rotation starts at the connection id so partitions fan out
// instead of all taking the first edge.
func (c *Connection) pinnedDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	port := "443"
	if _, parsed, err := net.SplitHostPort(addr); err == nil {
		port = parsed
	}
	edges := c.pinnedEdges
	if len(edges) == 0 {
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
	}
	start := 0
	if c.ID != 0 {
		start = c.ID % len(edges)
	}
	offset := int(c.edgeCursor.Add(1) - 1)
	var lastErr error
	for i := 0; i < len(edges); i++ {
		edge := edges[(start+offset+i)%len(edges)]
		host, edgePort, err := net.SplitHostPort(edge)
		if err != nil {
			host, edgePort = edge, port
		}
		conn, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, net.JoinHostPort(host, edgePort))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
	if err == nil {
		return conn, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, err
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

	c.handlersMu.RLock()
	streamCount := len(c.handlers)
	c.handlersMu.RUnlock()

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
		c.pings.Add(1)
		c.extendReadDeadline(conn)
		if err := conn.WriteControl(websocket.PongMessage, []byte(payload), time.Now().Add(writeWait)); err != nil {
			c.pongErrors.Add(1)
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
	if remote := conn.UnderlyingConn().RemoteAddr(); remote != nil {
		c.remoteAddr.Store(remote.String())
	}
	nowMs := time.Now().UnixMilli()
	c.lastMessageAt.Store(nowMs)
	c.dialedAtMs.Store(nowMs)
	// The stream count is bounded by maxStreamsPerConnection (URL budget), far below MaxInt32.
	c.streams.Store(int32(streamCount)) //nolint:gosec // G115: bounded by maxStreamsPerConnection.
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
		out += encodeStreamName(stream)
	}
	return out
}

// encodeStreamName percent-encodes the stream name for the combined-stream query. The
// venue publishes symbols that are not ASCII (measured 2026-11-24: 币安人生USDT), and a raw
// UTF-8 request target is not valid; the "@" stays literal because it is the documented
// symbol/channel separator, and the venue decodes each name after splitting on "/".
func encodeStreamName(stream string) string {
	return strings.ReplaceAll(url.QueryEscape(stream), "%40", "@")
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
			c.stopWith(InitiatorRemote, fmt.Errorf("binancews: read: %w", err))
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
		c.stopWith(InitiatorLocalRenewal, errors.New("binancews: scheduled connection renewal"))
	}
}

// report records the terminal error and hands it to the watcher.
//
// The retained copy is not decoration: the channel holds ONE error, and a second one used to be
// discarded in silence, leaving a reconnect with no cause.
func (c *Connection) report(err error) {
	if err == nil {
		return
	}
	c.lastErrMu.Lock()
	c.lastErr = err
	c.lastErrMu.Unlock()
	select {
	case c.errCh <- err:
	default:
		c.dropped.Add(1)
	}
}

// CloseEvidence reports why the connection ended. Zero before it stops.
func (c *Connection) CloseEvidence() CloseEvidence {
	c.evidenceMu.Lock()
	defer c.evidenceMu.Unlock()
	return c.evidence
}

// SetSisterLastFrameAge stamps the partition's sibling health into the retained evidence. The
// transport cannot know its siblings; the partitioned owner is the only one that can.
func (c *Connection) SetSisterLastFrameAge(age time.Duration) {
	c.evidenceMu.Lock()
	c.evidence.SisterLastFrameAge = age
	c.evidenceMu.Unlock()
}

// TerminalError reports the last error the connection produced, retained even when the watcher
// channel was full.
func (c *Connection) TerminalError() error {
	c.lastErrMu.Lock()
	defer c.lastErrMu.Unlock()
	return c.lastErr
}

// DroppedReports counts terminal errors that did not reach the watcher channel.
func (c *Connection) DroppedReports() int64 { return c.dropped.Load() }

// closeCodeOf extracts the venue's close code, when the error carries one.
func closeCodeOf(err error) (int, string) {
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return closeErr.Code, closeErr.Text
	}
	return 0, ""
}

// stopWith records the evidence BEFORE closing the socket: the close that follows must not erase
// the code the venue already sent.
func (c *Connection) stopWith(initiator string, err error) {
	c.closeOnce.Do(func() {
		code, reason := closeCodeOf(err)
		dialAge := time.Duration(0)
		if dialedAt := c.dialedAtMs.Load(); dialedAt > 0 {
			dialAge = time.Since(time.UnixMilli(dialedAt))
		}
		lastFrameAge := time.Duration(0)
		if last := c.lastMessageAt.Load(); last > 0 {
			lastFrameAge = time.Since(time.UnixMilli(last))
		}
		remoteAddr, _ := c.remoteAddr.Load().(string)
		c.evidenceMu.Lock()
		c.evidence = CloseEvidence{
			Code:         code,
			Reason:       reason,
			Initiator:    initiator,
			DialAge:      dialAge,
			Streams:      int(c.streams.Load()),
			Component:    c.component,
			RemoteAddr:   remoteAddr,
			LastFrameAge: lastFrameAge,
			Pings:        c.pings.Load(),
			PongErrors:   c.pongErrors.Load(),
		}
		c.evidenceMu.Unlock()
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
	c.stopWith(InitiatorLocalShutdown, nil)
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
