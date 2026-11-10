package distribution

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// Hub concurrency model (see openspec/changes/realtime-push-propagation/
// hub-concurrency-design.md): a single-owner actor (run) owns ALL registry state
// with zero locks, plus one decoupled poller. Foreign locks and JSON marshaling stay
// OUT of the actor. The client-facing per-scope sequence is stamped here by the
// hub, independent of the producer's per-channel Redis sequence, so a web client can
// detect gaps within its own subscription.
//
// The transport is injected through Member. The hub never touches a socket, a ticket or
// a WebSocket connection: it owns membership, subscription bookkeeping, fan-out order,
// per-scope sequencing, the bounded lossless lanes and open-candle conflation. That is
// exactly the machinery a second consumer (or a test) can drive without a network.
const (
	// PollInterval is the open-candle poll cadence. It is exported because the realtime
	// edge's integration tests assert response timing against it.
	PollInterval = 2 * time.Second

	cmdBuf    = 64   // control plane
	ingestBuf = 4096 // sized to the worst-case fan-in of simultaneous closes

	// Defaults bound a hostile or misconfigured edge; both are overridable at construction.
	defaultMaxMembers       = 4096
	defaultMaxSubscriptions = 128
)

// HubOption configures resource limits at construction.
type HubOption func(*Hub)

// WithMaxMembers caps the number of registered members. A member beyond the cap is
// rejected before any goroutine starts.
func WithMaxMembers(n int) HubOption {
	return func(h *Hub) {
		if n > 0 {
			h.maxMembers = n
		}
	}
}

// WithMaxSubscriptions caps the number of scopes one member may subscribe to. A member
// that exceeds it is removed and its connection closed.
func WithMaxSubscriptions(n int) HubOption {
	return func(h *Hub) {
		if n > 0 {
			h.maxSubscriptions = n
		}
	}
}

// ErrHubStopped reports that an operation that needs the actor could not reach it
// because the hub has already stopped.
var ErrHubStopped = errors.New("distribution: hub stopped")

// OpenCandleSource is the in-process source of the merged open candle. It is satisfied
// by *engine.CandleEngine and is called ONLY from the poller (never inside the actor).
type OpenCandleSource interface {
	PeekLatestCandle(exchange, marketType, symbol, tf string) (candle.Candle, bool)
}

// Member is one connection registered with the Hub. The hub owns the member's
// membership and subscription bookkeeping; the member owns its transport, its lossless
// lane and its conflated-open slot. Implementations must be safe for the actor to call
// concurrently with their own writer (SetOpen/DeleteOpen/SignalOpen are the shared
// conflation seam).
type Member interface {
	// Lane returns the bounded lossless queue the hub pushes to. The hub is the sole
	// sender and closer; the member's writer is the sole receiver.
	Lane() *Lane
	// SetOpen publishes the latest open candle for scope. Overwrite is conflation:
	// only the newest value per scope is ever delivered.
	SetOpen(scope Scope, c candle.Candle)
	// DeleteOpen discards a pending open for scope (delete-on-close): an open never
	// survives its own bucket's close.
	DeleteOpen(scope Scope)
	// SignalOpen wakes the member's writer to flush conflated opens. It must not block.
	SignalOpen()
	// Start is called inside the actor turn that registers the member. It must start
	// the transport's goroutines through h.Go and return immediately.
	Start(h *Hub)
	// Reject closes a connection the hub refuses to own (it is shutting down), telling
	// the peer to reconnect elsewhere instead of waiting out a read deadline.
	Reject()
}

type cmdKind uint8

const (
	cmdRegister cmdKind = iota
	cmdUnregister
	cmdSubscribe
	cmdUnsubscribe
)

type command struct {
	kind  cmdKind
	m     Member
	scope Scope
}

// ingestMsg is a raw close/revised event fed to the hub by the engine hook. The hub
// stamps the client-facing sequence and timestamp; the producer's Redis sequence is
// separate.
type ingestMsg struct {
	typ    EventType
	scope  Scope
	candle candle.Candle
}

type scopedCandle struct {
	scope  Scope
	candle candle.Candle
}

// Stats is an actor-observed snapshot of registry sizes.
type Stats struct {
	Clients int
	Scopes  int
}

type parkRequest struct {
	ack     chan struct{}
	release chan struct{}
}

// Hub fans out candle events to subscribed members.
type Hub struct {
	opens OpenCandleSource
	clock func() int64 // ms epoch, injectable

	cmds             chan command
	ingest           chan ingestMsg
	openBatch        chan []scopedCandle
	scopeReq         chan chan []Scope
	statsReq         chan chan Stats
	parkReq          chan parkRequest
	done             chan struct{}
	finished         chan struct{}
	maxMembers       int
	maxSubscriptions int

	// State PRIVATE to run(); no mutex — the actor is the sole owner.
	members        map[Member]struct{}
	index          map[Scope]map[Member]struct{}
	subs           map[Member]map[Scope]struct{}
	closeSeq       map[Scope]uint64
	lastOpenLU     map[Scope]int64
	lastOpenCandle map[Scope]candle.Candle
	lastClosedTs   map[Scope]int64

	connWg sync.WaitGroup // transport goroutines started via Go() inside the actor
	pollWg sync.WaitGroup
	stop   sync.Once
}

// NewHub builds a hub. opens supplies the merged open candle (in-process); clock
// returns ms epoch (time.Now().UnixMilli in production).
func NewHub(opens OpenCandleSource, clock func() int64, opts ...HubOption) *Hub {
	h := &Hub{
		opens:            opens,
		clock:            clock,
		cmds:             make(chan command, cmdBuf),
		ingest:           make(chan ingestMsg, ingestBuf),
		openBatch:        make(chan []scopedCandle, 1),
		scopeReq:         make(chan chan []Scope),
		statsReq:         make(chan chan Stats),
		parkReq:          make(chan parkRequest),
		done:             make(chan struct{}),
		finished:         make(chan struct{}),
		maxMembers:       defaultMaxMembers,
		maxSubscriptions: defaultMaxSubscriptions,
		members:          make(map[Member]struct{}),
		index:            make(map[Scope]map[Member]struct{}),
		subs:             make(map[Member]map[Scope]struct{}),
		closeSeq:         make(map[Scope]uint64),
		lastOpenLU:       make(map[Scope]int64),
		lastOpenCandle:   make(map[Scope]candle.Candle),
		lastClosedTs:     make(map[Scope]int64),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Start launches the actor and the poller.
func (h *Hub) Start() {
	go h.run()
	h.pollWg.Add(1)
	go h.pollLoop()
}

// Stop signals shutdown and waits, bounded by ctx, for the actor to drain every
// connection. It is safe to call multiple times.
func (h *Hub) Stop(ctx context.Context) {
	h.stop.Do(func() { close(h.done) })
	select {
	case <-h.finished:
	case <-ctx.Done():
	}
}

// Done returns the shutdown signal channel. It closes exactly once, on the first Stop.
func (h *Hub) Done() <-chan struct{} { return h.done }

// Finished returns the channel closed when the actor has drained and returned.
func (h *Hub) Finished() <-chan struct{} { return h.finished }

// Now returns the injected clock value in ms epoch.
func (h *Hub) Now() int64 { return h.clock() }

// IngestClose feeds a closed bucket to the hub (called by the engine hook, off-actor).
// Never blocks past shutdown.
func (h *Hub) IngestClose(scope Scope, candle candle.Candle) {
	h.ingestEvent(EventCandleClose, scope, candle)
}

// IngestRevised feeds a gap-recovery correction of an already-closed bucket.
func (h *Hub) IngestRevised(scope Scope, candle candle.Candle) {
	h.ingestEvent(EventCandleRevised, scope, candle)
}

func (h *Hub) ingestEvent(typ EventType, scope Scope, candle candle.Candle) {
	select {
	case h.ingest <- ingestMsg{typ: typ, scope: scope, candle: candle}:
	case <-h.done:
	}
}

// Register registers a member (called by the transport after a successful handshake).
// Rejected cleanly if the hub is shutting down — no goroutine is ever started for a
// rejected connection.
func (h *Hub) Register(m Member) {
	// The shutdown check MUST be its own select, ahead of the send. cmds is BUFFERED, so
	// during shutdown both cases of a two-case select are ready and Go picks one
	// uniformly at random: roughly half the registers would land in a buffer that run()
	// no longer reads, leaving the connection owned by nobody — never closed, and the
	// peer never told to go away.
	select {
	case <-h.done:
		m.Reject()
		return
	default:
	}
	select {
	case h.cmds <- command{kind: cmdRegister, m: m}:
	case <-h.done:
		m.Reject()
	}
}

// Unregister asks the actor to remove a member. Safe after shutdown.
func (h *Hub) Unregister(m Member) {
	h.send(command{kind: cmdUnregister, m: m})
}

// Subscribe asks the actor to attach a member to a scope.
func (h *Hub) Subscribe(m Member, scope Scope) {
	h.send(command{kind: cmdSubscribe, m: m, scope: scope})
}

// Unsubscribe asks the actor to detach a member from a scope.
func (h *Hub) Unsubscribe(m Member, scope Scope) {
	h.send(command{kind: cmdUnsubscribe, m: m, scope: scope})
}

// send routes a control command from a transport goroutine to the actor, with a
// shutdown escape so no goroutine leaks when the hub stops.
func (h *Hub) send(cmd command) {
	select {
	case h.cmds <- cmd:
	case <-h.done:
	}
}

// Go tracks and starts one transport goroutine. It must be called from inside the actor
// (normally from Member.Start) so its Add can never race the shutdown Wait.
func (h *Hub) Go(fn func()) {
	h.connWg.Add(1)
	go func() {
		defer h.connWg.Done()
		fn()
	}()
}

func (h *Hub) run() {
	defer close(h.finished)
	for {
		select {
		case cmd := <-h.cmds:
			h.handleCommand(cmd)
		case msg := <-h.ingest:
			h.fanout(msg)
		case batch := <-h.openBatch:
			h.handleOpenBatch(batch)
		case reply := <-h.scopeReq:
			h.replyScopes(reply)
		case reply := <-h.statsReq:
			reply <- Stats{Clients: len(h.members), Scopes: len(h.index)}
		case req := <-h.parkReq:
			close(req.ack)
			select {
			case <-req.release:
			case <-h.done:
			}
		case <-h.done:
			h.drainShutdown()
			return
		}
	}
}

func (h *Hub) handleCommand(cmd command) {
	switch cmd.kind {
	case cmdRegister:
		if len(h.members) >= h.maxMembers {
			cmd.m.Reject()
			return
		}
		// Add+start are atomic inside the actor, so they never race connWg.Wait.
		h.members[cmd.m] = struct{}{}
		h.subs[cmd.m] = make(map[Scope]struct{})
		cmd.m.Start(h)
	case cmdUnregister:
		h.removeClient(cmd.m)
	case cmdSubscribe:
		h.doSubscribe(cmd.m, cmd.scope)
	case cmdUnsubscribe:
		h.doUnsubscribe(cmd.m, cmd.scope)
	}
}

// fanout delivers a close/revised event to every subscriber of its scope, stamping a
// per-scope monotonic sequence for client-side gap detection. A member whose lossless
// lane is full is evicted (applied AFTER the range, never mutating index mid-iter).
func (h *Hub) fanout(msg ingestMsg) {
	scope := msg.scope
	h.closeSeq[scope]++
	// MONOTONIC, not last-write. IngestRevised corrects an OLDER bucket, and this map is the
	// seam anchor the client compares its history window against: an anchor that walks
	// backwards makes a perfectly good window look stale and forces a refetch that fixes nothing.
	if msg.candle.Timestamp > h.lastClosedTs[scope] {
		h.lastClosedTs[scope] = msg.candle.Timestamp
	}
	env := NewEnvelope(msg.typ, scope, msg.candle, h.closeSeq[scope], h.clock())

	var victims []Member
	for m := range h.index[scope] {
		m.DeleteOpen(scope) // delete-on-close (INV-5): no open survives its bucket's close
		if !m.Lane().Push(Frame{Envelope: env}) {
			victims = append(victims, m)
		}
	}
	for _, m := range victims {
		h.removeClient(m)
	}
}

// removeClient is idempotent, garbage-collects now-empty scope maps, and is the SOLE
// closer of a member's lane (guarded by membership => impossible to send-on-closed).
func (h *Hub) removeClient(m Member) {
	if _, ok := h.members[m]; !ok {
		return
	}
	for scope := range h.subs[m] {
		h.detach(m, scope)
	}
	delete(h.subs, m)
	delete(h.members, m)
	m.Lane().Close()
}

// detach removes a member from one scope's index and garbage-collects the per-scope
// state once the last subscriber leaves.
func (h *Hub) detach(m Member, scope Scope) {
	delete(h.subs[m], scope)
	set := h.index[scope]
	if set == nil {
		return
	}
	delete(set, m)
	if len(set) == 0 {
		delete(h.index, scope)
		delete(h.closeSeq, scope)
		delete(h.lastOpenLU, scope)
		delete(h.lastOpenCandle, scope)
		delete(h.lastClosedTs, scope)
	}
}

// doSubscribe enqueues the snapshot BEFORE registering the member in the index, in the
// same actor turn as fanout, giving the total order [snapshot@B, delta@B+1, ...] with
// no gap/dup/reorder.
func (h *Hub) doSubscribe(m Member, scope Scope) {
	if _, ok := h.members[m]; !ok {
		return // membership guard: never touch an evicted member (anti send-on-closed)
	}
	if _, ok := h.subs[m][scope]; ok {
		return // idempotent: a second subscribe must not inject a stale second snapshot
	}
	if len(h.subs[m]) >= h.maxSubscriptions {
		h.removeClient(m)
		return
	}
	// The snapshot carries the seam anchor alongside the cursor.
	//
	// baseline is an EVENT counter: it increments on revisions too, it is deleted when
	// the last subscriber of a scope leaves, and nothing maps it to a bar. A client
	// cannot use it to ask "does my history window reach the live edge?". baselineTime
	// can, because bar identity in this system IS the timestamp.
	snap := &Snapshot{
		Scope:        scope,
		Baseline:     h.closeSeq[scope],
		BaselineTime: h.lastClosedTs[scope],
	}
	if cnd, ok := h.lastOpenCandle[scope]; ok {
		snap.Open = &cnd
	}
	if !m.Lane().Push(Frame{Snapshot: snap}) { // one frame, exempt from the backlog cap
		h.removeClient(m)
		return
	}
	if h.index[scope] == nil {
		h.index[scope] = make(map[Member]struct{})
	}
	h.index[scope][m] = struct{}{}
	h.subs[m][scope] = struct{}{}
}

func (h *Hub) doUnsubscribe(m Member, scope Scope) {
	if _, ok := h.members[m]; !ok {
		return
	}
	if _, ok := h.subs[m][scope]; !ok {
		return
	}
	h.detach(m, scope)
}

// handleOpenBatch applies conflated open-candle updates from the poller: non-regression
// + dedupe, then overwrite each subscriber's pending slot and wake it non-blockingly.
func (h *Hub) handleOpenBatch(batch []scopedCandle) {
	for _, sc := range batch {
		scope := sc.scope
		if sc.candle.Timestamp <= h.lastClosedTs[scope] {
			continue // non-regression (INV-5): never show an open at/behind its close
		}
		if sc.candle.LastUpdate == h.lastOpenLU[scope] {
			continue // dedupe: nothing changed since last poll
		}
		h.lastOpenLU[scope] = sc.candle.LastUpdate
		h.lastOpenCandle[scope] = sc.candle
		for m := range h.index[scope] {
			m.SetOpen(scope, sc.candle) // overwrite = conflation (latest-wins)
			m.SignalOpen()
		}
	}
}

func (h *Hub) replyScopes(reply chan []Scope) {
	scopes := make([]Scope, 0, len(h.index))
	for scope := range h.index {
		scopes = append(scopes, scope)
	}
	reply <- scopes
}

// pollLoop owns the tick. Each tick it asks the actor for the active scopes, peeks the
// engine OFF-actor (never holding ce.mu inside run), and ships only the changed opens.
func (h *Hub) pollLoop() {
	defer h.pollWg.Done()
	ticker := time.NewTicker(PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.PollOnce()
		case <-h.done:
			return
		}
	}
}

// PollOnce runs one open-candle poll tick. It is exported for deterministic tests and
// benchmarks that must not wait out pollInterval.
func (h *Hub) PollOnce() {
	reply := make(chan []Scope, 1)
	select {
	case h.scopeReq <- reply:
	case <-h.done:
		return
	}
	var scopes []Scope
	select {
	case scopes = <-reply:
	case <-h.done:
		return
	}
	if len(scopes) == 0 {
		return
	}
	batch := make([]scopedCandle, 0, len(scopes))
	for _, scope := range scopes {
		if cnd, ok := h.opens.PeekLatestCandle(scope.Exchange, scope.MarketType, scope.Symbol, scope.Timeframe); ok {
			batch = append(batch, scopedCandle{scope: scope, candle: cnd})
		}
	}
	if len(batch) == 0 {
		return
	}
	select {
	case h.openBatch <- batch:
	case <-h.done:
	}
}

// drainShutdown closes every member's lossless lane (triggering the transport writers to
// exit) and waits for the tracked goroutines, so goleak.VerifyNone is clean.
func (h *Hub) drainShutdown() {
	h.pollWg.Wait()
	for m := range h.members {
		delete(h.members, m)
		m.Lane().Close()
	}
	h.connWg.Wait()
}

// SnapshotStats returns actor-observed registry sizes. It is the race-free way for
// tests, health endpoints and benchmarks to read state the actor owns.
func (h *Hub) SnapshotStats() Stats {
	reply := make(chan Stats, 1)
	select {
	case h.statsReq <- reply:
	case <-h.done:
		return Stats{}
	}
	select {
	case s := <-reply:
		return s
	case <-h.done:
		return Stats{}
	}
}

// ActiveScopes returns the fan-out index keys as observed by the actor.
func (h *Hub) ActiveScopes() []Scope {
	reply := make(chan []Scope, 1)
	select {
	case h.scopeReq <- reply:
	case <-h.done:
		return nil
	}
	select {
	case scopes := <-reply:
		return scopes
	case <-h.done:
		return nil
	}
}

// PendingIngest reports how many ingest messages are still queued for the actor. len on
// a channel is safe to call concurrently; the value is a test/observability barrier, not
// a consistency guarantee on its own.
func (h *Hub) PendingIngest() int { return len(h.ingest) }

// NoopOpenBatch enqueues an empty open batch and returns once the actor has taken it.
// An empty batch is a genuine no-op (handleOpenBatch ranges over zero elements); the
// completion is the proof that everything the caller enqueued before it was dequeued.
func (h *Hub) NoopOpenBatch(ctx context.Context) error {
	select {
	case h.openBatch <- nil:
		return nil
	case <-h.done:
		return ErrHubStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ParkActor blocks the actor's command loop until the returned resume function is
// called. It exists so integration tests can prove a caller synchronizes on the actor
// instead of racing it; production never parks the hub.
func (h *Hub) ParkActor(ctx context.Context) (resume func(), err error) {
	release := make(chan struct{})
	ack := make(chan struct{})
	var once sync.Once
	resume = func() { once.Do(func() { close(release) }) }

	select {
	case h.parkReq <- parkRequest{ack: ack, release: release}:
	case <-h.done:
		return nil, ErrHubStopped
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-ack:
		return resume, nil
	case <-h.done:
		return nil, ErrHubStopped
	case <-ctx.Done():
		// The actor may already have accepted the request and be blocked on release.
		resume()
		return nil, ctx.Err()
	}
}
