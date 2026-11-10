package distribution

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// Actor-side benchmarks for the extracted machinery. They build the registry DIRECTLY
// (no actor goroutine, no transport) because the question here is the marginal cost of
// one fan-out delivery and one conflation tick on the single-owner actor, not the cost
// of a channel hand-off or a socket write. The end-to-end transport benchmarks stay in
// adapters/inbound/realtime.

const (
	benchEventsBuf = 2048
	drainEvery     = 512
)

func benchMatrix() []int {
	return []int{1, 10, 100}
}

type benchOpens struct {
	mu   sync.Mutex
	data map[Scope]candle.Candle
}

func newBenchOpens() *benchOpens { return &benchOpens{data: map[Scope]candle.Candle{}} }

func (f *benchOpens) PeekLatestCandle(ex, mt, sym, tf string) (candle.Candle, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.data[Scope{Exchange: ex, MarketType: mt, Symbol: sym, Timeframe: tf}]
	return c, ok
}

func benchScope(i int) Scope {
	return Scope{
		Exchange:   "binance",
		MarketType: "future",
		Symbol:     fmt.Sprintf("SYM%03dUSDT", i),
		Timeframe:  "4h",
	}
}

func closedCandle(ts int64, closePrice float64) candle.Candle {
	return candle.Candle{Timestamp: ts, LastUpdate: ts, Open: 1, High: 2, Low: 0.5, Close: closePrice, Volume: 10}
}

// benchMember is a transport-free Member. It counts nothing and retains nothing: the
// benchmark must price the actor, not an allocation sink.
type benchMember struct {
	lane *Lane

	mu   sync.Mutex
	open map[Scope]candle.Candle
	wake chan struct{}
}

func newBenchMember() *benchMember {
	return &benchMember{
		lane: NewLane(benchEventsBuf),
		open: make(map[Scope]candle.Candle),
		wake: make(chan struct{}, 1),
	}
}

func (m *benchMember) Lane() *Lane { return m.lane }

func (m *benchMember) SetOpen(scope Scope, c candle.Candle) {
	m.mu.Lock()
	m.open[scope] = c
	m.mu.Unlock()
}

func (m *benchMember) DeleteOpen(scope Scope) {
	m.mu.Lock()
	delete(m.open, scope)
	m.mu.Unlock()
}

func (m *benchMember) SignalOpen() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *benchMember) Start(*Hub) {}
func (m *benchMember) Reject()    {}

// benchRegistry populates the actor's maps directly. Safe because the actor is not
// running: this is the isolated actor-side measurement.
func benchRegistry(nClients, nScopes int) (*Hub, []*benchMember) {
	h := NewHub(newBenchOpens(), func() int64 { return 1 })
	members := make([]*benchMember, 0, nClients)
	for i := 0; i < nClients; i++ {
		m := newBenchMember()
		h.members[m] = struct{}{}
		h.subs[m] = make(map[Scope]struct{}, nScopes)
		for s := 0; s < nScopes; s++ {
			scope := benchScope(s)
			if h.index[scope] == nil {
				h.index[scope] = make(map[Member]struct{}, nClients)
			}
			h.index[scope][m] = struct{}{}
			h.subs[m][scope] = struct{}{}
		}
		members = append(members, m)
	}
	return h, members
}

// drainPending empties every member's lossless lane in the CALLER's goroutine and
// returns how many frames it removed. The return value is what makes the fan-out
// benchmark falsifiable: counting the frames that actually reached the lanes is the only
// way to prove the timed loop moved payload rather than a cheaper (or empty) path.
func drainPending(members []*benchMember) int {
	n := 0
	for _, m := range members {
		for more := true; more; {
			select {
			case <-m.lane.Recv():
				n++
			default:
				more = false
			}
		}
	}
	return n
}

// BenchmarkHubFanoutClose measures the actor's marginal cost of one candle.close event,
// which is the global ceiling: run() is single-owner, so every member of every scope is
// served from this one goroutine.
func BenchmarkHubFanoutClose(b *testing.B) {
	for _, nClients := range benchMatrix() {
		for _, nScopes := range benchMatrix() {
			b.Run(fmt.Sprintf("clients=%d/scopes=%d", nClients, nScopes), func(b *testing.B) {
				h, members := benchRegistry(nClients, nScopes)
				scope := benchScope(0)
				cnd := closedCandle(1_700_000_000_000, 42)
				want := len(h.members)
				pending := 0
				delivered := 0

				b.ReportAllocs()
				for b.Loop() {
					cnd.Timestamp++
					h.fanout(ingestMsg{typ: EventCandleClose, scope: scope, candle: cnd})
					pending++
					if pending == drainEvery {
						b.StopTimer()
						delivered += drainPending(members)
						pending = 0
						b.StartTimer()
					}
				}
				b.StopTimer()
				delivered += drainPending(members)

				if len(h.members) != want {
					b.Fatalf("members evicted during benchmark: %d of %d left", len(h.members), want)
				}
				if wantDelivered := b.N * nClients; delivered != wantDelivered {
					b.Fatalf("fanout delivered %d frames, want %d (one per member per iteration)", delivered, wantDelivered)
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*nClients), "ns/delivery")
			})
		}
	}
}

func openBatchFor(nScopes int) []scopedCandle {
	batch := make([]scopedCandle, nScopes)
	for i := range batch {
		batch[i] = scopedCandle{scope: benchScope(i), candle: closedCandle(1_700_000_000_000, 1)}
	}
	return batch
}

// BenchmarkOpenBatchApply measures the actor half of ONE open_update poll tick: the
// non-regression check, the latest-wins dedupe, and the per-member conflation slot write.
func BenchmarkOpenBatchApply(b *testing.B) {
	for _, nClients := range benchMatrix() {
		for _, nScopes := range benchMatrix() {
			b.Run(fmt.Sprintf("clients=%d/scopes=%d", nClients, nScopes), func(b *testing.B) {
				h, members := benchRegistry(nClients, nScopes)
				batch := openBatchFor(nScopes)

				b.ReportAllocs()
				for b.Loop() {
					for i := range batch {
						batch[i].candle.LastUpdate++
					}
					h.handleOpenBatch(batch)
				}
				b.StopTimer()

				for mi, m := range members {
					m.mu.Lock()
					gotSlots := len(m.open)
					stale := -1
					for i := range batch {
						if m.open[batch[i].scope].LastUpdate != batch[i].candle.LastUpdate {
							stale = i
							break
						}
					}
					pendingWake := len(m.wake)
					m.mu.Unlock()
					if gotSlots != nScopes {
						b.Fatalf("member %d: %d conflation slots written, want %d", mi, gotSlots, nScopes)
					}
					if stale >= 0 {
						b.Fatalf("member %d: scope %d holds a stale open (dedupe/non-regression ate the batch)", mi, stale)
					}
					if pendingWake != 1 {
						b.Fatalf("member %d: wake signals pending = %d, want 1", mi, pendingWake)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*nScopes*nClients), "ns/slot")
			})
		}
	}
}

// nopPublisher isolates the producer's own cost (sequence, marshal, channel-name build)
// from network time.
type nopPublisher struct{ n atomic.Int64 }

func (p *nopPublisher) Publish(context.Context, string, []byte) error {
	p.n.Add(1)
	return nil
}

// BenchmarkProducerPublish measures the CPU added to each candle.close placed on the
// bus, on the ingestion hot path.
func BenchmarkProducerPublish(b *testing.B) {
	scope := benchScope(0)
	cnd := closedCandle(1_700_000_000_000, 42)

	b.Run("serial", func(b *testing.B) {
		pub := &nopPublisher{}
		p := NewAsyncProducer(pub, "gowhale:realtime", func() int64 { return 1 })
		b.ReportAllocs()
		for b.Loop() {
			p.PublishClose(scope, cnd)
		}
		b.StopTimer()
		p.Stop()
		got, dropped := pub.n.Load(), p.Dropped()
		if got+dropped != int64(b.N) {
			b.Fatalf("publisher received %d payloads and dropped %d, want %d accounted for", got, dropped, b.N)
		}
		b.ReportMetric(float64(dropped)/float64(b.N), "drop/op")
	})
	b.Run("parallel", func(b *testing.B) {
		pub := &nopPublisher{}
		p := NewAsyncProducer(pub, "gowhale:realtime", func() int64 { return 1 })
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				p.PublishClose(scope, cnd)
			}
		})
		b.StopTimer()
		p.Stop()
		got, dropped := pub.n.Load(), p.Dropped()
		if got+dropped != int64(b.N) {
			b.Fatalf("publisher received %d payloads and dropped %d, want %d accounted for", got, dropped, b.N)
		}
		b.ReportMetric(float64(dropped)/float64(b.N), "drop/op")
	})
}

var _ Member = (*benchMember)(nil)
