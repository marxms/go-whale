package questdb

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

type fakeAdmissionClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeAdmissionTimer
}
type fakeAdmissionTimer struct {
	clock   *fakeAdmissionClock
	when    time.Time
	ch      chan time.Time
	stopped bool
}

func (c *fakeAdmissionClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeAdmissionClock) NewTimer(d time.Duration) admissionTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeAdmissionTimer{clock: c, when: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	if d <= 0 {
		t.ch <- c.now
	}
	return t
}
func (t *fakeAdmissionTimer) C() <-chan time.Time { return t.ch }
func (t *fakeAdmissionTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	return was
}

func (c *fakeAdmissionClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range c.timers {
		if !t.stopped && !t.when.After(c.now) {
			select {
			case t.ch <- c.now:
			default:
			}
		}
	}
}

func waitAdmission(t *testing.T, p *Pipeline) {
	t.Helper()
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		p.stateMu.Lock()
		n := p.waiters
		p.stateMu.Unlock()
		if n > 0 {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("admission did not wait")
}

func newWaitingAdmission(t *testing.T) (*Pipeline, *fakeAdmissionClock, <-chan error) {
	t.Helper()
	p := NewPipeline(nil, 1, time.Hour)
	c := &fakeAdmissionClock{now: time.Unix(100, 0)}
	p.admissionClock = c
	p.buffer <- candleEntry{Candle: candle.Candle{Timestamp: 1}}
	done := make(chan error, 1)
	go func() {
		done <- p.EnqueueClosedBlocking(context.Background(), "binance", "future", "X", "5m", candle.Candle{Timestamp: 2})
	}()
	waitAdmission(t, p)
	return p, c, done
}

func TestAdmissionProgress_PredecessorExtendsWaitingWithoutReplay(t *testing.T) {
	p, c, done := newWaitingAdmission(t)
	// Confirm an existing predecessor via the actual coordinator path, not an enqueue event.
	sender := newFakeLineSender()
	q := &Store{ilpSender: sender, tableName: "candles", ctx: context.Background(), walPosition: func(context.Context) (coldWALPosition, error) { return coldWALPosition{writer: 9, sequencer: 9}, nil }}
	entries := scriptedEntries()
	q.pendingPlan = &coldDayPlan{days: []*coldDayWrite{{entries: entries[:1], acknowledged: true, targetKnown: true, target: 7}}}
	p.store = q
	c.advance(20 * time.Second)
	if !p.flushWorkerTurn(entries[3:]) || p.Stats().Flushed != 0 {
		t.Fatal("predecessor did not confirm independently")
	}
	c.advance(20 * time.Second)
	// Give the existing queued item to the worker and signal capacity.
	first := <-p.buffer
	p.signalCapacity()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("admission did not finish")
	}
	second := <-p.buffer
	if first.Candle.Timestamp != 1 || second.Candle.Timestamp != 2 || len(p.buffer) != 0 {
		t.Fatalf("admission reordered or repeated: %v %v", first, second)
	}
}

func TestAdmissionProgress_ConfirmationClearsFenceBeforeWake(t *testing.T) {
	p, _, done := newWaitingAdmission(t)
	// A waiter has not yet processed the prior error notification when retry confirms.
	p.stateMu.Lock()
	p.blockedPhase = "APPLY"
	p.stateMu.Unlock()
	p.publishPendingWithProgress("", true)
	<-p.buffer
	p.signalCapacity()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("confirmed predecessor retained stale fence: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("admission did not wake")
	}
}

func TestAdmissionProgress_TimeoutAndCancellation(t *testing.T) {
	t.Run("late confirmation cannot revive expired wait", func(t *testing.T) {
		p, c, done := newWaitingAdmission(t)
		p.stateMu.Lock()
		c.advance(31 * time.Second)
		p.confirmedProgressLocked()
		p.stateMu.Unlock()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("expired wait was revived")
		}
	})
	t.Run("new wait starts a fresh episode", func(t *testing.T) {
		p, c, first := newWaitingAdmission(t)
		<-p.buffer
		p.signalCapacity()
		select {
		case err := <-first:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("first admission did not finish")
		}
		c.advance(40 * time.Second)
		second := make(chan error, 1)
		go func() {
			second <- p.EnqueueClosedBlocking(context.Background(), "binance", "future", "X", "5m", candle.Candle{Timestamp: 3})
		}()
		waitAdmission(t, p)
		c.advance(29 * time.Second)
		select {
		case err := <-second:
			t.Fatalf("new wait ended early: %v", err)
		default:
		}
		c.advance(time.Second)
		select {
		case err := <-second:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("new wait failed to expire")
		}
	})
	t.Run("capacity is not confirmed progress", func(t *testing.T) {
		p, c, done := newWaitingAdmission(t)
		c.advance(20 * time.Second)
		p.signalCapacity()
		until := time.Now().Add(2 * time.Second)
		for {
			c.mu.Lock()
			n := len(c.timers)
			c.mu.Unlock()
			if n >= 2 {
				break
			}
			if time.Now().After(until) {
				t.Fatal("capacity wake was not observed")
			}
			runtime.Gosched()
		}
		c.advance(10 * time.Second)
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("capacity wake incorrectly renewed the deadline")
		}
	})
	t.Run("no progress", func(t *testing.T) {
		p, c, done := newWaitingAdmission(t)
		c.advance(30 * time.Second)
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout did not wake")
		}
		if len(p.buffer) != 1 {
			t.Fatal("unadmitted item entered queue")
		}
	})
	t.Run("fence", func(t *testing.T) {
		p, _, done := newWaitingAdmission(t)
		p.publishPending("UNCERTAIN")
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("fence accepted entry")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("fence did not wake")
		}
		if len(p.buffer) != 1 {
			t.Fatal("fenced entry admitted")
		}
	})
	t.Run("stop", func(t *testing.T) {
		p, _, done := newWaitingAdmission(t)
		p.cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("stop did not wake")
		}
	})
	t.Run("caller deadline", func(t *testing.T) {
		p := NewPipeline(nil, 1, time.Hour)
		p.buffer <- candleEntry{}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		if err := p.EnqueueClosedBlocking(ctx, "e", "m", "s", "5m", candle.Candle{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v", err)
		}
	})
}
