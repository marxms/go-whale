package binancesdk

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// monitoredConnection is the state the shared connection-health loop observes. The futures
// and spot clients own the same decision tree — stall clock hold, disconnect detection,
// stall detection — so it lives here exactly once; each client supplies its own state,
// labels and log text.
type monitoredConnection struct {
	log   *slog.Logger
	venue string
	scope string // market label: marketTypeFuture or marketTypeSpot.

	clientCtx        context.Context // cancels the whole client lifecycle
	messagesIn       *atomic.Uint64
	lastMessageAt    *atomic.Value // stores time.Time
	resubscribedAt   *atomic.Value // stores time.Time
	reconnecting     *atomic.Bool
	connected        *atomic.Bool
	metrics          Metrics
	requestReconnect func(reason string)

	shutdownLog     string
	disconnectedLog string
	stalledLog      string

	wg *sync.WaitGroup
}

// run starts the monitor goroutine. It exits when either the connection or the client
// context is done, and holds the stall clock across reconnect/resubscribe and its grace.
func (m monitoredConnection) run(connCtx context.Context) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		var lastMsgCount uint64
		stalledCount := 0

		for {
			select {
			case <-connCtx.Done():
				logger.OrDiscard(m.log).Info(m.shutdownLog, "reason", "connection_context_canceled")
				NewSDKEvent(EventMonitorStopped, m.venue, m.scope).WithReason("connection_context_canceled").Log()
				return
			case <-m.clientCtx.Done():
				logger.OrDiscard(m.log).Info(m.shutdownLog, "reason", "client_context_canceled")
				NewSDKEvent(EventMonitorStopped, m.venue, m.scope).WithReason("client_context_canceled").Log()
				return
			case <-ticker.C:
				currentMsgCount := m.messagesIn.Load()
				now := time.Now()

				lastMsg, _ := m.lastMessageAt.Load().(time.Time)
				age := now.Sub(lastMsg)
				m.metrics.setLastMessageAge(m.venue, m.scope, age.Seconds())

				// Hold the stall clock while a reconnect/resubscribe is in progress AND
				// through the post-resubscribe grace — the monitor must never interrupt
				// recovery or read the first-message gap as a stall. Re-arm from this tick.
				resubAt, _ := m.resubscribedAt.Load().(time.Time)
				if stallClockHeld(m.reconnecting.Load(), resubAt, now) {
					stalledCount = 0
					lastMsgCount = currentMsgCount
					if m.reconnecting.Load() {
						m.metrics.setHealthState(m.venue, m.scope, float64(HealthReconnecting))
					}
					continue
				}

				if !m.connected.Load() {
					stalledCount++
					m.metrics.setHealthState(m.venue, m.scope, float64(HealthDisconnected))
					logger.OrDiscard(m.log).Warn(m.disconnectedLog, "stalled_count", stalledCount)
					m.requestReconnect("connection not marked as connected")
					stalledCount = 0
					continue
				}

				if age > stallThreshold() {
					if currentMsgCount == lastMsgCount {
						stalledCount++
						m.metrics.setHealthState(m.venue, m.scope, float64(HealthStalled))
						logger.OrDiscard(m.log).Warn(m.stalledLog,
							"age", age,
							"messages_in", currentMsgCount,
							"stalled_count", stalledCount)
						m.requestReconnect(fmt.Sprintf("no messages for %v (stalledCount=%d)", age, stalledCount))
						stalledCount = 0
					} else {
						// Messages arriving but slowly; reset stalled count.
						stalledCount = 0
					}
				} else {
					stalledCount = 0
					m.metrics.setHealthState(m.venue, m.scope, float64(HealthHealthy))
				}
				lastMsgCount = currentMsgCount
			}
		}
	}()
}
