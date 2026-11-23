package binancesdk

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/binancews"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/platform/netdiag"
)

func (c *SDKClient) startSocketErrorWatchers(connCtx context.Context, clients []*binancews.Connection) {
	for pid, conn := range clients {
		if conn == nil {
			continue
		}
		c.connWg.Add(1)
		go func(partitionID int, connection *binancews.Connection) {
			defer c.connWg.Done()
			// The partition identity travels in the callback, so the reconnect can heal ONE socket
			// instead of closing the healthy sister for this one's fault.
			watchTransportError(connCtx, partitionID, connection, marketTypeFuture, c.metrics, c.netdiag,
				func(reason string) { c.requestReconnect(partitionID, reason) }, c.log, nil)
		}(pid, conn)
	}
}

func (c *SpotSDKClient) startSocketErrorWatchers(connCtx context.Context, clients []*binancews.Connection) {
	for pid, conn := range clients {
		if conn == nil {
			continue
		}
		c.connWg.Add(1)
		go func(partitionID int, connection *binancews.Connection) {
			defer c.connWg.Done()
			watchTransportError(connCtx, partitionID, connection, marketTypeSpot, c.metrics, c.netdiag, c.requestReconnect, c.log, nil)
		}(pid, conn)
	}
}

// watchTransportError classifies the first terminal event of one connection
// and asks its owner to reconnect. The owner retains retry and gap policy.
func watchTransportError(
	connCtx context.Context,
	partitionID int,
	conn *binancews.Connection,
	market string,
	metrics Metrics,
	correlator *netdiag.Correlator,
	requestReconnect func(string),
	log *slog.Logger,
	sisterAge func() time.Duration,
) {
	select {
	case err, ok := <-conn.Errors():
		if ok && err != nil {
			recordTransportError(connCtx, partitionID, conn, market, metrics, correlator, err, requestReconnect, log, sisterAge)
		}
	case <-conn.Done():
		// Done and Errors can become ready in the same instant. Prefer the
		// terminal cause so a close after a read failure still reconnects.
		select {
		case err, ok := <-conn.Errors():
			if ok && err != nil {
				recordTransportError(connCtx, partitionID, conn, market, metrics, correlator, err, requestReconnect, log, sisterAge)
			}
		default:
		}
	case <-connCtx.Done():
		return
	}
}

func recordTransportError(
	connCtx context.Context,
	partitionID int,
	conn *binancews.Connection,
	market string,
	metrics Metrics,
	correlator *netdiag.Correlator,
	err error,
	requestReconnect func(string),
	log *slog.Logger,
	sisterAge func() time.Duration,
) {
	if connCtx.Err() != nil {
		return
	}
	cause := classifyDisconnect(err)
	metrics.incDisconnectCause(venueBinance, market, cause)
	scope := ""
	if correlator != nil {
		scope = correlator.Record(venueBinance, time.Now().UnixMilli())
	}
	// The evidence is what turns "a socket failed" into an attributable event. 1006 is synthesized
	// locally and proves nothing about the venue; the code, the peer's reason (when there is one)
	// and whether WE closed it are the only facts that can assign blame.
	var evidence binancews.CloseEvidence
	if conn != nil {
		// The sibling's age is the one fact the transport cannot know; the owner supplies it and
		// it is stamped into the retained evidence before the log reads it.
		if sisterAge != nil {
			if age := sisterAge(); age > 0 {
				conn.SetSisterLastFrameAge(age)
			}
		}
		evidence = conn.CloseEvidence()
	}
	logger.OrDiscard(log).Warn("binance ws: partition socket error; triggering immediate reconnect",
		"market_type", market,
		"component", evidence.Component,
		"partition", partitionID,
		"cause", cause,
		"close_code", evidence.Code,
		"close_reason", evidence.Reason,
		"close_initiator", evidence.Initiator,
		"dial_age_ms", evidence.DialAge.Milliseconds(),
		"streams", evidence.Streams,
		"remote_addr", evidence.RemoteAddr,
		"last_frame_age_ms", evidence.LastFrameAge.Milliseconds(),
		"pings", evidence.Pings,
		"pong_errors", evidence.PongErrors,
		"sister_last_frame_age_ms", evidence.SisterLastFrameAge.Milliseconds(),
		"scope", scope,
		"error", err)
	requestReconnect(fmt.Sprintf("partition %d socket %s/%s", partitionID, cause, scope))
}
