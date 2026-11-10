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
			watchTransportError(connCtx, partitionID, connection, marketTypeFuture, c.metrics, c.netdiag, c.requestReconnect, c.log)
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
			watchTransportError(connCtx, partitionID, connection, marketTypeSpot, c.metrics, c.netdiag, c.requestReconnect, c.log)
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
) {
	select {
	case err, ok := <-conn.Errors():
		if ok && err != nil {
			recordTransportError(connCtx, partitionID, conn, market, metrics, correlator, err, requestReconnect, log)
		}
	case <-conn.Done():
		// Done and Errors can become ready in the same instant. Prefer the
		// terminal cause so a close after a read failure still reconnects.
		select {
		case err, ok := <-conn.Errors():
			if ok && err != nil {
				recordTransportError(connCtx, partitionID, conn, market, metrics, correlator, err, requestReconnect, log)
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
	_ *binancews.Connection,
	market string,
	metrics Metrics,
	correlator *netdiag.Correlator,
	err error,
	requestReconnect func(string),
	log *slog.Logger,
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
	logger.OrDiscard(log).Warn("binance ws: partition socket error; triggering immediate reconnect",
		"market_type", market,
		"partition", partitionID,
		"cause", cause,
		"scope", scope,
		"error", err)
	requestReconnect(fmt.Sprintf("partition %d socket %s/%s", partitionID, cause, scope))
}
