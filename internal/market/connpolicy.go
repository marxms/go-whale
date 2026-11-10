package market

import "time"

// ConnPolicy is what a venue documents about one WebSocket connection. Every
// zero value means "the venue does not document this value"; the transport must
// never substitute a default, because a single global deadline is what killed a
// quiet futures socket whose server pings every three minutes.
//
// DocsURL is mandatory whenever any other field is non-zero, and a test enforces
// it. The route map is keyed by stream class ("trade", "aggTrade", "kline",
// "markPrice", "depth", "account") because Binance USD-M serves different classes
// on different paths; an empty map means the venue has no route split.
type ConnPolicy struct {
	DocsURL           string
	ServerPing        time.Duration
	PongTimeout       time.Duration
	ClientPing        time.Duration
	ReadIdle          time.Duration
	MaxLifetime       time.Duration
	MaxStreamsPerConn int
	MaxConnsPerIP     int
	ConnWindow        time.Duration
	PingEncoding      PingEncoding
	Routes            map[string]string
}

// PingEncoding is how a venue carries its keepalive on the wire.
type PingEncoding string

const (
	// PingEncodingProtocol is a WebSocket control frame (Binance).
	PingEncodingProtocol PingEncoding = "protocol"
	// PingEncodingJSON is an application frame such as {"ping":"<ms>"} (BingX spot).
	PingEncodingJSON PingEncoding = "json"
	// PingEncodingText is an application frame such as "Ping" (BingX swap).
	PingEncodingText PingEncoding = "text"
)

const (
	binanceSpotDocs    = "https://developers.binance.com/en/docs/products/spot/web-socket-streams"
	binanceFuturesDocs = "https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect"
	bingXWebSocketDocs = "https://bingx-api.github.io/docs-v3/#/en/Quick%20Start/WebSocket%20Rules/WebSocket%20Rules"
)

const (
	routePublic  = "/public"
	routeMarket  = "/market"
	routePrivate = "/private"
)

// ConnPolicyFor returns the documented policy for one scope. The market type is
// the canonical one: a BingX "swap" is Future here.
func ConnPolicyFor(exchangeID string, mt MarketType) (ConnPolicy, bool) {
	exchange, err := ParseExchange(exchangeID)
	if err != nil || !mt.Valid() {
		return ConnPolicy{}, false
	}

	switch exchange {
	case Binance:
		if mt == Spot {
			return ConnPolicy{
				DocsURL:           binanceSpotDocs,
				ServerPing:        20 * time.Second,
				PongTimeout:       60 * time.Second,
				ReadIdle:          60 * time.Second,
				MaxLifetime:       24 * time.Hour,
				MaxStreamsPerConn: 1024,
				MaxConnsPerIP:     300,
				ConnWindow:        5 * time.Minute,
				PingEncoding:      PingEncodingProtocol,
			}, true
		}
		return ConnPolicy{
			DocsURL:           binanceFuturesDocs,
			ServerPing:        3 * time.Minute,
			PongTimeout:       10 * time.Minute,
			ReadIdle:          4 * time.Minute,
			MaxLifetime:       24 * time.Hour,
			MaxStreamsPerConn: 1024,
			MaxConnsPerIP:     300,
			ConnWindow:        5 * time.Minute,
			PingEncoding:      PingEncodingProtocol,
			Routes: map[string]string{
				"trade":     routePublic,
				"aggTrade":  routeMarket,
				"kline":     routeMarket,
				"markPrice": routeMarket,
				"depth":     routePublic,
				"account":   routePrivate,
			},
		}, true
	case BingX:
		encoding := PingEncodingJSON
		if mt == Future {
			encoding = PingEncodingText
		}
		return ConnPolicy{
			DocsURL:           bingXWebSocketDocs,
			ServerPing:        5 * time.Second,
			MaxStreamsPerConn: 200,
			PingEncoding:      encoding,
		}, true
	}
	return ConnPolicy{}, false
}
