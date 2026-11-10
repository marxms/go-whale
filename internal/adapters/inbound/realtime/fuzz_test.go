package realtime

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/marxms/go-whale/internal/candle"
)

// fuzzing the two parsers that sit on an untrusted boundary.
//
// Both are reachable from outside the process: Unmarshal parses whatever arrives on the
// Redis bus (any process with the credentials can publish onto the prefix), and the
// readPump parses whatever a connected browser sends. A parser panic in either is not a
// bad request — it takes down the whole GoWhale process (or, on the other side, the
// analyzer), because neither the actor nor the pumps recover panics. The example-based
// tests only ever feed these parsers bytes WE produced, so they cannot find that class of
// bug.
//
// These targets are cheap in the default `go test` run (only the seed corpus executes) and
// become real fuzzing under `-fuzz=FuzzX`.

// wireGoldenSeeds returns the frozen wire-contract goldens. They are the highest-value
// seeds available: they are the exact bytes production emits, so the fuzzer starts from
// valid structure and mutates outward instead of spending its budget rediscovering the
// shape of a frame.
func wireGoldenSeeds(tb testing.TB) [][]byte {
	tb.Helper()
	entries, err := os.ReadDir(goldenDir)
	require.NoError(tb, err)
	var out [][]byte
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(goldenDir, e.Name()))
		require.NoError(tb, err)
		out = append(out, data)
	}
	require.NotEmpty(tb, out, "wire goldens must exist to seed the fuzzers")
	return out
}

// FuzzEnvelopeUnmarshalNeverPanics pins that the bus parser is total: ANY byte string is
// either a parsed envelope or an error, never a panic.
//
// The failure this impedes is a crash-loop of the whole consumer from one malformed
// publish. Redis Pub/Sub enforces no schema and the prefix is shared, so a mis-configured
// or older producer (see the two-producers-one-prefix guardrail) can put arbitrary
// payloads on the channel.
//
// It also pins a stronger property on the accept path: a parsed envelope must be a FIXED
// POINT of marshal→unmarshal. If it were not, an analyzer that re-emitted what it received
// (or a golden regenerated from parsed input) would silently drift from the contract.
func FuzzEnvelopeUnmarshalNeverPanics(f *testing.F) {
	for _, seed := range wireGoldenSeeds(f) {
		f.Add(seed)
	}
	// Hostile shapes a hand-written table would not think to include: truncation, type
	// confusion on every field, deep nesting, numeric overflow, NUL and invalid UTF-8.
	for _, seed := range []string{
		``,
		`null`,
		`[]`,
		`{`,
		`{"v":1,"seq":`,
		`{"v":"1","seq":"x","type":42,"scope":[],"candle":"nope","emittedAt":{}}`,
		`{"seq":-1}`,
		`{"seq":18446744073709551615}`,
		`{"candle":{"open":1e999}}`,
		`{"candle":{"timestamp":99999999999999999999999}}`,
		`{"scope":{"symbol":"\u0000BTC"}}`,
		"{\"scope\":{\"symbol\":\"\xff\xfe\"}}",
		`{"v":1,"type":"candle.close","scope":{},"candle":{}}`,
		`[[[[[[[[[[1]]]]]]]]]]`,
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		env, err := Unmarshal(data)
		if err != nil {
			return
		}
		reEncoded, err := env.Marshal()
		require.NoError(t, err, "an envelope that parsed must be re-encodable")
		again, err := Unmarshal(reEncoded)
		require.NoError(t, err, "our own output must parse")
		require.Equal(t, env, again, "marshal/unmarshal must be a fixed point for a parsed envelope")
	})
}

// FuzzEnvelopeRoundTripPreservesFields walks the contract in the other direction: for
// arbitrary field VALUES, does the wire preserve them?
//
// The failure this impedes is silent field loss on the bus. `omitempty` on the order-flow
// aggregates already erases legitimate zeros (pinned separately in wire_contract_test.go);
// this target hunts the rest — a tag collision, a numeric type too narrow for a real
// value, or a float that marshals to something no JSON parser accepts. That last one is
// load-bearing: encoding/json REFUSES NaN/+Inf instead of emitting bare `NaN`, which is
// what keeps a single poisoned candle from producing a frame every consumer rejects.
func FuzzEnvelopeRoundTripPreservesFields(f *testing.F) {
	f.Add(uint64(42), "candle.close", "binance", "future", "BTC/USDT", "4h",
		int64(1_700_000_000_000), int64(1_700_000_001_900), int64(1_700_000_014_400),
		42000.5, 42500.75, 41900.25, 42310.1, 1234.5678)
	f.Add(uint64(0), "", "", "", "", "", int64(0), int64(0), int64(0), 0.0, 0.0, 0.0, 0.0, 0.0)
	f.Add(^uint64(0), "candle.revised", "bybit", "spot", "ETH/USDT:USDT", "1w",
		int64(math.MaxInt64), int64(math.MinInt64), int64(-1),
		// math.Copysign, not the literal -0.0: an untyped Go constant cannot BE negative zero
		// (-0.0 folds to +0.0), and negative zero is exactly the value that would expose a
		// sign-losing round trip.
		math.SmallestNonzeroFloat64, math.MaxFloat64, math.Copysign(0, -1), 1e-300, 1e300)
	f.Add(uint64(7), "candle.open_update", "\xff", "f", "\x00", "1m",
		int64(1), int64(2), int64(3), math.NaN(), 1.0, 1.0, 1.0, 1.0)

	f.Fuzz(func(t *testing.T, seq uint64, typ, exchange, marketType, symbol, timeframe string,
		ts, lastUpdate, emittedAt int64, open, high, low, closePx, volume float64,
	) {
		env := NewEnvelope(
			EventType(typ),
			Scope{Exchange: exchange, MarketType: marketType, Symbol: symbol, Timeframe: timeframe},
			candle.Candle{
				Timestamp:  ts,
				LastUpdate: lastUpdate,
				Open:       open,
				High:       high,
				Low:        low,
				Close:      closePx,
				Volume:     volume,
			},
			seq, emittedAt,
		)

		finite := isFinite(open) && isFinite(high) && isFinite(low) && isFinite(closePx) && isFinite(volume)
		data, err := env.Marshal()
		if !finite {
			require.Error(t, err,
				"a non-finite price must be REJECTED at the boundary, never emitted as bare NaN/Inf that no consumer can parse")
			return
		}
		require.NoError(t, err)

		got, err := Unmarshal(data)
		require.NoError(t, err, "our own output must parse")

		if !utf8.ValidString(typ) || !utf8.ValidString(exchange) || !utf8.ValidString(marketType) ||
			!utf8.ValidString(symbol) || !utf8.ValidString(timeframe) {
			// encoding/json substitutes U+FFFD for invalid UTF-8, so a byte-exact round trip
			// is impossible BY DESIGN for these inputs. Symbols are ASCII on every supported
			// venue, so the property that still matters here is only that the pipeline stays
			// total: it must not error and must not panic.
			return
		}
		require.Equal(t, env, got, "every envelope field must survive the wire byte-for-byte")
	})
}

func isFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// FuzzClientControlFrameKeepsHubHealthy drives arbitrary bytes through the REAL inbound
// path — fakeConn.ReadMessage -> readPump -> cmds -> actor — instead of re-implementing the
// parse in the test (an in-test copy of `json.Unmarshal` + action switch would pass even if
// production stopped validating entirely).
//
// Three properties are asserted per input, each mapping to a concrete way a hostile browser
// could hurt the process:
// 1. the control plane still applies a well-formed subscribe afterwards — a frame that
// wedged the single actor goroutine would freeze EVERY connected client, not just the
// sender;
// 2. one client frame creates AT MOST one subscription — an amplification bug here turns
// one socket into unbounded index growth;
// 3. the client is neither disconnected nor starved — a malformed frame must be ignored
// (readPump `continue`), never escalate to eviction or to a dead data plane.
func FuzzClientControlFrameKeepsHubHealthy(f *testing.F) {
	for _, seed := range wireGoldenSeeds(f) {
		f.Add(seed) // an OUTBOUND frame fed inbound: valid JSON, no "action" => must be ignored
	}
	for _, seed := range []string{
		`{"action":"subscribe","scope":{"exchange":"binance","marketType":"futures","symbol":"BTCUSDT","timeframe":"4h"}}`,
		`{"action":"unsubscribe","scope":{"exchange":"binance","marketType":"futures","symbol":"BTCUSDT","timeframe":"4h"}}`,
		`{"action":"subscribe"}`,                        // zero-value scope
		`{"action":"SUBSCRIBE","scope":{}}`,             // verb is case-sensitive
		`{"action":"drop-table","scope":{}}`,            // unknown verb
		`{"action":["subscribe"],"scope":{}}`,           // type confusion on the verb
		`{"action":"subscribe","scope":"BTCUSDT"}`,      // type confusion on the scope
		`{"action":"subscribe","scope":{"symbol":1}}`,   // type confusion inside the scope
		`{"Action":"subscribe","Scope":{}}`,             // Go-cased keys (json is case-insensitive)
		`{"action":"subscribe","scope":{},"extra":[1]}`, // unknown trailing field
		``,
		`{`,
		`0`,
		"\x00\x01\x02",
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		h := NewHub(newFakeOpens(), func() int64 { return 1 })
		h.Start()

		fc := newFakeConnBuf(4)
		h.add(newClient(fc, h.eventsBufSize))

		// Distinctive enough that fuzzer mutations will not produce it by accident, which is
		// what lets its presence in the index act as a processing barrier.
		barrier := Scope{Exchange: "binance", MarketType: "futures", Symbol: "FUZZBARRIER", Timeframe: "4h"}
		require.True(t, sendCtl(fc, data))
		require.True(t, sendCtl(fc, subReq(t, "subscribe", barrier)))

		// readPump reads its socket in order and cmds is a single FIFO channel, so the
		// barrier being applied PROVES the hostile frame was already fully processed.
		require.Eventually(t, func() bool {
			_, ok := h.scopesForTest()[barrier]
			return ok
		}, eventuallyWait, eventuallyTick, "control plane still applies a well-formed subscribe after the hostile frame")

		require.LessOrEqual(t, len(h.scopesForTest()), 2,
			"a single client frame must create at most one subscription")
		require.Equal(t, 1, h.statsForTest().Clients,
			"a malformed control frame must be ignored, not escalated to a disconnect")

		h.IngestClose(barrier, closedCandle(1_000, 1))
		require.Eventually(t, func() bool {
			return len(fc.framesOfType(string(EventCandleClose))) == 1
		}, eventuallyWait, eventuallyTick, "the data plane still serves this client after the hostile frame")

		_ = fc.Close()
		stopHub(t, h)
	})
}

// TestClientRequestJSONIsCaseInsensitive documents the consequence of encoding/json's
// case-insensitive field matching that the fuzz seeds above exercise: `{"Action":...}`
// dispatches exactly like `{"action":...}`. It is pinned because it is the kind of
// behavior a future "strict decoder" refactor (DisallowUnknownFields, a hand-rolled
// parser) would silently change, breaking any client that happens to send Go-cased keys.
func TestClientRequestJSONIsCaseInsensitive(t *testing.T) {
	var req clientRequest
	require.NoError(t, json.Unmarshal([]byte(`{"Action":"subscribe","Scope":{"Symbol":"BTCUSDT"}}`), &req))
	require.Equal(t, "subscribe", req.Action)
	require.Equal(t, "BTCUSDT", req.Scope.Symbol)
}
