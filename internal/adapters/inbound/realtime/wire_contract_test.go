package realtime

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// The wire contract is consumed by TWO independent codebases that cannot import this
// package: the analyzer (`parseEnvelope`, over the Redis bus) and the web client
// (`parseStreamFrame`, over the WebSocket edge). Nothing else in the suite pins the
// actual BYTES those consumers must accept, so a renamed JSON tag would keep every Go
// and every TS test green while breaking production silently.
//
// These goldens close that hole: this test is the sole producer of
// `testdata/wire/*.json`, and the TS suites on both consumer sides read the very same
// files. Regenerate deliberately with `go test ./internal/adapters/inbound/realtime -run TestWireContract -update`;
// a regenerated golden is a CONTRACT CHANGE and must be matched on both consumers.
// updateGoldens reports whether -update was passed. The flag is registered once in TestMain.
func updateGoldens() bool {
	f := flag.Lookup("update")
	return f != nil && f.Value.String() == "true"
}

const goldenDir = "testdata/wire"

// fullCandle is the maximal candle: every serializable field non-zero, so a golden
// pins the name of every field a consumer could read.
func fullCandle() candle.Candle {
	return candle.Candle{
		Timestamp:        1_700_000_000_000,
		Open:             42_000.5,
		High:             42_500.75,
		Low:              41_900.25,
		Close:            42_310.1,
		Volume:           1234.5678,
		CreatedAt:        1_700_000_000_100,
		LastUpdate:       1_700_000_001_900,
		TakerBuyVolume:   700.5,
		TakerSellVolume:  534.0678,
		Source:           "binance",
		AggregateMethod:  "trade-merge",
		SideUnknownCount: 3,
		LateTradeCount:   1,
		HasGap:           false,
	}
}

func canonicalScope() Scope {
	return Scope{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "4h"}
}

// TestWireContractGoldens marshals every frame kind a consumer can receive and pins the
// bytes. The frames are built through the SAME functions production uses
// (marshalEventFrame / Envelope.Marshal), never a hand-written literal — otherwise the
// golden would pin the test's idea of the contract instead of the shipped one.
func TestWireContractGoldens(t *testing.T) {
	t.Parallel()

	scope := canonicalScope()
	full := fullCandle()

	// A bar whose order-flow aggregates are a legitimate ZERO (perfectly balanced taker
	// flow / zero-crossing cumulative). Every one of those fields is `omitempty`, so this
	// is the case where the wire cannot distinguish "0" from "absent" — see
	// TestWireContractOmitemptyErasesLegitimateZero.
	zeroFlow := full
	zeroFlow.TakerBuyVolume = 0
	zeroFlow.TakerSellVolume = 0

	openBar := full
	openBar.Timestamp = 1_700_000_014_400_000

	cases := []struct {
		name  string
		build func(t *testing.T) []byte
	}{
		{
			// `baselineTime` is set here on purpose. `fanout` writes the cursor and the anchor in
			// the same call, so `baseline > 0` without an anchor is a shape the server cannot
			// produce — and a golden that encodes an impossible combination leaves the byte-level
			// cross-language net blind to the field's real value. The web consumer reads these
			// exact bytes, so a dropped field or a typo'd JSON tag has to fail HERE.
			name: "snapshot-with-open",
			build: func(t *testing.T) []byte {
				return mustMarshalFrame(t, outFrame{snap: &snapshotPayload{
					scope: scope, baseline: 41, baselineTime: 1_700_000_010_800_000, open: &openBar,
				}})
			},
		},
		{
			// A scope with no open bar cached yet: `open` must be ABSENT (omitempty), not null,
			// and the client must treat that as "no live bar yet", not as a reset.
			name: "snapshot-without-open",
			build: func(t *testing.T) []byte {
				return mustMarshalFrame(t, outFrame{snap: &snapshotPayload{scope: scope, baseline: 0}})
			},
		},
		{
			name: "candle-close",
			build: func(t *testing.T) []byte {
				return mustMarshalFrame(t, outFrame{
					env: NewEnvelope(EventCandleClose, scope, full, 42, 1_700_000_014_400_123),
				})
			},
		},
		{
			name: "candle-revised",
			build: func(t *testing.T) []byte {
				return mustMarshalFrame(t, outFrame{
					env: NewEnvelope(EventCandleRevised, scope, full, 43, 1_700_000_014_500_000),
				})
			},
		},
		{
			// Opens carry Seq 0 by design: they are outside gap detection.
			name: "candle-open-update",
			build: func(t *testing.T) []byte {
				return mustMarshalFrame(t, outFrame{
					env: NewEnvelope(EventCandleOpenUpdate, scope, openBar, 0, 1_700_000_014_402_000),
				})
			},
		},
		{
			// The liveness frame. `build` returns the very bytes the writePump puts on
			// the socket — not a literal rebuilt here — so the golden pins the shipped value.
			// It belongs in this corpus because a web client RECEIVES it: it is deliberately not
			// parsed (parseStreamFrame returns null for an unknown type), and the golden is what
			// lets the web side prove that against the real frame instead of its idea of it.
			name: "heartbeat",
			build: func(_ *testing.T) []byte {
				return buildHeartbeatFrame()
			},
		},
		{
			name: "candle-close-zero-orderflow",
			build: func(t *testing.T) []byte {
				return mustMarshalFrame(t, outFrame{
					env: NewEnvelope(EventCandleClose, scope, zeroFlow, 44, 1_700_000_028_800_000),
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := indentJSON(t, tc.build(t))
			path := filepath.Join(goldenDir, tc.name+".json")

			if updateGoldens() {
				if err := os.MkdirAll(goldenDir, 0o750); err != nil {
					t.Fatalf("mkdir %s: %v", goldenDir, err)
				}
				if err := os.WriteFile(path, got, 0o600); err != nil {
					t.Fatalf("write golden %s: %v", path, err)
				}
				t.Logf("golden updated: %s", path)
				return
			}

			want, err := os.ReadFile(path) //nolint:gosec // G304: test-fixture path created or selected by the test.
			if err != nil {
				t.Fatalf("read golden %s: %v (run with -update to create it)", path, err)
			}
			if string(got) != string(want) {
				t.Errorf("WIRE CONTRACT DRIFT in %s.\n--- got ---\n%s\n--- want (golden) ---\n%s\n"+
					"If this change is intentional, regenerate with -update AND update the analyzer "+
					"(parseEnvelope) and web (parseStreamFrame) consumers.", tc.name, got, want)
			}
		})
	}
}

// TestWireContractTopLevelKeys asserts the field NAMES each frame kind exposes,
// independent of the golden bytes. A golden diff already catches a rename, but this gives
// the failure a precise message ("key added/removed") instead of a whole-document diff.
func TestWireContractTopLevelKeys(t *testing.T) {
	t.Parallel()

	scope := canonicalScope()
	full := fullCandle()

	tests := []struct {
		name string
		data []byte
		want []string
	}{
		{
			name: "snapshot exposes v/type/scope/baseline/open",
			data: mustMarshalFrame(t, outFrame{snap: &snapshotPayload{scope: scope, baseline: 7, open: &full}}),
			want: []string{"baseline", "open", "scope", "type", "v"},
		},
		{
			name: "snapshot without open omits the open key entirely",
			data: mustMarshalFrame(t, outFrame{snap: &snapshotPayload{scope: scope, baseline: 7}}),
			want: []string{"baseline", "scope", "type", "v"},
		},
		{
			// The seam anchor is what lets a client ask "does my history window reach the
			// live edge?" — a question `baseline` alone cannot answer, because it is an event
			// counter with no mapping to a bar.
			name: "snapshot carries the seam anchor when a close has been counted",
			data: mustMarshalFrame(t, outFrame{
				snap: &snapshotPayload{scope: scope, baseline: 7, baselineTime: 1_700_000_000_000_000},
			}),
			want: []string{"baseline", "baselineTime", "scope", "type", "v"},
		},
		{
			// Baseline 0 means nothing has closed, so there is no seam to verify. Emitting
			// `baselineTime: 0` would be an anchor the client could not distinguish from a real
			// bucket at epoch — absence is the honest encoding.
			name: "snapshot with no counted close omits the anchor rather than sending zero",
			data: mustMarshalFrame(t, outFrame{snap: &snapshotPayload{scope: scope, baseline: 0}}),
			want: []string{"baseline", "scope", "type", "v"},
		},
		{
			name: "delta exposes v/seq/type/scope/candle/emittedAt",
			data: mustMarshalFrame(t, outFrame{env: NewEnvelope(EventCandleClose, scope, full, 1, 2)}),
			want: []string{"candle", "emittedAt", "scope", "seq", "type", "v"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var obj map[string]json.RawMessage
			if err := json.Unmarshal(tt.data, &obj); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			got := make([]string, 0, len(obj))
			for k := range obj {
				got = append(got, k)
			}
			sort.Strings(got)
			if len(got) != len(tt.want) {
				t.Fatalf("key set = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("key set = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// TestWireContractScopeKeys pins the scope field names. The scope is the routing key on
// BOTH consumers (analyzer filters exchange/marketType, web matches its subscription), so
// a rename here silently routes every event to nobody.
func TestWireContractScopeKeys(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(canonicalScope())
	if err != nil {
		t.Fatalf("marshal scope: %v", err)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil {
		t.Fatalf("unmarshal scope: %v", err)
	}
	for _, key := range []string{"exchange", "marketType", "symbol", "timeframe"} {
		if _, ok := obj[key]; !ok {
			t.Errorf("scope is missing routing key %q (got %v)", key, obj)
		}
	}
	if len(obj) != 4 {
		t.Errorf("scope has %d keys, want exactly 4 — an added field is a contract change", len(obj))
	}
}

// TestWireContractOmitemptyErasesLegitimateZero pins a REAL semantic loss in the contract,
// so it is a known, asserted property instead of a surprise in production.
//
// An authoritative zero taker volume ERASED by omitempty is INDISTINGUISHABLE on the wire
// from absent. That was tolerable only while the derived fields existed, because a consumer
// could recover the value from them — the web still carries that workaround, and its own
// comment says the pair is "what recovers a balanced bar whose cvdIncrement of 0 the wire
// erased".
//
// With the derived fields gone the pair is the ONLY source, so erasing it would destroy the
// measurement outright. Measured across the fleet: ~1% of five-minute bars have one side
// legitimately at zero — and those are the bars carrying the strongest order-flow signal,
// not noise.
//
// So the pair is now always on the wire. Absence means "producer older than this contract";
// 0/0 means "volume traded but no side could be classified", which consumers treat as
// abstention. Two different answers, two different encodings.
func TestWireContractZeroOrderFlowReachesTheConsumer(t *testing.T) {
	t.Parallel()

	oneSided := fullCandle()
	oneSided.TakerBuyVolume = 12.5
	oneSided.TakerSellVolume = 0 // a real 100% buy-aggression bar

	env := NewEnvelope(EventCandleClose, canonicalScope(), oneSided, 9, 1)
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded struct {
		Candle map[string]json.RawMessage `json:"candle"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, required := range []string{"takerBuyVolume", "takerSellVolume", "volume"} {
		if _, present := decoded.Candle[required]; !present {
			t.Errorf("%q is ABSENT — it must always be on the wire, or a legitimate zero becomes "+
				"indistinguishable from an unmeasured bar and there is no derived field left to "+
				"recover it from", required)
		}
	}
	if got := string(decoded.Candle["takerSellVolume"]); got != "0" {
		t.Errorf("takerSellVolume = %s, want 0 — the zero side of a one-sided bar was altered", got)
	}

	// The derived values must NOT come back. Each was a second implementation of a formula the
	// consumer already owns, and they had provably drifted from the pair on gap-recovered
	// buckets, where the engine max()-ed each field independently.
	for _, gone := range []string{
		"cvdIncrement", "cvdCumulative", "cvdCumulativeSet", "aggressionRatio",
	} {
		if _, present := decoded.Candle[gone]; present {
			t.Errorf("%q is back on the wire — derived order flow belongs to the consumer", gone)
		}
	}
}

// TestWireContractForwardCompatibleUnknownField documents that the Go side tolerates an
// unknown field, so a future producer/consumer skew does not hard-fail the older peer.
func TestWireContractForwardCompatibleUnknownField(t *testing.T) {
	t.Parallel()

	raw := `{"v":1,"seq":5,"type":"candle.close","scope":{"exchange":"binance",` +
		`"marketType":"future","symbol":"BTC/USDT","timeframe":"4h"},` +
		`"candle":{"timestamp":1700000000000,"open":1,"high":2,"low":0.5,"close":1.5,"volume":10},` +
		`"emittedAt":1700000000100,"someFutureField":{"nested":true}}`

	env, err := Unmarshal([]byte(raw))
	if err != nil {
		t.Fatalf("an unknown field must not break parsing: %v", err)
	}
	if env.Seq != 5 || env.Type != EventCandleClose || env.Candle.Timestamp != 1_700_000_000_000 {
		t.Errorf("unexpected decode: seq=%d type=%s ts=%d", env.Seq, env.Type, env.Candle.Timestamp)
	}
}

// TestWireContractEventTypeStrings pins the type discriminants. Both consumers switch on
// these exact strings; the design prose says "candle.open-update" in places while the code
// emits "candle.open_update", so the code is the authority and it is pinned here.
func TestWireContractEventTypeStrings(t *testing.T) {
	t.Parallel()

	tests := map[EventType]string{
		EventCandleClose:      "candle.close",
		EventCandleOpenUpdate: "candle.open_update",
		EventCandleRevised:    "candle.revised",
		EventHeartbeat:        "heartbeat",
	}
	for got, want := range tests {
		if string(got) != want {
			t.Errorf("event type = %q, want %q", string(got), want)
		}
	}
	if SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d; a bump is a breaking contract change that both consumers must handle", SchemaVersion)
	}
}

// TestWireContractChannelNaming pins the Redis channel names the analyzer subscribes to.
func TestWireContractChannelNaming(t *testing.T) {
	t.Parallel()

	// DEFAULT_CHANNEL_PREFIX in the analyzer's realtime-subscriber.service.ts.
	const analyzerDefaultPrefix = "gowhale:realtime"

	if got, want := Channel(analyzerDefaultPrefix, EventCandleClose), "gowhale:realtime:candle.close"; got != want {
		t.Errorf("close channel = %q, want %q (analyzer subscribes to this literal)", got, want)
	}
	if got, want := Channel(analyzerDefaultPrefix, EventCandleRevised), "gowhale:realtime:candle.revised"; got != want {
		t.Errorf("revised channel = %q, want %q (analyzer subscribes to this literal)", got, want)
	}
}

// TestWireContractHeartbeatIsAdditive pins from both sides: the liveness frame is a
// well-formed, type-discriminated member of the wire, and it ARRIVED WITHOUT DISTURBING
// ANYTHING. The second half is the one worth a test — an "additive" change is a claim, and a
// claim about a contract two other codebases parse has to be enforced, not asserted in prose.
//
// The corpus check is the piece the golden comparison cannot make: TestWireContractGoldens only
// reads the goldens its own table names, so deleting a case together with its file leaves it
// green. Listing the directory instead makes the frame SET the assertion — one frame gained,
// none lost, none renamed.
func TestWireContractHeartbeatIsAdditive(t *testing.T) {
	t.Parallel()

	var beat map[string]json.RawMessage
	if err := json.Unmarshal(buildHeartbeatFrame(), &beat); err != nil {
		t.Fatalf("the heartbeat the server writes is not valid JSON: %v (raw=%s)", err, buildHeartbeatFrame())
	}

	got := make([]string, 0, len(beat))
	for k := range beat {
		got = append(got, k)
	}
	sort.Strings(got)
	if len(got) != 2 || got[0] != "type" || got[1] != "v" {
		t.Fatalf("heartbeat key set = %v, want [type v]", got)
	}
	if want := `"` + string(EventHeartbeat) + `"`; string(beat["type"]) != want {
		t.Errorf("heartbeat type = %s, want %s — the discriminant is how every consumer routes the frame",
			beat["type"], want)
	}
	if string(beat["v"]) != "1" || SchemaVersion != 1 {
		t.Errorf("heartbeat v = %s (SchemaVersion=%d) — the liveness frame rides the SAME versioned "+
			"envelope as the others, and adding it must not bump the version", beat["v"], SchemaVersion)
	}

	// The frame is a property of the CONNECTION, not of a subscription. Carrying any of these
	// would make it look like a scoped event and invite a consumer to reconcile candle state
	// from a liveness signal.
	for _, absent := range []string{"scope", "candle", "seq", "emittedAt", "baseline", "open"} {
		if _, present := beat[absent]; present {
			t.Errorf("heartbeat carries %q — it belongs to no scope and reports no data", absent)
		}
	}

	files, err := os.ReadDir(goldenDir)
	if err != nil {
		t.Fatalf("read %s: %v", goldenDir, err)
	}
	names := make([]string, 0, len(files))
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".json" {
			names = append(names, f.Name())
		}
	}
	sort.Strings(names)
	want := []string{
		"candle-close-zero-orderflow.json",
		"candle-close.json",
		"candle-open-update.json",
		"candle-revised.json",
		"heartbeat.json", // the only addition
		"snapshot-with-open.json",
		"snapshot-without-open.json",
	}
	if len(names) != len(want) {
		t.Fatalf("golden corpus = %v, want %v — a frame was added or dropped without both consumers knowing", names, want)
	}
	for i := range names {
		if names[i] != want[i] {
			t.Fatalf("golden corpus = %v, want %v", names, want)
		}
	}
}

func mustMarshalFrame(t *testing.T, f outFrame) []byte {
	t.Helper()
	data, err := marshalEventFrame(f.frame())
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	return data
}

// indentJSON normalizes the golden to a stable, reviewable shape. Go's encoder emits
// struct fields in declaration order, so indentation is the only normalization needed.
func indentJSON(t *testing.T, data []byte) []byte {
	t.Helper()
	out, err := json.MarshalIndent(json.RawMessage(data), "", "  ")
	if err != nil {
		t.Fatalf("indent: %v", err)
	}
	return append(out, '\n')
}
