package questdb

import (
	"strings"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// TestTimestampLiteral_IsUnambiguousQuestDBTimestamp pins the format.
//
// QuestDB only prunes partitions when the designated-timestamp predicate is a LITERAL it
// can resolve while planning. The literal must be an unambiguous UTC timestamp; anything
// else either fails to parse or is read in another zone, which silently shifts the window.
func TestTimestampLiteral_IsUnambiguousQuestDBTimestamp(t *testing.T) {
	t.Parallel()

	got := timestampLiteral(time.Date(2026, 7, 28, 3, 4, 5, 123456000, time.FixedZone("BRT", -3*3600)))

	const want = "'2026-07-28T06:04:05.123456Z'"
	if got != want {
		t.Fatalf("timestampLiteral = %s, want %s (must be normalised to UTC with microsecond precision)", got, want)
	}
}

// TestTimestampLiteral_CannotBreakOutOfTheQuotes is the injection guard.
//
// These literals are concatenated into SQL, so the format must be closed by construction.
// time.Time cannot carry a quote, and the formatter emits a fixed layout — this test exists
// so a future "make it configurable" change cannot quietly open a hole.
func TestTimestampLiteral_CannotBreakOutOfTheQuotes(t *testing.T) {
	t.Parallel()

	for _, ts := range []time.Time{
		time.Unix(0, 0),
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2262, 4, 11, 23, 47, 16, 854775000, time.UTC),
		{},
	} {
		got := timestampLiteral(ts)
		if !strings.HasPrefix(got, "'") || !strings.HasSuffix(got, "'") {
			t.Errorf("timestampLiteral(%v) = %s — must be quote-delimited", ts, got)
		}
		if strings.Count(got, "'") != 2 {
			t.Errorf("timestampLiteral(%v) = %s — exactly two quotes, or the literal is breakable", ts, got)
		}
	}
}

// TestColdReadQuery_EmbedsTheTimestampInterval is the RED guard for the real defect.
//
// The read used `timestamp >= $5 AND timestamp <= $6`. Measured on the production host against a
// 10M-row / 368-partition table: with BIND PARAMETERS the planner cannot build the interval,
// so it scans every partition and filters row by row — a JVM thread dump caught it inside
// FilteredRecordCursor → EqSymStrFunctionFactory → Chars.equals, i.e. a per-row STRING
// comparison over the whole table, pinning QuestDB at its 2-CPU ceiling.
//
// With LITERALS the same query plans as:
//
//	Interval backward scan on: candles
//	  intervals: [("2026-07-27T00:00:00Z","2026-07-28T03:00:00Z")]
//
// 42-412ms against 1.501ms for the unpruned form.
//
// This is what made the coldReadFrom window bound ineffective: the window was computed
// correctly and then handed over in a shape the planner ignores. The bound is only real
// once it reaches the SQL as a literal.
func TestColdReadQuery_EmbedsTheTimestampInterval(t *testing.T) {
	t.Parallel()

	from := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 7, 28, 3, 0, 0, 0, time.UTC)

	q := coldRangeQuery("candles", true, from, to)

	for _, want := range []string{
		"timestamp >= '2026-07-27T00:00:00.000000Z'",
		"timestamp <= '2026-07-28T03:00:00.000000Z'",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("query missing literal interval %q — without it QuestDB cannot prune partitions:\n%s", want, q)
		}
	}

	// The SYMBOL predicates stay parameterised: they are untrusted-ish identifiers, and the
	// table has no index on them (SHOW COLUMNS reports indexed=false), so they are a filter
	// either way. Only the interval changes the plan.
	for _, want := range []string{"exchange = $1", "market_type = $2", "symbol = $3", "timeframe = $4"} {
		if !strings.Contains(q, want) {
			t.Errorf("query lost the bound parameter %q — SYMBOL predicates must NOT be interpolated:\n%s", want, q)
		}
	}

	// Assert on the PREDICATE, not on a placeholder number: LIMIT legitimately keeps a bind
	// parameter, and its index shifts when the timestamp ones are removed.
	if strings.Contains(q, "timestamp >= $") || strings.Contains(q, "timestamp <= $") {
		t.Errorf("timestamp is still a bind parameter — that is the defect:\n%s", q)
	}
}

// TestCompletenessQuery_EmbedsTheLowerBound: the completeness scan had the same defect, and
// it runs on a timer over every (exchange, market_type) x timeframe scope, so an unpruned
// form there is a recurring full scan rather than a one-off.
func TestCompletenessQuery_EmbedsTheLowerBound(t *testing.T) {
	t.Parallel()

	from := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	q := completenessQuery("candles", from)

	if !strings.Contains(q, "timestamp >= '2026-07-26T12:00:00.000000Z'") {
		t.Errorf("completeness scan is not pruned by a literal interval:\n%s", q)
	}
	if strings.Contains(q, "$4") {
		t.Errorf("timestamp is still a bind parameter:\n%s", q)
	}
}

// TestFlattenSorted_EmitsTimestampAscending is the guard for the ingestion-order defect.
//
// The write loop ranged the groups map directly. Go randomizes map iteration, and every
// group restarts at its own earliest timestamp, so one commit carried symbol A t1..tn then
// symbol B t1..tn — out of order. QuestDB answers out-of-order ingestion by sorting and
// merging the affected partitions instead of appending.
//
// The irony is that batching the sweep into a single commit is what created this: with one
// commit per group, order was monotonic by construction. This test keeps the batching AND
// the order.
func TestFlattenSorted_EmitsTimestampAscending(t *testing.T) {
	t.Parallel()

	k := func(sym string) candle.SeriesKey {
		return mustSeriesKey("binance", "future", sym, "5m")
	}
	groups := map[candle.SeriesKey][]candle.Candle{
		k("BTC/USDT"): {{Timestamp: 100}, {Timestamp: 400}, {Timestamp: 700}},
		k("ETH/USDT"): {{Timestamp: 200}, {Timestamp: 500}, {Timestamp: 800}},
		k("SOL/USDT"): {{Timestamp: 300}, {Timestamp: 600}, {Timestamp: 900}},
	}

	rows := flattenSorted(groups)

	if len(rows) != 9 {
		t.Fatalf("flattenSorted dropped rows: got %d, want 9", len(rows))
	}
	for i := 1; i < len(rows); i++ {
		if rows[i].candle.Timestamp < rows[i-1].candle.Timestamp {
			t.Fatalf("row %d goes back in time (%d after %d) — ILP would take the O3 merge path",
				i, rows[i].candle.Timestamp, rows[i-1].candle.Timestamp)
		}
	}
	// The key must travel WITH its candle: sorting must not detach a row from its series.
	for _, r := range rows {
		want := map[int64]string{
			100: "BTC/USDT", 400: "BTC/USDT", 700: "BTC/USDT",
			200: "ETH/USDT", 500: "ETH/USDT", 800: "ETH/USDT",
			300: "SOL/USDT", 600: "SOL/USDT", 900: "SOL/USDT",
		}[r.candle.Timestamp]
		if r.key.Scope.Symbol.String() != want {
			t.Errorf("row ts=%d carries symbol %q, want %q — sorting detached the row from its series",
				r.candle.Timestamp, r.key.Scope.Symbol.String(), want)
		}
	}
}

// TestFlattenSorted_EmptyIsEmpty: the caller short-circuits on len(groups)==0, but a group
// holding an empty slice must not produce a phantom row.
func TestFlattenSorted_EmptyIsEmpty(t *testing.T) {
	t.Parallel()

	got := flattenSorted(map[candle.SeriesKey][]candle.Candle{
		mustSeriesKey("binance", "future", "BTC/USDT", "5m"): {},
	})
	if len(got) != 0 {
		t.Fatalf("expected no rows, got %d", len(got))
	}
}
