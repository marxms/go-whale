package config

// cap_coverage.go — THE RULE THAT DECIDES WHETHER A CELL CAN BE CAPPED.
//
// # WHAT THE CAP DESTROYS, AND WHY THAT CHANGES THE UNIT OF THE QUESTION
//
// `ZREMRANGEBYRANK` removes MEMBERS. A member of the derivatives series is the ENTIRE
// serialized snapshot, so capping does not shorten the horizon of ONE field: it shortens the
// horizon of EVERY field that member carries, all at once. A field with no durable column
// disappears with the member and comes back from nowhere.
//
// Hence the shape of the rule. The `(timeframe, field)` cell is the unit of the REFUSAL — it
// is what the message names — but the TRIGGER is the member: a single uncovered field makes
// the whole cell uncappable. That is why widening the durable tier to more timeframes does
// not release any cap: the widening happens on the TIMEFRAME axis, and what is missing is on
// the FIELD axis. The two axes are independent, and the cap needs both.
//
// # WHY WRITING ALONE IS NOT ENOUGH
//
// There is no `SELECT *` on the durable read path: the query LISTS the columns it asks for. A
// column that is written and outside that list is unreachable — the chart client never
// receives it — so it does not replace the hot window for any purpose. Coverage, here, is
// written ∧ readable, and a rule that asked only about writing would approve capping a cell
// whose only deliverable copy is exactly the one the cap deletes.
//
// # WHY ROW EXISTENCE DOES NOT ENTER
//
// "The timeframe has a durable row" is the question the integrity checker answers, and it is
// good for what it does: warning. It is TERRIBLE for deciding a cap, for two reasons. First,
// it is per TIMEFRAME, and the data the cap destroys is per FIELD — a timeframe with rows may
// have seven fields that no column carries. Second, it is RUNTIME state, so a cap derived
// from it would turn on and off as the table fills, and retention that changes by itself is
// the definition of retention nobody can audit.

import "sort"

// DurableCellCoverage is the durable coverage PER FIELD, from the point of view of whoever
// decides a cap.
//
// All three sides are READ from the binary that is starting: the coverage declaration, the
// column list the read asks for, and the set of timeframes the durable source serves. None of
// them is an estimate, which is why the verdict derived here is the same on every run of the
// same build.
type DurableCellCoverage struct {
	// FieldColumn is the whole declaration: field → durable column. An EMPTY string means no
	// column, in any timeframe — the field exists only inside the ZSET member.
	//
	// The map is COMPLETE, including uncovered fields. Passing only the covered ones would
	// hide exactly the information the rule depends on.
	FieldColumn map[string]string
	// ReadableColumns are the columns the route serving the series actually ASKS FOR.
	ReadableColumns []string
	// DurableTimeframes are the timeframes the durable tier writes. `nil` means NOT READ, and
	// the timeframe axis then produces no finding — absence of measurement is not absence.
	DurableTimeframes []string
}

// Declared reports whether the coverage was READ.
//
// `nil` is NOT "nothing covered". The two readings produce opposite verdicts — one would
// refuse every cap, the other cannot produce a verdict at all — and collapsing them would
// silently turn an unread measurement into a refusal.
func (c DurableCellCoverage) Declared() bool { return c.FieldColumn != nil }

// UncoveredMemberFields returns, sorted and with the reason, the fields of the MEMBER that
// the durable tier does not carry for this timeframe. An empty list means: the cell can be
// capped.
//
// THE REASON TRAVELS WITH THE FIELD because the three cases are different fixes — give the
// field a column, ask for the column on read, or extend the tier to the timeframe — and a
// refusal that only says "uncovered" makes the operator guess which one.
func (c DurableCellCoverage) UncoveredMemberFields(timeframe string) []string {
	if !c.Declared() {
		return nil
	}

	readable := make(map[string]bool, len(c.ReadableColumns))
	for _, column := range c.ReadableColumns {
		readable[column] = true
	}

	// The timeframe axis is only judged when it was read. Without the list it disappears from
	// the finding instead of becoming a false "outside" that would accuse every cell for a
	// missing measurement.
	timeframeServed := true
	if c.DurableTimeframes != nil {
		timeframeServed = false
		want := normalizeTimeframeKey(timeframe)
		for _, tf := range c.DurableTimeframes {
			if normalizeTimeframeKey(tf) == want {
				timeframeServed = true
				break
			}
		}
	}

	var out []string
	for field, column := range c.FieldColumn {
		switch {
		case column == "":
			out = append(out, field+" (no durable column: it exists only inside the ZSET member)")
		case !timeframeServed:
			out = append(out, field+" ("+column+" outside the durable tier for "+timeframe+")")
		case !readable[column]:
			out = append(out, field+" ("+column+" is written and the read does NOT ask for it)")
		}
	}
	sort.Strings(out)
	return out
}
