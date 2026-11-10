package coinalyze

import (
	"log/slog"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// The history-response audit: what came back against what was requested.
//
// # WHY THIS EXISTS
//
// A window beyond the provider's retention does not return an error: it returns HTTP 200
// with what exists. Three weeks presented as a year is a truncation nobody sees. And the
// response array carries the symbol in each element because the provider OMITS the element
// of a symbol with no data — reading that array by position attributes one symbol's series
// to another in the exact cycle in which an omission happens.
//
// The audit returns no `error`: short coverage and an omitted symbol are FINDINGS about the
// data, not failures of the call. The report carries the finding to whoever decides, and
// the log records it for whoever operates.

// CoinalyzeHistoryPoint is one point of `history`.
//
// `t` arrives in SECONDS — measured, `t: 1683849600` — unlike the snapshot of this same API,
// which arrives in milliseconds. Reading it as milliseconds throws the whole series to 1970.
type CoinalyzeHistoryPoint struct {
	T int64   `json:"t"`
	O float64 `json:"o"`
	H float64 `json:"h"`
	L float64 `json:"l"`
	C float64 `json:"c"`
}

// CoinalyzeHistorySeries is one element of the response array.
type CoinalyzeHistorySeries struct {
	Symbol  string                  `json:"symbol"`
	History []CoinalyzeHistoryPoint `json:"history"`
}

// CoinalyzeLongShortPoint is one point of `/long-short-ratio-history`, which has its OWN
// body — measured: `{"t":1787270400,"r":0.9554,"l":48.86,"s":51.14}`.
//
// Decoding it through the OHLC struct above does not error: `l` matches the candle's LOW
// field and the long share becomes "low". Same field name, another quantity, no error.
type CoinalyzeLongShortPoint struct {
	T int64 `json:"t"`
	// R is the long/short ratio, in the same unit as the existing source (measured: 0.9554
	// on both APIs for the same bar).
	R float64 `json:"r"`
	// L and S are the shares in PERCENT (48.86 / 51.14), which is also the unit in which
	// the existing source WRITES — it publishes a fraction and multiplies by 100 before
	// writing.
	L float64 `json:"l"`
	S float64 `json:"s"`
}

// CoinalyzeLongShortSeries is one element of the `/long-short-ratio-history` array.
type CoinalyzeLongShortSeries struct {
	Symbol  string                    `json:"symbol"`
	History []CoinalyzeLongShortPoint `json:"history"`
}

// CoinalyzeHistoryRequest is what was requested, and the ruler against which the response
// is measured.
type CoinalyzeHistoryRequest struct {
	Symbols  []string
	From     time.Time
	To       time.Time
	Interval time.Duration
}

// CoinalyzeGap is a piece of the requested window that did not come back.
type CoinalyzeGap struct {
	From time.Time
	To   time.Time
}

// CoinalyzeSeriesAudit is the verdict on ONE requested symbol.
//
// `Present` and `Partial` are distinct outcomes: absent means the provider did not answer
// for that symbol, and partial means it answered with less than was requested. An empty
// series is present AND partial — it answered, saying it has nothing.
type CoinalyzeSeriesAudit struct {
	Symbol  string
	Present bool
	Points  []CoinalyzeHistoryPoint
	Partial bool
	Gaps    []CoinalyzeGap
}

// CoinalyzeAuditHistory pairs each REQUESTED symbol with what the provider returned.
//
// The report has exactly one item per requested symbol, in the order requested — a report
// that spoke only of what came back would be the same silence under another name.
// coinalyzeCheckCoverage audits ONE requested symbol against the stamps that came back, and
// records what the coverage requirements demand.
//
// It exists separately because the long/short response has its OWN BODY (`r`, `l`, `s`
// instead of `o,h,l,c`) and does not fit the OHLC struct — but the audit does not look at
// the body, it looks at WHICH stamps came. Without this separation, a symbol omitted from
// long/short produces no warning, and a truncated window is not marked.
func coinalyzeCheckCoverage(log *slog.Logger, req CoinalyzeHistoryRequest, symbol string, stamps []int64, present bool) []CoinalyzeGap {
	log = logger.OrDiscard(log)
	if !present {
		log.Warn("coinalyze symbol absent from the history response",
			"symbol", symbol, "window_from", req.From.UTC(), "window_to", req.To.UTC())
		return nil
	}

	gaps := coinalyzeGapsOf(req, stamps)
	for _, gap := range gaps {
		// The gap is recorded WITH ITS INTERVAL, not as a bare "something was missing".
		log.Warn("coinalyze symbol partially covered",
			"symbol", symbol,
			"gap_from", gap.From.UTC(),
			"gap_to", gap.To.UTC(),
			"window_from", req.From.UTC(),
			"window_to", req.To.UTC())
	}
	return gaps
}

// coinalyzeStampsOf extracts the stamps of an OHLC series.
func coinalyzeStampsOf(points []CoinalyzeHistoryPoint) []int64 {
	stamps := make([]int64, len(points))
	for i, point := range points {
		stamps[i] = point.T
	}
	return stamps
}

// CoinalyzeLongShortAudit is the report for ONE symbol of the long/short response.
type CoinalyzeLongShortAudit struct {
	Symbol  string
	Present bool
	Points  []CoinalyzeLongShortPoint
	Partial bool
	Gaps    []CoinalyzeGap
}

// CoinalyzeAuditLongShort is the SAME audit, on the response that has its own body.
//
// The coverage requirements speak of "a history response", without excepting an endpoint.
// Long/short is one third of the cycle's history requests, and it was precisely the third
// nobody measured.
func CoinalyzeAuditLongShort(req CoinalyzeHistoryRequest, response []CoinalyzeLongShortSeries, logs ...*slog.Logger) []CoinalyzeLongShortAudit {
	log := logger.OrDiscard(logs...)
	// The index is by FIELD, never by position in the array.
	bySymbol := make(map[string]CoinalyzeLongShortSeries, len(response))
	for _, series := range response {
		bySymbol[series.Symbol] = series
	}

	audits := make([]CoinalyzeLongShortAudit, 0, len(req.Symbols))
	for _, symbol := range req.Symbols {
		series, present := bySymbol[symbol]
		stamps := make([]int64, len(series.History))
		for i, point := range series.History {
			stamps[i] = point.T
		}

		gaps := coinalyzeCheckCoverage(log, req, symbol, stamps, present)
		if !present {
			audits = append(audits, CoinalyzeLongShortAudit{Symbol: symbol})
			continue
		}
		audits = append(audits, CoinalyzeLongShortAudit{
			Symbol:  symbol,
			Present: true,
			Points:  series.History,
			Partial: len(gaps) > 0,
			Gaps:    gaps,
		})
	}
	return audits
}

// CoinalyzeAuditHistory audits a history response against the request that produced it.
func CoinalyzeAuditHistory(req CoinalyzeHistoryRequest, response []CoinalyzeHistorySeries, logs ...*slog.Logger) []CoinalyzeSeriesAudit {
	log := logger.OrDiscard(logs...)
	// The index is by FIELD, never by position in the array.
	bySymbol := make(map[string]CoinalyzeHistorySeries, len(response))
	for _, series := range response {
		bySymbol[series.Symbol] = series
	}

	audits := make([]CoinalyzeSeriesAudit, 0, len(req.Symbols))
	for _, symbol := range req.Symbols {
		series, present := bySymbol[symbol]
		gaps := coinalyzeCheckCoverage(log, req, symbol, coinalyzeStampsOf(series.History), present)
		if !present {
			audits = append(audits, CoinalyzeSeriesAudit{Symbol: symbol})
			continue
		}

		audits = append(audits, CoinalyzeSeriesAudit{
			Symbol:  symbol,
			Present: true,
			// A partial series STILL delivers what came: marking and discarding would be the
			// same loss with a label on top.
			Points:  series.History,
			Partial: len(gaps) > 0,
			Gaps:    gaps,
		})
	}

	return audits
}

// coinalyzeGapsOf compares the interval covered by the points with the requested interval.
//
// # THE ONE-INTERVAL TOLERANCE, AND WHY IT IS NOT LOOSENESS
//
// Coinalyze labels the START of the interval. In a daily series requested up to now, the
// last label is today 00:00 UTC — a bucket still filling, not a lost day. Without the
// tolerance, every daily series would be marked partial on every cycle, and a detector that
// always accuses, accuses nothing.
func coinalyzeGapsOf(req CoinalyzeHistoryRequest, stamps []int64) []CoinalyzeGap {
	if len(stamps) == 0 {
		// The provider answered for this symbol saying it has nothing in this window: the
		// whole window is the gap.
		return []CoinalyzeGap{{From: req.From, To: req.To}}
	}

	// The covered span comes from the VALUES, not from the array order: reading the first
	// and last elements as if they were the smallest and largest makes a descending series
	// look incomplete.
	earliest, latest := stamps[0], stamps[0]
	for _, stamp := range stamps[1:] {
		if stamp < earliest {
			earliest = stamp
		}
		if stamp > latest {
			latest = stamp
		}
	}

	first := time.Unix(earliest, 0).UTC()
	last := time.Unix(latest, 0).UTC()

	var gaps []CoinalyzeGap
	if first.Sub(req.From) > req.Interval {
		gaps = append(gaps, CoinalyzeGap{From: req.From, To: first})
	}
	if req.To.Sub(last) > req.Interval {
		gaps = append(gaps, CoinalyzeGap{From: last, To: req.To})
	}
	return gaps
}
