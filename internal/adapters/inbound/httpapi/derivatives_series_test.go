package httpapi

// This handler is exercised by the derivatives envelope suite only; the route was
// removed from the public surface. Keeping it in a _test.go file removes it from the
// production binary without losing the envelope coverage.
import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/marxms/go-whale/internal/derivatives"
)

// GetDerivativesSeries returns historical derivatives snapshots.
// GET /api/derivatives/{symbol}/series?exchange=&marketType=&limit=&timeframe=
//
// Reads directly from the per-timeframe ZSET (identical to candle pattern).
// No aggregation — data is already stored at the correct timeframe resolution.
func (h *MarketDataHandler) GetDerivativesSeries(w http.ResponseWriter, r *http.Request) {
	symbol := decodeSymbol(chi.URLParam(r, "symbol"))
	exchange := r.URL.Query().Get("exchange")
	marketType := r.URL.Query().Get("marketType")

	if exchange == "" {
		exchange = h.defaultExchange
	}
	if marketType == "" {
		marketType = h.defaultMarketType
	}

	if symbol == "" {
		writeJSON(h.log, w, http.StatusBadRequest, map[string]string{responseKeyError: errMsgSymbolRequired})
		return
	}

	limit := parseLimit(r, 800)

	timeframe := r.URL.Query().Get("timeframe")
	if timeframe == "" {
		timeframe = h.tfReg.Default()
	}

	// THE WINDOW CAN GO PAST THE ZSET HORIZON, and that is what the durable tier exists for.
	//
	// The ZSET keeps one cap per timeframe; on the daily that is thirty days, because that
	// is what the existing source retains. The second provenance serves 1,200 points of the
	// same daily, and they live in the durable table — serving only the ZSET would hand the
	// cap back and the whole deep-window feature would be invisible to the user, which is the
	// defect the independent verification caught (the reader existed and had no caller).
	//
	// A NIL `h.derivWindow` is declared degradation: without QuestDB, the ZSET answers
	// alone, exactly as before.
	snapshots, baseline, cached, err := h.derivativesReadingSeries(r.Context(), exchange, marketType, symbol, timeframe, limit)
	if err != nil {
		writeJSON(h.log, w, http.StatusInternalServerError, map[string]interface{}{
			responseKeyStatus: "error",
			responseKeyError:  err.Error(),
		})
		return
	}

	// The read path does not post-process:
	//
	// 1. It does NOT compute oiChangePct from the NOTIONAL field, i.e. from contracts ×
	// price — a column that measures price wearing another name.
	// 2. It does NOT forward-fill fundingRate down the series so every bucket carries a
	// number. Funding is a step function known only at settlement; filling it turns
	// "not measured here" into "measured here", which is the exact substitution the
	// integrity spec forbids.
	//
	// DECLARED CONSEQUENCE: fundingRate is absent in the overwhelming majority of buckets —
	// present in live buckets, where the poller writes the rate in force, and in backfilled
	// buckets that coincide with a settlement. Absence is on the wire as absence; consumers
	// must render it as such and must not read it as zero.
	// See declareProvenance. The family read here has a single writer.
	snapshots = declareProvenance(snapshots, derivatives.SourceVenue)

	points := make([]DerivativesSeriesPoint, 0, len(snapshots))
	for _, snap := range snapshots {
		if snap == nil {
			continue
		}
		sourceTimestamp := snap.ObservedAt
		if sourceTimestamp <= 0 {
			sourceTimestamp = snap.Timestamp
		}
		points = append(points, DerivativesSeriesPoint{
			Timestamp:       snap.Timestamp,
			SourceTimestamp: sourceTimestamp,
			Snapshot:        snap,
		})
	}

	reading := h.readingForSeries(r.Context(), exchange, marketType, symbol, timeframe, snapshots, baseline, cached)
	writeJSON(h.log, w, http.StatusOK, DerivativesSeriesResponse{
		Status:      "ok",
		Symbol:      symbol,
		Exchange:    exchange,
		MarketType:  marketType,
		Timeframe:   timeframe,
		Count:       len(points),
		Data:        points,
		Funding:     deriveFunding(snapshots),
		Normalized:  reading,
		Measurement: h.measurementDeclaration(timeframe),
		Coverage:    newSeriesCoverage(limit, len(points), points),
	})
}
