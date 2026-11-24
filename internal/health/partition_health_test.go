package health_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/inbound/httpapi"
	"github.com/marxms/go-whale/internal/health"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// TestHealthReportExposesPerPartitionRows proves /health reports one row per live feed
// partition instead of a fleet aggregate: a silent partition must be identifiable by id.
func TestHealthReportExposesPerPartitionRows(t *testing.T) {
	now := time.Now()
	rows := []health.PartitionHealth{
		{
			ID: 1, Symbols: 300, Frames: 1_200, LastFrameAgeMs: 250,
			Restores: 1, LastCloseAtMs: now.Add(-time.Second).UnixMilli(),
		},
		{ID: 2, Symbols: 301, Frames: 30, LastFrameAgeMs: 90_000, Reconnects: 2, SilentSymbols: 301},
	}
	report := health.NewAggregator(health.Probes{
		StartedAt: func() time.Time { return now.Add(-2 * health.StartupGrace) },
		Connections: func() []health.Connection {
			return []health.Connection{{Exchange: "binance", MarketType: "future", Connected: true, Healthy: true, Symbols: 2}}
		},
		Partitions: func() ([]health.PartitionHealth, bool) { return rows, true },
	}).Evaluate()

	if len(report.Partitions) != 2 {
		t.Fatalf("partitions = %+v, want one row per live partition", report.Partitions)
	}
	if report.Partitions[0].ID != 1 || report.Partitions[0].Symbols != 300 {
		t.Fatalf("first partition = %+v, want id=1 symbols=300", report.Partitions[0])
	}
	if report.Partitions[1].ID != 2 || report.Partitions[1].LastFrameAgeMs != 90_000 {
		t.Fatalf("second partition = %+v, want id=2 age=90000", report.Partitions[1])
	}
	// Reconnects e o último close fazem parte da linha, não só de um teste vizinho.
	if report.Partitions[0].LastCloseAtMs == 0 || report.Partitions[1].Reconnects != 2 {
		t.Fatalf("per-partition rows lost reconnects/last close: %+v", report.Partitions)
	}
	// Símbolos sem produtor por partição: a partição silenciosa carrega todos os seus.
	if report.Partitions[1].SilentSymbols != 301 || report.Partitions[0].SilentSymbols != 0 {
		t.Fatalf("silent symbols = %d/%d, want 0/301",
			report.Partitions[0].SilentSymbols, report.Partitions[1].SilentSymbols)
	}
	// Observational only: the silent-fraction verdict is a separate probe, so the row alone
	// must not turn the process unhealthy.
	if report.Status == "unhealthy" {
		t.Fatalf("status = %q, want the partition row not to decide the verdict", report.Status)
	}

	// The rows reach the wire payload under `partitions`.
	mc := httpapi.NewMetricsCollector(shapeHybridManager{stats: []streaming.ConnectionStats{{
		ExchangeID: "binance", MarketType: "future", Connected: true, Healthy: true, Symbols: 2,
	}}})
	mc.SetPartitions(func() ([]health.PartitionHealth, bool) { return rows, true })
	rec := httptest.NewRecorder()
	mc.Health(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("HTTP code = %d, want 200", rec.Code)
	}
	var payload struct {
		Partitions []health.PartitionHealth `json:"partitions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode /health: %v", err)
	}
	if len(payload.Partitions) != 2 || payload.Partitions[1].ID != 2 {
		t.Fatalf("payload partitions = %+v, want both rows", payload.Partitions)
	}
}

// TestHealthReportExposesPartitionFramesReconnects proves the per-partition counters and the
// newest close instant survive into the report.
func TestHealthReportExposesPartitionFramesReconnects(t *testing.T) {
	closeAt := time.Now().Add(-5 * time.Second).UnixMilli()
	report := health.NewAggregator(health.Probes{
		Partitions: func() ([]health.PartitionHealth, bool) {
			return []health.PartitionHealth{{
				ID: 7, Symbols: 12, Frames: 9_001, LastFrameAgeMs: 1_200,
				Reconnects: 3, Restores: 4, LastCloseAtMs: closeAt, SilentSymbols: 0,
				LastCloseCode: 1006, LastCloseCause: "transport_unattributed",
				LastCloseDialAgeMs: 90_000, LastCloseStreams: 12, LastCloseSisterAgeMs: 250,
			}}, true
		},
	}).Evaluate()

	if len(report.Partitions) != 1 {
		t.Fatalf("partitions = %+v, want one row", report.Partitions)
	}
	row := report.Partitions[0]
	if row.Frames != 9_001 || row.Reconnects != 3 || row.Restores != 4 {
		t.Fatalf("row = %+v, want frames/reconnects/restores preserved", row)
	}
	if row.LastCloseAtMs != closeAt {
		t.Fatalf("last close = %d, want %d", row.LastCloseAtMs, closeAt)
	}
	// A close is attributable per partition: code, cause, dial age, streams and sister health.
	if row.LastCloseCode != 1006 || row.LastCloseCause != "transport_unattributed" {
		t.Fatalf("close evidence = %d/%q, want 1006/transport_unattributed", row.LastCloseCode, row.LastCloseCause)
	}
	if row.LastCloseDialAgeMs != 90_000 || row.LastCloseStreams != 12 || row.LastCloseSisterAgeMs != 250 {
		t.Fatalf("close detail = %+v, want dial age, streams and sister age", row)
	}

	// No feed mounted: absence is reported as no rows, never as a fabricated partition.
	absent := health.NewAggregator(health.Probes{}).Evaluate()
	if len(absent.Partitions) != 0 {
		t.Fatalf("partitions without a probe = %+v, want none", absent.Partitions)
	}
}
