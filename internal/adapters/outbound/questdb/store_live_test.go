package questdb

import (
	"context"
	"testing"
	"time"

	questdb "github.com/questdb/go-questdb-client/v3"

	"github.com/marxms/go-whale/internal/candle"
)

// The live candle round trip is the other half of "written AND readable". The unit tests
// prove the decisions; only the engine proves that ILP and pgwire agree on every declared
// column — the SELECT list and the Scan targets are positional, so a column added on one
// side and not the other stays green in every fake.
const liveCandleProbeTable = "candles_roundtrip_live_probe"

func TestCandleRoundTripLive_EveryDeclaredColumnIsWrittenAndReadBack(t *testing.T) {
	pool := liveQuestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// The probe table carries the SAME contract as `candles`: DAY, WAL, DEDUP on the five
	// identity keys. A table created any other way would not exercise the production shape.
	if _, err := pool.Exec(ctx, candlesTableContract(liveCandleProbeTable).ddl); err != nil {
		t.Fatalf("probe CREATE TABLE: %v", err)
	}
	t.Cleanup(func() {
		limpeza, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelar()
		if _, err := pool.Exec(limpeza, "DROP TABLE IF EXISTS "+liveCandleProbeTable); err != nil {
			t.Errorf("the probe left residue: DROP TABLE failed: %v", err)
		}
	})

	sender, err := questdb.LineSenderFromConf(ctx, candleILPConfig(liveILPAddr(t)))
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	store := &Store{
		ilpSender:  sender,
		ilpConfStr: candleILPConfig(liveILPAddr(t)),
		pgPool:     pool,
		tableName:  liveCandleProbeTable,
		ctx:        ctx,
		cancel:     cancel,
		lastFlush:  time.Now(),
	}
	t.Cleanup(func() { _ = sender.Close(context.Background()) })

	bucket := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	key, err := candle.SeriesKeyOf("binance", "future", "RT/PROBE", "1h")
	if err != nil {
		t.Fatalf("SeriesKeyOf: %v", err)
	}
	want := candle.Candle{
		Timestamp:       bucket.UnixMilli(),
		Open:            100.5,
		High:            101.25,
		Low:             99.75,
		Close:           100.125,
		Volume:          42.5,
		TakerBuyVolume:  30.25,
		TakerSellVolume: 12.25,
	}
	if err := store.WriteBatches(ctx, map[candle.SeriesKey][]candle.Candle{key: {want}}); err != nil {
		t.Fatalf("WriteBatches: %v", err)
	}

	// ILP acknowledges ingestion, not WAL application. The production path waits on the
	// same barrier; this test must not paper over that asynchrony with a fixed sleep.
	target, err := store.CaptureApplyTarget(ctx)
	if err != nil {
		t.Fatalf("CaptureApplyTarget: %v", err)
	}
	if err := store.WaitApplied(ctx, target); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}

	got, err := store.QueryRange(ctx, "binance", "future", "RT/PROBE", "1h",
		bucket.Add(-time.Hour), bucket.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("%d row(s) read, wanted 1 — the written row is not reachable through the read", len(got))
	}
	row := got[0]
	for _, caso := range []struct {
		nome      string
		got, want float64
	}{
		{"open", row.Open, want.Open},
		{"high", row.High, want.High},
		{"low", row.Low, want.Low},
		{"close", row.Close, want.Close},
		{"volume", row.Volume, want.Volume},
		{"taker_buy_volume", row.TakerBuyVolume, want.TakerBuyVolume},
		{"taker_sell_volume", row.TakerSellVolume, want.TakerSellVolume},
	} {
		if caso.got != caso.want {
			t.Errorf("%s = %v, wanted %v — column declared and not returned by the read", caso.nome, caso.got, caso.want)
		}
	}
	if row.Timestamp != want.Timestamp {
		t.Errorf("timestamp = %d, wanted %d", row.Timestamp, want.Timestamp)
	}
}
