package questdb

import (
	"context"
	"os"
	"testing"
	"time"

	questdb "github.com/questdb/go-questdb-client/v3"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/storetest"
	"github.com/marxms/go-whale/internal/derivatives"
	derivstoretest "github.com/marxms/go-whale/internal/derivatives/storetest"
)

// These suites are the "real engine" half: only QuestDB proves ILP/PGX
// agreement on column order and type, the DAY/WAL/DEDUP contract and the
// ascending/descending order each read promises.
//
// They REFUSE to run unless the operator also declares the target disposable.
// The shared LAN databases must never be reached by a suite that drops probe
// tables; the extra variable is the difference between a test and an incident.
func requireDisposableQuestDB(t *testing.T) {
	t.Helper()
	if os.Getenv("QDB_LIVE_DISPOSABLE") != "1" {
		t.Skip("QDB_LIVE_DISPOSABLE != 1 — the suites create and drop tables; only run against a disposable QuestDB")
	}
}

// liveStore builds a real Store over the live pool/sender with a probe table.
func liveStore(t *testing.T, table, probeDDL string, dropAtStart bool) *Store {
	t.Helper()
	pool := liveQuestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	if dropAtStart {
		if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("DROP TABLE %s: %v", table, err)
		}
	}
	if _, err := pool.Exec(ctx, probeDDL); err != nil {
		t.Fatalf("CREATE TABLE %s: %v", table, err)
	}
	t.Cleanup(func() {
		limpeza, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelar()
		if _, err := pool.Exec(limpeza, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Errorf("the probe left residue: DROP TABLE %s failed: %v", table, err)
		}
	})

	sender, err := questdb.LineSenderFromConf(ctx, candleILPConfig(liveILPAddr(t)))
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	t.Cleanup(func() { _ = sender.Close(context.Background()) })

	return &Store{
		ilpSender:  sender,
		ilpConfStr: candleILPConfig(liveILPAddr(t)),
		pgPool:     pool,
		tableName:  table,
		ctx:        ctx,
		cancel:     cancel,
		lastFlush:  time.Now(),
		priors:     emptyPriorSource{},
	}
}

// syncedCandleStore waits on the WAL apply barrier after every candle batch.
// ILP acknowledges ingestion, not application; without this the suite would
// race its own reads and could pass or fail by timing.
type syncedCandleStore struct{ *Store }

func (s syncedCandleStore) WriteBatches(ctx context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	if err := s.Store.WriteBatches(ctx, groups); err != nil {
		return err
	}
	target, err := s.CaptureApplyTarget(ctx)
	if err != nil {
		return err
	}
	return s.WaitApplied(ctx, target)
}

const candleStoretestTable = "candles_contract_t30"

// TestQuestDBCold_SatisfiesTheCandleContractSuite proves the durable candle
// port against QuestDB 8.2.3: ILP write, PGX read, DEDUP and descending range.
func TestQuestDBCold_SatisfiesTheCandleContractSuite(t *testing.T) {
	requireDisposableQuestDB(t)
	store := liveStore(t, candleStoretestTable, candlesTableContract(candleStoretestTable).ddl, true)

	storetest.RunCold(t, func(_ *testing.T) storetest.ColdStore {
		return syncedCandleStore{Store: store}
	})
}

// syncedDerivativesStore waits on the WAL apply barrier after a derivatives
// batch. The production writer does not wait (the poller's next cycle does not
// depend on the row being readable immediately); the suite does.
type syncedDerivativesStore struct{ *Store }

func (s syncedDerivativesStore) WriteDerivativesBatch(ctx context.Context, points []derivatives.DerivativesColdPoint) error {
	if err := s.Store.WriteDerivativesBatch(ctx, points); err != nil {
		return err
	}
	target, err := s.CaptureApplyTarget(ctx)
	if err != nil {
		return err
	}
	return s.WaitApplied(ctx, target)
}

// TestQuestDBCold_SatisfiesTheDerivativesContractSuite proves every durable
// derivatives column, the source partition and DEDUP against QuestDB 8.2.3.
func TestQuestDBCold_SatisfiesTheDerivativesContractSuite(t *testing.T) {
	requireDisposableQuestDB(t)
	pool := liveQuestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+derivativesTableName); err != nil {
		t.Fatalf("DROP TABLE %s: %v", derivativesTableName, err)
	}
	if _, err := pool.Exec(ctx, derivativesTableContract().ddl); err != nil {
		t.Fatalf("CREATE TABLE %s: %v", derivativesTableName, err)
	}
	t.Cleanup(func() {
		limpeza, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelar()
		if _, err := pool.Exec(limpeza, "DROP TABLE IF EXISTS "+derivativesTableName); err != nil {
			t.Errorf("the probe left residue: DROP TABLE %s failed: %v", derivativesTableName, err)
		}
	})
	if err := ensureColumns(ctx, pgTableSchema{pool: pool}, derivativesTableName,
		derivativesRequiredColumns(), derivativesSchemaSettle()); err != nil {
		t.Fatalf("durable columns: %v", err)
	}

	sender, err := questdb.LineSenderFromConf(ctx, candleILPConfig(liveILPAddr(t)))
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	t.Cleanup(func() { _ = sender.Close(context.Background()) })
	store := &Store{
		ilpSender:  sender,
		ilpConfStr: candleILPConfig(liveILPAddr(t)),
		pgPool:     pool,
		// The WAL barrier reads wal_tables() by q.tableName. Derivatives rows go
		// to the constant table, so the probe must point the metadata query there.
		tableName: derivativesTableName,
		ctx:       ctx,
		cancel:    cancel,
		lastFlush: time.Now(),
		priors:    emptyPriorSource{},
	}

	derivstoretest.RunCold(t, func(_ *testing.T) derivstoretest.ColdStore {
		return syncedDerivativesStore{Store: store}
	})
}
