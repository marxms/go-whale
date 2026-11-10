package questdb

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// fakeTableCatalog records EVERY emitted statement and lets the test decide what `tables()`
// answers. The record is the proof that no ALTER is emitted: the seam accepts any DDL,
// including an `ALTER`, so its absence is measured and not presumed.
type fakeTableCatalog struct {
	statements []string
	facts      map[string]tableFacts
	absent     map[string]bool
	execErr    error
	readErr    error
	reads      []string

	// upsertKeys and keysErr serve the DEDUP verification, which reads the SAME metadata
	// through the same seam. See verifyTableDedup.
	upsertKeys map[string]map[string]bool
	keysErr    error
	keyReads   []string
}

func (f *fakeTableCatalog) execDDL(_ context.Context, ddl string) error {
	f.statements = append(f.statements, ddl)
	return f.execErr
}

func (f *fakeTableCatalog) readTableFacts(_ context.Context, table string) (tableFacts, bool, error) {
	f.reads = append(f.reads, table)
	if f.readErr != nil {
		return tableFacts{}, false, f.readErr
	}
	if f.absent[table] {
		return tableFacts{}, false, nil
	}
	facts, ok := f.facts[table]
	return facts, ok, nil
}

func (f *fakeTableCatalog) readUpsertKeys(_ context.Context, table string) (map[string]bool, error) {
	f.keyReads = append(f.keyReads, table)
	if f.keysErr != nil {
		return nil, f.keysErr
	}
	return f.upsertKeys[table], nil
}

func conformingCatalog(table string) *fakeTableCatalog {
	return &fakeTableCatalog{
		facts: map[string]tableFacts{
			table: {PartitionBy: "DAY", WAL: true, Dedup: true},
		},
	}
}

// TestEnsureTable_ReleOContratoDepoisDoCreate — the mechanism emits the `CREATE` and RE-READS
// `tables()` — in that order —, and a conforming table passes.
//
// THE ORDER IS THE PROPERTY. Reading before the `CREATE` would answer about the table that
// existed, not the one the mechanism guarantees.
func TestEnsureTable_ReleOContratoDepoisDoCreate(t *testing.T) {
	fake := conformingCatalog("candles")

	if err := ensureTable(context.Background(), fake, candlesTableContract("candles")); err != nil {
		t.Fatalf("conforming table refused: %v", err)
	}

	if len(fake.statements) != 1 {
		t.Fatalf("%d statement(s) emitted (%v), wanted 1 `CREATE TABLE`", len(fake.statements), fake.statements)
	}
	if !strings.Contains(fake.statements[0], "CREATE TABLE IF NOT EXISTS candles") {
		t.Fatalf("the emitted statement is not the declared CREATE: %q", fake.statements[0])
	}
	if len(fake.reads) != 1 || fake.reads[0] != "candles" {
		t.Fatalf("tables() reads: %v — wanted exactly one, of `candles`", fake.reads)
	}
}

// TestEnsureTable_ParticaoDivergenteRecusaESemAlter — the case an ILP-born table produces:
// `CREATE TABLE IF NOT EXISTS` returns a NIL error over it while `PARTITION BY` is already
// wrong and is immutable.
func TestEnsureTable_ParticaoDivergenteRecusaESemAlter(t *testing.T) {
	fake := conformingCatalog("candles")
	fake.facts["candles"] = tableFacts{PartitionBy: "MONTH", WAL: true, Dedup: true}

	err := ensureTable(context.Background(), fake, candlesTableContract("candles"))
	if err == nil {
		t.Fatal("divergent partition passed as success — it is the silent success the mechanism exists to kill")
	}
	if !errors.Is(err, ErrSchemaContract) {
		t.Fatalf("the divergence was not classified as contract: %v", err)
	}
	for _, esperado := range []string{"candles", "partitionBy", "DAY", "MONTH"} {
		if !strings.Contains(err.Error(), esperado) {
			t.Errorf("the error does not name %q: %v", esperado, err)
		}
	}
	if !strings.Contains(err.Error(), "human decision") {
		t.Errorf("the error does not declare the divergence for a human decision: %v", err)
	}

	// NO `ALTER` to converge. The seam would accept one.
	for _, s := range fake.statements {
		if strings.Contains(strings.ToUpper(s), "ALTER") {
			t.Fatalf("the mechanism tried to converge the partition with ALTER: %q", s)
		}
	}
	if len(fake.statements) != 1 {
		t.Fatalf("%d statement(s) emitted (%v), wanted only the CREATE", len(fake.statements), fake.statements)
	}
}

// TestEnsureTable_WalEDedupDivergentesRecusam — the contract's other two fields. Without WAL
// there is no DEDUP, and without DEDUP every re-seed DUPLICATES instead of overwriting.
func TestEnsureTable_WalEDedupDivergentesRecusam(t *testing.T) {
	casos := []struct {
		nome  string
		lido  tableFacts
		campo string
	}{
		{"wal disabled", tableFacts{PartitionBy: "DAY", WAL: false, Dedup: true}, "walEnabled"},
		{"dedup disabled", tableFacts{PartitionBy: "DAY", WAL: true, Dedup: false}, "dedup"},
	}
	for _, caso := range casos {
		t.Run(caso.nome, func(t *testing.T) {
			fake := conformingCatalog("candles")
			fake.facts["candles"] = caso.lido

			err := ensureTable(context.Background(), fake, candlesTableContract("candles"))
			if err == nil {
				t.Fatalf("%s passed as success", caso.nome)
			}
			if !errors.Is(err, ErrSchemaContract) {
				t.Fatalf("not classified as contract: %v", err)
			}
			if !strings.Contains(err.Error(), caso.campo) {
				t.Errorf("the error does not name field %q: %v", caso.campo, err)
			}
		})
	}
}

// TestEnsureTable_AusenciaEmTablesNaoEhAprovacao — the `CREATE` was accepted and `tables()`
// does not know the table. It is the same trap as `pgx.ErrNoRows` becoming `nil`: absence of a
// row is not proof of conformity.
func TestEnsureTable_AusenciaEmTablesNaoEhAprovacao(t *testing.T) {
	fake := &fakeTableCatalog{absent: map[string]bool{"candles": true}}

	err := ensureTable(context.Background(), fake, candlesTableContract("candles"))
	if err == nil {
		t.Fatal("tables() without a row passed as a verified contract")
	}
	if !errors.Is(err, ErrSchemaContract) {
		t.Fatalf("absence after CREATE is not contract: %v", err)
	}
	if !strings.Contains(err.Error(), "did not return a row") {
		t.Errorf("the error does not say what was read: %v", err)
	}
}

// TestEnsureTables_CobreAsDuasTabelasQueOBinarioEscreve — `derivatives` had never had its
// contract checked by anything, and it is the second table this binary writes.
func TestEnsureTables_CobreAsDuasTabelasQueOBinarioEscreve(t *testing.T) {
	fake := &fakeTableCatalog{facts: map[string]tableFacts{
		"candles":     {PartitionBy: "DAY", WAL: true, Dedup: true},
		"derivatives": {PartitionBy: "DAY", WAL: true, Dedup: true},
	}}
	store := &Store{tableName: "candles", catalog: fake}

	if err := store.EnsureTables(context.Background()); err != nil {
		t.Fatalf("two conforming tables refused: %v", err)
	}

	lidas := map[string]bool{}
	for _, t := range fake.reads {
		lidas[t] = true
	}
	for _, querida := range []string{"candles", "derivatives"} {
		if !lidas[querida] {
			t.Errorf("the contract of %q was not checked (read: %v)", querida, fake.reads)
		}
	}
}

// TestEnsureTables_DivergenciaDeDerivativesSobe — the new table cannot be the one that goes
// unnoticed, which is exactly what happened with its DEDUP.
func TestEnsureTables_DivergenciaDeDerivativesSobe(t *testing.T) {
	fake := &fakeTableCatalog{facts: map[string]tableFacts{
		"candles":     {PartitionBy: "DAY", WAL: true, Dedup: true},
		"derivatives": {PartitionBy: "NONE", WAL: true, Dedup: true},
	}}
	store := &Store{tableName: "candles", catalog: fake}

	err := store.EnsureTables(context.Background())
	if err == nil {
		t.Fatal("divergent partition in `derivatives` passed as success")
	}
	if !errors.Is(err, ErrSchemaContract) {
		t.Fatalf("not classified as contract: %v", err)
	}
	if !strings.Contains(err.Error(), "derivatives") {
		t.Errorf("the error does not name the table: %v", err)
	}
}

// TestEnsureTables_TablesIlegivelNaoRecusaOArranque — the SEPARATION of the two classes. Being
// unable to read asserts nothing wrong about the table, and turning a cold-tier unavailability
// into a service that does not come up would have no way back.
func TestEnsureTables_TablesIlegivelNaoRecusaOArranque(t *testing.T) {
	fake := &fakeTableCatalog{readErr: errors.New("connection refused")}
	store := &Store{tableName: "candles", catalog: fake}

	if err := store.EnsureTables(context.Background()); err != nil {
		t.Fatalf("unavailability became a contract refusal: %v", err)
	}
}

// TestTableContracts_DeclaramParticaoWalEDedup pins the DECLARED values against the sidecar
// DDL (`scripts/questdb/questdb-init.sql:23-25` and `:129-131`). A declared value derived from
// the read one would make the comparison a tautology.
func TestTableContracts_DeclaramParticaoWalEDedup(t *testing.T) {
	contratos := tableContractsFor("candles")
	if len(contratos) != 2 {
		t.Fatalf("%d contract(s) declared, wanted 2 (candles and derivatives)", len(contratos))
	}
	for _, c := range contratos {
		if c.partitionBy != "DAY" {
			t.Errorf("%s: partitionBy declared %q, the DDL says DAY", c.name, c.partitionBy)
		}
		if !c.wal || !c.dedup {
			t.Errorf("%s: wal=%v dedup=%v, the DDL declares both", c.name, c.wal, c.dedup)
		}
		if !strings.Contains(c.ddl, "PARTITION BY DAY") || !strings.Contains(c.ddl, "DEDUP UPSERT KEYS") {
			t.Errorf("%s: the contract DDL does not create the table with the shape it declares:\n%s", c.name, c.ddl)
		}
	}
}
