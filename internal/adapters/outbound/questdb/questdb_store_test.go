package questdb

import (
	"context"
	"errors"
	"strings"
	"testing"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// dedupCatalog assembles metadata in which both tables conform, so each test breaks EXACTLY
// one fact.
func dedupCatalog() *fakeTableCatalog {
	chaves := func(keys []string) map[string]bool {
		m := map[string]bool{}
		for _, k := range keys {
			m[k] = true
		}
		return m
	}
	return &fakeTableCatalog{
		facts: map[string]tableFacts{
			"candles":     {PartitionBy: "DAY", WAL: true, Dedup: true},
			"derivatives": {PartitionBy: "DAY", WAL: true, Dedup: true},
		},
		upsertKeys: map[string]map[string]bool{
			"candles":     chaves(requiredDedupKeys()),
			"derivatives": chaves(derivativesDedupKeys()),
		},
	}
}

// TestVerifyDedup_CobreTodaTabelaQueOBinarioEscreve — the verification must cover both tables.
//
// Checking only `q.tableName`, which answers `candles`, leaves `derivatives` — the table this
// line of work grows — with its DEDUP unchecked by anything. A table without DEDUP does not
// refuse writes: it DUPLICATES, and the duplicate only shows up in what the series sums.
func TestVerifyDedup_CobreTodaTabelaQueOBinarioEscreve(t *testing.T) {
	fake := dedupCatalog()
	store := &Store{tableName: "candles", catalog: fake}

	if err := store.VerifyDedup(context.Background()); err != nil {
		t.Fatalf("two conforming tables refused: %v", err)
	}

	conferidas := map[string]bool{}
	for _, tabela := range fake.keyReads {
		conferidas[tabela] = true
	}
	for _, querida := range []string{"candles", "derivatives"} {
		if !conferidas[querida] {
			t.Errorf("the DEDUP of %q was not checked (checked: %v)", querida, fake.keyReads)
		}
	}
}

// TestVerifyDedup_DedupDesligadoEmDerivativesReprova — the outcome the widened coverage exists
// to produce. It was invisible before.
func TestVerifyDedup_DedupDesligadoEmDerivativesReprova(t *testing.T) {
	fake := dedupCatalog()
	fake.facts["derivatives"] = tableFacts{PartitionBy: "DAY", WAL: true, Dedup: false}
	store := &Store{tableName: "candles", catalog: fake}

	err := store.VerifyDedup(context.Background())
	if err == nil {
		t.Fatal("`derivatives` without DEDUP passed as verified")
	}
	if !strings.Contains(err.Error(), "derivatives") {
		t.Errorf("the error does not name the table: %v", err)
	}
	if !strings.Contains(err.Error(), "DEDUP disabled") {
		t.Errorf("the error does not say what was read: %v", err)
	}
}

// TestVerifyDedup_DerivativesExigeSourceNaChave — the key of `derivatives` is NOT that of
// `candles`. `source` is in it because Coinalyze and Binance write the same bucket of the same
// symbol; without that term one provenance silently overwrites the other. Coverage that reused
// the `candles` key would approve exactly that state.
func TestVerifyDedup_DerivativesExigeSourceNaChave(t *testing.T) {
	fake := dedupCatalog()
	semSource := map[string]bool{}
	for _, k := range requiredDedupKeys() { // the `candles` key, which has no `source`
		semSource[k] = true
	}
	fake.upsertKeys["derivatives"] = semSource
	store := &Store{tableName: "candles", catalog: fake}

	err := store.VerifyDedup(context.Background())
	if err == nil {
		t.Fatal("`derivatives` without `source` in the UPSERT key passed as verified")
	}
	if !strings.Contains(err.Error(), "source") {
		t.Errorf("the error does not name the missing key: %v", err)
	}
}

// TestVerifyDedup_SemLinhaEhErroENaoSucesso — an absent row must fail the check.
//
// Returning `nil` on `pgx.ErrNoRows` means: the table does not exist, the verification
// APPROVES, ILP creates it with whatever shape it infers, and the idempotency everything else
// depends on never existed. Under `--components 3` — the most used mode — the sidecar that
// would create it does not run. Absence is the worst outcome, not the most benign.
func TestVerifyDedup_SemLinhaEhErroENaoSucesso(t *testing.T) {
	fake := dedupCatalog()
	fake.absent = map[string]bool{"derivatives": true}
	store := &Store{tableName: "candles", catalog: fake}

	err := store.VerifyDedup(context.Background())
	if err == nil {
		t.Fatal("the query without a row passed as a successful verification")
	}
	if !strings.Contains(err.Error(), "derivatives") {
		t.Errorf("the error does not name the absent table: %v", err)
	}
	if !strings.Contains(err.Error(), "NO ROW") {
		t.Errorf("the error does not say the query returned no row: %v", err)
	}
}

// TestVerifyDedup_MetadataIlegivelSegueNaoFatal — the fail-open that STILL applies: a column
// renamed by a QuestDB version asserts nothing about the table, and disabling the cold tier
// because of it would be a sentence without proof.
func TestVerifyDedup_MetadataIlegivelSegueNaoFatal(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	fake := dedupCatalog()
	fake.readErr = errors.New(`column "dedup" does not exist`)
	store := &Store{log: log, tableName: "candles", catalog: fake}

	if err := store.VerifyDedup(context.Background()); err != nil {
		t.Fatalf("unreadable metadata became a failure: %v", err)
	}

	var declared bool
	for _, record := range sink.Records() {
		if strings.Contains(record.Text(), "metadata unreadable") {
			declared = true
		}
	}
	if !declared {
		t.Fatalf("the non-verification was not declared: %v", sink.Records())
	}
}

// TestVerifyDedup_ChavesIlegiveisNaoViramSucessoSilencioso — the keys could not be read, WAL
// and DEDUP are on, boot continues — but with its own WARN, not with a clean `nil` that is
// confused with "verified".
func TestVerifyDedup_ChavesIlegiveisNaoViramSucessoSilencioso(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	fake := dedupCatalog()
	fake.keysErr = errors.New("permission denied on table_columns()")
	store := &Store{log: log, tableName: "candles", catalog: fake}

	if err := store.VerifyDedup(context.Background()); err != nil {
		t.Fatalf("unreadable keys became a failure: %v", err)
	}

	var declared bool
	for _, record := range sink.Records() {
		if strings.Contains(record.Text(), "upsert keys unverified") {
			declared = true
		}
	}
	if !declared {
		t.Fatalf("the skipped verification was not declared: %v", sink.Records())
	}
}

// TestVerifyDedup_WalDesligadoVenceAsChavesIlegiveis — `validateDedupMeta`'s precedence over the
// unreadable-keys path: WAL disabled is a READ fact and fails the check even when the key set
// could not be verified.
func TestVerifyDedup_WalDesligadoVenceAsChavesIlegiveis(t *testing.T) {
	fake := dedupCatalog()
	fake.keysErr = errors.New("permission denied on table_columns()")
	fake.facts["candles"] = tableFacts{PartitionBy: "DAY", WAL: false, Dedup: true}
	store := &Store{tableName: "candles", catalog: fake}

	err := store.VerifyDedup(context.Background())
	if err == nil {
		t.Fatal("WAL disabled passed because the keys could not be read")
	}
	if !strings.Contains(err.Error(), "WAL disabled") {
		t.Errorf("the error does not say what was read: %v", err)
	}
}
