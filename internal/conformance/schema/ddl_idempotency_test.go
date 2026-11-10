package schema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var (
	createTable = regexp.MustCompile(`(?is)CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([A-Za-z_]\w*)\s*\((.*?)\)\s*(.*?);`)
	addColumn   = regexp.MustCompile(`(?is)ALTER\s+TABLE\s+[A-Za-z_]\w*\s+ADD\s+COLUMN\s+(IF\s+NOT\s+EXISTS\s+)?`)
	partitionBy = regexp.MustCompile(`(?i)PARTITION\s+BY\s+(\w+)`)
	upsertKeys  = regexp.MustCompile(`(?i)DEDUP\s+UPSERT\s+KEYS\s*\(([^)]*)\)`)
)

const (
	ddlPath      = "scripts/questdb/questdb-init.sql"
	expectedPath = "scripts/questdb/questdb-expected-schema.json"
)

type expectedTable struct {
	PartitionBy string   `json:"partitionBy"`
	WALEnabled  bool     `json:"walEnabled"`
	Dedup       bool     `json:"dedup"`
	UpsertKeys  []string `json:"upsertKeys"`
}

type expectedSchema struct {
	Tables map[string]expectedTable `json:"tables"`
}

type sqlTable struct {
	PartitionBy string
	WALEnabled  bool
	Dedup       bool
	UpsertKeys  []string
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("repository root: %v", err)
	}
	return root
}

func readFile(t *testing.T, rel string) string {
	t.Helper()
	//nolint:gosec // G304: the path is a fixed fixture under the repository root.
	content, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(content)
}

func parseSQL(t *testing.T, rel string) map[string]sqlTable {
	t.Helper()
	tables := map[string]sqlTable{}
	for _, match := range createTable.FindAllStringSubmatch(readFile(t, rel), -1) {
		name := match[2]
		tail := match[4]
		table := sqlTable{
			WALEnabled: regexp.MustCompile(`(?i)\bWAL\b`).MatchString(tail),
			Dedup:      regexp.MustCompile(`(?i)\bDEDUP\b`).MatchString(tail),
		}
		if partition := partitionBy.FindStringSubmatch(tail); partition != nil {
			table.PartitionBy = strings.ToUpper(partition[1])
		}
		if keys := upsertKeys.FindStringSubmatch(tail); keys != nil {
			for _, key := range strings.Split(keys[1], ",") {
				if key = strings.TrimSpace(strings.ToLower(key)); key != "" {
					table.UpsertKeys = append(table.UpsertKeys, key)
				}
			}
		}
		sort.Strings(table.UpsertKeys)
		tables[name] = table
	}
	return tables
}

func loadExpected(t *testing.T, rel string) expectedSchema {
	t.Helper()
	var expected expectedSchema
	if err := json.Unmarshal([]byte(readFile(t, rel)), &expected); err != nil {
		t.Fatalf("decode %s: %v", rel, err)
	}
	return expected
}

func TestDDLIdempotencyMarkers(t *testing.T) {
	content := readFile(t, ddlPath)
	for _, match := range createTable.FindAllStringSubmatch(content, -1) {
		if strings.TrimSpace(match[1]) == "" {
			t.Errorf("%s: CREATE TABLE %s lacks IF NOT EXISTS", ddlPath, match[2])
		}
	}
	for _, match := range addColumn.FindAllStringSubmatch(content, -1) {
		if strings.TrimSpace(match[1]) == "" {
			t.Errorf("%s: ALTER TABLE ... ADD COLUMN lacks IF NOT EXISTS", ddlPath)
		}
	}
}

func TestDDLExpectedSchemaContracts(t *testing.T) {
	tables := parseSQL(t, ddlPath)
	expected := loadExpected(t, expectedPath)
	if len(expected.Tables) == 0 {
		t.Fatalf("%s: no table declared", expectedPath)
	}
	for name, want := range expected.Tables {
		got, ok := tables[name]
		if !ok {
			t.Errorf("%s: %s is declared and absent from the DDL", expectedPath, name)
			continue
		}
		if got.PartitionBy != want.PartitionBy {
			t.Errorf("%s.%s.partitionBy: DDL=%q JSON=%q", expectedPath, name, got.PartitionBy, want.PartitionBy)
		}
		if got.WALEnabled != want.WALEnabled {
			t.Errorf("%s.%s.walEnabled: DDL=%v JSON=%v", expectedPath, name, got.WALEnabled, want.WALEnabled)
		}
		if got.Dedup != want.Dedup {
			t.Errorf("%s.%s.dedup: DDL=%v JSON=%v", expectedPath, name, got.Dedup, want.Dedup)
		}
		wantKeys := make([]string, len(want.UpsertKeys))
		for index, key := range want.UpsertKeys {
			wantKeys[index] = strings.ToLower(key)
		}
		sort.Strings(wantKeys)
		if strings.Join(got.UpsertKeys, ",") != strings.Join(wantKeys, ",") {
			t.Errorf("%s.%s.upsertKeys: DDL=%v JSON=%v", expectedPath, name, got.UpsertKeys, wantKeys)
		}
	}
	for name := range tables {
		if _, ok := expected.Tables[name]; !ok {
			t.Errorf("%s: %s is created by the DDL and absent from the expected JSON", ddlPath, name)
		}
	}
}

func TestDDLTablesCoverage(t *testing.T) {
	expected := loadExpected(t, expectedPath)
	for _, name := range []string{"candles", "derivatives"} {
		if _, ok := expected.Tables[name]; !ok {
			t.Errorf("GoWhale schema lacks %s", name)
		}
	}
}
