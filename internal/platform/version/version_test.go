package version

import (
	"strings"
	"testing"
)

func TestCurrentKeepsLinkedVersion(t *testing.T) {
	old := Version
	Version = "0.1.0"
	defer func() { Version = old }()

	if got := Current().Version; got != "0.1.0" {
		t.Fatalf("Current().Version = %q, want the linker-stamped 0.1.0", got)
	}
}

func TestCurrentCarriesVersionAndCommit(t *testing.T) {
	info := Current()

	if info.Version != "devel" {
		t.Fatalf("test build version = %q, want devel without linker flags", info.Version)
	}
	if info.Commit == "" {
		t.Fatal("commit is empty")
	}
	if info.Date == "" {
		t.Fatal("build date is empty")
	}
	if got := info.String(); !strings.Contains(got, "gowhale "+info.Version) {
		t.Fatalf("String() = %q, want the product and version", got)
	}
}
