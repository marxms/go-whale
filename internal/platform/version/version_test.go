package version

import (
	"strings"
	"testing"
)

func TestCurrentCarriesVersionAndCommit(t *testing.T) {
	info := Current()

	if info.Version == "" {
		t.Fatal("version is empty")
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
