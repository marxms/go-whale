package questdb

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
)

type coldEmptyRows struct {
	pgx.Rows
}

func (*coldEmptyRows) Next() bool { return false }
func (*coldEmptyRows) Err() error { return nil }

// QueryRange tests retain the transport proof. This isolates the scanner's empty
// resource contract from the pgx protocol allocations shared by both hints.
func TestScanCandlesCompat_EmptyDoesNotAllocate(t *testing.T) {
	for _, hasTakerSell := range []bool{true, false} {
		for _, hint := range []int{100, 800, 1025} {
			t.Run(fmt.Sprintf("modern_%t_limit_%d", hasTakerSell, hint), func(t *testing.T) {
				var rows pgx.Rows = &coldEmptyRows{}
				allocs := testing.AllocsPerRun(1000, func() {
					got, err := scanCandlesCompat(rows, hasTakerSell, hint)
					if err != nil || got != nil {
						t.Fatalf("empty scanner result: got %v, error %v", got, err)
					}
				})
				if allocs != 0 {
					t.Fatalf("empty scanner allocated %g objects, want 0", allocs)
				}
			})
		}
	}
}
