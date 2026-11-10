package pruning

import (
	"context"
	"fmt"
	"testing"
)

// ============================================================================
// MOCKS
// ============================================================================

type mockRedis struct {
	keys      map[string][]string // pattern -> keys
	unlinked  []string
	unlinkErr error
	scanErr   error
}

func (m *mockRedis) ScanKeys(_ context.Context, _ uint64, pattern string) ([]string, uint64, error) {
	if m.scanErr != nil {
		return nil, 0, m.scanErr
	}
	keys, ok := m.keys[pattern]
	if !ok {
		return nil, 0, nil
	}
	return keys, 0, nil
}

func (m *mockRedis) UnlinkKeys(_ context.Context, keys ...string) (int64, error) {
	if m.unlinkErr != nil {
		return 0, m.unlinkErr
	}
	m.unlinked = append(m.unlinked, keys...)
	return int64(len(keys)), nil
}

// allowAllGates is the license for cases exercising the sweep, not its ordering with the gates.
// The case proving refusal uses a false predicate.
func allowAllGates() bool { return true }

// newTestPruner assembles the pruner with an ALREADY OBSERVED floor.
//
// The floor is 1 because these cases exercise the SWEEP with one or two active symbols, and a pass
// without a recorded floor refuses to remove — on purpose (see orphan_guard.go). The guard has its
// own cases in orphan_sweep_test.go; here it stays out of the way.
func newTestPruner(redis KeyStore) *Pruner {
	return &Pruner{
		redis:         redis,
		floor:         &fakeAnchorFloor{value: 1, known: true},
		gatesApproved: func() bool { return true },
	}
}

// fakeAnchorFloor is the persisted floor, in memory.
type fakeAnchorFloor struct {
	value    int
	known    bool
	readErr  error
	writeErr error
	writes   []int
}

func (f *fakeAnchorFloor) AnchorFloor(context.Context) (int, bool, error) {
	if f.readErr != nil {
		return 0, false, f.readErr
	}
	return f.value, f.known, nil
}

func (f *fakeAnchorFloor) SetAnchorFloor(_ context.Context, n int) error {
	if f.writeErr != nil {
		return f.writeErr
	}
	f.writes = append(f.writes, n)
	f.value, f.known = n, true
	return nil
}

// ============================================================================
// TESTS: PruneRedis
// ============================================================================

func TestPruner_PruneRedis_OnlyOrphans(t *testing.T) {
	t.Parallel()

	redis := &mockRedis{
		keys: map[string][]string{
			"derivatives:snapshot:*:*:*": {
				"derivatives:snapshot:latest:binance:future:BTC/USDT",
				"derivatives:snapshot:latest:binance:future:XRP/USDT",
			},
		},
	}
	p := newTestPruner(redis)

	active := []string{"BTC/USDT", "ETH/USDT"}
	err := p.PruneRedis(context.Background(), active)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(redis.unlinked) != 1 || redis.unlinked[0] != "derivatives:snapshot:latest:binance:future:XRP/USDT" {
		t.Fatalf("expected UNLINK XRP/USDT key, got %v", redis.unlinked)
	}
}

func TestPruner_PruneRedis_NoKeys(t *testing.T) {
	t.Parallel()

	redis := &mockRedis{keys: map[string][]string{}}
	p := newTestPruner(redis)

	err := p.PruneRedis(context.Background(), []string{"BTC/USDT"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(redis.unlinked) != 0 {
		t.Fatalf("expected no UNLINK, got %v", redis.unlinked)
	}
}

// TestPruner_PruneRedis_AllPatterns verifies every supported key pattern is
// scanned and orphans across snapshot, series and last_trade keys are UNLINKed
// while active-symbol keys are preserved.
func TestPruner_PruneRedis_AllPatterns(t *testing.T) {
	t.Parallel()

	redis := &mockRedis{
		keys: map[string][]string{
			"derivatives:snapshot:*:*:*": {
				"derivatives:snapshot:latest:binance:future:BTC/USDT",
				"derivatives:snapshot:latest:binance:future:XRP/USDT",
			},
			"derivatives:series:*:*:*:*": {
				"derivatives:series:binance:future:SOL/USDT:1m",
			},
			"last_trade:*:*:*": {
				"last_trade:binance:future:BTC/USDT",
				"last_trade:binance:future:DOGE/USDT",
			},
		},
	}
	p := newTestPruner(redis)

	active := []string{"BTC/USDT"}
	if err := p.PruneRedis(context.Background(), active); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := map[string]struct{}{
		"derivatives:snapshot:latest:binance:future:XRP/USDT": {},
		"derivatives:series:binance:future:SOL/USDT:1m":       {},
		"last_trade:binance:future:DOGE/USDT":                 {},
	}
	if len(redis.unlinked) != len(want) {
		t.Fatalf("expected %d UNLINKed keys, got %v", len(want), redis.unlinked)
	}
	for _, k := range redis.unlinked {
		if _, ok := want[k]; !ok {
			t.Fatalf("unexpected UNLINKed key (active symbol or unknown): %s", k)
		}
	}
}

// TestPruner_PruneRedis_ScanError surfaces a wrapped scan error and never
// UNLINKs anything when the scan itself fails.
func TestPruner_PruneRedis_ScanError(t *testing.T) {
	t.Parallel()

	redis := &mockRedis{scanErr: fmt.Errorf("connection refused")}
	p := newTestPruner(redis)

	err := p.PruneRedis(context.Background(), []string{"BTC/USDT"})
	if err == nil {
		t.Fatal("expected error")
	}
	if len(redis.unlinked) != 0 {
		t.Fatalf("expected no UNLINK on scan error, got %v", redis.unlinked)
	}
}

// TestPruner_RefusesBeforeTheIntegrityGates — the sweep is a destructive path and cannot even
// begin before the gates approve.
//
// The mutant it kills: an unconditional `PruneRedis`. A read-only pass still measures the wrong
// keyspace, and the next one removes with the blessing of a license never granted.
func TestPruner_RefusesBeforeTheIntegrityGates(t *testing.T) {
	t.Parallel()

	casos := []struct {
		nome  string
		gates func() bool
	}{
		{"license denied", func() bool { return false }},
		{"license not declared", nil},
	}
	for _, caso := range casos {
		t.Run(caso.nome, func(t *testing.T) {
			redis := &mockRedis{
				keys: map[string][]string{
					"derivatives:snapshot:*:*:*": {"derivatives:snapshot:latest:binance:future:ORPHAN/USDT"},
				},
			}
			p := &Pruner{
				redis:         redis,
				floor:         &fakeAnchorFloor{value: 1, known: true},
				gatesApproved: caso.gates,
			}

			if err := p.PruneRedis(context.Background(), []string{"BTC/USDT"}); err != nil {
				t.Fatalf("the refusal is not an error: %v", err)
			}
			if len(redis.unlinked) != 0 {
				t.Fatalf("the sweep removed %v before the gates", redis.unlinked)
			}
		})
	}
}

// TestPruner_PruneRedis_ContextCancelled honors cancellation before scanning.
func TestPruner_PruneRedis_ContextCancelled(t *testing.T) {
	t.Parallel()

	redis := &mockRedis{
		keys: map[string][]string{
			"derivatives:snapshot:*:*:*": {"derivatives:snapshot:latest:binance:future:XRP/USDT"},
		},
	}
	p := newTestPruner(redis)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.PruneRedis(ctx, []string{"BTC/USDT"})
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if len(redis.unlinked) != 0 {
		t.Fatalf("expected no UNLINK after cancellation, got %v", redis.unlinked)
	}
}

// ============================================================================
// TESTS: extractSymbolFromKey
// ============================================================================

func TestExtractSymbolFromKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		want string
	}{
		{"snapshot", "derivatives:snapshot:latest:binance:future:BTC/USDT", "BTC/USDT"},
		{"series", "derivatives:series:binance:future:ETH/USDT:1m", "ETH/USDT"},
		{"last_trade", "last_trade:binance:future:XRP/USDT", "XRP/USDT"},
		{"unknown", "some:other:key", ""},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := extractSymbolFromKey(tt.key); got != tt.want {
				t.Fatalf("extractSymbolFromKey(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}
