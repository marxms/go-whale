package health

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Two synthetic INFO snapshots, equal in everything except what each test moves. It is the only
// way to assert "the signal flips EXACTLY at the crossing" — with a single snapshot, any
// threshold passes.
const (
	infoMemoryAt42 = `# Memory
used_memory:1555013632
maxmemory:3670016000
maxmemory_policy:volatile-ttl
`
	infoStatsQuiet = `# Stats
evicted_keys:77842
expired_keys:12
`
	infoKeyspaceProd = `# Keyspace
db0:keys=82766,expires=64045,avg_ttl=0
`
)

// TestRedisBudget_TheContainerCeilingIsTheSecondOccupancy pins the second occupancy.
//
// The defect it catches is observing only `maxmemory`: with `maxmemory` at 3,500 MiB and the
// container at 4,096 MiB, the dataset can sit in a band where the `maxmemory` ratio is
// comfortable while the container ratio has already tripped the alarm. That is where the
// process dies with the panel green.
func TestRedisBudget_TheContainerCeilingIsTheSecondOccupancy(t *testing.T) {
	const mib = int64(1) << 20

	// used_memory 2,000 MiB · maxmemory 3,500 MiB (57%) · container 2,500 MiB (80%).
	// The first ratio is below 70%; the second is not.
	sample := RedisBudgetSample{
		UsedMemoryBytes:     2000 * mib,
		MaxMemoryBytes:      3500 * mib,
		ContainerLimitBytes: 2500 * mib,
		Keys:                82766,
		Expires:             64045,
	}

	signal := NewRedisBudgetMonitor(0.70).Observe(sample)

	if signal.MaxMemoryOccupancy > 0.70 {
		t.Fatalf("scenario no longer holds: maxmemory occupancy = %.3f, should be below the threshold", signal.MaxMemoryOccupancy)
	}
	if !signal.Alert {
		t.Fatalf("no alarm with the container at %.1f%% — the second occupancy is not being observed",
			signal.ContainerOccupancy*100)
	}
	reasons := strings.Join(signal.Reasons, " | ")
	if !strings.Contains(reasons, "CONTAINER") {
		t.Fatalf("the alarm does not NAME the occupancy that fired: %s", reasons)
	}
	if strings.Contains(reasons, "maxmemory occupancy") {
		t.Fatalf("the alarm blamed maxmemory, which is inside the threshold: %s", reasons)
	}
}

// TestRedisBudget_SignalFlipsExactlyAtTheCrossing — two snapshots differing by one byte around
// the threshold.
func TestRedisBudget_SignalFlipsExactlyAtTheCrossing(t *testing.T) {
	const ceiling = int64(1000)

	tests := []struct {
		name      string
		used      int64
		wantAlert bool
	}{
		{name: "below the threshold", used: 699, wantAlert: false},
		{name: "exactly at the threshold", used: 700, wantAlert: false},
		{name: "one byte above", used: 701, wantAlert: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signal := NewRedisBudgetMonitor(0.70).Observe(RedisBudgetSample{
				UsedMemoryBytes:     tt.used,
				MaxMemoryBytes:      ceiling,
				ContainerLimitBytes: ceiling,
			})
			if signal.Alert != tt.wantAlert {
				t.Fatalf("alert = %v with used=%d/%d (%.3f), want %v",
					signal.Alert, tt.used, ceiling, signal.MaxMemoryOccupancy, tt.wantAlert)
			}
		})
	}
}

// TestRedisBudget_EvictionAlertsOnTheDerivativeNotTheAbsoluteValue pins the eviction signal.
//
// 77,842 evicted keys that are STABLE are an old crisis; the same number growing is a crisis in
// progress. Alerting on `evicted_keys > 0` would scream forever after the first incident and be
// turned off in the second week.
func TestRedisBudget_EvictionAlertsOnTheDerivativeNotTheAbsoluteValue(t *testing.T) {
	monitor := NewRedisBudgetMonitor(0.70)
	base := RedisBudgetSample{UsedMemoryBytes: 1, MaxMemoryBytes: 1000, ContainerLimitBytes: 1000, EvictedKeys: 77842}

	first := monitor.Observe(base)
	if first.Alert {
		t.Fatalf("the first sample alarmed without anything to compare against: %v", first.Reasons)
	}
	if first.EvictedDeltaKnown {
		t.Fatalf("the first sample claimed to know a derivative that does not exist")
	}

	// IDENTICAL second sample: counter high, but flat.
	flat := monitor.Observe(base)
	if flat.Alert {
		t.Fatalf("high but STABLE counter alarmed: %v", flat.Reasons)
	}
	if !flat.EvictedDeltaKnown || flat.EvictedDelta != 0 {
		t.Fatalf("delta = %d (known=%v), want 0 known", flat.EvictedDelta, flat.EvictedDeltaKnown)
	}

	// Third: a single key more.
	growing := base
	growing.EvictedKeys = 77843
	moved := monitor.Observe(growing)
	if !moved.Alert {
		t.Fatalf("evicted_keys grew and nothing alarmed: %v", moved.Reasons)
	}
	if moved.EvictedDelta != 1 {
		t.Fatalf("delta = %d, want 1", moved.EvictedDelta)
	}
	if !strings.Contains(strings.Join(moved.Reasons, " | "), "evicted_keys grew 1") {
		t.Fatalf("the alarm does not say HOW MUCH it grew: %v", moved.Reasons)
	}
}

// TestParseRedisBudget_ReadsTheThreeSectionsAndNamesWhatItCouldNot pins the parsed sample.
func TestParseRedisBudget_ReadsTheThreeSectionsAndNamesWhatItCouldNot(t *testing.T) {
	sample := ParseRedisBudget(infoMemoryAt42, infoStatsQuiet, infoKeyspaceProd, 4096*(1<<20))

	if sample.UsedMemoryBytes != 1555013632 {
		t.Fatalf("used_memory = %d", sample.UsedMemoryBytes)
	}
	if sample.MaxMemoryBytes != 3670016000 {
		t.Fatalf("maxmemory = %d", sample.MaxMemoryBytes)
	}
	if sample.EvictedKeys != 77842 {
		t.Fatalf("evicted_keys = %d", sample.EvictedKeys)
	}
	// 82,766 keys, 64,045 with an expiry ⇒ 18,721 that NEVER enter the eviction queue.
	if got := sample.KeysWithoutTTL(); got != 18721 {
		t.Fatalf("keys without TTL = %d, want 18721", got)
	}
	if len(sample.Unreadable) != 0 {
		t.Fatalf("nothing should be unreadable: %v", sample.Unreadable)
	}
}

// TestParseRedisBudget_MissingContainerCeilingIsNamedNotInvented — copying the `4G` from the
// compose into the code would be the classic expired quotation.
func TestParseRedisBudget_MissingContainerCeilingIsNamedNotInvented(t *testing.T) {
	sample := ParseRedisBudget(infoMemoryAt42, infoStatsQuiet, infoKeyspaceProd, 0)

	if sample.ContainerLimitBytes != 0 {
		t.Fatalf("container ceiling = %d, want 0 when it was not read", sample.ContainerLimitBytes)
	}
	if sample.ContainerOccupancy() != 0 {
		t.Fatalf("container occupancy = %v without a read denominator", sample.ContainerOccupancy())
	}
	var named bool
	for _, f := range sample.Unreadable {
		if f == "container-limit" {
			named = true
		}
	}
	if !named {
		t.Fatalf("the absent ceiling was not NAMED: %v", sample.Unreadable)
	}

	signal := NewRedisBudgetMonitor(0.70).Observe(sample)
	if !strings.Contains(strings.Join(signal.Reasons, " | "), "container-limit") {
		t.Fatalf("the signal does not declare the missing fact: %v", signal.Reasons)
	}
}

// TestRedisBudget_UndeclaredThresholdNeverAlerts — same rule as the gates: without a declared
// threshold there is no verdict, only the measurement.
func TestRedisBudget_UndeclaredThresholdNeverAlerts(t *testing.T) {
	signal := NewRedisBudgetMonitor(0).Observe(RedisBudgetSample{
		UsedMemoryBytes: 999, MaxMemoryBytes: 1000, ContainerLimitBytes: 1000,
	})
	if signal.Alert {
		t.Fatalf("alarmed against a threshold that was never declared: %v", signal.Reasons)
	}
	if signal.AlertRatioDeclared {
		t.Fatalf("the signal claimed to have a declared threshold")
	}
	if !strings.Contains(strings.Join(signal.Reasons, " | "), "retention.occupancy_alert_ratio missing") {
		t.Fatalf("the absent threshold was not declared: %v", signal.Reasons)
	}
}

// TestParseKeyspace_SumsEveryDatabase — the count is the instance's, not db0's.
func TestParseKeyspace_SumsEveryDatabase(t *testing.T) {
	sample := ParseRedisBudget("", "", "# Keyspace\ndb0:keys=10,expires=4\ndb1:keys=6,expires=1\n", 1)
	if sample.Keys != 16 || sample.Expires != 5 {
		t.Fatalf("keys/expires = %d/%d, want 16/5", sample.Keys, sample.Expires)
	}
	if got := sample.KeysWithoutTTL(); got != 11 {
		t.Fatalf("keys without TTL = %d, want 11", got)
	}
}

// TestContainerNameFromAddr — the container name is the host part of `redis.addr`, which the
// configuration already declares. No new vocabulary.
func TestContainerNameFromAddr(t *testing.T) {
	tests := []struct {
		addr string
		want string
	}{
		{addr: "redis:6379", want: "redis"},
		{addr: " redis:6379 ", want: "redis"},
		{addr: "localhost:6379", want: "localhost"},
		{addr: "redis", want: "redis"},
	}
	for _, tt := range tests {
		if got := ContainerNameFromAddr(tt.addr); got != tt.want {
			t.Fatalf("ContainerNameFromAddr(%q) = %q, want %q", tt.addr, got, tt.want)
		}
	}
}

// TestDockerHostConfigMemory_ReadsTheDeclaredCeiling — the ceiling comes from `HostConfig.Memory`,
// which is where the daemon keeps it. A fake unix server answers in the daemon's place.
func TestDockerHostConfigMemory_ReadsTheDeclaredCeiling(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "docker.sock")
	var listenCfg net.ListenConfig
	listener, err := listenCfg.Listen(t.Context(), "unix", socket)
	if err != nil {
		t.Skipf("unix socket unavailable in this environment: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	var requested string
	server := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = r.URL.Path
		_ = json.NewEncoder(w).Encode(map[string]any{
			"HostConfig": map[string]any{"Memory": 4294967296},
		})
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	got, err := DockerHostConfigMemory(context.Background(), socket, "redis")
	if err != nil {
		t.Fatalf("DockerHostConfigMemory: %v", err)
	}
	if got != 4294967296 {
		t.Fatalf("ceiling = %d, want 4294967296 (4 GiB, as the compose file declares)", got)
	}
	if requested != "/containers/redis/json" {
		t.Fatalf("requested path = %q, want /containers/redis/json", requested)
	}
}

// TestDockerHostConfigMemory_UnreachableDaemonIsAnErrorNotZeroLimit — "I could not ask" and
// "there is no limit" are different answers, and confusing them would erase the second
// occupancy silently in the environment where the socket is not mounted.
func TestDockerHostConfigMemory_UnreachableDaemonIsAnErrorNotZeroLimit(t *testing.T) {
	absent := filepath.Join(t.TempDir(), "absent.sock")
	if _, err := os.Stat(absent); err == nil {
		t.Fatalf("the socket should be absent")
	}

	got, err := DockerHostConfigMemory(context.Background(), absent, "redis")
	if err == nil {
		t.Fatalf("unreachable daemon returned limit %d without an error", got)
	}
	if got != 0 {
		t.Fatalf("limit = %d with an error, want 0", got)
	}
}
