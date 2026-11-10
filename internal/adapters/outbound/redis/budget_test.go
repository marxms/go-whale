package redisstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

// seedFamily writes one ZSET per requested cardinality, every member padded to `payload`
// bytes. The padding is what makes the measurement falsifiable: b_F is the marginal cost of
// one member, so doubling the member has to move b_F and nothing else.
func seedFamily(ctx context.Context, t *testing.T, c *redis.Client, prefix string, cards []int64, payload int) []string {
	t.Helper()
	keys := make([]string, 0, len(cards))
	for i, card := range cards {
		key := fmt.Sprintf("%s:%d:%d", prefix, i, card)
		members := make([]redis.Z, 0, card)
		for j := int64(0); j < card; j++ {
			members = append(members, redis.Z{
				Score:  float64(j),
				Member: fmt.Sprintf("%08d|%s", j, strings.Repeat("x", payload)),
			})
		}
		if err := c.ZAdd(ctx, key, members...).Err(); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
		keys = append(keys, key)
	}
	return keys
}

// TestMeasureCoefficients_BytesPerMemberFollowsThePayload.
//
// THE MUTANT IT KILLS: the one carrying a literal b_F. A hand-written coefficient does not know
// the payload changed, so it returns the SAME number against both datasets. Here the second
// dataset has a member exactly `delta` bytes larger than the first, and the requirement is that
// b_F rises by that delta — not "that it changes", but that it changes BY THE RIGHT VALUE.
func TestMeasureCoefficients_BytesPerMemberFollowsThePayload(t *testing.T) {
	ctx := context.Background()
	cards := []int64{4, 400}

	const smallPayload = 24
	const largePayload = 48
	const delta = largePayload - smallPayload

	measure := func(payload int) Coefficients {
		t.Helper()
		mr := miniredis.RunT(t)
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = c.Close() })
		keys := seedFamily(ctx, t, c, "fam", cards, payload)
		got, err := MeasureCoefficients(ctx, c, "candles", keys)
		if err != nil {
			t.Fatalf("MeasureCoefficients(payload=%d): %v", payload, err)
		}
		return got
	}

	small := measure(smallPayload)
	large := measure(largePayload)

	if small.BytesPerMember <= 0 || large.BytesPerMember <= 0 {
		t.Fatalf("bytes-per-member must be positive: small=%v large=%v", small.BytesPerMember, large.BytesPerMember)
	}
	if large.BytesPerMember == small.BytesPerMember {
		t.Fatalf("b_F did not move when the member doubled: %v — the coefficient is not being measured",
			large.BytesPerMember)
	}

	gotDelta := large.BytesPerMember - small.BytesPerMember
	if math.Abs(gotDelta-float64(delta)) > float64(delta)*0.10 {
		t.Fatalf("b_F moved by %.2f B/member, want %d ±10%% (small=%.2f large=%.2f)",
			gotDelta, delta, small.BytesPerMember, large.BytesPerMember)
	}
}

// TestMeasureCoefficients_FitReproducesTheMeasuredSizes checks that K_F is a real intercept
// and not a leftover: the fitted model has to reproduce the measured size of a key at each
// sampled cardinality.
func TestMeasureCoefficients_FitReproducesTheMeasuredSizes(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	cards := []int64{3, 40, 400}
	keys := seedFamily(t.Context(), t, c, "fit", cards, 32)

	got, err := MeasureCoefficients(ctx, c, "candles", keys)
	if err != nil {
		t.Fatalf("MeasureCoefficients: %v", err)
	}

	for i, key := range keys {
		measured, err := c.MemoryUsage(ctx, key).Result()
		if err != nil {
			t.Fatalf("MEMORY USAGE %s: %v", key, err)
		}
		fitted := got.BytesPerKey + got.BytesPerMember*float64(cards[i])
		if math.Abs(fitted-float64(measured)) > float64(measured)*0.10 {
			t.Fatalf("card=%d: fitted %.0f B vs measured %d B (>10%% apart)", cards[i], fitted, measured)
		}
	}
}

// TestMeasureCoefficients_ReportsSampleSizeAndBand: n and the band must travel with the
// numbers. A coefficient with no sample size is a claim with no evidence.
func TestMeasureCoefficients_ReportsSampleSizeAndBand(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	cards := []int64{5, 50, 500, 501}
	keys := seedFamily(t.Context(), t, c, "band", cards, 16)

	got, err := MeasureCoefficients(ctx, c, "indicators", keys)
	if err != nil {
		t.Fatalf("MeasureCoefficients: %v", err)
	}
	if got.SampleKeys != len(keys) {
		t.Fatalf("SampleKeys = %d, want %d", got.SampleKeys, len(keys))
	}
	if len(got.Cardinalities) != len(cards) {
		t.Fatalf("Cardinalities = %v, want %d distinct", got.Cardinalities, len(cards))
	}
	if got.BandPct < 0 || got.BandPct > 10 {
		t.Fatalf("BandPct = %.2f%%, want a tight band (0..10%%) for a clean linear sample", got.BandPct)
	}
	if got.Family != "indicators" {
		t.Fatalf("Family = %q, want %q", got.Family, "indicators")
	}
	if s := got.String(); !strings.Contains(s, "n=4") || !strings.Contains(s, "b_indicators=") {
		t.Fatalf("String() = %q, want it to carry the family and n", s)
	}
}

// TestMeasureCoefficients_RefusesASingleCardinality is the guard against the exact mistake of
// deriving K from a single-cardinality sample, where K and b are inseparable by construction.
func TestMeasureCoefficients_RefusesASingleCardinality(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	keys := seedFamily(t.Context(), t, c, "flat", []int64{400, 400, 400}, 32)

	_, err := MeasureCoefficients(ctx, c, "candles", keys)
	if !errors.Is(err, ErrSingleCardinality) {
		t.Fatalf("err = %v, want ErrSingleCardinality — one cardinality cannot separate K from b", err)
	}
}

// TestMeasureCoefficients_RefusesAnEmptySample — no measurement means no coefficient, not a
// fallback constant.
func TestMeasureCoefficients_RefusesAnEmptySample(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	_, err := MeasureCoefficients(ctx, c, "candles", []string{"absent:1", "absent:2"})
	if !errors.Is(err, ErrNoCoefficientSample) {
		t.Fatalf("err = %v, want ErrNoCoefficientSample", err)
	}
}

// TestSampleFamilyKeys_ReturnsFamilyKeysUpToTheBudget — the sample comes from the family's
// own keyspace, not from a hand-written list.
func TestSampleFamilyKeys_ReturnsFamilyKeysUpToTheBudget(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	seedFamily(t.Context(), t, c, "candles:binance:future:BTC", []int64{3, 4, 5, 6, 7}, 8)
	seedFamily(t.Context(), t, c, "indicators:binance:future:BTC", []int64{3, 4}, 8)

	got, err := SampleFamilyKeys(ctx, c, "candles:*", 3)
	if err != nil {
		t.Fatalf("SampleFamilyKeys: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	for _, k := range got {
		if !strings.HasPrefix(k, "candles:") {
			t.Fatalf("sampled %q, which is not in the family", k)
		}
	}
}

// registerConfigGet teaches the test server to answer CONFIG GET from a mutable table, so the
// test can RAISE the directive and watch the read number follow it.
func registerConfigGet(t *testing.T, mr *miniredis.Miniredis, table map[string]*string) {
	t.Helper()
	err := mr.Server().Register("CONFIG", func(c *server.Peer, _ string, args []string) {
		if len(args) < 2 || !strings.EqualFold(args[0], "GET") {
			c.WriteError("ERR unsupported CONFIG subcommand")
			return
		}
		val, ok := table[strings.ToLower(args[1])]
		if !ok {
			c.WriteLen(0)
			return
		}
		c.WriteLen(2)
		c.WriteBulk(strings.ToLower(args[1]))
		c.WriteBulk(*val)
	})
	if err != nil {
		t.Fatalf("register CONFIG: %v", err)
	}
}

// staticInfoProbe delegates CONFIG GET to a live client and serves a synthetic INFO body.
// miniredis has no `INFO memory` section, and design  asks for synthetic INFO snapshots
// anyway — the CONFIG half still goes over the wire.
type staticInfoProbe struct {
	inner RedisServerProbe
	info  string
	err   error
}

func (p staticInfoProbe) ConfigGet(ctx context.Context, parameter string) (map[string]string, error) {
	return p.inner.ConfigGet(ctx, parameter)
}

func (p staticInfoProbe) InfoSection(_ context.Context, _ string) (string, error) {
	return p.info, p.err
}

const syntheticInfoMemory = `# Memory
used_memory:1555013632
used_memory_human:1.45G
maxmemory:3670016000
maxmemory_human:3.42G
maxmemory_policy:volatile-ttl
mem_fragmentation_ratio:1.08
`

// TestReadServerFacts_ListpackCliffFollowsTheServerDirective is the ZERO HARDCODE proof for the
// number the fatal gate compares against.
//
// THE MUTANT IT KILLS: the one carrying a literal `512`. The directive is server config, not
// physics — raising it on the test server must change the number read.
func TestReadServerFacts_ListpackCliffFollowsTheServerDirective(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	entries := "512"
	value := "128"
	registerConfigGet(t, mr, map[string]*string{
		"zset-max-listpack-entries": &entries,
		"zset-max-listpack-value":   &value,
	})

	probe := staticInfoProbe{inner: NewRedisServerProbe(c), info: syntheticInfoMemory}

	first, err := ReadServerFacts(ctx, probe, t.TempDir())
	if err != nil {
		t.Fatalf("ReadServerFacts: %v", err)
	}
	if first.ZSetMaxListpackEntries != 512 {
		t.Fatalf("ZSetMaxListpackEntries = %d, want 512 (what the server declares)", first.ZSetMaxListpackEntries)
	}
	if first.ZSetMaxListpackValue != 128 {
		t.Fatalf("ZSetMaxListpackValue = %d, want 128", first.ZSetMaxListpackValue)
	}

	entries = "1024"

	second, err := ReadServerFacts(ctx, probe, t.TempDir())
	if err != nil {
		t.Fatalf("ReadServerFacts after raising the directive: %v", err)
	}
	if second.ZSetMaxListpackEntries != 1024 {
		t.Fatalf("ZSetMaxListpackEntries = %d after raising the directive to 1024 — the value is not being read",
			second.ZSetMaxListpackEntries)
	}
}

// TestReadServerFacts_MemoryNumbersComeFromInfo — maxmemory and used_memory are read, never
// presumed, and the occupancy is their ratio.
func TestReadServerFacts_MemoryNumbersComeFromInfo(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	registerConfigGet(t, mr, map[string]*string{})

	probe := staticInfoProbe{inner: NewRedisServerProbe(c), info: syntheticInfoMemory}
	facts, err := ReadServerFacts(ctx, probe, t.TempDir())
	if err != nil {
		t.Fatalf("ReadServerFacts: %v", err)
	}

	if facts.UsedMemoryBytes != 1555013632 {
		t.Fatalf("UsedMemoryBytes = %d, want 1555013632", facts.UsedMemoryBytes)
	}
	if facts.MaxMemoryBytes != 3670016000 {
		t.Fatalf("MaxMemoryBytes = %d, want 3670016000", facts.MaxMemoryBytes)
	}
	if facts.MaxMemoryPolicy != "volatile-ttl" {
		t.Fatalf("MaxMemoryPolicy = %q, want %q", facts.MaxMemoryPolicy, "volatile-ttl")
	}
	want := 1555013632.0 / 3670016000.0
	if math.Abs(facts.OccupancyRatio()-want) > 1e-9 {
		t.Fatalf("OccupancyRatio = %v, want %v", facts.OccupancyRatio(), want)
	}
}

// TestReadServerFacts_UnreadFactStaysZeroAndIsNamed — a fact that was not read is never
// substituted by a literal, and the caller is told which one is missing so it can degrade to
// a warning instead of issuing a verdict it has no evidence for.
func TestReadServerFacts_UnreadFactStaysZeroAndIsNamed(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	// CONFIG is deliberately NOT registered: the server does not answer it.

	probe := staticInfoProbe{inner: NewRedisServerProbe(c), info: syntheticInfoMemory}
	facts, err := ReadServerFacts(ctx, probe, t.TempDir())
	if err != nil {
		t.Fatalf("ReadServerFacts: %v", err)
	}
	if facts.ZSetMaxListpackEntries != 0 {
		t.Fatalf("ZSetMaxListpackEntries = %d, want 0 — an unread directive must not gain a value",
			facts.ZSetMaxListpackEntries)
	}
	if !containsString(facts.Unreadable, "zset-max-listpack-entries") {
		t.Fatalf("Unreadable = %v, want it to name zset-max-listpack-entries", facts.Unreadable)
	}
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// TestContainerMemoryLimitBytes reads the ceiling that actually kills the process.
// maxmemory can be comfortable while the cgroup is not.
func TestContainerMemoryLimitBytes(t *testing.T) {
	tests := []struct {
		name     string
		file     string
		contents string
		want     int64
		wantErr  bool
	}{
		{name: "cgroup v2 limit", file: "memory.max", contents: "4294967296\n", want: 4294967296},
		{name: "cgroup v2 unlimited", file: "memory.max", contents: "max\n", want: 0},
		{name: "cgroup v1 limit", file: "memory/memory.limit_in_bytes", contents: "4294967296\n", want: 4294967296},
		{name: "cgroup v1 unlimited sentinel", file: "memory/memory.limit_in_bytes", contents: "9223372036854771712\n", want: 0},
		{name: "no cgroup at all", file: "", want: 0, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			if tt.file != "" {
				path := filepath.Join(root, tt.file)
				if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
				if err := os.WriteFile(path, []byte(tt.contents), 0o600); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			got, err := ContainerMemoryLimitBytes(root)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("want error when no cgroup file exists, got limit=%d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ContainerMemoryLimitBytes: %v", err)
			}
			if got != tt.want {
				t.Fatalf("limit = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestServerFacts_ContainerOccupancyIsTheSecondOccupancy: two ratios, two ceilings.
func TestServerFacts_ContainerOccupancyIsTheSecondOccupancy(t *testing.T) {
	facts := ServerFacts{
		UsedMemoryBytes:     3 << 30,
		MaxMemoryBytes:      4 << 30,
		ContainerLimitBytes: 4 << 30,
	}
	if got := facts.OccupancyRatio(); math.Abs(got-0.75) > 1e-9 {
		t.Fatalf("OccupancyRatio = %v, want 0.75", got)
	}
	if got := facts.ContainerOccupancyRatio(); math.Abs(got-0.75) > 1e-9 {
		t.Fatalf("ContainerOccupancyRatio = %v, want 0.75", got)
	}

	unbounded := ServerFacts{UsedMemoryBytes: 1 << 30}
	if got := unbounded.OccupancyRatio(); got != 0 {
		t.Fatalf("OccupancyRatio with no maxmemory = %v, want 0 (no invented denominator)", got)
	}
	if got := unbounded.ContainerOccupancyRatio(); got != 0 {
		t.Fatalf("ContainerOccupancyRatio with no cgroup = %v, want 0", got)
	}
}

// TestProjectFamily_UsesTheSmallerOfCapAndAvailable checks min(cap_F, disp_F).
//
// THE MUTANT IT KILLS: the one projecting by the declared cap. A cap the fleet never reaches
// costs not one byte, and ignoring that over-reports every shallow timeframe — a "cap 26 on
// deriv 1w" gain was once counted as MiB when it was worth 0.
func TestProjectFamily_UsesTheSmallerOfCapAndAvailable(t *testing.T) {
	coef := Coefficients{Family: "candles", BytesPerMember: 400, BytesPerKey: 100, SampleKeys: 40}

	got := ProjectFamily(FamilyProjectionInput{
		Family:               "candles",
		Universe:             500,
		KeysPerSymbol:        map[string]float64{"5m": 1, "1w": 1},
		CapByTimeframe:       map[string]int64{"5m": 521, "1w": 800},
		AvailableByTimeframe: map[string]int64{"5m": 800, "1w": 17},
		Coefficients:         coef,
	})

	if len(got.ByTimeframe) != 2 {
		t.Fatalf("rows = %d, want 2", len(got.ByTimeframe))
	}
	byTF := map[string]TimeframeProjection{}
	for _, row := range got.ByTimeframe {
		byTF[row.Timeframe] = row
	}

	if byTF["5m"].EffectiveDepth != 521 {
		t.Fatalf("5m effective depth = %d, want 521 (cap binds, fleet holds more)", byTF["5m"].EffectiveDepth)
	}
	if byTF["1w"].EffectiveDepth != 17 {
		t.Fatalf("1w effective depth = %d, want 17 (fleet binds, cap never reached)", byTF["1w"].EffectiveDepth)
	}

	want5m := 500.0 * (100 + 400*521) / float64(1<<20)
	if math.Abs(byTF["5m"].MiB-want5m) > 1e-6 {
		t.Fatalf("5m MiB = %v, want %v", byTF["5m"].MiB, want5m)
	}
	want1w := 500.0 * (100 + 400*17) / float64(1<<20)
	if math.Abs(byTF["1w"].MiB-want1w) > 1e-6 {
		t.Fatalf("1w MiB = %v, want %v", byTF["1w"].MiB, want1w)
	}
	if math.Abs(got.TotalMiB-(want5m+want1w)) > 1e-6 {
		t.Fatalf("TotalMiB = %v, want %v", got.TotalMiB, want5m+want1w)
	}
}

// TestProjectFamily_HorizonIsDepthInWallTime: the sheet has to state the cap as TIME, because
// one 1d member costs 288× the wall time of one 5m member.
func TestProjectFamily_HorizonIsDepthInWallTime(t *testing.T) {
	got := ProjectFamily(FamilyProjectionInput{
		Family:               "candles",
		Universe:             1,
		KeysPerSymbol:        map[string]float64{"5m": 1, "1d": 1},
		CapByTimeframe:       map[string]int64{"5m": 521, "1d": 800},
		AvailableByTimeframe: map[string]int64{},
		Coefficients:         Coefficients{BytesPerMember: 1, BytesPerKey: 0},
	})

	byTF := map[string]TimeframeProjection{}
	for _, row := range got.ByTimeframe {
		byTF[row.Timeframe] = row
	}
	if byTF["5m"].HorizonSeconds != 521*300 {
		t.Fatalf("5m horizon = %ds, want %ds", byTF["5m"].HorizonSeconds, 521*300)
	}
	if byTF["1d"].HorizonSeconds != 800*86400 {
		t.Fatalf("1d horizon = %ds, want %ds", byTF["1d"].HorizonSeconds, 800*86400)
	}
}

// TestParseInfoFields_SplitsOnTheFirstColon — RDB values carry colons of their own.
func TestParseInfoFields_SplitsOnTheFirstColon(t *testing.T) {
	got := ParseInfoFields("# Memory\r\nused_memory:42\r\nrdb_last_save:2026:08:24\r\n\r\n")
	if got["used_memory"] != "42" {
		t.Fatalf("used_memory = %q, want %q", got["used_memory"], "42")
	}
	if got["rdb_last_save"] != "2026:08:24" {
		t.Fatalf("rdb_last_save = %q, want %q", got["rdb_last_save"], "2026:08:24")
	}
	if _, ok := got["# Memory"]; ok {
		t.Fatalf("comment line leaked into the fields: %v", got)
	}
}
