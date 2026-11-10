package redisstore

// budget.go — the Redis memory budget as a FUNCTION, with MEASURED coefficients.
//
// THE GUIDELINE THIS FILE EXISTS TO SERVE: "we must not hardcode anything, and every hot
// datum in Redis must be flexible and well sized". A literal cost coefficient is a stale
// citation with decimals: it describes the payload of the day it was measured and does not
// know the payload changed.
//
// That is why NOTHING here is a hand-written number:
//
//	K_F, b_F                    -> MEMORY USAGE over N keys of the family itself at >=2
//	                              distinct cardinalities, at startup
//	zset-max-listpack-entries   -> CONFIG GET (a server directive, not physics)
//	maxmemory / used_memory     -> INFO memory
//	container ceiling           -> cgroup (the same number `docker inspect` publishes in
//	                              HostConfig.Memory, and the one that kills the process)
//
// The cost model is linear in cardinality:
//
//	MEMORY USAGE(key) ≈ K_F + b_F · card(key)
//
// and the family projection is:
//
//	MiB(F, A, cap_F) = (1/2^20) · Σ_tf [ A · m_F(tf) · ( K_F + b_F · min(cap_F(tf), disp_F(tf)) ) ]

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/candle"
)

// DefaultCgroupRoot is where the kernel publishes the container ceiling to the process
// living inside it. It is a PATH, not a tuning number: nothing about the budget is decided
// here, only where to go read it.
const DefaultCgroupRoot = "/sys/fs/cgroup"

var (
	// ErrNoCoefficientSample means no key in the sample answered both ZCARD and MEMORY USAGE.
	ErrNoCoefficientSample = errors.New("storage: no key in the sample yielded a usable measurement")

	// ErrSingleCardinality is the failure mode behind a WRONG coefficient (K off by 17×).
	// With a single cardinality, K and b are inseparable by construction: every (K, b) pair
	// on the line K + b·c = usage fits the data equally well.
	// Refusing to answer is the only honest outcome — falling back to a literal is exactly
	// the defect this file exists to remove.
	ErrSingleCardinality = errors.New("storage: coefficients need ≥2 distinct cardinalities; K and b are inseparable from one")

	// ErrDegenerateFit means the fit produced a non-positive bytes-per-member, which is not a
	// measurement of anything.
	ErrDegenerateFit = errors.New("storage: measured bytes-per-member is not positive; sample is degenerate")
)

// Coefficients is the measured cost model of one key family.
//
// SampleKeys (n) and BandPct travel WITH the numbers and are printed next to the projection.
// A coefficient without its sample size is a claim without evidence, and the sizing sheet has
// to let an operator tell "measured over 40 keys, ±6%" apart from "measured over 2, ±80%".
type Coefficients struct {
	Family string

	// BytesPerMember is b_F: the marginal cost of one more member.
	BytesPerMember float64

	// BytesPerKey is K_F: the fixed cost of the key itself, independent of cardinality.
	BytesPerKey float64

	// SampleKeys is n: how many keys answered both ZCARD and MEMORY USAGE.
	SampleKeys int

	// Cardinalities are the distinct ZCARDs the fit spans, ascending. len ≥ 2 always.
	Cardinalities []int64

	// BandPct is the relative dispersion of the per-key residual against the fitted model,
	// as a percentage of the mean measured size. It is the honest error bar on the projection.
	BandPct float64
}

// String renders the coefficients in the shape the sizing sheet prints them.
func (c Coefficients) String() string {
	return fmt.Sprintf("b_%s=%.1f B/member (n=%d, ±%.0f%%, cards=%v)  K=%.0f B/key",
		c.Family, c.BytesPerMember, c.SampleKeys, c.BandPct, c.Cardinalities, c.BytesPerKey)
}

// CardinalityMeasurer is the slice of the Redis client that the measurement needs.
// *redis.Client satisfies it as-is.
type CardinalityMeasurer interface {
	ZCard(ctx context.Context, key string) *redis.IntCmd
	MemoryUsage(ctx context.Context, key string, samples ...int) *redis.IntCmd
}

// KeyScanner is the slice of the Redis client that sampling needs.
type KeyScanner interface {
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
}

// SampleFamilyKeys walks the keyspace with SCAN and returns up to `want` keys matching the
// family pattern. SCAN is used rather than KEYS because this runs at startup against a live
// server: KEYS is O(N) under the single-threaded lock and this measurement is not worth
// stalling ingestion for.
func SampleFamilyKeys(ctx context.Context, s KeyScanner, pattern string, want int) ([]string, error) {
	if want <= 0 {
		return nil, nil
	}
	out := make([]string, 0, want)
	var cursor uint64
	for {
		keys, next, err := s.Scan(ctx, cursor, pattern, int64(want)).Result()
		if err != nil {
			return nil, fmt.Errorf("scan %q: %w", pattern, err)
		}
		for _, k := range keys {
			out = append(out, k)
			if len(out) >= want {
				return out, nil
			}
		}
		if next == 0 {
			return out, nil
		}
		cursor = next
	}
}

// MeasureCoefficients fits K_F and b_F for one family from live measurements.
//
// It groups the sampled keys by ZCARD, averages MEMORY USAGE inside each group, and runs an
// ordinary least-squares fit of mean-size against cardinality across the groups. With exactly
// two groups the fit collapses to the two-point solution — which is what  of the design
// did by hand, three times, from three independent pairs.
func MeasureCoefficients(ctx context.Context, m CardinalityMeasurer, family string, keys []string) (Coefficients, error) {
	type observation struct {
		card  int64
		bytes float64
	}

	obs := make([]observation, 0, len(keys))
	for _, key := range keys {
		card, err := m.ZCard(ctx, key).Result()
		if err != nil || card <= 0 {
			continue
		}
		used, err := m.MemoryUsage(ctx, key).Result()
		if err != nil || used <= 0 {
			continue
		}
		obs = append(obs, observation{card: card, bytes: float64(used)})
	}
	if len(obs) == 0 {
		return Coefficients{}, ErrNoCoefficientSample
	}

	// Group means: one point per distinct cardinality, so a cardinality that happens to
	// dominate the keyspace does not drag the slope onto its own noise.
	sums := make(map[int64]float64, len(obs))
	counts := make(map[int64]int, len(obs))
	for _, o := range obs {
		sums[o.card] += o.bytes
		counts[o.card]++
	}
	cards := make([]int64, 0, len(sums))
	for c := range sums {
		cards = append(cards, c)
	}
	sort.Slice(cards, func(i, j int) bool { return cards[i] < cards[j] })
	if len(cards) < 2 {
		return Coefficients{}, ErrSingleCardinality
	}

	var meanCard, meanSize float64
	for _, c := range cards {
		meanCard += float64(c)
		meanSize += sums[c] / float64(counts[c])
	}
	meanCard /= float64(len(cards))
	meanSize /= float64(len(cards))

	var num, den float64
	for _, c := range cards {
		dc := float64(c) - meanCard
		ds := sums[c]/float64(counts[c]) - meanSize
		num += dc * ds
		den += dc * dc
	}
	if den == 0 {
		return Coefficients{}, ErrSingleCardinality
	}
	b := num / den
	if b <= 0 {
		return Coefficients{}, ErrDegenerateFit
	}
	k := meanSize - b*meanCard

	// Band: dispersion of the per-key residual against the fit, relative to the mean measured
	// size. It is computed over EVERY key, not over the group means — the group means would
	// report ±0% for the two-group case and hide the spread that actually exists.
	var sumBytes, sumSqResid float64
	for _, o := range obs {
		resid := o.bytes - (k + b*float64(o.card))
		sumSqResid += resid * resid
		sumBytes += o.bytes
	}
	band := 0.0
	if mean := sumBytes / float64(len(obs)); mean > 0 {
		band = math.Sqrt(sumSqResid/float64(len(obs))) / mean * 100
	}

	return Coefficients{
		Family:         family,
		BytesPerMember: b,
		BytesPerKey:    k,
		SampleKeys:     len(obs),
		Cardinalities:  cards,
		BandPct:        band,
	}, nil
}

// ============================================================================
// SERVER FACTS — the numbers the SERVER knows and the code must not invent
// ============================================================================

// RedisServerProbe reads the server-level facts this design forbids from being literals.
// It is an interface and not *redis.Client so that the INFO half can be fed a synthetic
// snapshot in tests without a Redis that speaks INFO memory.
type RedisServerProbe interface {
	ConfigGet(ctx context.Context, parameter string) (map[string]string, error)
	InfoSection(ctx context.Context, section string) (string, error)
}

type clientServerProbe struct{ c *redis.Client }

// NewRedisServerProbe adapts a live client to the probe interface.
func NewRedisServerProbe(c *redis.Client) RedisServerProbe { return clientServerProbe{c: c} }

func (p clientServerProbe) ConfigGet(ctx context.Context, parameter string) (map[string]string, error) {
	return p.c.ConfigGet(ctx, parameter).Result()
}

func (p clientServerProbe) InfoSection(ctx context.Context, section string) (string, error) {
	return p.c.Info(ctx, section).Result()
}

// ServerFacts are the read numbers the budget and the gates stand on.
//
// A FACT THAT WAS NOT READ STAYS ZERO. It is never substituted by a literal fallback: a gate
// that compares against a made-up 512 is worse than a gate that declares it could not measure,
// because the first one reports a verdict it has no evidence for. Unreadable names what failed
// so the caller can degrade to a warning instead of a false verdict.
type ServerFacts struct {
	MaxMemoryBytes         int64
	UsedMemoryBytes        int64
	MaxMemoryPolicy        string
	ZSetMaxListpackEntries int64
	ZSetMaxListpackValue   int64
	ContainerLimitBytes    int64
	Unreadable             []string
}

// OccupancyRatio is used_memory over maxmemory. It returns 0 when maxmemory is 0 —
// an unbounded Redis has no occupancy, and reporting one would be an invented denominator.
func (f ServerFacts) OccupancyRatio() float64 {
	if f.MaxMemoryBytes <= 0 {
		return 0
	}
	return float64(f.UsedMemoryBytes) / float64(f.MaxMemoryBytes)
}

// ContainerOccupancyRatio is used_memory over the CONTAINER ceiling — the second occupancy,
// and the one that kills the process. maxmemory can be comfortable while the cgroup is not.
func (f ServerFacts) ContainerOccupancyRatio() float64 {
	if f.ContainerLimitBytes <= 0 {
		return 0
	}
	return float64(f.UsedMemoryBytes) / float64(f.ContainerLimitBytes)
}

// ReadServerFacts collects every server-level number the budget needs.
//
// It returns an error only when NOTHING could be read; a partially answered server yields
// the facts it did answer plus the names of the ones it did not.
func ReadServerFacts(ctx context.Context, p RedisServerProbe, cgroupRoot string) (ServerFacts, error) {
	facts := ServerFacts{}
	read := 0

	if raw, err := p.InfoSection(ctx, "memory"); err == nil {
		fields := ParseInfoFields(raw)
		if v, ok := parseInt64Field(fields, "maxmemory"); ok {
			facts.MaxMemoryBytes = v
			read++
		} else {
			facts.Unreadable = append(facts.Unreadable, "maxmemory")
		}
		if v, ok := parseInt64Field(fields, "used_memory"); ok {
			facts.UsedMemoryBytes = v
			read++
		} else {
			facts.Unreadable = append(facts.Unreadable, "used_memory")
		}
		if v, ok := fields["maxmemory_policy"]; ok && v != "" {
			facts.MaxMemoryPolicy = v
			read++
		} else {
			facts.Unreadable = append(facts.Unreadable, "maxmemory_policy")
		}
	} else {
		facts.Unreadable = append(facts.Unreadable, "maxmemory", "used_memory", "maxmemory_policy")
	}

	for _, directive := range []struct {
		name string
		dst  *int64
	}{
		{"zset-max-listpack-entries", &facts.ZSetMaxListpackEntries},
		{"zset-max-listpack-value", &facts.ZSetMaxListpackValue},
	} {
		v, err := p.ConfigGet(ctx, directive.name)
		if err != nil {
			facts.Unreadable = append(facts.Unreadable, directive.name)
			continue
		}
		if n, ok := parseInt64Field(v, directive.name); ok {
			*directive.dst = n
			read++
			continue
		}
		facts.Unreadable = append(facts.Unreadable, directive.name)
	}

	if limit, err := ContainerMemoryLimitBytes(cgroupRoot); err == nil && limit > 0 {
		facts.ContainerLimitBytes = limit
		read++
	} else {
		facts.Unreadable = append(facts.Unreadable, "container-limit")
	}

	if read == 0 {
		return facts, fmt.Errorf("storage: no server fact could be read (unreadable: %s)", strings.Join(facts.Unreadable, ", "))
	}
	return facts, nil
}

// ParseInfoFields turns an INFO section body into its key/value pairs. Comment lines (#) and
// blank lines are dropped; everything else splits on the FIRST colon, because values such as
// the RDB timestamps contain colons of their own.
func ParseInfoFields(raw string) map[string]string {
	out := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimRight(line, "\r")
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, ":")
		if idx <= 0 {
			continue
		}
		out[line[:idx]] = strings.TrimSpace(line[idx+1:])
	}
	return out
}

func parseInt64Field(fields map[string]string, name string) (int64, bool) {
	raw, ok := fields[name]
	if !ok || raw == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// ContainerMemoryLimitBytes reads the ceiling that actually kills the process.
//
// `docker inspect` publishes the same number as HostConfig.Memory; from inside the container
// the kernel exposes it in the cgroup filesystem — cgroup v2 in `memory.max`, cgroup v1 in
// `memory/memory.limit_in_bytes`. It returns 0 for "no limit", which is what an unconstrained
// host looks like, so the caller can tell "unlimited" from "4.096 MiB" instead of guessing.
func ContainerMemoryLimitBytes(cgroupRoot string) (int64, error) {
	if cgroupRoot == "" {
		cgroupRoot = DefaultCgroupRoot
	}

	if v, err := readCgroupLimit(filepath.Join(cgroupRoot, "memory.max")); err == nil {
		return v, nil
	}
	v, err := readCgroupLimit(filepath.Join(cgroupRoot, "memory", "memory.limit_in_bytes"))
	if err != nil {
		return 0, fmt.Errorf("storage: no cgroup memory limit under %q: %w", cgroupRoot, err)
	}
	return v, nil
}

func readCgroupLimit(path string) (int64, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is composed from a configured cgroup root
	if err != nil {
		return 0, err
	}
	raw := strings.TrimSpace(string(data))
	if raw == "max" {
		return 0, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", path, err)
	}
	// cgroup v1 spells "unlimited" as a saturated page counter rather than a word. Anything
	// at that magnitude is not a container ceiling, it is the absence of one.
	if v <= 0 || v >= math.MaxInt64/2 {
		return 0, nil
	}
	return v, nil
}

// ============================================================================
// THE PROJECTION
// ============================================================================

// FamilyProjectionInput carries every term of the projection for one family. None of them is
// a constant: A comes from SCARD, the caps from config, disp from ZCARD sampled on the fleet,
// and the coefficients from MeasureCoefficients.
type FamilyProjectionInput struct {
	Family string

	// Universe is A — the active symbol count, resolved by SCARD active_symbols:{ex}:{mt}.
	Universe int64

	// KeysPerSymbol is m_F(tf): how many keys of this family exist per symbol per timeframe.
	// It is 1 for candles and derivatives and ~14 for indicators, and it is NOT uniform
	// across timeframes, which is why it is a map and not a scalar.
	KeysPerSymbol map[string]float64

	// CapByTimeframe is cap_F(tf), already floored where the family has a floor.
	CapByTimeframe map[string]int64

	// AvailableByTimeframe is disp_F(tf): what the fleet actually holds. A cap above disp
	// costs nothing, and a model that ignores that over-reports every shallow timeframe.
	AvailableByTimeframe map[string]int64

	Coefficients Coefficients
}

// TimeframeProjection is one row of the sizing sheet.
type TimeframeProjection struct {
	Timeframe      string
	Keys           float64
	Cap            int64
	Available      int64
	EffectiveDepth int64
	HorizonSeconds int64
	MiB            float64
}

// FamilyProjection is the projected residency of one family, per timeframe and in total.
type FamilyProjection struct {
	Family       string
	Coefficients Coefficients
	ByTimeframe  []TimeframeProjection
	TotalMiB     float64
}

// ProjectFamily instantiates the budget function for a family projection.
func ProjectFamily(in FamilyProjectionInput) FamilyProjection {
	timeframes := make([]string, 0, len(in.CapByTimeframe))
	for tf := range in.CapByTimeframe {
		timeframes = append(timeframes, tf)
	}
	sort.Slice(timeframes, func(i, j int) bool {
		return candle.ParseTimeframeToSeconds(timeframes[i]) < candle.ParseTimeframeToSeconds(timeframes[j])
	})

	out := FamilyProjection{Family: in.Family, Coefficients: in.Coefficients}
	for _, tf := range timeframes {
		declaredCap := in.CapByTimeframe[tf]
		available := in.AvailableByTimeframe[tf]

		// min(cap, disp): a cap the fleet never reaches is a cap nobody pays for. When disp
		// is unknown (0) the cap is the only honest bound, so it is used as-is.
		depth := declaredCap
		if available > 0 && available < declaredCap {
			depth = available
		}

		m := in.KeysPerSymbol[tf]
		keys := float64(in.Universe) * m
		mib := keys * (in.Coefficients.BytesPerKey + in.Coefficients.BytesPerMember*float64(depth)) / float64(1<<20)

		out.ByTimeframe = append(out.ByTimeframe, TimeframeProjection{
			Timeframe:      tf,
			Keys:           keys,
			Cap:            declaredCap,
			Available:      available,
			EffectiveDepth: depth,
			HorizonSeconds: depth * candle.ParseTimeframeToSeconds(tf),
			MiB:            mib,
		})
		out.TotalMiB += mib
	}
	return out
}
