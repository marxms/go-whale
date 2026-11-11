package health

// redis_budget.go — continuous observability of the TWO OCCUPANCIES.
//
// There are two, and observing only the first is the defect:
//
//	used_memory / maxmemory         → the one Redis knows about and evicts against
//	used_memory / container ceiling → the one that KILLS THE PROCESS, and about which Redis knows nothing
//
// With a 4,096 MiB Redis container and `maxmemory` at 3,500 MiB, the real headroom between the
// dataset and the OOM killer is ~596 MiB, not the host's 8.8 GiB of RAM. A dashboard showing
// only the first ratio can read "42% occupancy" while the cgroup sits at 92% — and the process
// dies with the chart green.
//
// Three signals, because the crisis had three symptoms and no alarm:
//
//	1. occupancy (both)           — only noticed at 99.92%
//	2. growth of evicted_keys     — 77,842 evicted keys accumulated without a warning. In a
//	                                healthy regime the counter is STABLE, so what matters is
//	                                the DERIVATIVE between two samples, not the absolute value
//	3. keys without TTL           — 18,721 of them never enter the eviction queue, and it is
//	                                that fraction that decides whether the current policy can
//	                                reclaim memory or not

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// RedisBudgetSample is one observation of the server.
//
// Every field is READ. A field the server did not report stays zero and is named in Unreadable —
// the same rule the coefficients and the gates follow: a signal with no measurement behind it is
// worse than a missing signal, because it is believed.
type RedisBudgetSample struct {
	UsedMemoryBytes     int64
	MaxMemoryBytes      int64
	ContainerLimitBytes int64
	EvictedKeys         int64
	Keys                int64
	Expires             int64
	Unreadable          []string
}

// KeysWithoutTTL is the population that a volatile-* policy can never reclaim.
func (s RedisBudgetSample) KeysWithoutTTL() int64 {
	if s.Keys <= 0 {
		return 0
	}
	without := s.Keys - s.Expires
	if without < 0 {
		return 0
	}
	return without
}

// MaxMemoryOccupancy is used_memory over maxmemory. Zero when maxmemory was not read — an
// invented denominator is worse than an absent ratio.
func (s RedisBudgetSample) MaxMemoryOccupancy() float64 {
	if s.MaxMemoryBytes <= 0 {
		return 0
	}
	return float64(s.UsedMemoryBytes) / float64(s.MaxMemoryBytes)
}

// ContainerOccupancy is used_memory over the CONTAINER ceiling — the second occupancy, and the
// one that kills the process.
func (s RedisBudgetSample) ContainerOccupancy() float64 {
	if s.ContainerLimitBytes <= 0 {
		return 0
	}
	return float64(s.UsedMemoryBytes) / float64(s.ContainerLimitBytes)
}

// ParseRedisBudget turns the raw INFO sections into a sample. It takes the sections as strings
// so that a synthetic snapshot is a first-class input: the discrimination this file needs is
// "does the signal flip exactly at the crossing", and that is only answerable by feeding two
// snapshots that differ by one number.
func ParseRedisBudget(infoMemory, infoStats, infoKeyspace string, containerLimitBytes int64) RedisBudgetSample {
	sample := RedisBudgetSample{ContainerLimitBytes: containerLimitBytes}
	if containerLimitBytes <= 0 {
		sample.Unreadable = append(sample.Unreadable, "container-limit")
	}

	memory := parseInfoFields(infoMemory)
	for _, field := range []struct {
		name string
		dst  *int64
	}{
		{"used_memory", &sample.UsedMemoryBytes},
		{"maxmemory", &sample.MaxMemoryBytes},
	} {
		if v, ok := parseInt64(memory, field.name); ok {
			*field.dst = v
		} else {
			sample.Unreadable = append(sample.Unreadable, field.name)
		}
	}

	if v, ok := parseInt64(parseInfoFields(infoStats), "evicted_keys"); ok {
		sample.EvictedKeys = v
	} else {
		sample.Unreadable = append(sample.Unreadable, "evicted_keys")
	}

	keys, expires, ok := parseKeyspace(infoKeyspace)
	if ok {
		sample.Keys, sample.Expires = keys, expires
	} else {
		sample.Unreadable = append(sample.Unreadable, "keyspace")
	}

	return sample
}

// RedisBudgetSignal is what an operator reads.
type RedisBudgetSignal struct {
	// Alert is true while any of the three signals is out of regime.
	Alert bool
	// Reasons NAMES what fired. An alert without a reason sends the operator to grep.
	Reasons            []string
	MaxMemoryOccupancy float64
	ContainerOccupancy float64
	KeysWithoutTTL     int64
	EvictedDelta       int64
	EvictedDeltaKnown  bool
	AlertRatioDeclared bool
}

func (s RedisBudgetSignal) String() string {
	state := "OK"
	if s.Alert {
		state = "ALERTA"
	}
	line := fmt.Sprintf("[REDIS-BUDGET] %s · maxmemory=%.1f%% · container=%.1f%% · keys without TTL=%d",
		state, s.MaxMemoryOccupancy*100, s.ContainerOccupancy*100, s.KeysWithoutTTL)
	if s.EvictedDeltaKnown {
		line += fmt.Sprintf(" · evicted_keys +%d since a amostra anterior", s.EvictedDelta)
	}
	if len(s.Reasons) > 0 {
		line += " · " + strings.Join(s.Reasons, " · ")
	}
	return line
}

// RedisBudgetMonitor turns a series of samples into a signal. It keeps the previous sample
// because the eviction signal is a DERIVATIVE: `evicted_keys` at 77.842 and flat is an old
// crisis; the same number growing is a current one.
type RedisBudgetMonitor struct {
	mu         sync.Mutex
	alertRatio float64
	previous   *RedisBudgetSample
	metrics    RedisBudgetMetrics
}

// SetMetrics injects the observability port. The composition root calls it before the
// monitor observes its first sample.
func (m *RedisBudgetMonitor) SetMetrics(metrics RedisBudgetMetrics) { m.metrics = metrics }

// NewRedisBudgetMonitor takes the declared alert ratio. Zero means UNDECLARED: the monitor then
// reports the occupancies without ever claiming they crossed a threshold it was never given.
func NewRedisBudgetMonitor(alertRatio float64) *RedisBudgetMonitor {
	return &RedisBudgetMonitor{alertRatio: alertRatio}
}

// Observe records one sample and returns the signal it produces.
func (m *RedisBudgetMonitor) Observe(sample RedisBudgetSample) RedisBudgetSignal {
	m.mu.Lock()
	defer m.mu.Unlock()

	signal := RedisBudgetSignal{
		MaxMemoryOccupancy: sample.MaxMemoryOccupancy(),
		ContainerOccupancy: sample.ContainerOccupancy(),
		KeysWithoutTTL:     sample.KeysWithoutTTL(),
		AlertRatioDeclared: m.alertRatio > 0,
	}

	if m.alertRatio > 0 {
		// The two occupancies, against the SAME threshold. Observing only the first is the
		// defect: it can read 42% while the cgroup is at 92%.
		if signal.MaxMemoryOccupancy > m.alertRatio {
			signal.Alert = true
			signal.Reasons = append(signal.Reasons,
				fmt.Sprintf("maxmemory occupancy %.1f%% above %.0f%%", signal.MaxMemoryOccupancy*100, m.alertRatio*100))
		}
		if signal.ContainerOccupancy > m.alertRatio {
			signal.Alert = true
			signal.Reasons = append(signal.Reasons,
				fmt.Sprintf("CONTAINER occupancy %.1f%% above %.0f%% — this ceiling is what kills the process",
					signal.ContainerOccupancy*100, m.alertRatio*100))
		}
	} else {
		signal.Reasons = append(signal.Reasons, "threshold NOT declared (retention.occupancy_alert_ratio missing)")
	}

	if m.previous != nil {
		signal.EvictedDeltaKnown = true
		signal.EvictedDelta = sample.EvictedKeys - m.previous.EvictedKeys
		if signal.EvictedDelta > 0 {
			signal.Alert = true
			signal.Reasons = append(signal.Reasons,
				fmt.Sprintf("evicted_keys grew %d since the previous sample — in a healthy regime it is STABLE",
					signal.EvictedDelta))
		}
	}

	if len(sample.Unreadable) > 0 {
		names := append([]string(nil), sample.Unreadable...)
		sort.Strings(names)
		signal.Reasons = append(signal.Reasons, "facts NOT read (they stay zero, never literal): "+strings.Join(names, ", "))
	}

	snapshot := sample
	m.previous = &snapshot

	m.publish(sample, signal)
	return signal
}

func (m *RedisBudgetMonitor) publish(sample RedisBudgetSample, signal RedisBudgetSignal) {
	m.metrics.setOccupancy("maxmemory", signal.MaxMemoryOccupancy)
	m.metrics.setOccupancy("container", signal.ContainerOccupancy)
	m.metrics.setKeysWithoutTTL(float64(signal.KeysWithoutTTL))
	m.metrics.setKeysTotal(float64(sample.Keys))
	m.metrics.setEvictedKeys(float64(sample.EvictedKeys))
	if signal.Alert {
		m.metrics.setAlert(1)
		return
	}
	m.metrics.setAlert(0)
}

// parseInfoFields splits an INFO section body into key/value pairs, on the FIRST colon: values
// such as the RDB timestamps carry colons of their own.
func parseInfoFields(raw string) map[string]string {
	out := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(strings.TrimRight(line, "\r"))
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

func parseInt64(fields map[string]string, name string) (int64, bool) {
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

// parseKeyspace reads `db0:keys=82766,expires=64045,avg_ttl=0`, summing across every database.
// The difference between the two is the population that a volatile-* policy cannot reclaim.
func parseKeyspace(raw string) (keys, expires int64, ok bool) {
	for name, body := range parseInfoFields(raw) {
		if !strings.HasPrefix(name, "db") {
			continue
		}
		for _, part := range strings.Split(body, ",") {
			pair := strings.SplitN(strings.TrimSpace(part), "=", 2)
			if len(pair) != 2 {
				continue
			}
			value, err := strconv.ParseInt(pair[1], 10, 64)
			if err != nil {
				continue
			}
			switch pair[0] {
			case "keys":
				keys += value
				ok = true
			case "expires":
				expires += value
			}
		}
	}
	return keys, expires, ok
}
