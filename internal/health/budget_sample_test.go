package health

import (
	"context"
	"errors"
	"testing"
)

func staticInfoReader(sections map[string]string) InfoReader {
	return func(_ context.Context, section string) (string, error) {
		raw, ok := sections[section]
		if !ok {
			return "", errors.New("not declared in this fake")
		}
		return raw, nil
	}
}

// TestSampleRedisBudget_FallsBackToTheDeclaredCeilingWhenTheDaemonIsAbsent — the Docker
// socket is not mounted in this service, so the YAML declaration is what sustains the
// second occupancy.
func TestSampleRedisBudget_FallsBackToTheDeclaredCeilingWhenTheDaemonIsAbsent(t *testing.T) {
	read := staticInfoReader(map[string]string{
		"memory":   "# Memory\nused_memory:3770000000\nmaxmemory:3670016000\n",
		"stats":    "# Stats\nevicted_keys:77842\n",
		"keyspace": "# Keyspace\ndb0:keys=82766,expires=64045\n",
	})

	sample := SampleRedisBudget(context.Background(), read, "redis-inexistente", 4096)

	if sample.ContainerLimitBytes != 4096*(1<<20) {
		t.Fatalf("ceiling = %d, want 4096 MiB from the YAML declaration", sample.ContainerLimitBytes)
	}
	if got := sample.ContainerOccupancy(); got < 0.85 || got > 0.90 {
		t.Fatalf("container occupancy = %.3f, want ~0.86", got)
	}
	if sample.KeysWithoutTTL() != 18721 {
		t.Fatalf("keys without TTL = %d, want 18721", sample.KeysWithoutTTL())
	}
}

// TestSampleRedisBudget_UndeclaredCeilingStaysUnread — with no daemon and no declaration,
// the ceiling stays zero and the container occupancy is not invented.
func TestSampleRedisBudget_UndeclaredCeilingStaysUnread(t *testing.T) {
	read := staticInfoReader(map[string]string{
		"memory": "# Memory\nused_memory:1\nmaxmemory:2\n",
	})

	sample := SampleRedisBudget(context.Background(), read, "redis-inexistente", 0)
	if sample.ContainerLimitBytes != 0 || sample.ContainerOccupancy() != 0 {
		t.Fatalf("invented ceiling: limit=%d occupancy=%v", sample.ContainerLimitBytes, sample.ContainerOccupancy())
	}
}
