package health

import "context"

// InfoReader reads one INFO section. The composition root supplies a closure over
// the live Redis client, so this package does not depend on a Redis driver.
type InfoReader func(ctx context.Context, section string) (string, error)

// SampleRedisBudget reads the three INFO sections and resolves the container
// ceiling.
//
// The order of preference for the ceiling matters: the daemon's LIVE value first,
// the YAML declaration second. The declaration exists because the Docker socket is
// not mounted in this service, and mounting it would trade a signal for a privilege
// escalation.
func SampleRedisBudget(
	ctx context.Context,
	read InfoReader,
	containerName string,
	declaredLimitMiB int,
) RedisBudgetSample {
	section := func(name string) string {
		raw, err := read(ctx, name)
		if err != nil {
			return ""
		}
		return raw
	}

	ceiling := int64(0)
	if live, err := DockerHostConfigMemory(ctx, DefaultDockerSocket, containerName); err == nil && live > 0 {
		ceiling = live
	} else if declaredLimitMiB > 0 {
		ceiling = int64(declaredLimitMiB) * (1 << 20)
	}

	return ParseRedisBudget(section("memory"), section("stats"), section("keyspace"), ceiling)
}
