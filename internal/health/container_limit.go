package health

// container_limit.go — the CONTAINER CEILING Redis occupies, read from where it is declared.
//
// Why the local cgroup does not answer this question: the streamer and Redis run in different
// containers with different limits (streamer 3 GiB, Redis 4 GiB). Reading
// `/sys/fs/cgroup/memory.max` from inside the streamer answers a different, legitimate question
// — "what is my ceiling" — while the ceiling that decides whether the Redis dataset takes
// `redis-server` to the OOM killer is the REDIS container's. From outside, the only place that
// ceiling is declared at runtime is `HostConfig.Memory` in the Docker daemon.
//
// `INFO memory` does not answer it either: Redis's `total_system_memory` comes from
// `/proc/meminfo`, which inside a container is still the HOST's RAM (8.8 GiB measured), not the
// cgroup limit. Reading that number as a ceiling would be wrong by 2.2x in the dangerous
// direction.
//
// The number never becomes a literal. When the Docker socket is not mounted, the read fails,
// the value stays ZERO and the signal declares that it did not read. Copying the `4G` from the
// compose file into the code would keep alerting against an outdated limit after someone raises
// it.
//
// The container name is not new vocabulary: it is the host part of `redis.addr`
// (`redis:6379`), which the configuration already declares.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// DefaultDockerSocket is where the daemon listens. It is a PATH, not a tuning number.
const DefaultDockerSocket = "/var/run/docker.sock"

// ContainerNameFromAddr extracts the container name from a Redis address such as
// `redis:6379`. Compose resolves a service name to its container, so the host part IS the
// name — which is why this needs no configuration key of its own.
func ContainerNameFromAddr(addr string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return strings.TrimSpace(addr)
	}
	return host
}

// DockerHostConfigMemory reads `HostConfig.Memory` for one container from the Docker daemon.
// It returns 0 for "no limit configured", and an error when the daemon could not be asked —
// and the caller must treat that error as "not read", never as "unlimited".
func DockerHostConfigMemory(ctx context.Context, socketPath, containerName string) (int64, error) {
	if socketPath == "" {
		socketPath = DefaultDockerSocket
	}
	if containerName == "" {
		return 0, fmt.Errorf("liveness: container name is empty; nothing to inspect")
	}

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "unix", socketPath)
			},
		},
		Timeout: 3 * time.Second,
	}

	// The host in the URL is ignored by the unix dialer above; the daemon requires one anyway.
	url := "http://docker/containers/" + containerName + "/json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("liveness: build docker inspect request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("liveness: docker inspect %q: %w", containerName, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("liveness: docker inspect %q returned %d", containerName, resp.StatusCode)
	}

	var payload struct {
		HostConfig struct {
			Memory int64 `json:"Memory"`
		} `json:"HostConfig"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, fmt.Errorf("liveness: decode docker inspect %q: %w", containerName, err)
	}
	return payload.HostConfig.Memory, nil
}
