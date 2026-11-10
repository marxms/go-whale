package app

import (
	"fmt"

	prommetrics "github.com/marxms/go-whale/internal/adapters/outbound/prometheus"
	"github.com/marxms/go-whale/internal/platform/netdiag"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// buildObservability creates the ONE Prometheus registry for the process. Every
// collector is defined and registered by the Prometheus adapter; /metrics serves this
// registry, never the default gatherer. It returns the provider because the storage
// and HTTP builders need capability-scoped slices of it.
func (a *App) buildObservability() (*prommetrics.Provider, error) {
	registry := promclient.NewRegistry()
	provider, err := prommetrics.New(registry)
	if err != nil {
		return nil, fmt.Errorf("metrics: %w", err)
	}

	a.metricsHandler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	a.metrics = AppMetrics{
		CoinalyzeCycleSeconds: provider.CoinalyzeCycleSeconds(),
		CoinalyzeCycleOverrun: provider.CoinalyzeCycleOverrun(),
		BootBucketSettle:      provider.BootBucketSettle(),
		GappedBucketRepair:    provider.GappedBucketRepair(),
	}
	a.streamMetrics = provider.Streaming()
	a.binanceMetrics = provider.Binance()
	a.ingestionMetrics = provider.Ingestion()
	a.engineMetrics = provider.Engine()
	a.collectorMetrics = provider.Collector()
	a.recoveryMetrics = provider.Recovery()
	a.redisMetrics = provider.Redis()
	a.questdbMetrics = provider.QuestDB()
	a.venueMetrics = provider.Venue()
	a.realtimeMetrics = provider.Realtime()
	a.netdiagMetrics = provider.Netdiag()
	a.healthRedisMetrics = provider.HealthRedis()
	a.netdiagCorrelator = netdiag.New()
	a.netdiagCorrelator.SetMetrics(a.netdiagMetrics)

	return provider, nil
}
