package consumerLibrary

import (
	"fmt"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/hashicorp/go-metrics"
)

type MetricsCollector struct {
	enabled bool
	sink    *metrics.InmemSink
	metrics *metrics.Metrics
	logger  log.Logger

	lastDump time.Time
}

func newMetricCollector(config *LogHubConfig, logger log.Logger) *MetricsCollector {
	metricsCollector := &MetricsCollector{
		enabled:  !config.metricDisabled,
		logger:   logger,
		lastDump: time.Now(),
	}
	if config.metricDisabled {
		return metricsCollector
	}
	metricsCollector.sink = metrics.NewInmemSink(15*time.Second, time.Minute)
	metricConfig := metrics.DefaultConfig("aliyun-log")
	metricConfig.EnableHostname = false
	metricConfig.EnableHostnameLabel = false
	metricConfig.EnableRuntimeMetrics = false
	metricConfig.TimerGranularity = time.Microsecond

	metricsCollector.metrics, _ = metrics.New(metricConfig, metricsCollector.sink)
	return metricsCollector
}

func (m *MetricsCollector) maybeDumpMetrics() {
	if !m.enabled || time.Since(m.lastDump) < time.Second*30 {
		return
	}
	m.lastDump = time.Now()

	// intervals := m.sink.Data()
	// if len(intervals) == 0 {
	// 	return
	// }
	// interval := intervals[len(intervals)-1]
	// interval.RLock()
	// defer interval.RUnlock()
	// todo: dump metrics
	i, _ := m.sink.DisplayMetrics(nil, nil)
	fmt.Printf("%#v\n", i.(metrics.MetricsSummary).Samples)
}

func (m *MetricsCollector) sampleMetrics(shard int, name string, time time.Time) {
	if !m.enabled {
		return
	}
	m.metrics.MeasureSinceWithLabels([]string{name}, time, []metrics.Label{
		{
			Name:  "shard",
			Value: strconv.Itoa(shard),
		},
	})
}
