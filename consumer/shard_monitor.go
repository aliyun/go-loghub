package consumerLibrary

import (
	"fmt"
	"math"
	"time"

	"go.uber.org/atomic"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/go-kit/kit/log"
)

type MonitorMetrics struct {
	fetchReqFailedCount atomic.Int64
	logRawSize          atomic.Int64
	fetchLogHistogram   TimeHistogram // in us

	processFailedCount atomic.Int64
	processHistogram   TimeHistogram // in us
}

type ShardMonitor struct {
	shard          int
	reportInterval time.Duration
	lastReportTime time.Time
	metrics        atomic.Value // *MonitorMetrics
}

func newShardMonitor(shard int, reportInterval time.Duration) *ShardMonitor {
	monitor := &ShardMonitor{
		shard:          shard,
		reportInterval: reportInterval,
		lastReportTime: time.Now(),
	}
	monitor.metrics.Store(&MonitorMetrics{})
	return monitor
}

func (m *ShardMonitor) RecordFetchRequest(plm *sls.PullLogMeta, err error, start time.Time) {
	metrics := m.metrics.Load().(*MonitorMetrics)
	if err != nil {
		metrics.fetchReqFailedCount.Inc()
	} else {
		metrics.logRawSize.Add(int64(plm.RawSize))
	}
	metrics.fetchLogHistogram.AddSample(float64(time.Since(start).Microseconds()))
}

func (m *ShardMonitor) RecordProcess(err error, start time.Time) {
	metrics := m.metrics.Load().(*MonitorMetrics)
	if err != nil {
		metrics.processFailedCount.Inc()
	}
	metrics.processHistogram.AddSample(float64(time.Since(start).Microseconds()))
}

func (m *ShardMonitor) getAndResetMetrics() *MonitorMetrics {
	// we dont need cmp and swap, only one thread would call m.metrics.Store
	old := m.metrics.Load().(*MonitorMetrics)
	m.metrics.Store(&MonitorMetrics{})
	return old
}

func (m *ShardMonitor) shouldReport() bool {
	return time.Since(m.lastReportTime) >= m.reportInterval
}

func (m *ShardMonitor) reportByLogger(logger log.Logger) {
	m.lastReportTime = time.Now()
	metrics := m.getAndResetMetrics()
	logger.Log("msg", "report status",
		"fetchFailed", metrics.fetchReqFailedCount.Load(),
		"logRawSize", metrics.logRawSize.Load(),
		"processFailed", metrics.processFailedCount.Load(),
		"fetch", metrics.fetchLogHistogram.String(),
		"process", metrics.processHistogram.String(),
	)
}

type TimeHistogram struct {
	Count     atomic.Int64
	Sum       atomic.Float64
	SumSquare atomic.Float64
}

func (h *TimeHistogram) AddSample(v float64) {
	h.Count.Inc()
	h.Sum.Add(v)
	h.SumSquare.Add(v * v)
}

func (h *TimeHistogram) String() string {
	avg := h.Avg()
	stdDev := h.StdDev()
	count := h.Count.Load()
	return fmt.Sprintf("{avg: %.1fus, stdDev: %.1fus, count: %d}", avg, stdDev, count)
}

func (h *TimeHistogram) Avg() float64 {
	count := h.Count.Load()
	if count == 0 {
		return 0
	}
	return h.Sum.Load() / float64(count)
}

func (h *TimeHistogram) StdDev() float64 {
	count := h.Count.Load()
	if count < 2 {
		return 0
	}
	div := float64(count * (count - 1))
	num := (float64(count) * h.SumSquare.Load()) - math.Pow(h.Sum.Load(), 2)
	return math.Sqrt(num / div)
}
