package consumerLibrary

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"go.uber.org/atomic"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// todo: refine the sleep time
const noProgressSleepTime = 500 * time.Millisecond
const processFailedSleepTime = 50 * time.Millisecond
const fetchFailedSleepTime = 100 * time.Millisecond // todo: use backoff interval, [1, 2, 4, 8, ...]
const shutdownFailedSleepTime = 100 * time.Millisecond
const flushCheckPointFailedSleepTime = 100 * time.Millisecond

type ShardConsumerWorker struct {
	client                    *ConsumerClient
	consumerCheckPointTracker *DefaultCheckPointTracker
	processor                 Processor
	shardId                   int
	metricsCollector          *MetricsCollector

	logger                 log.Logger
	lastCheckpointSaveTime time.Time
	shutDownFlag           *atomic.Bool
	stopped                *atomic.Bool
	startOnce              sync.Once
}

func newShardConsumerWorker(shardId int, consumerClient *ConsumerClient, consumerHeartBeat *ConsumerHeartBeat, processor Processor, logger log.Logger, metricsCollector *MetricsCollector) *ShardConsumerWorker {
	shardConsumeWorker := &ShardConsumerWorker{
		processor:                 processor,
		consumerCheckPointTracker: initConsumerCheckpointTracker(shardId, consumerClient, consumerHeartBeat, logger),
		client:                    consumerClient,
		shardId:                   shardId,
		logger:                    log.With(logger, "shard", shardId),
		shutDownFlag:              atomic.NewBool(false),
		stopped:                   atomic.NewBool(false),
		lastCheckpointSaveTime:    time.Now(),
		metricsCollector:          metricsCollector,
	}
	return shardConsumeWorker
}

func (c *ShardConsumerWorker) ensureStarted() {
	c.startOnce.Do(func() {
		go c.runLoop()
	})
}

func (c *ShardConsumerWorker) runLoop() {
	level.Info(c.logger).Log("msg", "runLoop started")
	defer func() {
		c.recoverIfPanic("runLoop panic")
		c.doShutDown()
	}()

	cursor := c.getInitCursor()
	level.Info(c.logger).Log("msg", "runLoop got init cursor", "cursor", cursor)

	for !c.shutDownFlag.Load() {
		fetchTime := time.Now()
		shouldCallProcess, logGroupList, plm, sleepTime := c.fetchLogs(cursor)
		if sleepTime > 0 {
			time.Sleep(sleepTime)
		}
		if !shouldCallProcess {
			continue
		}

		cursor = c.callProcess(cursor, logGroupList, plm)
		if c.shutDownFlag.Load() {
			break
		}

		c.sleepUtilNextFetch(fetchTime, plm)
	}
}

func (consumer *ShardConsumerWorker) getInitCursor() string {
	for !consumer.shutDownFlag.Load() {
		initCursor, err := consumer.consumerInitializeTask()
		if err == nil {
			return initCursor
		}
		time.Sleep(100 * time.Millisecond)
	}
	return ""
}

func (c *ShardConsumerWorker) fetchLogs(cursor string) (shouldCallProcess bool, logGroupList *sls.LogGroupList, plm *sls.PullLogMeta, sleepTime time.Duration) {
	defer c.metricsCollector.sampleMetrics(c.shardId, "fetchLogs", time.Now())
	logGroupList, plm, err := c.client.pullLogs(c.shardId, cursor)
	if err != nil {
		return false, nil, nil, fetchFailedSleepTime
	}

	c.consumerCheckPointTracker.setCurrentCursor(cursor)
	c.consumerCheckPointTracker.setNextCursor(plm.NextCursor)

	if cursor == plm.NextCursor { // already reach end of shard
		c.saveCheckPointIfNeeded()
		return false, nil, nil, noProgressSleepTime
	}
	return true, logGroupList, plm, 0
}

func (c *ShardConsumerWorker) callProcess(cursor string, logGroupList *sls.LogGroupList, plm *sls.PullLogMeta) (nextCursor string) {
	for !c.shutDownFlag.Load() {
		rollBackCheckpoint, err := c.processInternal(logGroupList)
		c.saveCheckPointIfNeeded() // todo: should we save checkpoint here even if failed
		if err != nil {
			level.Error(c.logger).Log("msg", "process func returns an error", "err", err)
		}
		if rollBackCheckpoint != "" {
			level.Warn(c.logger).Log("msg", "Rollback checkpoint by user",
				"rollBackCheckpoint", rollBackCheckpoint)
			return rollBackCheckpoint
		}
		if err == nil {
			return plm.NextCursor
		}
		time.Sleep(processFailedSleepTime)
	}
	return cursor
}

func (c *ShardConsumerWorker) processInternal(logGroup *sls.LogGroupList) (rollBackCheckpoint string, err error) {
	defer c.metricsCollector.sampleMetrics(c.shardId, "processInternal", time.Now())
	defer func() {
		if r := c.recoverIfPanic("your process function paniced"); r != nil {
			err = fmt.Errorf("get a panic when process: %v", r)
		}
	}()

	return c.processor.Process(c.shardId, logGroup, c.consumerCheckPointTracker)
}

// call user shutdown func and flush checkpoint
func (c *ShardConsumerWorker) doShutDown() {
	level.Info(c.logger).Log("msg", "start to shutdown, begin to call processor shutdown")
	for {
		err := c.processor.Shutdown(c.consumerCheckPointTracker) // todo: should we catch panic here?
		if err == nil {
			break
		}
		level.Error(c.logger).Log("msg", "failed to call processor shutdown", "err", err)
		time.Sleep(shutdownFailedSleepTime)
	}

	level.Info(c.logger).Log("msg", "call processor shutdown succeed, begin to flush checkpoint")

	for {
		err := c.consumerCheckPointTracker.flushCheckPoint()
		if err == nil {
			break
		}
		level.Error(c.logger).Log("msg", "failed to flush checkpoint when shutting down", "err", err)
		time.Sleep(flushCheckPointFailedSleepTime)
	}
	level.Info(c.logger).Log("msg", "shutting down completed, bye")
	c.stopped.Store(true)
}

// todo: refine sleep time, make it more reasonable
func (c *ShardConsumerWorker) sleepUtilNextFetch(lastFetchSuccessTime time.Time, plm *sls.PullLogMeta) {
	sinceLastFetch := time.Since(lastFetchSuccessTime)
	if sinceLastFetch > time.Duration(c.client.option.DataFetchIntervalInMs)*time.Millisecond {
		return
	}

	lastFetchRawSize := plm.RawSize
	lastFetchGroupCount := plm.Count
	if c.client.option.Query != "" {
		lastFetchRawSize = plm.RawSizeBeforeQuery
		lastFetchGroupCount = plm.DataCountBeforeQuery
	}

	if lastFetchGroupCount >= c.client.option.MaxFetchLogGroupCount || lastFetchRawSize >= 4*1024*1024 {
		return
	}
	// negative or zero sleepTime is ok
	if lastFetchGroupCount < 100 && lastFetchRawSize < 1024*1024 {
		time.Sleep(500*time.Millisecond - sinceLastFetch)
		return
	}
	if lastFetchGroupCount < 500 && lastFetchRawSize < 2*1024*1024 {
		time.Sleep(200*time.Millisecond - sinceLastFetch)
		return
	}

	time.Sleep(50*time.Millisecond - sinceLastFetch)
}

func (c *ShardConsumerWorker) saveCheckPointIfNeeded() {
	if c.client.option.AutoCommitDisabled {
		return
	}
	if time.Since(c.lastCheckpointSaveTime) > time.Millisecond*time.Duration(c.client.option.AutoCommitIntervalInMS) {
		c.consumerCheckPointTracker.flushCheckPoint()
		c.lastCheckpointSaveTime = time.Now()
	}
}

func (c *ShardConsumerWorker) shutdown() {
	level.Info(c.logger).Log("msg", "shutting down by others")
	c.shutDownFlag.Store(true)
}

func (c *ShardConsumerWorker) isStopped() bool {
	return c.stopped.Load()
}

func (c *ShardConsumerWorker) recoverIfPanic(reason string) any {
	if r := recover(); r != nil {
		stackBuf := make([]byte, 1<<16)
		n := runtime.Stack(stackBuf, false)
		level.Error(c.logger).Log("msg", "get panic in shard consumer worker",
			"reason", reason,
			"error", r, "stack", stackBuf[:n])
		return r
	}
	return nil
}
