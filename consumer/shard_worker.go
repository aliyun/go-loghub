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

	logger                 log.Logger
	lastCheckpointSaveTime time.Time
	shutDownFlag           *atomic.Bool
	stopped                *atomic.Bool
	startOnce              sync.Once
}

func newShardConsumerWorker(shardId int, consumerClient *ConsumerClient, consumerHeartBeat *ConsumerHeartBeat, processor Processor, logger log.Logger) *ShardConsumerWorker {
	shardConsumeWorker := &ShardConsumerWorker{
		processor:                 processor,
		consumerCheckPointTracker: initConsumerCheckpointTracker(shardId, consumerClient, consumerHeartBeat, logger),
		client:                    consumerClient,
		shardId:                   shardId,
		logger:                    logger,
		shutDownFlag:              atomic.NewBool(false),
		stopped:                   atomic.NewBool(false),
		lastCheckpointSaveTime:    time.Now(),
	}
	return shardConsumeWorker
}

func (c *ShardConsumerWorker) ensureStarted() {
	c.startOnce.Do(func() {
		go c.runLoop()
	})
}

func (c *ShardConsumerWorker) runLoop() {
	defer func() {
		if r := recover(); r != nil {
			stackBuf := make([]byte, 1<<16)
			n := runtime.Stack(stackBuf, false)
			level.Error(c.logger).Log("msg", "get panic in shard consumer worker runLoop",
				"shard", c.shardId,
				"error", r, "stack", stackBuf[:n])
		}
		c.doShutDown()
		c.stopped.Store(true)
	}()

	cursor := c.doInitCursor()

	for !c.shutDownFlag.Load() {
		// pull data
		logGroup, pullLogMeta, err := c.client.pullLogs(c.shardId, cursor)
		if err != nil {
			time.Sleep(fetchFailedSleepTime)
			continue
		}
		lastFetchSuccessTime := time.Now()
		// todo: refine this
		c.consumerCheckPointTracker.setCurrentCursor(pullLogMeta.NextCursor)
		c.consumerCheckPointTracker.setNextCursor(pullLogMeta.NextCursor)

		if cursor == pullLogMeta.NextCursor {
			c.saveCheckPointIfNeeded()
			time.Sleep(noProgressSleepTime) // already reach end of shard
			continue
		}

		// process
		for !c.shutDownFlag.Load() {
			rollBackCheckpoint, processErr := c.doProcess(logGroup)
			c.saveCheckPointIfNeeded() // todo: should we save checkpoint here even if failed
			if processErr != nil {
				level.Error(c.logger).Log("msg", "process failed", "shard", c.shardId, "err", err)
			}
			if rollBackCheckpoint != "" {
				cursor = rollBackCheckpoint
				level.Warn(c.logger).Log(
					"msg", "Checkpoints set for users have been reset",
					"shard", c.shardId,
					"rollBackCheckpoint", rollBackCheckpoint,
				)
			}
			// process ok or rollback, we should trigger next fetch
			if rollBackCheckpoint != "" || processErr == nil {
				break
			}
			time.Sleep(processFailedSleepTime)
		}

		sleepTime := c.timeToNextFetch(lastFetchSuccessTime, pullLogMeta)
		if sleepTime > 0 {
			time.Sleep(sleepTime)
		}
	}
}

func (consumer *ShardConsumerWorker) doInitCursor() string {
	for !consumer.shutDownFlag.Load() {
		initCursor, err := consumer.consumerInitializeTask()
		if err == nil {
			return initCursor
		}
		time.Sleep(100 * time.Millisecond)
	}
	return ""
}

func (c *ShardConsumerWorker) doProcess(logGroup *sls.LogGroupList) (rollBackCheckpoint string, err error) {
	defer func() {
		if r := recover(); r != nil {
			stackBuf := make([]byte, 1<<16)
			n := runtime.Stack(stackBuf, false)
			level.Error(c.logger).Log("msg", "get panic in your process function",
				"shard", c.shardId, "error", r, "stack", stackBuf[:n])
			err = fmt.Errorf("get a panic when process: %v", r)
		}
	}()

	rollBackCheckpoint, err = c.processor.Process(c.shardId, logGroup, c.consumerCheckPointTracker)
	return rollBackCheckpoint, err
}

// call user shutdown func and flush checkpoint
func (c *ShardConsumerWorker) doShutDown() {

	level.Warn(c.logger).Log("msg", "begin to call processor shutdown", "shard", c.shardId)
	for {
		err := c.processor.Shutdown(c.consumerCheckPointTracker)
		if err == nil {
			break
		}
		level.Error(c.logger).Log("msg", "failed to call processor shutdown", "shard", c.shardId, "err", err)
		time.Sleep(shutdownFailedSleepTime)
	}

	level.Warn(c.logger).Log("msg", "call processor shutdown end, begin to flush checkpoint", "shard", c.shardId)

	for {
		err := c.consumerCheckPointTracker.flushCheckPoint()
		if err == nil {
			break
		}
		level.Error(c.logger).Log("msg", "failed to flush checkpoint when shutdown", "shard", c.shardId, "err", err)
		time.Sleep(flushCheckPointFailedSleepTime)
	}
	level.Warn(c.logger).Log("msg", "shard worker status shutdown_complete", "shard", c.shardId)
}

func (c *ShardConsumerWorker) timeToNextFetch(lastFetchSuccessTime time.Time, pullLogMeta *sls.PullLogMeta) time.Duration {
	sinceLastFetch := time.Since(lastFetchSuccessTime)
	if sinceLastFetch > time.Duration(c.client.option.DataFetchIntervalInMs)*time.Millisecond {
		return 0
	}

	lastFetchRawSize := pullLogMeta.RawSize
	lastFetchGroupCount := pullLogMeta.Count
	if c.client.option.Query != "" {
		lastFetchRawSize = pullLogMeta.RawSizeBeforeQuery
		lastFetchGroupCount = pullLogMeta.DataCountBeforeQuery
	}

	if lastFetchGroupCount >= c.client.option.MaxFetchLogGroupCount || lastFetchRawSize >= 4*1024*1024 {
		return 0
	}
	if lastFetchGroupCount < 100 && lastFetchRawSize < 1024*1024 {
		// The time used here is in milliseconds.
		return 500*time.Millisecond - sinceLastFetch
	}
	if lastFetchGroupCount < 500 && lastFetchRawSize < 2*1024*1024 {
		return 200*time.Millisecond - sinceLastFetch
	}
	return 50*time.Millisecond - sinceLastFetch
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
	c.shutDownFlag.Store(true)
}

func (c *ShardConsumerWorker) isStopped() bool {
	return c.stopped.Load()
}
