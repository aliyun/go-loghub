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

const NO_PROGRESS_SLEEP_TIME = 500 * time.Millisecond
const PROCESS_ERROR_OCCUR_SLEEP_TIME = 100 * time.Millisecond
const FETCH_FAILED_SLEEP_TIME = 100 * time.Millisecond
const CALL_USER_SHUTDOWN_FAILED_SLEEP_TIME = 100 * time.Millisecond
const FLUSH_CHECKPOINT_BEFORE_SHUTDOWN_FAILED_SLEEP_TIME = 100 * time.Millisecond

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

func initShardConsumerWorker(shardId int, consumerClient *ConsumerClient, consumerHeartBeat *ConsumerHeartBeat, processor Processor, logger log.Logger) *ShardConsumerWorker {
	shardConsumeWorker := &ShardConsumerWorker{
		processor:                 processor,
		consumerCheckPointTracker: initConsumerCheckpointTracker(shardId, consumerClient, consumerHeartBeat, logger),
		client:                    consumerClient,
		shardId:                   shardId,
		logger:                    logger,
		shutDownFlag:              atomic.NewBool(false),
		stopped:                   atomic.NewBool(true),
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
	c.stopped.Store(false)

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

	// init
	cursor := c.doInitCursor()
	lastFetchSuccessTime := time.Now()

	for !c.shutDownFlag.Load() {
		// pull data
		logGroup, pullLogMeta, err := c.client.pullLogs(c.shardId, cursor)
		if err != nil {
			time.Sleep(FETCH_FAILED_SLEEP_TIME)
			continue
		}
		lastFetchSuccessTime = time.Now()
		c.consumerCheckPointTracker.setCurrentCursor(pullLogMeta.NextCursor)
		c.consumerCheckPointTracker.setNextCursor(pullLogMeta.NextCursor)

		if cursor == pullLogMeta.NextCursor {
			c.saveCheckPointIfNeeded()
			time.Sleep(NO_PROGRESS_SLEEP_TIME) // already reach end of shard
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
			time.Sleep(PROCESS_ERROR_OCCUR_SLEEP_TIME)
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

func (consumer *ShardConsumerWorker) doProcess(logGroup *sls.LogGroupList) (rollBackCheckpoint string, err error) {
	defer func() {
		if r := recover(); r != nil {
			stackBuf := make([]byte, 1<<16)
			n := runtime.Stack(stackBuf, false)
			level.Error(consumer.logger).Log("msg", "get panic in your process function", "error", r, "stack", stackBuf[:n])
			err = fmt.Errorf("get a panic when process: %v", r)
		}
	}()

	rollBackCheckpoint, err = consumer.processor.Process(consumer.shardId, logGroup, consumer.consumerCheckPointTracker)
	return rollBackCheckpoint, err
}

func (consumer *ShardConsumerWorker) doShutDown() {
	// call user shutdown func
	level.Warn(consumer.logger).Log("msg", "begin to call processor shutdown", "shard", consumer.shardId)
	for {
		err := consumer.processor.Shutdown(consumer.consumerCheckPointTracker)
		if err == nil {
			break
		}
		level.Error(consumer.logger).Log("msg", "failed to call processor shutdown", "shard", consumer.shardId, "err", err)
		time.Sleep(CALL_USER_SHUTDOWN_FAILED_SLEEP_TIME)
	}
	level.Warn(consumer.logger).Log("msg", "call processor shutdown end", "shard", consumer.shardId)

	// save checkpoint before exit
	level.Warn(consumer.logger).Log("msg", "begin to flush checkpoint before exit", "shard", consumer.shardId)
	for {
		err := consumer.consumerCheckPointTracker.flushCheckPoint()
		if err == nil {
			break
		}
		level.Error(consumer.logger).Log("msg", "failed to flush checkpoint when shutdown", "shard", consumer.shardId, "err", err)
		time.Sleep(FLUSH_CHECKPOINT_BEFORE_SHUTDOWN_FAILED_SLEEP_TIME)
	}
	level.Warn(consumer.logger).Log("msg", "shard worker status shutdown_complete", "shard", consumer.shardId)
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

func (consumer *ShardConsumerWorker) saveCheckPointIfNeeded() {
	if consumer.client.option.AutoCommitDisabled {
		return
	}
	if time.Since(consumer.lastCheckpointSaveTime) > time.Millisecond*time.Duration(consumer.client.option.AutoCommitIntervalInMS) {
		consumer.consumerCheckPointTracker.flushCheckPoint()
		consumer.lastCheckpointSaveTime = time.Now()
	}
}

func (consumer *ShardConsumerWorker) shutdown() {
	consumer.shutDownFlag.Store(true)
}

func (consumer *ShardConsumerWorker) isStopped() bool {
	return consumer.stopped.Load()
}
