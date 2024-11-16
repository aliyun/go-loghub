package consumerLibrary

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/go-kit/kit/log/level"
)

func (consumer *ShardConsumerWorker) consumerInitializeTask() (string, error) {
	// read checkpoint firstly
	checkpoint, err := consumer.client.getCheckPoint(consumer.shardId)
	if err != nil {
		return "", err
	}
	if checkpoint != "" {
		consumer.consumerCheckPointTracker.initCheckPoint(checkpoint)
		return checkpoint, nil
	}

	if consumer.client.option.CursorPosition == BEGIN_CURSOR {
		cursor, err := consumer.client.getCursor(consumer.shardId, "begin")
		if err != nil {
			level.Warn(consumer.logger).Log("msg", "get beginCursor error", "shard", consumer.shardId, "error", err)
		}
		return cursor, err
	}
	if consumer.client.option.CursorPosition == END_CURSOR {
		cursor, err := consumer.client.getCursor(consumer.shardId, "end")
		if err != nil {
			level.Warn(consumer.logger).Log("msg", "get endCursor error", "shard", consumer.shardId, "error", err)
		}
		return cursor, err
	}
	if consumer.client.option.CursorPosition == SPECIAL_TIMER_CURSOR {
		cursor, err := consumer.client.getCursor(consumer.shardId, fmt.Sprintf("%v", consumer.client.option.CursorStartTime))
		if err != nil {
			level.Warn(consumer.logger).Log("msg", "get specialCursor error", "shard", consumer.shardId, "error", err)
		}
		return cursor, err
	}
	level.Warn(consumer.logger).Log("msg", "CursorPosition setting error, please reset with BEGIN_CURSOR or END_CURSOR or SPECIAL_TIMER_CURSOR")
	return "", errors.New("CursorPositionError")
}

func (consumer *ShardConsumerWorker) nextFetchTask() (hasProgress bool, err error) {
	// update last fetch time, for control fetch frequency
	consumer.lastFetchTime = time.Now()
	cursor := consumer.nextFetchCursor
	logGroup, pullLogMeta, err := consumer.client.pullLogs(consumer.shardId, cursor)
	if err != nil {
		return false, err
	}
	// set cursors user to decide whether to save according to the execution of `process`
	consumer.consumerCheckPointTracker.setCurrentCursor(consumer.nextFetchCursor)
	consumer.lastFetchLogGroupList = logGroup
	consumer.nextFetchCursor = pullLogMeta.NextCursor
	consumer.lastFetchRawSize = pullLogMeta.RawSize
	consumer.lastFetchGroupCount = pullLogMeta.Count
	if consumer.client.option.Query != "" {
		consumer.lastFetchRawSizeBeforeQuery = pullLogMeta.RawSizeBeforeQuery
		consumer.lastFetchGroupCountBeforeQuery = pullLogMeta.DataCountBeforeQuery
		if consumer.lastFetchRawSizeBeforeQuery == -1 {
			consumer.lastFetchRawSizeBeforeQuery = 0
		}
		if consumer.lastFetchGroupCountBeforeQuery == -1 {
			consumer.lastFetchGroupCountBeforeQuery = 0
		}
	}
	consumer.consumerCheckPointTracker.setNextCursor(consumer.nextFetchCursor)
	level.Debug(consumer.logger).Log(
		"shardId", consumer.shardId,
		"fetch log count", consumer.lastFetchGroupCount,
	)

	// if cursor == nextCursor, no progress is needed
	if cursor == pullLogMeta.NextCursor {
		consumer.lastFetchLogGroupList = nil
		consumer.saveCheckPointIfNeeded()
		return false, nil
	}

	return true, nil
}

func (consumer *ShardConsumerWorker) consumerProcessTask() (rollBackCheckpoint string, err error) {
	// If the user's consumption function reports a panic error, it will be captured and retry until sucessed.
	defer func() {
		if r := recover(); r != nil {
			stackBuf := make([]byte, 1<<16)
			n := runtime.Stack(stackBuf, false)
			level.Error(consumer.logger).Log("msg", "get panic in your process function", "error", r, "stack", stackBuf[:n])
			err = fmt.Errorf("get a panic when process: %v", r)
		}
	}()
	if consumer.lastFetchLogGroupList != nil {
		rollBackCheckpoint, err = consumer.processor.Process(consumer.shardId, consumer.lastFetchLogGroupList, consumer.consumerCheckPointTracker)
		consumer.saveCheckPointIfNeeded()
		if err != nil {
			return
		}
		consumer.lastFetchLogGroupList = nil
	}

	return
}
