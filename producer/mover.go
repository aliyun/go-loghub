package producer

import (
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"go.uber.org/atomic"
)

type Mover struct {
	moverShutDownFlag *atomic.Bool
	retryQueue        *RetryQueue
	ioWorker          *IoWorker
	logAccumulator    *LogAccumulator
	logger            log.Logger
	threadPool        *IoThreadPool
}

func initMover(logAccumulator *LogAccumulator, retryQueue *RetryQueue, ioWorker *IoWorker, logger log.Logger, threadPool *IoThreadPool) *Mover {
	mover := &Mover{
		moverShutDownFlag: atomic.NewBool(false),
		retryQueue:        retryQueue,
		ioWorker:          ioWorker,
		logAccumulator:    logAccumulator,
		logger:            logger,
		threadPool:        threadPool,
	}
	return mover

}

func (mover *Mover) run(moverWaitGroup *sync.WaitGroup, config *ProducerConfig) {
	defer moverWaitGroup.Done()
	defer mover.sendRemaining()

	for !mover.moverShutDownFlag.Load() {
		sleepMs := config.LingerMs
		nowTimeMs := time.Now().UnixMilli()
		toSendBatches := make([]*ProducerBatch, 0)

		mover.logAccumulator.lock.Lock()
		for key, batch := range mover.logAccumulator.logGroupData {
			if batch == nil {
				continue
			}
			timeInterval := batch.createTimeMs + config.LingerMs - nowTimeMs
			if timeInterval <= 0 {
				toSendBatches = append(toSendBatches, batch)
				mover.logAccumulator.logGroupData[key] = nil
			} else if sleepMs > timeInterval {
				sleepMs = timeInterval
			}
		}
		mover.logAccumulator.lock.Unlock()

		for _, batch := range toSendBatches {
			mover.threadPool.addTask(batch)
		}

		retryBatches := mover.retryQueue.getRetryBatch(mover.moverShutDownFlag.Load())
		if len(retryBatches) > 0 {
			for _, batch := range retryBatches {
				mover.threadPool.addTask(batch)
			}
			time.Sleep(time.Millisecond)
		} else {
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		}

		mover.clearEmptyKeys()
	}

}

func (mover *Mover) clearEmptyKeys() {
	mover.logAccumulator.lock.Lock()
	if len(mover.logAccumulator.logGroupData) > 1000 {
		for key, batch := range mover.logAccumulator.logGroupData {
			if batch == nil {
				delete(mover.logAccumulator.logGroupData, key)
			}
		}
	}
	mover.logAccumulator.lock.Unlock()
}

func (mover *Mover) sendRemaining() {
	mover.logAccumulator.lock.Lock()
	for _, batch := range mover.logAccumulator.logGroupData {
		if batch != nil && batch.totalDataSize > 0 {
			mover.threadPool.addTask(batch)
		}
	}
	mover.logAccumulator.logGroupData = make(map[string]*ProducerBatch)
	mover.logAccumulator.lock.Unlock()

	producerBatchList := mover.retryQueue.getRetryBatch(mover.moverShutDownFlag.Load())
	for _, batch := range producerBatchList {
		mover.threadPool.addTask(batch)
	}

	level.Info(mover.logger).Log("msg", "mover thread send remaining complete")
}
