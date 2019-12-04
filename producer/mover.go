package producer

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"sync"
	"time"
)

type Mover struct {
	moverShutDownFlag bool
	retryQueue        *RetryQueue
	ioWorker          *IoWorker
	logAccumulator    *LogAccumulator
	logger            log.Logger
	threadPool        *IoThreadPool
}

func initMover(logAccumulator *LogAccumulator, retryQueue *RetryQueue, ioWorker *IoWorker, logger log.Logger, threadPool *IoThreadPool) *Mover {
	mover := &Mover{
		moverShutDownFlag: false,
		retryQueue:        retryQueue,
		ioWorker:          ioWorker,
		logAccumulator:    logAccumulator,
		logger:            logger,
		threadPool:        threadPool,
	}
	return mover

}

func (mover *Mover) sendToServer(key interface{}, batch *ProducerBatch, config *ProducerConfig) {
	defer ioLock.Unlock()
	ioLock.Lock()
	if value, ok := mover.logAccumulator.logGroupData.Load(key); !ok {
		return
	} else if GetTimeMs(time.Now().UnixNano())-value.(*ProducerBatch).createTimeMs < config.LingerMs {
		return
	}
	mover.threadPool.addTask(batch)
	mover.logAccumulator.logGroupData.Delete(key)
}

func (mover *Mover) run(moverWaitGroup *sync.WaitGroup, config *ProducerConfig) {
	defer moverWaitGroup.Done()
	for !mover.moverShutDownFlag {
		sleepMs := config.LingerMs
		mapCount := 0
		mover.logAccumulator.logGroupData.Range(func(key, value interface{}) bool {
			mapCount = mapCount + 1
			if batch, ok := value.(*ProducerBatch); ok {
				timeInterval := batch.createTimeMs + config.LingerMs - GetTimeMs(time.Now().UnixNano())
				if timeInterval <= 0 {
					level.Debug(mover.logger).Log("msg", "mover groutine execute sent producerBatch to IoWorker")
					mover.sendToServer(key, batch, config)
				} else {
					if sleepMs > timeInterval {
						sleepMs = timeInterval
					}
				}
			}
			return true
		})
		if mapCount == 0 {
			level.Debug(mover.logger).Log("msg", "No cached data in producer, waiting for new data to be written")
			sleepMs = config.LingerMs
		}

		retryProducerBatchList := mover.retryQueue.getRetryBatch(mover.moverShutDownFlag)
		if retryProducerBatchList == nil {
			// If there is nothing to send in the retry queue, just wait for the minimum time that was given to me last time.
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		} else {
			count := len(retryProducerBatchList)
			for i := 0; i < count; i++ {
				mover.threadPool.addTask(retryProducerBatchList[i])
			}
		}

	}
	mover.logAccumulator.logGroupData.Range(func(key, batch interface{}) bool {
		mover.threadPool.addTask(batch.(*ProducerBatch))
		mover.logAccumulator.logGroupData.Delete(key)
		return true
	})

	producerBatchList := mover.retryQueue.getRetryBatch(mover.moverShutDownFlag)
	count := len(producerBatchList)
	for i := 0; i < count; i++ {
		mover.threadPool.addTask(producerBatchList[i])
	}
	level.Info(mover.logger).Log("msg", "mover thread closure complete")
}
