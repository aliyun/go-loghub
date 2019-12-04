package producer

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"sync"
	"time"
)

type IoThreadPool struct {
	threadPoolShutDownFlag bool
	queue                  []*ProducerBatch
	lock                   sync.RWMutex
	ioworker               *IoWorker
	logger                 log.Logger
}

func initIoThreadPool(ioworker *IoWorker, logger log.Logger) *IoThreadPool {
	return &IoThreadPool{
		threadPoolShutDownFlag: false,
		queue:                  []*ProducerBatch{},
		ioworker:               ioworker,
		logger:                 logger,
	}
}

func (threadPool *IoThreadPool) addTask(batch *ProducerBatch) {
	defer threadPool.lock.Unlock()
	threadPool.lock.Lock()
	threadPool.queue = append(threadPool.queue, batch)
}

func (threadPool *IoThreadPool) popTask() *ProducerBatch {
	defer threadPool.lock.Unlock()
	threadPool.lock.Lock()
	batch := threadPool.queue[0]
	threadPool.queue = threadPool.queue[1:]
	return batch
}

func (threadPool *IoThreadPool) start(ioWorkerWaitGroup *sync.WaitGroup, ioThreadPoolwait *sync.WaitGroup) {
	defer ioThreadPoolwait.Done()
	for {
		if len(threadPool.queue) > 0 {
			select {
			case threadPool.ioworker.maxIoWorker <- 1:
				ioWorkerWaitGroup.Add(1)
				go threadPool.ioworker.sendToServer(threadPool.popTask(), ioWorkerWaitGroup)
			}
		} else {
			if !threadPool.threadPoolShutDownFlag {
				time.Sleep(100 * time.Millisecond)
			} else {
				level.Info(threadPool.logger).Log("msg", "All cache tasks in the thread pool have been successfully sent")
				break
			}
		}
	}

}
