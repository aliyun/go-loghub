package producer

import (
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"go.uber.org/atomic"
)

type IoThreadPool struct {
	threadPoolShutDownFlag *atomic.Bool
	taskCh                 chan *ProducerBatch
	ioworker               *IoWorker
	logger                 log.Logger
	stopped                *atomic.Bool
}

func initIoThreadPool(ioworker *IoWorker, logger log.Logger) *IoThreadPool {
	return &IoThreadPool{
		threadPoolShutDownFlag: atomic.NewBool(false),
		taskCh:                 make(chan *ProducerBatch, 100000),
		ioworker:               ioworker,
		logger:                 logger,
		stopped:                atomic.NewBool(false),
	}
}

func (threadPool *IoThreadPool) addTask(batch *ProducerBatch) {
	threadPool.taskCh <- batch
}

func (threadPool *IoThreadPool) start(ioWorkerWaitGroup *sync.WaitGroup, ioThreadPoolwait *sync.WaitGroup) {
	defer ioThreadPoolwait.Done()
	for task := range threadPool.taskCh {
		if task == nil {
			level.Info(threadPool.logger).Log("msg", "All cache tasks in the thread pool have been successfully sent")
			threadPool.stopped.Store(true)
			return
		}

		threadPool.ioworker.startSendTask(ioWorkerWaitGroup)
		go func(producerBatch *ProducerBatch) {
			defer threadPool.ioworker.closeSendTask(ioWorkerWaitGroup)
			threadPool.ioworker.sendToServer(producerBatch)
		}(task)
	}
}

func (threadPool *IoThreadPool) ShutDown() {
	old := threadPool.threadPoolShutDownFlag.Swap(true)
	if !old {
		close(threadPool.taskCh)
	}
}

func (threadPool *IoThreadPool) Stopped() bool {
	return threadPool.stopped.Load()
}
