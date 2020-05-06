package producer

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	sls "github.com/sanallen/aliyun-log-go-sdk"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

type CallBack interface {
	Success(result *Result)
	Fail(result *Result)
}

type IoWorker struct {
	taskCount              int64
	client                 *sls.Client
	retryQueue             *RetryQueue
	retryQueueShutDownFlag bool
	logger                 log.Logger
	maxIoWorker            chan int64
	noRetryStatusCodeMap   map[int]*string
}

func initIoWorker(client *sls.Client, retryQueue *RetryQueue, logger log.Logger, maxIoWorkerCount int64, errorStatusMap map[int]*string) *IoWorker {
	return &IoWorker{
		client:                 client,
		retryQueue:             retryQueue,
		taskCount:              0,
		retryQueueShutDownFlag: false,
		logger:                 logger,
		maxIoWorker:            make(chan int64, maxIoWorkerCount),
		noRetryStatusCodeMap:   errorStatusMap,
	}
}

func (ioWorker *IoWorker) sendToServer(producerBatch *ProducerBatch, ioWorkerWaitGroup *sync.WaitGroup) {
	level.Debug(ioWorker.logger).Log("msg", "ioworker send data to server")
	defer ioWorker.closeSendTask(ioWorkerWaitGroup)
	var err error
	atomic.AddInt64(&ioWorker.taskCount, 1)
	if producerBatch.shardHash != nil {
		err = ioWorker.client.PostLogStoreLogs(producerBatch.getProject(), producerBatch.getLogstore(), producerBatch.logGroup, producerBatch.getShardHash())
	} else {
		err = ioWorker.client.PutLogs(producerBatch.getProject(), producerBatch.getLogstore(), producerBatch.logGroup)
	}
	if err == nil {
		level.Debug(ioWorker.logger).Log("msg", "sendToServer suecssed,Execute successful callback function")
		if producerBatch.attemptCount < producerBatch.maxReservedAttempts {
			attempt := createAttempt(true, "", "", "", GetTimeMs(time.Now().UnixNano()))
			producerBatch.result.attemptList = append(producerBatch.result.attemptList, attempt)
		}
		producerBatch.result.successful = true
		// After successful delivery, producer removes the batch size sent out
		atomic.AddInt64(&producerLogGroupSize, -producerBatch.totalDataSize)
		if len(producerBatch.callBackList) > 0 {
			for _, callBack := range producerBatch.callBackList {
				callBack.Success(producerBatch.result)
			}
		}
	} else {
		if ioWorker.retryQueueShutDownFlag {
			if len(producerBatch.callBackList) > 0 {
				for _, callBack := range producerBatch.callBackList {
					callBack.Fail(producerBatch.result)
				}
			}
			return
		}
		level.Info(ioWorker.logger).Log("msg", "sendToServer failed", "error", err)
		if slsError, ok := err.(*sls.Error); ok {
			if _, ok := ioWorker.noRetryStatusCodeMap[int(slsError.HTTPCode)]; ok {
				ioWorker.excuteFailedCallback(producerBatch)
				return
			}
		}
		if producerBatch.attemptCount < producerBatch.maxRetryTimes {
			if producerBatch.attemptCount < producerBatch.maxReservedAttempts {
				slsError := err.(*sls.Error)
				level.Info(ioWorker.logger).Log("msg", "sendToServer failed,start retrying", "retry times", producerBatch.attemptCount, "requestId", slsError.RequestID, "error code", slsError.Code, "error message", slsError.Message)
				attempt := createAttempt(false, slsError.RequestID, slsError.Code, slsError.Message, GetTimeMs(time.Now().UnixNano()))
				producerBatch.result.attemptList = append(producerBatch.result.attemptList, attempt)
			}
			producerBatch.result.successful = false
			producerBatch.attemptCount += 1
			retryWaitTime := producerBatch.baseRetryBackoffMs * int64(math.Pow(2, float64(producerBatch.attemptCount)-1))
			if retryWaitTime < producerBatch.maxRetryIntervalInMs {
				producerBatch.nextRetryMs = GetTimeMs(time.Now().UnixNano()) + retryWaitTime
			} else {
				producerBatch.nextRetryMs = GetTimeMs(time.Now().UnixNano()) + producerBatch.maxRetryIntervalInMs
			}
			level.Debug(ioWorker.logger).Log("msg", "Submit to the retry queue after meeting the retry criteria。")
			ioWorker.retryQueue.sendToRetryQueue(producerBatch, ioWorker.logger)
		} else {
			ioWorker.excuteFailedCallback(producerBatch)
		}
	}
}

func (ioWorker *IoWorker) closeSendTask(ioWorkerWaitGroup *sync.WaitGroup) {
	ioWorkerWaitGroup.Done()
	atomic.AddInt64(&ioWorker.taskCount, -1)
	<-ioWorker.maxIoWorker
}

func (ioWorker *IoWorker) excuteFailedCallback(producerBatch *ProducerBatch) {
	level.Info(ioWorker.logger).Log("msg", "sendToServer failed,Execute failed callback function")
	if len(producerBatch.callBackList) > 0 {
		for _, callBack := range producerBatch.callBackList {
			callBack.Fail(producerBatch.result)
		}
	}
}
