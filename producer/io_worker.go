package producer

import (
	"sync"
	"sync/atomic"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	uberatomic "go.uber.org/atomic"
)

type CallBack interface {
	Success(result *Result)
	Fail(result *Result)
}

type IoWorker struct {
	taskCount              int64
	client                 sls.ClientInterface
	retryQueue             *RetryQueue
	retryQueueShutDownFlag *uberatomic.Bool
	logger                 log.Logger
	maxIoWorker            chan int64
	noRetryStatusCodeMap   map[int]*string
	producer               *Producer
}

func initIoWorker(client sls.ClientInterface, retryQueue *RetryQueue, logger log.Logger, maxIoWorkerCount int64, errorStatusMap map[int]*string, producer *Producer) *IoWorker {
	return &IoWorker{
		client:                 client,
		retryQueue:             retryQueue,
		taskCount:              0,
		retryQueueShutDownFlag: uberatomic.NewBool(false),
		logger:                 logger,
		maxIoWorker:            make(chan int64, maxIoWorkerCount),
		noRetryStatusCodeMap:   errorStatusMap,
		producer:               producer,
	}
}

func (ioWorker *IoWorker) sendToServer(producerBatch *ProducerBatch) {
	level.Debug(ioWorker.logger).Log("msg", "ioworker send data to server")
	sendBegin := time.Now()
	var err error
	if producerBatch.isUseMetricStoreUrl() {
		// not use compress type now
		err = ioWorker.client.PutLogsWithMetricStoreURL(producerBatch.getProject(), producerBatch.getLogstore(), producerBatch.logGroup)
	} else {
		req := &sls.PostLogStoreLogsRequest{
			LogGroup:     producerBatch.logGroup,
			HashKey:      producerBatch.getShardHash(),
			CompressType: ioWorker.producer.producerConfig.CompressType,
			Processor:    ioWorker.producer.producerConfig.Processor,
		}
		err = ioWorker.client.PostLogStoreLogsV2(producerBatch.getProject(), producerBatch.getLogstore(), req)
	}
	sendEnd := time.Now()

	// send ok
	if err == nil {
		level.Debug(ioWorker.logger).Log("msg", "sendToServer success")
		defer ioWorker.producer.monitor.recordSuccess(sendBegin, sendEnd)
		producerBatch.OnSuccess(sendBegin)
		// After successful delivery, producer removes the batch size sent out
		atomic.AddInt64(&ioWorker.producer.producerLogGroupSize, -producerBatch.totalDataSize)
		return
	}

	slsError := parseSlsError(err)
	canRetry := ioWorker.canRetry(producerBatch, slsError)
	level.Error(ioWorker.logger).Log("msg", "sendToServer failed",
		"retryTimes", producerBatch.attemptCount,
		"requestId", slsError.RequestID,
		"errorCode", slsError.Code,
		"errorMessage", slsError.Message,
		"canRetry", canRetry)
	if !canRetry {
		defer ioWorker.producer.monitor.recordFailure(sendBegin, sendEnd)
		producerBatch.OnFail(slsError, sendBegin)
		atomic.AddInt64(&ioWorker.producer.producerLogGroupSize, -producerBatch.totalDataSize)
		return
	}

	// do retry
	ioWorker.producer.monitor.recordRetry(sendEnd.Sub(sendBegin))
	producerBatch.addAttempt(slsError, sendBegin)
	producerBatch.nextRetryMs = producerBatch.getRetryBackoffIntervalMs() + time.Now().UnixMilli()
	level.Debug(ioWorker.logger).Log("msg", "Submit to the retry queue after meeting the retry criteria。")
	ioWorker.retryQueue.sendToRetryQueue(producerBatch, ioWorker.logger)
}

func parseSlsError(err error) *sls.Error {
	if slsError, ok := err.(*sls.Error); ok {
		return slsError
	}
	return &sls.Error{
		Message: err.Error(),
	}
}

func (ioWorker *IoWorker) canRetry(producerBatch *ProducerBatch, err *sls.Error) bool {
	if ioWorker.retryQueueShutDownFlag.Load() {
		return false
	}
	if _, ok := ioWorker.noRetryStatusCodeMap[int(err.HTTPCode)]; ok {
		return false
	}
	return producerBatch.attemptCount < producerBatch.maxRetryTimes
}

func (ioWorker *IoWorker) closeSendTask(ioWorkerWaitGroup *sync.WaitGroup) {
	<-ioWorker.maxIoWorker
	atomic.AddInt64(&ioWorker.taskCount, -1)
	ioWorkerWaitGroup.Done()
}

func (ioWorker *IoWorker) startSendTask(ioWorkerWaitGroup *sync.WaitGroup) {
	atomic.AddInt64(&ioWorker.taskCount, 1)
	ioWorker.maxIoWorker <- 1
	ioWorkerWaitGroup.Add(1)
}
