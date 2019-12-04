package producer

import (
	"github.com/aliyun/aliyun-log-go-sdk"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TimeoutExecption      = "TimeoutExecption"
	IllegalStateException = "IllegalStateException"
	Project 			  = "project"
)

var producerLogGroupSize int64
var ioLock sync.RWMutex
var clientGroup *sync.Map

type Producer struct {
	producerConfig        *ProducerConfig
	logAccumulator        *LogAccumulator
	mover                 *Mover
	threadPool            *IoThreadPool
	moverWaitGroup        *sync.WaitGroup
	ioWorkerWaitGroup     *sync.WaitGroup
	ioThreadPoolWaitGroup *sync.WaitGroup
	adjustShardHash       bool
	buckets               int
	logger                log.Logger
}

func InitProducer(producerConfig *ProducerConfig) *Producer {
	logger := logConfig(producerConfig)
	clientGroup = &sync.Map{}
	finalProducerConfig := validateProducerConfig(producerConfig)
	retryQueue := initRetryQueue()
	errorStatusMap := func() map[int]*string {
		errorCodeMap := map[int]*string{}
		for _, v := range producerConfig.NoRetryStatusCodeList {
			errorCodeMap[int(v)] = nil
		}
		return errorCodeMap
	}()
	PutNewProducerConfig(producerConfig)
	ioWorker := initIoWorker(retryQueue, logger, finalProducerConfig.MaxIoWorkerCount, errorStatusMap)
	threadPool := initIoThreadPool(ioWorker, logger)
	logAccumulator := initLogAccumulator(finalProducerConfig, ioWorker, logger, threadPool)
	mover := initMover(logAccumulator, retryQueue, ioWorker, logger, threadPool)
	producer := &Producer{
		producerConfig: finalProducerConfig,
		logAccumulator: logAccumulator,
		mover:          mover,
		threadPool:     threadPool,
		buckets:        finalProducerConfig.Buckets,
	}
	producer.moverWaitGroup = &sync.WaitGroup{}
	producer.ioWorkerWaitGroup = &sync.WaitGroup{}
	producer.ioThreadPoolWaitGroup = &sync.WaitGroup{}
	producer.logger = logger
	return producer
}

func validateProducerConfig(producerConfig *ProducerConfig) *ProducerConfig {
	logger := logConfig(producerConfig)
	if producerConfig.MaxReservedAttempts <= 0 {
		level.Warn(logger).Log("msg", "This MaxReservedAttempts parameter must be greater than zero,program auto correction to default value")
		producerConfig.MaxReservedAttempts = 11
	}
	if producerConfig.MaxBatchCount > 40960 || producerConfig.MaxBatchCount <= 0 {
		level.Warn(logger).Log("msg", "The parameter MaxBatchCount exceeds the set maximum and has been reset to the set maximum of 40960.")
		producerConfig.MaxBatchCount = 40960
	}
	if producerConfig.MaxBatchSize > 1024*1024*5 || producerConfig.MaxBatchSize <= 0 {
		level.Warn(logger).Log("msg", "The parameter MaxBatchSize exceeds the settable maximum and has reset a single logGroup memory size of up to 5M.")
		producerConfig.MaxBatchSize = 1024 * 1024 * 5
	}
	if producerConfig.MaxIoWorkerCount <= 0 {
		level.Warn(logger).Log("msg", "The MaxIoWorkerCount parameter cannot be less than zero and has been reset to the default value of 50")
		producerConfig.MaxIoWorkerCount = 50
	}
	if producerConfig.BaseRetryBackoffMs <= 0 {
		level.Warn(logger).Log("msg", "The BaseRetryBackoffMs parameter cannot be less than zero and has been reset to the default value of 100 milliseconds")
		producerConfig.BaseRetryBackoffMs = 100
	}
	if producerConfig.TotalSizeLnBytes <= 0 {
		level.Warn(logger).Log("msg", "The TotalSizeLnBytes parameter cannot be less than zero and has been reset to the default value of 100M")
		producerConfig.TotalSizeLnBytes = 100 * 1024 * 1024
	}
	if producerConfig.LingerMs < 100 {
		level.Warn(logger).Log("msg", "The LingerMs parameter cannot be less than 100 milliseconds and has been reset to the default value of 2000 milliseconds")
		producerConfig.LingerMs = 2000
	}
	return producerConfig
}

func (producer *Producer) HashSendLogWithCallBack(project, logstore, shardHash, topic, source string, log *sls.Log, callback CallBack) error {
	err := producer.waitTime()
	if err != nil {
		return err
	}
	var newShardHash string
	if producer.adjustShardHash {
		newShardHash, err = AdjustHash(shardHash, producer.buckets)
		if err != nil {
			return err
		}
	}
	return producer.logAccumulator.addLogToProducerBatch(project, logstore, newShardHash, topic, source, log, callback)
}

func (producer *Producer) HashSendLogListWithCallBack(project, logstore, shardHash, topic, source string, logList []*sls.Log, callback CallBack) (err error) {

	err = producer.waitTime()
	if err != nil {
		return err
	}
	var newShardHash string
	if producer.adjustShardHash {
		newShardHash, err = AdjustHash(shardHash, producer.buckets)
		if err != nil {
			return err
		}
	}
	return producer.logAccumulator.addLogToProducerBatch(project, logstore, newShardHash, topic, source, logList, callback)
}

func (producer *Producer) SendLog(project, logstore, topic, source string, log *sls.Log) error {
	err := producer.waitTime()
	if err != nil {
		return err
	}
	return producer.logAccumulator.addLogToProducerBatch(project, logstore, "", topic, source, log, nil)
}

func (producer *Producer) SendLogList(project, logstore, topic, source string, logList []*sls.Log) (err error) {
	err = producer.waitTime()
	if err != nil {
		return err
	}

	return producer.logAccumulator.addLogToProducerBatch(project, logstore, "", topic, source, logList, nil)

}

func (producer *Producer) HashSendLog(project, logstore, shardHash, topic, source string, log *sls.Log) error {
	err := producer.waitTime()
	if err != nil {
		return err
	}
	var newShardHash string
	if producer.adjustShardHash {
		newShardHash, err = AdjustHash(shardHash, producer.buckets)
		if err != nil {
			return err
		}
	}
	return producer.logAccumulator.addLogToProducerBatch(project, logstore, newShardHash, topic, source, log, nil)
}

func (producer *Producer) HashSendLogList(project, logstore, shardHash, topic, source string, logList []*sls.Log) (err error) {
	err = producer.waitTime()
	if err != nil {
		return err
	}
	var newShardHash string
	if producer.adjustShardHash {
		newShardHash, err = AdjustHash(shardHash, producer.buckets)
		if err != nil {
			return err
		}
	}
	return producer.logAccumulator.addLogToProducerBatch(project, logstore, newShardHash, topic, source, logList, nil)

}

func (producer *Producer) SendLogWithCallBack(project, logstore, topic, source string, log *sls.Log, callback CallBack) error {
	err := producer.waitTime()
	if err != nil {
		return err
	}
	return producer.logAccumulator.addLogToProducerBatch(project, logstore, "", topic, source, log, callback)
}

func (producer *Producer) SendLogListWithCallBack(project, logstore, topic, source string, logList []*sls.Log, callback CallBack) (err error) {
	err = producer.waitTime()
	if err != nil {
		return err
	}
	return producer.logAccumulator.addLogToProducerBatch(project, logstore, "", topic, source, logList, callback)

}

func (producer *Producer) waitTime() error {

	if producer.producerConfig.MaxBlockSec > 0 {
		for i := 0; i < producer.producerConfig.MaxBlockSec; i++ {

			if atomic.LoadInt64(&producerLogGroupSize) > producer.producerConfig.TotalSizeLnBytes {
				time.Sleep(time.Second)
			} else {
				return nil
			}
		}
		level.Error(producer.logger).Log("msg", "Over producer set maximum blocking time")
		return errors.New(TimeoutExecption)
	} else if producer.producerConfig.MaxBlockSec == 0 {
		if atomic.LoadInt64(&producerLogGroupSize) > producer.producerConfig.TotalSizeLnBytes {
			level.Error(producer.logger).Log("msg", "Over producer set maximum blocking time")
			return errors.New(TimeoutExecption)
		}
	} else if producer.producerConfig.MaxBlockSec < 0 {
		for {
			if atomic.LoadInt64(&producerLogGroupSize) > producer.producerConfig.TotalSizeLnBytes {
				time.Sleep(time.Second)
			} else {
				return nil
			}
		}
	}
	return nil
}

func (producer *Producer) Start() {
	producer.moverWaitGroup.Add(1)
	level.Info(producer.logger).Log("msg", "producer mover start")
	go producer.mover.run(producer.moverWaitGroup, producer.producerConfig)
	producer.ioThreadPoolWaitGroup.Add(1)
	go producer.threadPool.start(producer.ioWorkerWaitGroup, producer.ioThreadPoolWaitGroup)
}

// Limited closing transfer parameter nil, safe closing transfer timeout time, timeout Ms parameter in milliseconds
func (producer *Producer) Close(timeoutMs int64) error {
	startCloseTime := time.Now().Unix()
	producer.sendCloseProdcerSignal()
	producer.moverWaitGroup.Wait()
	producer.threadPool.threadPoolShutDownFlag = true
	for {
		taskCount := atomic.LoadInt64(&producer.mover.ioWorker.taskCount)
		if taskCount != 0 && time.Now().Unix()-startCloseTime <= timeoutMs {
			time.Sleep(time.Second)
		} else if taskCount == 0 && len(producer.threadPool.queue) == 0 {
			level.Info(producer.logger).Log("msg", "All groutines of producee have been shut down")
			return nil
		} else if time.Now().Unix()-startCloseTime > timeoutMs {
			level.Warn(producer.logger).Log("msg", "The producer timeout closes, and some of the cached data may not be sent properly")
			return errors.New(IllegalStateException)
		} else {
			time.Sleep(time.Second)
		}
	}

}

func (producer *Producer) SafeClose() {
	producer.sendCloseProdcerSignal()
	producer.moverWaitGroup.Wait()
	producer.threadPool.threadPoolShutDownFlag = true
	producer.ioThreadPoolWaitGroup.Wait()
	producer.ioWorkerWaitGroup.Wait()
	level.Info(producer.logger).Log("msg", "Producer close finish")
}

func (producer *Producer) sendCloseProdcerSignal() {
	level.Info(producer.logger).Log("msg", "producer start closing")
	producer.mover.moverShutDownFlag = true
	producer.logAccumulator.shutDownFlag = true
	producer.mover.ioWorker.retryQueueShutDownFlag = true
}

func PutNewProducerConfig(producerConfig *ProducerConfig) {
	client := &sls.Client{
		Endpoint:        producerConfig.Endpoint,
		AccessKeyID:     producerConfig.AccessKeyID,
		AccessKeySecret: producerConfig.AccessKeySecret,
		SecurityToken:   producerConfig.SecurityToken,
	}
	clientGroup.Store(Project, client)
}

func getProducerClient() *sls.Client {
	 if value, ok :=clientGroup.Load(Project); ok == true {
		if client, ok := value.(*sls.Client); ok {
			return client
		}
	 }
	 return nil
}
