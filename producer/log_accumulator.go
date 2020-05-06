package producer

import (
	"errors"
	"github.com/sanallen/aliyun-log-go-sdk"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"strings"
	"sync"
	"sync/atomic"
)

type LogAccumulator struct {
	lock           sync.RWMutex
	logGroupData   sync.Map //map[string]*ProducerBatch,
	producerConfig *ProducerConfig
	ioWorker       *IoWorker
	shutDownFlag   bool
	logger         log.Logger
	threadPool     *IoThreadPool
}

func initLogAccumulator(config *ProducerConfig, ioWorker *IoWorker, logger log.Logger, threadPool *IoThreadPool) *LogAccumulator {
	return &LogAccumulator{
		producerConfig: config,
		ioWorker:       ioWorker,
		shutDownFlag:   false,
		logger:         logger,
		threadPool:     threadPool,
	}
}

func (logAccumulator *LogAccumulator) addOrSendProducerBatch(key, project, logstore, logTopic, logSource, shardHash string, producerBatch *ProducerBatch, log interface{}, callback CallBack) {
	totalDataCount := producerBatch.getLogGroupCount() + 1
	if int64(producerBatch.totalDataSize) > logAccumulator.producerConfig.MaxBatchSize && producerBatch.totalDataSize < 5242880 && totalDataCount <= logAccumulator.producerConfig.MaxBatchCount {
		producerBatch.addLogToLogGroup(log)
		if callback != nil {
			producerBatch.addProducerBatchCallBack(callback)
		}
		logAccumulator.sendToServer(key, producerBatch)
	} else if int64(producerBatch.totalDataSize) <= logAccumulator.producerConfig.MaxBatchSize && totalDataCount <= logAccumulator.producerConfig.MaxBatchCount {
		producerBatch.addLogToLogGroup(log)
		if callback != nil {
			producerBatch.addProducerBatchCallBack(callback)
		}
	} else {
		logAccumulator.sendToServer(key, producerBatch)
		logAccumulator.createNewProducerBatch(log, callback, key, project, logstore, logTopic, logSource, shardHash)
	}
}

// In this function，Naming with mlog is to avoid conflicts with the introduced kit/log package names.
func (logAccumulator *LogAccumulator) addLogToProducerBatch(project, logstore, shardHash, logTopic, logSource string,
	logData interface{}, callback CallBack) error {
	defer logAccumulator.lock.Unlock()
	logAccumulator.lock.Lock()
	if logAccumulator.shutDownFlag {
		level.Warn(logAccumulator.logger).Log("msg", "Producer has started and shut down and cannot write to new logs")
		return errors.New("Producer has started and shut down and cannot write to new logs")
	}

	key := logAccumulator.getKeyString(project, logstore, logTopic, shardHash, logSource)
	if mlog, ok := logData.(*sls.Log); ok {
		if data, ok := logAccumulator.logGroupData.Load(key); ok == true {
			producerBatch := data.(*ProducerBatch)
			logSize := int64(GetLogSizeCalculate(mlog))
			atomic.AddInt64(&producerBatch.totalDataSize, logSize)
			atomic.AddInt64(&producerLogGroupSize, logSize)
			logAccumulator.addOrSendProducerBatch(key, project, logstore, logTopic, logSource, shardHash, producerBatch, mlog, callback)
		} else {
			logAccumulator.createNewProducerBatch(mlog, callback, key, project, logstore, logTopic, logSource, shardHash)
		}
	} else if logList, ok := logData.([]*sls.Log); ok {
		if data, ok := logAccumulator.logGroupData.Load(key); ok == true {
			producerBatch := data.(*ProducerBatch)
			logListSize := int64(GetLogListSize(logList))
			atomic.AddInt64(&producerBatch.totalDataSize, logListSize)
			atomic.AddInt64(&producerLogGroupSize, logListSize)
			logAccumulator.addOrSendProducerBatch(key, project, logstore, logTopic, logSource, shardHash, producerBatch, logList, callback)

		} else {
			logAccumulator.createNewProducerBatch(logList, callback, key, project, logstore, logTopic, logSource, shardHash)
		}
	} else {
		level.Error(logAccumulator.logger).Log("msg", "Invalid logType")
		return errors.New("Invalid logType")
	}
	return nil

}

func (logAccumulator *LogAccumulator) createNewProducerBatch(logType interface{}, callback CallBack, key, project, logstore, logTopic, logSource, shardHash string) {
	level.Debug(logAccumulator.logger).Log("msg", "Create a new ProducerBatch")

	if mlog, ok := logType.(*sls.Log); ok {
		newProducerBatch := initProducerBatch(mlog, callback, project, logstore, logTopic, logSource, shardHash, logAccumulator.producerConfig)
		logAccumulator.logGroupData.Store(key, newProducerBatch)
	} else if logList, ok := logType.([]*sls.Log); ok {
		newProducerBatch := initProducerBatch(logList, callback, project, logstore, logTopic, logSource, shardHash, logAccumulator.producerConfig)
		logAccumulator.logGroupData.Store(key, newProducerBatch)
	}
}

func (logAccumulator *LogAccumulator) sendToServer(key string, producerBatch *ProducerBatch) {
	defer ioLock.Unlock()
	ioLock.Lock()
	level.Debug(logAccumulator.logger).Log("msg", "Send producerBatch to IoWorker from logAccumulator")
	logAccumulator.threadPool.addTask(producerBatch)
	logAccumulator.logGroupData.Delete(key)

}

func (logAccumulator *LogAccumulator) getKeyString(project, logstore, logTopic, shardHash, logSource string) string {
	var key strings.Builder
	key.WriteString(project)
	key.WriteString(Delimiter)
	key.WriteString(logstore)
	key.WriteString(Delimiter)
	key.WriteString(logTopic)
	key.WriteString(Delimiter)
	key.WriteString(shardHash)
	key.WriteString(Delimiter)
	key.WriteString(logSource)
	return key.String()
}
