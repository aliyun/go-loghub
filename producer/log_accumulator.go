package producer

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	uberatomic "go.uber.org/atomic"
)

type LogAccumulator struct {
	lock           sync.RWMutex
	logGroupData   map[string]*ProducerBatch
	producerConfig *ProducerConfig
	ioWorker       *IoWorker
	shutDownFlag   *uberatomic.Bool
	logger         log.Logger
	threadPool     *IoThreadPool
	producer       *Producer
	packIdGenrator *PackIdGenerator
	logGroupPool   LogGroupPool
}

func initLogAccumulator(config *ProducerConfig, ioWorker *IoWorker, logger log.Logger, threadPool *IoThreadPool, producer *Producer) *LogAccumulator {
	return &LogAccumulator{
		logGroupData:   make(map[string]*ProducerBatch),
		producerConfig: config,
		ioWorker:       ioWorker,
		shutDownFlag:   uberatomic.NewBool(false),
		logger:         logger,
		threadPool:     threadPool,
		producer:       producer,
		packIdGenrator: newPackIdGenerator(),
		logGroupPool:   newLogGroupPool(32, config),
	}
}

func (logAccumulator *LogAccumulator) addLogToProducerBatch(project, logstore, shardHash, logTopic, logSource string,
	logData interface{}, callback CallBack) error {
	if logAccumulator.shutDownFlag.Load() {
		level.Warn(logAccumulator.logger).Log("msg", "Producer has started and shut down and cannot write to new logs")
		return errors.New("Producer has started and shut down and cannot write to new logs")
	}
	if log, ok := logData.(*sls.Log); ok {
		logAccumulator.addLog(project, logstore, shardHash, logTopic, logSource, log, callback)
		return nil
	}
	if logList, ok := logData.([]*sls.Log); ok {
		logAccumulator.addLogList(project, logstore, shardHash, logTopic, logSource, logList, callback)
		return nil
	}
	level.Error(logAccumulator.logger).Log("msg", "Invalid logType")
	return errors.New("invalid logType")
}

func (logAccumulator *LogAccumulator) addLog(project, logstore, shardHash, logTopic, logSource string,
	log *sls.Log, callback CallBack) {
	key := logAccumulator.getKeyString(project, logstore, logTopic, shardHash, logSource)
	logSize := int64(GetLogSizeCalculate(log))

	atomic.AddInt64(&logAccumulator.producer.producerLogGroupSize, logSize)

	logAccumulator.lock.Lock()
	defer logAccumulator.lock.Unlock()

	producerBatch := logAccumulator.getOrCreateProducerBatch(key, project, logstore, logTopic, logSource, shardHash)
	producerBatch.addLog(log, logSize, callback)

	if producerBatch.meetSendCondition(logAccumulator.producerConfig) {
		logAccumulator.innerSendToServer(key, producerBatch)
	}
}

func (logAccumulator *LogAccumulator) addLogList(project, logstore, shardHash, logTopic, logSource string,
	logList []*sls.Log, callback CallBack) {
	key := logAccumulator.getKeyString(project, logstore, logTopic, shardHash, logSource)
	logListSize := int64(GetLogListSize(logList))

	atomic.AddInt64(&logAccumulator.producer.producerLogGroupSize, logListSize)

	logAccumulator.lock.Lock()
	defer logAccumulator.lock.Unlock()

	producerBatch := logAccumulator.getOrCreateProducerBatch(key, project, logstore, logTopic, logSource, shardHash)
	producerBatch.addLogList(logList, logListSize, callback)

	if producerBatch.meetSendCondition(logAccumulator.producerConfig) {
		logAccumulator.innerSendToServer(key, producerBatch)
	}
}

func (logAccumulator *LogAccumulator) getOrCreateProducerBatch(key, project, logstore, logTopic, logSource, shardHash string) *ProducerBatch {
	if producerBatch, ok := logAccumulator.logGroupData[key]; ok {
		return producerBatch
	}

	level.Debug(logAccumulator.logger).Log("msg", "Create a new ProducerBatch")
	batch := newProducerBatch(logAccumulator.logGroupPool, logAccumulator.packIdGenrator, project, logstore, logTopic, logSource, shardHash, logAccumulator.producerConfig)
	logAccumulator.logGroupData[key] = batch
	return batch
}

func (logAccumulator *LogAccumulator) innerSendToServer(key string, producerBatch *ProducerBatch) {
	level.Debug(logAccumulator.logger).Log("msg", "Send producerBatch to IoWorker from logAccumulator")
	logAccumulator.threadPool.addTask(producerBatch)
	delete(logAccumulator.logGroupData, key)
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
