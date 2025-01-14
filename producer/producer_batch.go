package producer

import (
	"math"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gogo/protobuf/proto"
)

var PACK_ID_KEY = "__pack_id__"

type ProducerBatch struct {
	// read only fields
	maxRetryIntervalInMs int64
	baseRetryBackoffMs   int64
	maxRetryTimes        int
	createTimeMs         int64
	project              string
	logstore             string
	shardHash            *string
	maxReservedAttempts  int
	useMetricStoreUrl    bool

	// read only after seal
	totalDataSize int64
	logGroup      *sls.LogGroup
	callBackList  []CallBack

	// transient fields, but rw by at most one thread
	attemptCount int
	nextRetryMs  int64
	result       *Result
}

func newProducerBatch(packIdGenerator *PackIdGenerator, project, logstore, logTopic, logSource, shardHash string, config *ProducerConfig) *ProducerBatch {
	logGroup := &sls.LogGroup{
		Topic:  proto.String(logTopic),
		Source: proto.String(logSource),
		Logs:   make([]*sls.Log, 0, config.MaxBatchCount+4),
	}

	if config.GeneratePackId {
		logGroup.LogTags = append(make([]*sls.LogTag, 0, len(config.LogTags)+1), config.LogTags...)
		logGroup.LogTags = append(logGroup.LogTags, &sls.LogTag{
			Key:   &PACK_ID_KEY,
			Value: proto.String(packIdGenerator.GeneratePackId(project, logstore)),
		})
	} else {
		logGroup.LogTags = config.LogTags
	}

	producerBatch := &ProducerBatch{
		logGroup:             logGroup,
		maxRetryIntervalInMs: config.MaxRetryBackoffMs,
		callBackList:         []CallBack{},
		createTimeMs:         time.Now().UnixMilli(),
		maxRetryTimes:        config.Retries,
		baseRetryBackoffMs:   config.BaseRetryBackoffMs,
		project:              project,
		logstore:             logstore,
		result:               initResult(),
		maxReservedAttempts:  config.MaxReservedAttempts,
		useMetricStoreUrl:    config.UseMetricStoreURL,
	}
	if shardHash != "" {
		producerBatch.shardHash = &shardHash
	}
	return producerBatch
}

func (producerBatch *ProducerBatch) getProject() string {
	return producerBatch.project
}

func (producerBatch *ProducerBatch) getLogstore() string {
	return producerBatch.logstore
}

func (producerBatch *ProducerBatch) getShardHash() *string {
	return producerBatch.shardHash
}

func (producerBatch *ProducerBatch) isUseMetricStoreUrl() bool {
	return producerBatch.useMetricStoreUrl
}

func (producerBatch *ProducerBatch) meetSendCondition(producerConfig *ProducerConfig) bool {
	return producerBatch.totalDataSize >= producerConfig.MaxBatchSize || len(producerBatch.logGroup.Logs) >= producerConfig.MaxBatchCount
}

func (producerBatch *ProducerBatch) addLog(log *sls.Log, size int64, callback CallBack) {
	producerBatch.logGroup.Logs = append(producerBatch.logGroup.Logs, log)
	producerBatch.totalDataSize += size
	if callback != nil {
		producerBatch.callBackList = append(producerBatch.callBackList, callback)
	}
}

func (producerBatch *ProducerBatch) addLogList(logList []*sls.Log, size int64, callback CallBack) {
	producerBatch.logGroup.Logs = append(producerBatch.logGroup.Logs, logList...)
	producerBatch.totalDataSize += size
	if callback != nil {
		producerBatch.callBackList = append(producerBatch.callBackList, callback)
	}
}

func (producerBatch *ProducerBatch) OnSuccess(begin time.Time) {
	producerBatch.addAttempt(nil, begin)
	if len(producerBatch.callBackList) > 0 {
		for _, callBack := range producerBatch.callBackList {
			callBack.Success(producerBatch.result)
		}
	}
}

func (producerBatch *ProducerBatch) OnFail(err *sls.Error, begin time.Time) {
	producerBatch.addAttempt(err, begin)
	if len(producerBatch.callBackList) > 0 {
		for _, callBack := range producerBatch.callBackList {
			callBack.Fail(producerBatch.result)
		}
	}
}

func (producerBatch *ProducerBatch) addAttempt(err *sls.Error, begin time.Time) {
	producerBatch.result.successful = (err == nil)
	producerBatch.attemptCount += 1

	if producerBatch.attemptCount > producerBatch.maxReservedAttempts {
		return
	}

	now := time.Now()
	if err == nil {
		attempt := createAttempt(true, "", "", "", now.UnixMilli(), now.Sub(begin).Milliseconds())
		producerBatch.result.attemptList = append(producerBatch.result.attemptList, attempt)
		return
	}

	attempt := createAttempt(false, err.RequestID, err.Code, err.Message, now.UnixMilli(), now.Sub(begin).Milliseconds())
	producerBatch.result.attemptList = append(producerBatch.result.attemptList, attempt)
}

func (producerBatch *ProducerBatch) getRetryBackoffIntervalMs() int64 {
	retryWaitTime := producerBatch.baseRetryBackoffMs * int64(math.Pow(2, float64(producerBatch.attemptCount)-1))
	if retryWaitTime < producerBatch.maxRetryIntervalInMs {
		return retryWaitTime
	}
	return producerBatch.maxRetryIntervalInMs
}
