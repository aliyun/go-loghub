package producer

import (
	"math"
	"sync"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gogo/protobuf/proto"
)

type ProducerBatch struct {
	totalDataSize        int64
	lock                 sync.RWMutex
	logGroup             *sls.LogGroup
	attemptCount         int
	baseRetryBackoffMs   int64
	nextRetryMs          int64
	maxRetryIntervalInMs int64
	callBackList         []CallBack
	createTimeMs         int64
	maxRetryTimes        int
	project              string
	logstore             string
	shardHash            *string
	result               *Result
	maxReservedAttempts  int
	useMetricStoreUrl    bool
}

func generatePackId(source string) string {
	srcData := source + time.Now().String()
	return ToMd5(srcData)[0:16]
}

func initProducerBatch(packIdGenerator *PackIdGenerator, logData interface{}, callBackFunc CallBack, project, logstore, logTopic, logSource, shardHash string, config *ProducerConfig) *ProducerBatch {
	logs := []*sls.Log{}

	if log, ok := logData.(*sls.Log); ok {
		logs = append(logs, log)
	} else if logList, ok := logData.([]*sls.Log); ok {
		logs = append(logs, logList...)
	}

	logGroup := &sls.LogGroup{
		Logs:    logs,
		LogTags: config.LogTags,
		Topic:   proto.String(logTopic),
		Source:  proto.String(logSource),
	}
	if config.GeneratePackId {
		packStr := packIdGenerator.GeneratePackId(project, logstore)
		logGroup.LogTags = append(logGroup.LogTags, &sls.LogTag{
			Key:   proto.String("__pack_id__"),
			Value: proto.String(packStr),
		})
	}
	currentTimeMs := GetTimeMs(time.Now().UnixNano())
	producerBatch := &ProducerBatch{
		logGroup:             logGroup,
		attemptCount:         0,
		maxRetryIntervalInMs: config.MaxRetryBackoffMs,
		callBackList:         []CallBack{},
		createTimeMs:         currentTimeMs,
		maxRetryTimes:        config.Retries,
		baseRetryBackoffMs:   config.BaseRetryBackoffMs,
		project:              project,
		logstore:             logstore,
		result:               initResult(),
		maxReservedAttempts:  config.MaxReservedAttempts,
		useMetricStoreUrl:    config.UseMetricStoreURL,
	}
	if shardHash == "" {
		producerBatch.shardHash = nil
	} else {
		producerBatch.shardHash = &shardHash
	}
	producerBatch.totalDataSize = int64(producerBatch.logGroup.Size())

	if callBackFunc != nil {
		producerBatch.callBackList = append(producerBatch.callBackList, callBackFunc)
	}
	return producerBatch
}

func (producerBatch *ProducerBatch) getProject() string {
	defer producerBatch.lock.RUnlock()
	producerBatch.lock.RLock()
	return producerBatch.project
}

func (producerBatch *ProducerBatch) getLogstore() string {
	defer producerBatch.lock.RUnlock()
	producerBatch.lock.RLock()
	return producerBatch.logstore
}

func (producerBatch *ProducerBatch) getShardHash() *string {
	defer producerBatch.lock.RUnlock()
	producerBatch.lock.RLock()
	return producerBatch.shardHash
}

func (producerBatch *ProducerBatch) getLogGroupCount() int {
	defer producerBatch.lock.RUnlock()
	producerBatch.lock.RLock()
	return len(producerBatch.logGroup.GetLogs())
}

func (producerBatch *ProducerBatch) isUseMetricStoreUrl() bool {
	defer producerBatch.lock.RUnlock()
	producerBatch.lock.RLock()
	return producerBatch.useMetricStoreUrl
}

func (producerBatch *ProducerBatch) addLogToLogGroup(log interface{}) {
	defer producerBatch.lock.Unlock()
	producerBatch.lock.Lock()
	if mlog, ok := log.(*sls.Log); ok {
		producerBatch.logGroup.Logs = append(producerBatch.logGroup.Logs, mlog)
	} else if logList, ok := log.([]*sls.Log); ok {
		producerBatch.logGroup.Logs = append(producerBatch.logGroup.Logs, logList...)
	}
}

func (producerBacth *ProducerBatch) addProducerBatchCallBack(callBack CallBack) {
	defer producerBacth.lock.Unlock()
	producerBacth.lock.Lock()
	producerBacth.callBackList = append(producerBacth.callBackList, callBack)
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
