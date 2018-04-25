package log_producer

import (
	aliyun_log "github.com/aliyun/aliyun-log-go-sdk"
	"sync"
	"time"
)

type PackageManager struct {
	ProjectPool *ProjectPool
	DataLocker  *sync.RWMutex
	DataMap     map[string]*PackageData
	Worker      *IOWorker
	CronWorker  *ControlWorker
	Config      *ProducerConfig
}

func (p *PackageManager) Add(projectName string, logstoreName string, shardHash string,
	loggroup *aliyun_log.LogGroup, callback ILogCallback) error {

	if callback != nil {
		callback.SetSendBeginTimeInMillis(time.Now().Unix())
	}

	linesCount := len(loggroup.GetLogs())
	if linesCount <= 0 {
		return nil
	}

	logBytes := loggroup.Size()

	topic := loggroup.GetTopic()
	source := loggroup.GetSource()
	key := projectName + "|" + logstoreName + "|" + topic + "|" + shardHash + "|" + source

	var data *PackageData
	var ok bool

retry:
	p.DataLocker.RLock()
	data, ok = p.DataMap[key]
	if !ok {
		p.DataLocker.RUnlock()

		p.DataLocker.Lock()
		if data, ok = p.DataMap[key]; !ok {
			data = &PackageData{
				Lock:           &sync.Mutex{},
				LogLinesCount:  0,
				PackageBytes:   0,
				ArriveTimeInMS: time.Now().UnixNano() / (1000 * 1000),
				ProjectName:    projectName,
				LogstoreName:   logstoreName,
				ShardHash:      shardHash,
				LogGroup: &aliyun_log.LogGroup{
					Topic:  &topic,
					Source: &source,
					Logs:   []*aliyun_log.Log{},
				},
				Logstore: p.ProjectPool.getLogstore(projectName, logstoreName),
			}
			p.DataMap[key] = data
		}

		data.Lock.Lock()
		p.DataLocker.Unlock()

	} else {
		data.Lock.Lock()
		if data.SendToQueue {
			// data may has been send to queue, so retry to get data
			data.Lock.Unlock()
			p.DataLocker.RUnlock()
			goto retry
		} else if data.LogLinesCount > 0 && (data.LogLinesCount+linesCount >= p.Config.LogsCountPerPackage || data.PackageBytes+logBytes >= p.Config.LogsBytesPerPackage || (time.Now().UnixNano()/(1000*1000)-data.ArriveTimeInMS) >= p.Config.PackageTimeoutInMS) {
			p.Worker.addPackage(data)
			p.DataLocker.RUnlock()
			p.DataLocker.Lock()
			p.DataMap[key] = nil
			p.DataLocker.Unlock()
			data.Lock.Unlock()

			goto retry
		}
	}

	defer data.Lock.Unlock()

	data.addLogs(loggroup.GetLogs(), callback)
	data.LogLinesCount += linesCount
	data.PackageBytes += logBytes

	if callback != nil {
		callback.SetSendBeginTimeInMillis(time.Now().UnixNano() / (1000 * 1000))
	}

	return nil
}

func (p *PackageManager) Flush() {

	p.DataLocker.Lock()
	for key, data := range p.DataMap {
		if data == nil {
			continue
		}
		data.Lock.Lock()
		p.Worker.addPackage(data)
		data.Lock.Unlock()
		p.DataMap[key] = nil
	}

	p.DataLocker.Unlock()
}
