package log_producer

import (
	aliyun_log "github.com/aliyun/aliyun-log-go-sdk"
	"log"
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

	var data *PackageData
	var ok bool

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

retry:
	log.Printf("add logs to data cache, key = %s\n", key)

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
			log.Println("data logs have been send to queur, so retry to get new data")

			data.Lock.Unlock()
			p.DataLocker.RUnlock()

			goto retry
		} else if data.LogLinesCount > 0 && (data.LogLinesCount+linesCount >= p.Config.LogsCountPerPackage || data.PackageBytes+logBytes >= p.Config.LogsBytesPerPackage || (time.Now().UnixNano()/(1000*1000)-data.ArriveTimeInMS) >= p.Config.PackageTimeoutInMS) {
			// submit data to sls server, if it reach limit of data cache
			log.Println("data logs that existed have reach limits of data cache, so send logs to server first, and then retry to get new data")
			p.Worker.addPackage(data)

			p.DataLocker.RUnlock()
			p.DataLocker.Lock()

			delete(p.DataMap, key)
			data.Lock.Unlock()
			p.DataLocker.Unlock()

			goto retry
		}
		p.DataLocker.RUnlock()
	}

	log.Println("data entry existed, so add logs to data cache")
	data.addLogs(loggroup.GetLogs(), callback)
	data.LogLinesCount += linesCount
	data.PackageBytes += logBytes

	if callback != nil {
		callback.SetSendBeginTimeInMillis(time.Now().UnixNano() / (1000 * 1000))
	}

	data.Lock.Unlock()

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
