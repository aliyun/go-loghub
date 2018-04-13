package log_producer

import (
	"fmt"
	aliyun_log "github.com/aliyun/aliyun-log-go-sdk"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/base"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/config"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/worker"
	"github.com/robfig/cron"
	"log"
	"sync"
	"time"
)

type ProjectPool struct {
	projectMap  map[string]*aliyun_log.LogProject
	logStoreMap map[string]*aliyun_log.LogStore
}

func (p *ProjectPool) UpdateProject(config *aliyun_log.LogProject) {
	if p.projectMap == nil {
		p.projectMap = make(map[string]*aliyun_log.LogProject)
	}

	if _, ok := p.projectMap[config.Name]; !ok {
		p.projectMap[config.Name] = config
	}
}

func (p *ProjectPool) GetProject(name string) *aliyun_log.LogProject {
	if p.projectMap == nil {
		return nil
	}

	return p.projectMap[name]
}

func (p *ProjectPool) getLogstore(projectName string, logstoreName string) (*aliyun_log.LogStore, error) {
	if p.logStoreMap == nil {
		p.logStoreMap = make(map[string]*aliyun_log.LogStore)
	}

	key := fmt.Sprintf("project|%s|logstore|%s", projectName, logstoreName)

	if _, ok := p.logStoreMap[key]; !ok {
		log_project := p.GetProject(projectName)
		if log_project == nil {
			log.Printf("can't get project %s\n", projectName)
			return nil, aliyun_log.NewClientError(aliyun_log.PROJECT_NOT_EXIST)
		}

		log_logstore, err2 := log_project.GetLogStore(logstoreName)
		if err2 != nil {
			log.Printf("can't get logstore %s, %v\n", projectName, err2)
			return nil, aliyun_log.NewClientError(aliyun_log.LOGSTORE_NOT_EXIST)
		}

		p.logStoreMap[key] = log_logstore
	}

	return p.logStoreMap[key], nil
}

type PackageManager struct {
	ProjectPool *ProjectPool
	MetaLocker  *sync.RWMutex
	DataLocker  *sync.RWMutex
	MetaMap     map[string]*PackageMeta
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

	logBytes := 0

	for _, log := range loggroup.GetLogs() {
		logBytes += log.Size()
	}

	key := projectName + "|" + logstoreName + "|" + loggroup.GetTopic() + "|" + shardHash + "|" + loggroup.GetSource()

	p.MetaLocker.RLock()
	meta := p.MetaMap[key]
	if meta == nil {
		p.MetaLocker.RUnlock()
		p.MetaLocker.Lock()
		meta = p.MetaMap[key]
		if meta == nil {
			meta = &PackageMeta{
				Lock:           &sync.Mutex{},
				Name:           "",
				LogLinesCount:  0,
				PackageBytes:   0,
				ArriveTimeInMS: time.Now().UnixNano() / (1000 * 1000),
			}
			p.MetaMap[key] = meta
		}

		meta.Lock.Lock()
		p.MetaLocker.Unlock()
	} else {
		meta.Lock.Lock()
		p.MetaLocker.RUnlock()
	}

	defer meta.Lock.Unlock()

	if meta.LogLinesCount > 0 && (meta.LogLinesCount+linesCount >= p.Config.LogsCountPerPackage || meta.PackageBytes+logBytes >= p.Config.LogsBytesPerPackage || (time.Now().UnixNano()/(1000*1000)-meta.ArriveTimeInMS) >= p.Config.PackageTimeoutInMS) {
		// make locked time little
		p.DataLocker.Lock()
		data := p.DataMap[key]
		p.DataMap[key] = &PackageData{}
		p.DataLocker.Unlock()

		p.Worker.AddPackage(data, meta.PackageBytes)
		meta.Clear()
	}

	p.DataLocker.RLock()
	data := p.DataMap[key]
	p.DataLocker.RUnlock()
	if data == nil {
		p.DataLocker.Lock()
		data = p.DataMap[key]
		if data == nil {
			data = &PackageData{
				ProjectName:  projectName,
				LogstoreName: logstoreName,
				ShardHash:    shardHash,
				LogGroup:     &aliyun_log.LogGroup{},
			}
			p.DataMap[key] = data
		}
		p.DataLocker.Unlock()

		data.Logstore, _ = p.ProjectPool.getLogstore(projectName, logstoreName)
	}

	data.AddLogs(loggroup.GetLogs(), callback)
	meta.LogLinesCount += linesCount
	meta.PackageBytes += logBytes

	if callback != nil {
		callback.SetSendBeginTimeInMillis(time.Now().UnixNano() / (1000 * 1000))
	}

	log.Printf("success to add data to %s\n", logstoreName)

	return nil
}

func (p *PackageManager) Flush() {

	p.MetaLocker.Lock()
	for key, meta := range p.MetaMap {
		meta.Lock.Lock()
		data := p.DataMap[key]
		p.Worker.AddPackage(data, meta.PackageBytes)
		meta.Lock.Unlock()
		p.DataMap[key] = nil
		p.MetaMap[key] = nil
	}

	p.MetaLocker.Unlock()

}

type LogProducer struct {
	packageManager *PackageManager
}

func (l *LogProducer) Send(project string, logstore string, shardHash string,
	loggroup *aliyun_log.LogGroup, callabck ILogCallback) error {
	return l.packageManager.Add(project, logstore, shardHash, loggroup, callabck)
}

func (l *LogProducer) Init(projectMap *ProjectPool, config *ProducerConfig) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	packageManager := &PackageManager{
		ProjectPool: projectMap,
		MetaLocker:  &sync.RWMutex{},
		DataLocker:  &sync.RWMutex{},
		MetaMap:     make(map[string]*PackageMeta),
		DataMap:     make(map[string]*PackageData),
		Worker:      &IOWorker{},
		CronWorker:  &ControlWorker{},
		Config:      config,
	}

	packageManager.CronWorker.PackageManager = packageManager
	packageManager.CronWorker.Init()
	packageManager.Worker.Init(config)

	l.packageManager = packageManager
}

func (l *LogProducer) Destroy() {
	l.packageManager.CronWorker.Stop()
	l.packageManager.Worker.Close()

	time.Sleep(1 * time.Second)
}

type ControlWorker struct {
	ScheduleFilterTimeoutPackageJob *cron.Cron
	PackageManager                  *PackageManager
}

func (c *ControlWorker) ScheduleFilterTimeoutPackageTask() {
	log.Println("scheduleFilterTimeoutPackageTask")
	p := c.PackageManager
	if p == nil {
		return
	}

	metaMap := p.MetaMap
	dataMap := p.DataMap

	p.MetaLocker.Lock()
	for key, meta := range metaMap {
		if meta == nil {
			continue
		}
		curr := time.Now().UnixNano() / (1000 * 1000)
		meta.Lock.Lock()
		p.MetaLocker.Unlock()
		if curr-meta.ArriveTimeInMS >= p.Config.PackageTimeoutInMS {
			data := dataMap[key]
			p.DataMap[key] = nil
			p.MetaMap[key] = nil
			p.Worker.AddPackage(data, meta.PackageBytes)
		}
		meta.Lock.Unlock()
		p.MetaLocker.Lock()
	}
	p.MetaLocker.Unlock()
}

func (c *ControlWorker) Init() {
	spec := "*, */10, *, *, *, *" // run every 1s

	filterTimeoutPackageJob := cron.New()
	filterTimeoutPackageJob.AddFunc(spec, c.ScheduleFilterTimeoutPackageTask)
	filterTimeoutPackageJob.Start()

	c.ScheduleFilterTimeoutPackageJob = filterTimeoutPackageJob
}

func (c *ControlWorker) Stop() {
	c.ScheduleFilterTimeoutPackageJob.Stop()
	// TODO clean all logs
}
