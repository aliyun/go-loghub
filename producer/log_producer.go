package log_producer

import (
	. "github.com/aliyun/aliyun-log-go-sdk"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/base"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/callback"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/config"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/worker"
	"github.com/robfig/cron"
	"log"
	"sync"
	"time"
)

type ProjectPool struct {
	projectMap map[string]*LogProject
}

func (p *ProjectPool) UpdateProject(config *LogProject) {
	if p.projectMap == nil {
		p.projectMap = make(map[string]*LogProject)
	}

	p.projectMap[config.Name] = config
}

func (p *ProjectPool) GetProject(name string) *LogProject {
	if p.projectMap == nil {
		return nil
	}

	return p.projectMap[name]
}

type PackageManager struct {
	ProjectPool *ProjectPool
	MetaLocker  *sync.RWMutex
	// Semaphore   sync.Mutex
	MetaMap    map[string]*PackageMeta
	DataMap    map[string]*PackageData
	Worker     *IOWorker
	CronWorker *ControlWorker
	Config     *ProducerConfig
}

func (p *PackageManager) Add(projectName string, logstoreName string, shardHash string,
	loggroup *LogGroup, callback ILogCallback) error {
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
				ArriveTimeInMS: time.Now().Unix() * 1000,
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
	data := p.DataMap[key]
	if meta.LogLinesCount > 0 && (meta.LogLinesCount+linesCount >= p.Config.LogsCountPerPackage || meta.PackageBytes+logBytes >= p.Config.LogsBytesPerPackage || (time.Now().Unix()*1000-meta.ArriveTimeInMS) >= p.Config.PackageTimeoutInMS) {
		p.Worker.AddPackage(data, meta.PackageBytes)
		delete(p.DataMap, key)
		data = nil
		meta.Clear()
	}

	if data == nil {
		sls_project := p.ProjectPool.GetProject(projectName)
		if sls_project == nil {
			log.Printf("can't get %s\n", projectName)
			return NewClientError(PROJECT_NOT_EXIST)
		}

		sls_logstore, err2 := sls_project.GetLogStore(logstoreName)
		if err2 != nil {
			log.Println(err2)
			return err2
		}

		data = &PackageData{
			ProjectName:  projectName,
			LogstoreName: logstoreName,
			Logstore:     sls_logstore,
			ShardHash:    shardHash,
			LogGroup:     &LogGroup{},
		}
		p.DataMap[key] = data
	}

	data.AddLogs(loggroup.GetLogs(), callback)
	meta.LogLinesCount += linesCount
	meta.PackageBytes += logBytes

	if callback != nil {
		callback.SetSendEndTimeInMillis(time.Now().Unix() * 1000)
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
	loggroup *LogGroup, callabck ILogCallback) error {
	return l.packageManager.Add(project, logstore, shardHash, loggroup, callabck)
}

func (l *LogProducer) Init(projectMap *ProjectPool, config *ProducerConfig) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	packageManager := &PackageManager{
		ProjectPool: projectMap,
		MetaLocker:  &sync.RWMutex{},
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
		curr := time.Now().Unix() * 1000
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
