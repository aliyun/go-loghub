package sls_producer

import (
	"fmt"
	. "github.com/aliyun/aliyun-log-go-sdk"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/base"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/callback"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/config"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/worker"
	"github.com/robfig/cron"
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
	loggroup *LogGroup, callback ILogCallback) {
	if callback != nil {
		callback.SetSendBeginTimeInMillis(time.Now().Unix())
	}

	// todo
	// if shardHash != nil {
	// }

	lineCounts := len(loggroup.GetLogs())
	if lineCounts <= 0 {
		return
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
				ArriveTimeInMS: time.Now().Unix(),
			}
			p.MetaMap[key] = meta
		}

		meta.Lock.Lock()
		p.MetaLocker.Unlock()
	} else {
		meta.Lock.Lock()
		p.MetaLocker.RUnlock()
	}

	data := p.DataMap[key]
	linesCount := 10
	if meta.LogLinesCount > 0 && (meta.LogLinesCount+linesCount >= GlobalProducerConfig.LogsCountPerPackage || meta.PackageBytes+logBytes >= GlobalProducerConfig.LogsBytesPerPackage || time.Now().Unix()-meta.ArriveTimeInMS >= GlobalProducerConfig.PackageTimeoutInMS) {
		p.Worker.AddPackage(data, meta.PackageBytes)
		// todo lock it
		delete(p.DataMap, key)
		data = nil
		meta.Clear()
	}

	if data == nil {
		sls_project := p.ProjectPool.GetProject(projectName)
		if sls_project == nil {
			fmt.Printf("can't get %s\n", projectName)
			return
		}
		sls_logstore, err2 := sls_project.GetLogStore(logstoreName)
		if err2 != nil {
			fmt.Println(err2)
			return
		}
		data = &PackageData{
			ProjectName:  projectName,
			LogstoreName: logstoreName,
			Logstore:     sls_logstore,
			ShardHash:    shardHash,
			LogGroup:     &LogGroup{},
			// Callbacks: LogCallback{},
		}
		p.DataMap[key] = data
	}

	data.AddLogs(loggroup.GetLogs(), callback)
	meta.LogLinesCount += linesCount
	meta.PackageBytes += logBytes
	meta.Lock.Unlock()

	if callback != nil {
		callback.SetSendEndTimeInMillis(time.Now().Unix())
	}

	fmt.Printf("success to add data to %s\n", logstoreName)
}

func (p *PackageManager) filterTimeoutPackage() {

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
	loggroup *LogGroup, callabck ILogCallback) {
	l.packageManager.Add(project, logstore, shardHash, loggroup, callabck)
}

func (l *LogProducer) Init(projectMap *ProjectPool) {

	controlWorker := ControlWorker{
		PackageManager: l.packageManager,
	}

	packageManager := PackageManager{
		ProjectPool: projectMap,
		MetaLocker:  &sync.RWMutex{},
		// Semaphore:   sync.Mutex{},
		MetaMap:    make(map[string]*PackageMeta),
		DataMap:    make(map[string]*PackageData),
		Worker:     &IOWorker{},
		CronWorker: &controlWorker,
	}

	packageManager.CronWorker.Init()
	packageManager.Worker.Init()

	l.packageManager = &packageManager
}

func (l *LogProducer) Destroy() {
	l.packageManager.Worker.Close()
	l.packageManager.CronWorker.Stop()

	// time.Sleep(GlobalProducerConfig.PackageTimeoutInMS * time.Millisecond)
}

type ControlWorker struct {
	ScheduleFilterTimeoutPackageJob *cron.Cron
	ScheduleFilterExpiredJob        *cron.Cron
	PackageManager                  *PackageManager
}

func (c *ControlWorker) Run() {

}

func (c *ControlWorker) ScheduleFilterTimeoutPackageTask() {
	fmt.Println("scheduleFilterTimeoutPackageTask")
	p := c.PackageManager
	if p == nil {
		return
	}

	metaMap := p.MetaMap
	dataMap := p.DataMap

	p.MetaLocker.Lock()
	for key, meta := range metaMap {
		curr := time.Now().Unix()
		meta.Lock.Lock()
		if curr-meta.ArriveTimeInMS >= GlobalProducerConfig.PackageTimeoutInMS {
			data := dataMap[key]
			p.DataMap[key] = nil
			p.MetaMap[key] = nil
			p.Worker.AddPackage(data, meta.PackageBytes)
		}
		meta.Lock.Unlock()
	}
	p.MetaLocker.Lock()
}

// TODO for refresh shard info
func (c *ControlWorker) ScheduleFilterExpiredTask() {
	fmt.Println("scheduleFilterExpiredTask")
}

func (c *ControlWorker) Init() {
	spec := "*/10, *, *, *, *, *" // run every 10s

	filterTimeoutPackageJob := cron.New()
	filterTimeoutPackageJob.AddFunc(spec, c.ScheduleFilterTimeoutPackageTask)
	filterTimeoutPackageJob.Start()

	filterExpiredJob := cron.New()
	filterExpiredJob.AddFunc(spec, c.ScheduleFilterExpiredTask)
	filterExpiredJob.Start()

	c.ScheduleFilterTimeoutPackageJob = filterTimeoutPackageJob
	c.ScheduleFilterExpiredJob = filterExpiredJob
}

func (c *ControlWorker) Stop() {
	c.ScheduleFilterExpiredJob.Stop()
	c.ScheduleFilterExpiredJob.Stop()
}
