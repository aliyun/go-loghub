package log_producer

import (
	aliyun_log "github.com/aliyun/aliyun-log-go-sdk"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

var (
	Debug   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
)

type LogProducer struct {
	packageManager *PackageManager
}

func (l *LogProducer) Send(project string, logstore string, shardHash string,
	loggroup *aliyun_log.LogGroup, callabck ILogCallback) error {
	return l.packageManager.Add(project, logstore, shardHash, loggroup, callabck)
}

/**
start cron-job, push to log server periodly, and send expired data to server
*/
func (l *LogProducer) Init(projectMap *ProjectPool, config *ProducerConfig) {
	// log.SetFlags(log.LstdFlags | log.Lshortfile)
	packageManager := &PackageManager{
		ProjectPool: projectMap,
		DataLocker:  &sync.RWMutex{},
		DataMap:     make(map[string]*PackageData),
		Worker:      &IOWorker{},
		CronWorker:  &ControlWorker{},
		Config:      config,
	}

	Debug = log.New(ioutil.Discard, "DEBUG: ", log.LstdFlags|log.Lshortfile)
	Info = log.New(ioutil.Discard, "Info: ", log.LstdFlags|log.Lshortfile)
	Warning = log.New(os.Stdout, "Warning: ", log.LstdFlags|log.Lshortfile)
	Error = log.New(os.Stdout, "Error: ", log.LstdFlags|log.Lshortfile)

	packageManager.CronWorker.PackageManager = packageManager
	packageManager.CronWorker.Init()
	packageManager.Worker.Init(config)

	l.packageManager = packageManager
}

func (l *LogProducer) Destroy() {
	l.packageManager.Flush()

	l.packageManager.CronWorker.Stop()
	l.packageManager.Worker.Close()
}
