package log_producer

import (
	. "github.com/aliyun/aliyun-log-go-sdk"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/callback"
	"log"
	"time"
)

type PackageData struct {
	ProjectName  string
	LogstoreName string
	Logstore     *LogStore
	ShardHash    string
	LogGroup     *LogGroup
	Callbacks    []ILogCallback
}

func (p *PackageData) MarkAddToIOBeginTime() {
	curr := time.Now().Unix()

	for _, cb := range p.Callbacks {
		cb.SetAddToIOQueueBeginTimeInMillis(curr)
		log.Println("markAddToIOBeginTime %s %v", p.Logstore, cb)
	}
}

func (p *PackageData) MarkAddToIOEndTime() {
	curr := time.Now().Unix()

	for _, cb := range p.Callbacks {
		cb.SetAddToIOQueueEndTimeInMillis(curr)
		log.Println("markAddToIOEndTime %s %v", p.Logstore, cb)
	}
}

func (p *PackageData) MarkCompleteIOBeginTimeInMillis(queueSize int) {
	curr := time.Now().Unix()

	for _, cb := range p.Callbacks {
		cb.SetCompleteIOBeginTimeInMillis(curr)
		cb.SetIOQueueSize(queueSize)
		log.Println("%v markCompleteIOBeginTimeInMillis %s %v", curr, p.Logstore, cb)
	}
}

func (p *PackageData) AddLogs(logs []*Log, callback ILogCallback) {

	tmp := p.LogGroup.Logs
	for _, log := range logs {
		tmp = append(tmp, log)
	}

	p.LogGroup.Logs = tmp

	if callback != nil {
		if p.Callbacks == nil {
			p.Callbacks = []ILogCallback{}
		}

		p.Callbacks = append(p.Callbacks, callback)
	}
}

func (p *PackageData) Clear() {
	p.Callbacks = []ILogCallback{}
	p.LogGroup.Logs = nil
}

func (p *PackageData) Callback(err error, srcOutFlow float32) {
	curr := time.Now().Unix()

	for _, cb := range p.Callbacks {
		cb.SetCompleteIOEndTimeInMillis(curr)
		cb.SetSendBytesPerSecond(int(srcOutFlow))
		cb.OnCompletion(err)
		log.Println("callback is called, %v %v", curr, cb)
	}
}
