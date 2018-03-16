package sls_producer

import (
	"fmt"
	. "github.com/aliyun/aliyun-log-go-sdk"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/callback"
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
		fmt.Println("markAddToIOBeginTime %s %v", p.Logstore, cb)
	}
}

func (p *PackageData) MarkAddToIOEndTime() {
	curr := time.Now().Unix()

	for _, cb := range p.Callbacks {
		cb.SetAddToIOQueueEndTimeInMillis(curr)
		fmt.Println("markAddToIOEndTime %s %v", p.Logstore, cb)
	}
}

func (p *PackageData) MarkCompleteIOBeginTimeInMillis(queueSize int) {
	curr := time.Now().Unix()

	for _, cb := range p.Callbacks {
		cb.SetCompleteIOBeginTimeInMillis(curr)
		cb.SetIOQueueSize(queueSize)
		fmt.Println("%v markCompleteIOBeginTimeInMillis %s %v", curr, p.Logstore, cb)
	}
}

func (p *PackageData) AddLogs(logs []*Log, callback ILogCallback) {

	tmp := p.LogGroup.Logs
	for _, log := range logs {
		tmp = append(tmp, log)
	}

	p.LogGroup.Logs = tmp

	if p.Callbacks == nil {
		p.Callbacks = []ILogCallback{}
	}
	p.Callbacks = append(p.Callbacks, callback)
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
		fmt.Println("test for callback, %v %v", curr, cb)
	}
}
