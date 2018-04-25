package log_producer

import (
	aliyun_log "github.com/aliyun/aliyun-log-go-sdk"
	"log"
	"strings"
	"time"
)

// default callback, user can extend its implementation
type DefaultLogCallback struct {
	Producer     *LogProducer
	ProjectName  string
	LogstoreName string
	ShardHash    string
	Loggroup     *aliyun_log.LogGroup
}

func (d *DefaultLogCallback) OnCompletion(err error) {
	if err == nil {
		return
	}

	log.Println("callback OnCompletion,", err)
	if strings.Contains(err.Error(), aliyun_log.WRITE_QUOTA_EXCEED) || strings.Contains(err.Error(), aliyun_log.PROJECT_QUOTA_EXCEED) || strings.Contains(err.Error(), aliyun_log.SHARD_WRITE_QUOTA_EXCEED) {
		//maybe you should split shard
		time.Sleep(100 * time.Millisecond)
	} else if strings.Contains(err.Error(), aliyun_log.INTERNAL_SERVER_ERROR) || strings.Contains(err.Error(), aliyun_log.SERVER_BUSY) {
		time.Sleep(100 * time.Millisecond)
	}

	//retry to send logs, if it failed again, no retry
	// d.Producer.Send(d.ProjectName, d.LogstoreName, d.ShardHash, d.Loggroup, nil)
}

func (d *DefaultLogCallback) SetSendBeginTimeInMillis(t int64) {
	// log.Println("SendBeginTimeInMillis ", t)
}

func (d *DefaultLogCallback) SetSendEndTimeInMillis(t int64) {
	// log.Println("SendEndTimeInMillis ", t)
}

func (d *DefaultLogCallback) SetAddToIOQueueBeginTimeInMillis(t int64) {
	// log.Println("AddToIOQueueBeginTimeInMillis ", t)
}

func (d *DefaultLogCallback) SetAddToIOQueueEndTimeInMillis(t int64) {
	// log.Println("AddToIOQueueEndTimeInMillis ", t)
}

func (d *DefaultLogCallback) SetCompleteIOBeginTimeInMillis(t int64) {
	// log.Println("CompleteIOBeginTimeInMillis ", t)
}

func (d *DefaultLogCallback) SetCompleteIOEndTimeInMillis(t int64) {
	// log.Println("CompleteIOEndTimeInMillis ", t)
}

func (d *DefaultLogCallback) SetIOQueueSize(t int) {
	// log.Println("IOQueueSize is ", t)
}

func (d *DefaultLogCallback) SetSendBytesPerSecond(t int) {
	// log.Println("SendBytesPerSecond is ", t)
}
