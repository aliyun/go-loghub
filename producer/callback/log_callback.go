package log_producer

import (
	aliyunlog "github.com/aliyun/aliyun-log-go-sdk"
	"log"
	"strings"
	"time"
)

type ILogCallback interface {
	OnCompletion(err error)
	SetSendBeginTimeInMillis(t int64)
	SetSendEndTimeInMillis(t int64)
	SetAddToIOQueueBeginTimeInMillis(t int64)
	SetAddToIOQueueEndTimeInMillis(t int64)
	SetCompleteIOBeginTimeInMillis(t int64)
	SetCompleteIOEndTimeInMillis(t int64)
	SetIOQueueSize(size int)
	SetSendBytesPerSecond(bps int)
}

// default callback, user can extend its implementation
type DefaultLogCallback struct{}

func (d *DefaultLogCallback) OnCompletion(err error) {
	log.Println(err)
	if strings.Contains(err.Error(), aliyunlog.WRITE_QUOTA_EXCEED) || strings.Contains(err.Error(), aliyunlog.PROJECT_QUOTA_EXCEED) || strings.Contains(err.Error(), aliyunlog.SHARD_WRITE_QUOTA_EXCEED) {
		//maybe you should split shard
		time.Sleep(500 * time.Millisecond)
	} else if strings.Contains(err.Error(), aliyunlog.INTERNAL_SERVER_ERROR) || strings.Contains(err.Error(), aliyunlog.SERVER_BUSY) {
		time.Sleep(200 * time.Millisecond)
	}

	//retry to send logs
}

func (d *DefaultLogCallback) SetSendBeginTimeInMillis(t int64) {
	log.Println("SendBeginTimeInMillis ", t)
}

func (d *DefaultLogCallback) SetSendEndTimeInMillis(t int64) {
	log.Println("SendEndTimeInMillis ", t)
}

func (d *DefaultLogCallback) SetAddToIOQueueBeginTimeInMillis(t int64) {
	log.Println("AddToIOQueueBeginTimeInMillis ", t)
}

func (d *DefaultLogCallback) SetAddToIOQueueEndTimeInMillis(t int64) {
	log.Println("AddToIOQueueEndTimeInMillis ", t)
}

func (d *DefaultLogCallback) SetCompleteIOBeginTimeInMillis(t int64) {
	log.Println("CompleteIOBeginTimeInMillis ", t)
}

func (d *DefaultLogCallback) SetCompleteIOEndTimeInMillis(t int64) {
	log.Println("CompleteIOEndTimeInMillis ", t)
}

func (d *DefaultLogCallback) SetIOQueueSize(t int) {
	log.Println("IOQueueSize is ", t)
}

func (d *DefaultLogCallback) SetSendBytesPerSecond(t int) {
	log.Println("SendBytesPerSecond is ", t)
}
