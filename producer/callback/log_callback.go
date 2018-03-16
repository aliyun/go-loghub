package sls_producer

type ILogCallback interface {
	OnCompletion(err error)
	SetSendBeginTimeInMillis(t int64)
	SetSendEndTimeInMillis(t int64)
	SetAddToIOQueueBeginTimeInMillis(t int64)
	SetAddToIOQueueEndTimeInMillis(t int64)
	SetCompleteIOBeginTimeInMillis(t int64)
	SetCompleteIOEndTimeInMillis(t int64)
	SetIOQueueSize(size int)
	SetSendBytesPerSecond(speed int)
}
