package log_producer

type ProducerConfig struct {

	// default millisecond timeout of cached package
	PackageTimeoutInMS int64

	// max count of logs per package, can't exceed 4096
	LogsCountPerPackage int

	// max bytes per package, can't exceed 5 Mb
	LogsBytesPerPackage int

	// memory limit of per producer, byte
	MemPoolSizeInByte int

	// max retry times when failed to send logs
	RetryTimes int

	// max size of io workers and package buffer
	IOWorkerCount int
}

// default setting of producer config
var DefaultGlobalProducerConfig = ProducerConfig{
	PackageTimeoutInMS:  3000,
	LogsCountPerPackage: 1024,
	LogsBytesPerPackage: 3 * 1024 * 1024,
	MemPoolSizeInByte:   100 * 1024 * 1024,
	RetryTimes:          3,
	IOWorkerCount:       10,
}
