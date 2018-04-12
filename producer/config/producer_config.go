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

	// interval in millisecond of update shardHash info
	// used when shard hash parameter is assigned
	ShardHashUpdateIntervalInMS int

	// max retry times when failed to send logs
	RetryTimes int

	// max size of io workers and package buffer
	IOWorkerCount int

	//userAgent, not required
	UserAgent string
}

// default setting of producer config
var DefaultGlobalProducerConfig = ProducerConfig{
	PackageTimeoutInMS:          3000,
	LogsCountPerPackage:         4096,
	LogsBytesPerPackage:         3 * 1024 * 1024,
	MemPoolSizeInByte:           100 * 1024 * 1024,
	ShardHashUpdateIntervalInMS: 10 * 60 * 1000,
	RetryTimes:                  3,
	IOWorkerCount:               20,
	UserAgent:                   "aliyun-log-producer-golang",
}
