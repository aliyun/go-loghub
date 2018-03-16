package sls_producer

type ProducerConfig struct {

	//被缓存起来的日志的发送超时时间，如果缓存超时，则会被立即发送，单位是毫秒
	PackageTimeoutInMS int64

	//每个缓存的日志包中包含日志数量的最大值，不能超过4096
	LogsCountPerPackage int

	//每个缓存的日志包的大小的上限，不能超过5MB，单位是字节
	LogsBytesPerPackage int

	//单个producer实例可以使用的内存的上限，单位是字节
	MemPoolSizeInByte int

	//当使用指定shardhash的方式发送日志时，这个参数需要被设置，否则不需要关心。后端merge线程会将映射到同一个shard的数据merge在一起，而shard关联的是一个hash区间
	//producer在处理时会将用户传入的hash映射成shard关联hash区间的最小值。每一个shard关联的hash区间，producer会定时从从loghub拉取，该参数的含义是每隔shardHashUpdateIntervalInMS毫秒
	//更新一次shard的hash区间。
	ShardHashUpdateIntervalInMS int

	//如果发送失败，重试的次数，如果超过该值，就会将异常作为callback的参数，交由用户处理。
	RetryTimes int

	//json or protobuf
	LogsFormat string

	//userAgent
	UserAgent string
}

func (config *ProducerConfig) Init() {
	config.PackageTimeoutInMS = 3000
	config.LogsCountPerPackage = 4096
	config.LogsBytesPerPackage = 3 * 1024 * 1024
	config.MemPoolSizeInByte = 100 * 1024 * 1024
	config.ShardHashUpdateIntervalInMS = 10 * 60 * 1000
	config.RetryTimes = 3
	config.UserAgent = "loghub-producer-go"
}

var GlobalProducerConfig = ProducerConfig{
	PackageTimeoutInMS:          3000,
	LogsCountPerPackage:         4096,
	LogsBytesPerPackage:         3 * 1024 * 1024,
	MemPoolSizeInByte:           100 * 1024 * 1024,
	ShardHashUpdateIntervalInMS: 10 * 60 * 1000,
	RetryTimes:                  3,
	UserAgent:                   "loghub-producer-go",
}
