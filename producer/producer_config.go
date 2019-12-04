package producer

const Delimiter = "|"

type ProducerConfig struct {
	TotalSizeLnBytes      int64
	MaxIoWorkerCount      int64
	MaxBlockSec           int
	MaxBatchSize          int64
	MaxBatchCount         int
	LingerMs              int64
	Retries               int
	MaxReservedAttempts   int
	BaseRetryBackoffMs    int64
	MaxRetryBackoffMs     int64
	AdjustShargHash       bool
	Buckets               int
	AllowLogLevel         string
	LogFileName           string
	IsJsonType            bool
	LogMaxSize            int
	LogMaxBackups         int
	LogCompress           bool
	Endpoint              string
	AccessKeyID           string
	AccessKeySecret       string
	NoRetryStatusCodeList []int
	SecurityToken         string
}

func GetDefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		TotalSizeLnBytes:      100 * 1024 * 1024,
		MaxIoWorkerCount:      50,
		MaxBlockSec:           60,
		MaxBatchSize:          512 * 1024,
		LingerMs:              2000,
		Retries:               10,
		MaxReservedAttempts:   11,
		BaseRetryBackoffMs:    100,
		MaxRetryBackoffMs:     50 * 1000,
		AdjustShargHash:       true,
		Buckets:               64,
		MaxBatchCount:         4096,
		NoRetryStatusCodeList: []int{400, 404},
	}
}
