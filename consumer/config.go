package consumerLibrary

import (
	"net/http"

	sls "github.com/aliyun/aliyun-log-go-sdk"
)

type LogHubConfig struct {
	//:param Endpoint:
	//:param AccessKeyID:
	//:param AccessKeySecret:
	//:param SecurityToken: If you use sts token to consume data, you must make sure consumer will be stopped before this token expired.
	//:param CredentialsProvider: CredentialsProvider that providers credentials(AccessKeyID, AccessKeySecret, StsToken)
	//:param Project:
	//:param Logstore:
	//:param Query: Filter rules Corresponding rules must be set when consuming based on rules, such as *| where a = 'xxx'
	//:param ConsumerGroupName:
	//:param ConsumerName:
	//:param CursorPosition: This options is used for initialization, will be ignored once consumer group is created and each shard has beeen started to be consumed.
	//  Provide three options ：BEGIN_CURSOR,END_CURSOR,SPECIAL_TIMER_CURSOR,when you choose SPECIAL_TIMER_CURSOR, you have to set CursorStartTime parameter.
	//:param HeartbeatIntervalInSecond:
	// default 20, once a client doesn't report to server * HeartbeatTimeoutInSecond seconds,
	// server will consider it's offline and re-assign its task to another consumer.
	// don't set the heatbeat interval too small when the network badwidth or performance of consumtion is not so good.
	//:param DataFetchIntervalInMs: default 200(Millisecond), don't configure it too small (<100Millisecond)
	//:param HeartbeatTimeoutInSecond:
	// default HeartbeatIntervalInSecond * 3, once a client doesn't report to server HeartbeatTimeoutInSecond seconds,
	// server will consider it's offline and re-assign its task to another consumer.
	//:param MaxFetchLogGroupCount: default 1000, fetch size in each request, normally use default. maximum is 1000, could be lower. the lower the size the memory efficiency might be better.
	//:param CursorStartTime: Will be used when cursor_position when could be "begin", "end", "specific time format in time stamp", it's log receiving time. The unit of parameter is seconds.
	//:param InOrder:
	// 	default False, during consuption, when shard is splitted,
	// 	if need to consume the newly splitted shard after its parent shard (read-only) is finished consumption or not.
	// 	suggest keep it as False (don't care) until you have good reasion for it.
	//:param AllowLogLevel: default info,optional: debug,info,warn,error
	//:param LogFileName: Setting Log File Path，for example "/root/log/log_file.log",default
	//:param IsJsonType: Set whether the log output type is JSON，default false.
	//:param LogMaxSize: MaxSize is the maximum size in megabytes of the log file before it gets rotated. It defaults to 100 megabytes.
	//:param LogMaxBackups:
	// 	MaxBackups is the maximum number of old log files to retain.  The default
	// 	is to retain all old log files (though MaxAge may still cause them to get
	// 	deleted.)
	//:param LogCompass: Compress determines if the rotated log files should be compressed using gzip.
	//:param CompressType: CompressType is the type of compression to use, default 0 standand for lz4
	//:param HTTPClient: custom http client for sending data to sls
	//:param AutoCommitDisabled: whether to disable commit checkpoint automatically, default is false, means auto commit checkpoint
	//	  Note that if you set autocommit to false, you must use InitConsumerWorkerWithCheckpointTracker instead of InitConsumerWorker
	//:param AutoCommitIntervalInSec: default auto commit interval, default is 30
	//:param AuthVersion: signature algorithm version, default is sls.AuthV1
	//:param Region: region of sls endpoint, eg. cn-hangzhou, region must be set if AuthVersion is sls.AuthV4
	Endpoint                  string
	AccessKeyID               string
	AccessKeySecret           string
	CredentialsProvider       sls.CredentialsProvider
	Project                   string
	Logstore                  string
	Query                     string
	ConsumerGroupName         string
	ConsumerName              string
	CursorPosition            string
	HeartbeatIntervalInSecond int
	HeartbeatTimeoutInSecond  int
	DataFetchIntervalInMs     int64
	MaxFetchLogGroupCount     int
	CursorStartTime           int64 // Unix time stamp; Units are seconds.
	InOrder                   bool
	AllowLogLevel             string
	LogFileName               string
	IsJsonType                bool
	LogMaxSize                int
	LogMaxBackups             int
	LogCompass                bool
	CompressType              int
	HTTPClient                *http.Client
	SecurityToken             string
	AutoCommitDisabled        bool
	AutoCommitIntervalInMS    int64
	AuthVersion               sls.AuthVersionType
	Region                    string
}

const (
	BEGIN_CURSOR         = "BEGIN_CURSOR"
	END_CURSOR           = "END_CURSOR"
	SPECIAL_TIMER_CURSOR = "SPECIAL_TIMER_CURSOR"
)

const (
	INITIALIZING      = "INITIALIZING"
	PULLING           = "PULLING"
	PROCESSING        = "PROCESSING"
	SHUTTING_DOWN     = "SHUTTING_DOWN"
	SHUTDOWN_COMPLETE = "SHUTDOWN_COMPLETE"
)
