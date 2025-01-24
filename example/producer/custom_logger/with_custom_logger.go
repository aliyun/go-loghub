package main

import (
	"os"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/producer"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"google.golang.org/protobuf/proto"
	"gopkg.in/natefinch/lumberjack.v2"
)

// README :
// This is a very simple example of creating a producer with custom logger

func main() {
	// write to file
	writer := &lumberjack.Logger{
		Filename:   "producer.log",
		MaxSize:    100,
		MaxBackups: 10,
		Compress:   false,
	}
	logger := log.NewLogfmtLogger(writer) // or write to stdout by: logger := log.NewLogfmtLogger(os.Stdout)
	// set log level
	logger = level.NewFilter(logger, level.AllowInfo())
	// add log time and caller info
	logger = log.With(logger, "time", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	producerConfig := producer.GetDefaultProducerConfig()
	producerConfig.Logger = logger // set producer logger
	producerConfig.Endpoint = os.Getenv("Endpoint")
	producerConfig.AccessKeyID = os.Getenv("AccessKeyID")
	producerConfig.AccessKeySecret = os.Getenv("AccessKeySecret")
	producerInstance, err := producer.NewProducer(producerConfig)
	if err != nil {
		panic(err)
	}
	producerInstance.Start()

	defer producerInstance.SafeClose()

	for {
		log := &sls.Log{
			Time:     proto.Uint32(uint32(time.Now().Unix())),
			Contents: []*sls.LogContent{{Key: proto.String("hello"), Value: proto.String("world")}},
		}
		producerInstance.SendLog("project", "logstore", "topic", "source", log)
		time.Sleep(time.Second)
	}
}
