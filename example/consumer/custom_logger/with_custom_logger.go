package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	consumerLibrary "github.com/aliyun/aliyun-log-go-sdk/consumer"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"gopkg.in/natefinch/lumberjack.v2"
)

// README :
// This is a very simple example of creating a consumer worker with custom logger.

func main() {
	//
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

	option := consumerLibrary.LogHubConfig{
		Endpoint:          "",
		AccessKeyID:       "",
		AccessKeySecret:   "",
		Project:           "",
		Logstore:          "",
		ConsumerGroupName: "",
		ConsumerName:      "",
		CursorPosition:    consumerLibrary.END_CURSOR,
		Logger:            logger, // set producer logger
	}

	consumerWorker := consumerLibrary.InitConsumerWorkerWithCheckpointTracker(option, process)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM)
	consumerWorker.Start()
	if _, ok := <-ch; ok {
		level.Info(consumerWorker.Logger).Log("msg", "get stop signal, start to stop consumer worker", "consumer worker name", option.ConsumerName)
		consumerWorker.StopAndWait()
	}
}

// Fill in your consumption logic here, and be careful not to change the parameters of the function and the return value,
// otherwise you will report errors.
func process(shardId int, logGroupList *sls.LogGroupList, checkpointTracker consumerLibrary.CheckPointTracker) (string, error) {
	fmt.Println(shardId, logGroupList)
	checkpointTracker.SaveCheckPoint(false)
	return "", nil
}
