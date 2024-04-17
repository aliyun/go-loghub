package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	consumerLibrary "github.com/aliyun/aliyun-log-go-sdk/consumer"
	"github.com/go-kit/kit/log/level"
)

// README :
// This is an E2E test, which creates another logstore under the same project to simulate consumption.
// If you don't want to use this method, you can comment out 60-67 lines of code and override your own process function
// Be careful not to change the parameter type of process function.

var option consumerLibrary.LogHubConfig
var client sls.Client
var logStore *sls.LogStore

func main() {
	option = consumerLibrary.LogHubConfig{
		Endpoint:          "",
		AccessKeyID:       "",
		AccessKeySecret:   "",
		Project:           "",
		Logstore:          "",
		ConsumerGroupName: "",
		ConsumerName:      "",
		// These options are used for initialization, will be ignored once consumer group is created and each shard has been started to be consumed.
		// Could be "begin", "end", "specific time format in time stamp", it's log receiving time.
		CursorPosition: consumerLibrary.BEGIN_CURSOR,
	}
	client = sls.Client{
		Endpoint:        option.Endpoint,
		AccessKeyID:     option.AccessKeyID,
		AccessKeySecret: option.AccessKeySecret,
	}
	logStore = &sls.LogStore{
		Name:       "copy-logstore",
		TTL:        1,
		ShardCount: 2,
	}
	err := client.CreateLogStoreV2(option.Project, logStore)
	if err != nil {
		fmt.Println(err)
	}
	consumerWorker := consumerLibrary.InitConsumerWorkerWithCheckpointTracker(option, process)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	consumerWorker.Start()
	if _, ok := <-ch; ok {
		level.Info(consumerWorker.Logger).Log("msg", "get stop signal, start to stop consumer worker", "consumer worker name", option.ConsumerName)
		consumerWorker.StopAndWait()
	}
}

func process(shardId int, logGroupList *sls.LogGroupList, checkpointTracker consumerLibrary.CheckPointTracker) (string, error) {
	for _, logGroup := range logGroupList.LogGroups {
		err := client.PutLogs(option.Project, "copy-logstore", logGroup)
		if err != nil {
			fmt.Println(err)
			// may lead to partial success
			return "", err
		}
	}
	fmt.Printf("shardId %v processing works sucess\n", shardId)
	checkpointTracker.SaveCheckPoint(false)
	return "", nil
}
