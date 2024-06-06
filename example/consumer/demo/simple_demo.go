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
// This is a very simple example of pulling data from your logstore and printing it for consumption.

func main() {
	option := consumerLibrary.LogHubConfig{
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

	consumerWorker := consumerLibrary.InitConsumerWorkerWithCheckpointTracker(option, process)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	consumerWorker.Start()
	if _, ok := <-ch; ok {
		level.Info(consumerWorker.Logger).Log("msg", "get stop signal, start to stop consumer worker", "consumer worker name", option.ConsumerName)
		consumerWorker.StopAndWait()
	}
}

// Fill in your consumption logic here, and be careful not to change the parameters of the function and the return value,
// otherwise you will report errors.
func process(shardId int, logGroupList *sls.LogGroupList, checkpointTracker consumerLibrary.CheckPointTracker) (string, error) {
	//fmt.Println(shardId, logGroupList)
	fmt.Println(shardId)
	for _, lg := range logGroupList.FastLogGroups {
		for _, tag := range lg.LogTags {
			fmt.Printf("[tag] %s : %s\n", tag.Key, tag.Value)
		}
		fmt.Printf("[source] %s\n", lg.Source)
		fmt.Printf("[topic] %s\n", lg.Topic)
		for _, log := range lg.Logs {
			fmt.Printf("[log] time = %d, nsTimePs = %d\n", log.Time, log.TimeNs)
			for _, content := range log.Contents {
				fmt.Printf("%s : %s\n", content.Key, content.Value)
			}
		}
	}

	checkpointTracker.SaveCheckPoint(false)
	return "", nil
}
