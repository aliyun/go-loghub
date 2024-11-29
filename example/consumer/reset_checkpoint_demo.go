package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	consumerLibrary "github.com/aliyun/aliyun-log-go-sdk/consumer"
	"github.com/go-kit/kit/log/level"
)

// README :
//   	该demo用来重置消费位点，当消费组已经存在，重新启动消费组不想去消费存量数据，从当前
// 时间点进行消费，请使用该demo。 请在 getCursor 函数里面使用自己的ak, project, logstore。
// 逻辑:
//      当启动该消费组，拉取数据后，在process消费函数里面进行判断, 如果shard 不在全局变量
// shardMap 里面，就重置消费位点为当前时间的cursor, 从当前时间进行消费，不在消费存量数据。
// Note: 使用该demo时，消费组必须是之前创建过并存在的。

func main() {
	option := consumerLibrary.LogHubConfig{
		Endpoint:          "",
		AccessKeyID:       "",
		AccessKeySecret:   "",
		Project:           "",
		Logstore:          "",
		ConsumerGroupName: "",
		ConsumerName:      "",
		// This options is used for initialization, will be ignored once consumer group is created and each shard has been started to be consumed.
		// Could be "begin", "end", "specific time format in time stamp", it's log receiving time.
		CursorPosition: consumerLibrary.BEGIN_CURSOR,
	}
	err := UpdateConsumerGroupCheckPoint(option)
	if err != nil {
		fmt.Println(err)
		return
	}
	consumerWorker := consumerLibrary.InitConsumerWorkerWithCheckpointTracker(option, process)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
	consumerWorker.Start()
	if _, ok := <-ch; ok {
		level.Info(consumerWorker.Logger).Log("msg", "get stop signal, start to stop consumer worker", "consumer worker name", option.ConsumerName)
		consumerWorker.StopAndWait()
	}
}

// Fill in your consumption logic here, and be careful not to change the parameters of the function and the return value,
// otherwise you will report errors.
func process(shardId int, logGroupList *sls.LogGroupList, checkpointTracker consumerLibrary.CheckPointTracker) (string, error) {
	// 这里填入自己的消费处理逻辑 和 cpt保存逻辑
	fmt.Println(logGroupList)
	return "", nil
}

func updateCheckpoint(config consumerLibrary.LogHubConfig, client sls.ClientInterface, shardId int) error {
	from := fmt.Sprintf("%d", time.Now().Unix())
	cursor, err := client.GetCursor(config.Project, config.Logstore, shardId, from)
	if err != nil {
		fmt.Println(err)
	}
	return client.UpdateCheckpoint(config.Project, config.Logstore, config.ConsumerGroupName, "", shardId, cursor, true)

}

func UpdateConsumerGroupCheckPoint(config consumerLibrary.LogHubConfig) error {
	client := sls.CreateNormalInterface(
		config.Endpoint,
		config.AccessKeyID,
		config.AccessKeySecret,
		"",
	)
	shards, err := client.ListShards(config.Project, config.Logstore)
	if err != nil {
		return err
	} else {
		for _, v := range shards {
			err = updateCheckpoint(config, client, v.ShardID)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
