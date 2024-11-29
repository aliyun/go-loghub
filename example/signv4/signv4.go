package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	consumerLibrary "github.com/aliyun/aliyun-log-go-sdk/consumer"
	"github.com/aliyun/aliyun-log-go-sdk/util"
	"github.com/go-kit/kit/log/level"
)

func main() {
	accessKeyId, accessKeySecret := "", ""              // replace with your access key and secret
	endpoint := "cn-hangzhou-intranet.log.aliyuncs.com" // replace with your endpoint

	client := sls.CreateNormalInterfaceV2(endpoint,
		sls.NewStaticCredentialsProvider(accessKeyId, accessKeySecret, ""))
	region, err := util.ParseRegion(endpoint) // parse region from endpoint
	if err != nil {
		panic(err)
	}
	client.SetRegion(region)          // region must be set if using signature v4
	client.SetAuthVersion(sls.AuthV4) // set signature v4

	client.GetProject("example-project") // call client API
}

func CreateSignV4Consumer() {
	accessKeyId, accessKeySecret := "", ""              // replace with your access key and secret
	endpoint := "cn-hangzhou-intranet.log.aliyuncs.com" // replace with your endpoint
	region, err := util.ParseRegion(endpoint)           // parse region from endpoint
	if err != nil {
		panic(err)
	}
	option := consumerLibrary.LogHubConfig{
		Endpoint:          endpoint,
		AccessKeyID:       accessKeyId,
		AccessKeySecret:   accessKeySecret,
		Project:           "example-project",
		Logstore:          "example-logstore",
		ConsumerGroupName: "example-consumer-group",
		ConsumerName:      "example-consumer-group-consumer-1",
		CursorPosition:    consumerLibrary.END_CURSOR,

		AuthVersion: sls.AuthV4, // use signature v4
		Region:      region,     // region must be set if using signature v4
	}
	// create consumer
	consumerWorker := consumerLibrary.InitConsumerWorkerWithCheckpointTracker(option, process)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	consumerWorker.Start()
	if _, ok := <-ch; ok {
		level.Info(consumerWorker.Logger).Log("msg", "get stop signal, start to stop consumer worker", "consumer worker name", option.ConsumerName)
		consumerWorker.StopAndWait()
	}
}

func process(shardId int, logGroupList *sls.LogGroupList, checkpointTracker consumerLibrary.CheckPointTracker) (string, error) {
	fmt.Println(shardId, logGroupList)
	checkpointTracker.SaveCheckPoint(true)
	return "", nil
}
