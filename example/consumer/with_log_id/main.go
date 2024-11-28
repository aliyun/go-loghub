package main

import (
	"fmt"
	"os"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	consumerLibrary "github.com/aliyun/aliyun-log-go-sdk/consumer"
)

func main() {
	option := consumerLibrary.LogHubConfig{
		Endpoint: os.Getenv("LOG_TEST_ENDPOINT"),
		CredentialsProvider: sls.NewStaticCredentialsProvider(
			os.Getenv("LOG_TEST_ACCESS_KEY_ID"),
			os.Getenv("LOG_TEST_ACCESS_KEY_SECRET"), ""),
		Project:           os.Getenv("LOG_TEST_PROJECT"),
		Logstore:          os.Getenv("LOG_TEST_LOGSTORE"),
		ConsumerGroupName: "test-consumer",
		ConsumerName:      "test-consumer-1",
		CursorPosition:    consumerLibrary.END_CURSOR,
	}

	worker := consumerLibrary.InitConsumerWorkerWithCheckpointTracker(option, process_with_log_id)

	worker.Start()
	defer worker.StopAndWait()
	for {
		time.Sleep(time.Second)
	}
	// worker.StopAndWait()
}

func process_with_log_id(shardId int, logGroupList *sls.LogGroupList, checkpointTracker consumerLibrary.CheckPointTracker) (string, error) {
	fmt.Printf("time: %s, shardId %d processing works success, logGroupSize: %d,\n",
		time.Now().Format("2006-01-02 15:04:05 000"),
		shardId, len(logGroupList.LogGroups))

	// start consume logs
	for _, logGroup := range logGroupList.LogGroups {
		// logGroupCursor is empty string if failed
		logGroupCursor := logGroup.GetCursor()
		fmt.Println("log group cursor: ", logGroupCursor)

		for i, log := range logGroup.Logs {
			// you can assamble the log_key yourself with shardId, logGroupCursor and log index in the logGroup
			log_key := fmt.Sprintf("%d|%s|%d", shardId, logGroupCursor, i)
			fmt.Printf("log %d has %d keyValues, and log key is: %s\n", i, len(log.Contents), log_key)
		}
	}
	return "", nil
}
