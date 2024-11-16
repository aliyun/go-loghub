package consumerLibrary

import (
	"fmt"
	"os"
	"testing"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
)

func TestStartAndStop(t *testing.T) {
	option := LogHubConfig{
		Endpoint:          "",
		AccessKeyID:       "",
		AccessKeySecret:   "",
		Project:           "",
		Logstore:          "",
		ConsumerGroupName: "",
		ConsumerName:      "",
		// This options is used for initialization, will be ignored once consumer group is created and each shard has been started to be consumed.
		// Could be "begin", "end", "specific time format in time stamp", it's log receiving time.
		CursorPosition: BEGIN_CURSOR,
	}

	worker := InitConsumerWorkerWithCheckpointTracker(option, process)

	worker.Start()
	worker.StopAndWait()
}

func process(shardId int, logGroupList *sls.LogGroupList, checkpointTracker CheckPointTracker) (string, error) {
	fmt.Printf("shardId %d processing works sucess, logGroupSize: %d\n", shardId, len(logGroupList.LogGroups))
	checkpointTracker.SaveCheckPoint(true)
	return "", nil
}

func TestStartAndStopCredentialsProvider(t *testing.T) {
	option := LogHubConfig{
		Endpoint: os.Getenv("LOG_TEST_ENDPOINT"),
		CredentialsProvider: sls.NewStaticCredentialsProvider(
			os.Getenv("LOG_TEST_ACCESS_KEY_ID"),
			os.Getenv("LOG_TEST_ACCESS_KEY_SECRET"), ""),
		Project:           os.Getenv("LOG_TEST_PROJECT"),
		Logstore:          os.Getenv("LOG_TEST_LOGSTORE"),
		ConsumerGroupName: "test-consumer",
		ConsumerName:      "test-consumer-1",
		// This options is used for initialization, will be ignored once consumer group is created and each shard has been started to be consumed.
		// Could be "begin", "end", "specific time format in time stamp", it's log receiving time.
		CursorPosition:     BEGIN_CURSOR,
		AutoCommitDisabled: false,
	}

	worker := InitConsumerWorkerWithCheckpointTracker(option, process)

	worker.Start()
	time.Sleep(time.Second * 20)
	worker.StopAndWait()
}

func TestConsumerQueryNoData(t *testing.T) {
	option := LogHubConfig{
		Endpoint: os.Getenv("LOG_TEST_ENDPOINT"),
		CredentialsProvider: sls.NewStaticCredentialsProvider(
			os.Getenv("LOG_TEST_ACCESS_KEY_ID"),
			os.Getenv("LOG_TEST_ACCESS_KEY_SECRET"), ""),
		Project:           os.Getenv("LOG_TEST_PROJECT"),
		Logstore:          os.Getenv("LOG_TEST_LOGSTORE"),
		ConsumerGroupName: "test-consumer",
		ConsumerName:      "test-consumer-1",
		CursorPosition:    END_CURSOR,
		Query:             "* | where \"request_method\" = 'GET'",
	}

	worker := InitConsumerWorkerWithCheckpointTracker(option, process)

	worker.Start()
	time.Sleep(time.Second * 2000)
	worker.StopAndWait()

}
