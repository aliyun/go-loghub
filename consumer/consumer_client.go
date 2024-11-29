package consumerLibrary

import (
	"fmt"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

type ConsumerClient struct {
	option        LogHubConfig
	client        sls.ClientInterface
	consumerGroup sls.ConsumerGroup
	logger        log.Logger
}

func initConsumerClient(option LogHubConfig, logger log.Logger) *ConsumerClient {
	// Setting configuration defaults
	if option.HeartbeatIntervalInSecond == 0 {
		option.HeartbeatIntervalInSecond = 20
	}
	if option.HeartbeatTimeoutInSecond == 0 {
		option.HeartbeatTimeoutInSecond = option.HeartbeatIntervalInSecond * 3
	}
	if option.DataFetchIntervalInMs == 0 {
		option.DataFetchIntervalInMs = 200
	}
	if option.MaxFetchLogGroupCount == 0 {
		option.MaxFetchLogGroupCount = 1000
	}
	if option.AutoCommitIntervalInMS == 0 {
		option.AutoCommitIntervalInMS = 60 * 1000
	}
	var client sls.ClientInterface
	if option.CredentialsProvider != nil {
		client = sls.CreateNormalInterfaceV2(option.Endpoint, option.CredentialsProvider)
	} else {
		client = sls.CreateNormalInterface(option.Endpoint,
			option.AccessKeyID,
			option.AccessKeySecret,
			option.SecurityToken)
	}
	client.SetUserAgent(option.ConsumerGroupName + "_" + option.ConsumerName)

	if option.HTTPClient != nil {
		client.SetHTTPClient(option.HTTPClient)
	}
	if option.AuthVersion != "" {
		client.SetAuthVersion(option.AuthVersion)
	}
	if option.Region != "" {
		client.SetRegion(option.Region)
	}

	consumerGroup := sls.ConsumerGroup{
		ConsumerGroupName: option.ConsumerGroupName,
		Timeout:           option.HeartbeatTimeoutInSecond,
		InOrder:           option.InOrder,
	}
	consumerClient := &ConsumerClient{
		option,
		client,
		consumerGroup,
		logger,
	}

	return consumerClient
}

func (consumer *ConsumerClient) createConsumerGroup() error {
	consumerGroups, err := consumer.client.ListConsumerGroup(consumer.option.Project, consumer.option.Logstore)
	if err != nil {
		return fmt.Errorf("list consumer group failed: %w", err)
	}
	alreadyExist := false
	for _, cg := range consumerGroups {
		if cg.ConsumerGroupName == consumer.consumerGroup.ConsumerGroupName {
			alreadyExist = true
			if (*cg) != consumer.consumerGroup {
				level.Info(consumer.logger).Log("msg", "this config is different from original config, try to override it", "old_config", cg)
			} else {
				level.Info(consumer.logger).Log("msg", "new consumer join the consumer group", "consumer name", consumer.option.ConsumerName,
					"group name", consumer.option.ConsumerGroupName)
				return nil
			}
		}
	}
	if alreadyExist {
		if err := consumer.client.UpdateConsumerGroup(consumer.option.Project, consumer.option.Logstore, consumer.consumerGroup); err != nil {
			return fmt.Errorf("update consumer group failed: %w", err)
		}
	} else {
		if err := consumer.client.CreateConsumerGroup(consumer.option.Project, consumer.option.Logstore, consumer.consumerGroup); err != nil {
			if slsError, ok := err.(*sls.Error); !ok || slsError.Code != "ConsumerGroupAlreadyExist" {
				return fmt.Errorf("create consumer group failed: %w", err)
			}
		}
	}

	return nil
}

func (consumer *ConsumerClient) heartBeat(heart []int) ([]int, error) {
	heldShard, err := consumer.client.HeartBeat(consumer.option.Project, consumer.option.Logstore, consumer.option.ConsumerGroupName, consumer.option.ConsumerName, heart)
	return heldShard, err
}

func (consumer *ConsumerClient) updateCheckPoint(shardId int, checkpoint string, forceSucess bool) error {
	return consumer.client.UpdateCheckpoint(consumer.option.Project, consumer.option.Logstore, consumer.option.ConsumerGroupName, consumer.option.ConsumerName, shardId, checkpoint, forceSucess)
}

// get a single shard checkpoint, if not，return ""
func (consumer *ConsumerClient) getCheckPoint(shardId int) (checkpoint string, err error) {
	checkPonitList := []*sls.ConsumerGroupCheckPoint{}
	for retry := 0; retry < 3; retry++ {
		checkPonitList, err = consumer.client.GetCheckpoint(consumer.option.Project, consumer.option.Logstore, consumer.consumerGroup.ConsumerGroupName)
		if err != nil {
			level.Info(consumer.logger).Log("msg", "shard Get checkpoint gets errors, starts to try again", "shard", shardId, "error", err)
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	if err != nil {
		return "", err
	}
	for _, checkPoint := range checkPonitList {
		if checkPoint.ShardID == shardId {
			return checkPoint.CheckPoint, nil
		}
	}
	return "", err
}

func (consumer *ConsumerClient) getCursor(shardId int, from string) (string, error) {
	cursor, err := consumer.client.GetCursor(consumer.option.Project, consumer.option.Logstore, shardId, from)
	return cursor, err
}

func (consumer *ConsumerClient) pullLogs(shardId int, cursor string) (gl *sls.LogGroupList, plm *sls.PullLogMeta, err error) {
	plr := &sls.PullLogRequest{
		Project:          consumer.option.Project,
		Logstore:         consumer.option.Logstore,
		ShardID:          shardId,
		Cursor:           cursor,
		Query:            consumer.option.Query,
		LogGroupMaxCount: consumer.option.MaxFetchLogGroupCount,
		CompressType:     consumer.option.CompressType,
	}
	for retry := 0; retry < 3; retry++ {
		gl, plm, err = consumer.client.PullLogsWithQuery(plr)
		if err != nil {
			slsError, ok := err.(*sls.Error)
			if ok {
				level.Warn(consumer.logger).Log("msg", "shard pull logs failed, occur sls error",
					"shard", shardId,
					"error", slsError,
					"tryTimes", retry+1,
					"cursor", cursor,
				)
				if slsError.HTTPCode == 403 {
					time.Sleep(5 * time.Second)
				}
			} else {
				level.Warn(consumer.logger).Log("msg", "unknown error when pull log",
					"shardId", shardId,
					"cursor", cursor,
					"error", err,
					"tryTimes", retry+1)
			}
			time.Sleep(200 * time.Millisecond)
		} else {
			return gl, plm, nil
		}
	}
	// If you can't retry the log three times, it will return to empty list and start pulling the log cursor,
	// so that next time you will come in and pull the function again, which is equivalent to a dead cycle.
	return
}
