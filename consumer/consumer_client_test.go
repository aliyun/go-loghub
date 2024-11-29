package consumerLibrary

import (
	"fmt"
	"testing"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
)

func InitOption() LogHubConfig {
	return LogHubConfig{
		Endpoint:                  "",
		AccessKeyID:               "",
		AccessKeySecret:           "",
		Project:                   "",
		Logstore:                  "",
		ConsumerGroupName:         "",
		ConsumerName:              "",
		CursorPosition:            "",
		HeartbeatIntervalInSecond: 5,
	}
}

func client() *sls.Client {
	option := InitOption()
	return &sls.Client{
		Endpoint:        option.Endpoint,
		AccessKeyID:     option.AccessKeyID,
		AccessKeySecret: option.AccessKeySecret,
	}
}

func consumerGroup() sls.ConsumerGroup {
	return sls.ConsumerGroup{
		ConsumerGroupName: InitOption().ConsumerGroupName,
		Timeout:           InitOption().HeartbeatIntervalInSecond * 2,
	}
}

func TestConsumerClient_createConsumerGroup(t *testing.T) {
	type fields struct {
		option        LogHubConfig
		client        *sls.Client
		consumerGroup sls.ConsumerGroup
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{"TestConsumerClient_createConsumerGroup", fields{InitOption(), client(), consumerGroup()}},
	}
	for _, tt := range tests {
		consumer := &ConsumerClient{
			option:        tt.fields.option,
			client:        tt.fields.client,
			consumerGroup: tt.fields.consumerGroup,
		}
		consumer.createConsumerGroup()
	}
}

func internalGetConsumerGroup(client sls.ClientInterface, project, logstore, groupName string) (sls.ConsumerGroup, error) {
	cgs, err := client.ListConsumerGroup(project, logstore)
	if err != nil {
		return sls.ConsumerGroup{}, err
	}
	for _, cg := range cgs {
		if cg.ConsumerGroupName == groupName {
			return *cg, nil
		}
	}

	return sls.ConsumerGroup{}, fmt.Errorf("consumer group not found")
}

func TestConsumerClient_updateConsumerGroup(t *testing.T) {
	logger := log.NewNopLogger()
	oldOption := InitOption()
	newOption := oldOption
	newOption.HeartbeatIntervalInSecond += 20
	oldClient := initConsumerClient(oldOption, logger)
	newClient := initConsumerClient(newOption, logger)
	// ready
	_ = oldClient.client.DeleteConsumerGroup(oldOption.Project, oldOption.Logstore, oldOption.ConsumerGroupName)
	assert.NotEqual(t, newClient.consumerGroup, oldClient.consumerGroup)
	// old config
	assert.Nil(t, oldClient.createConsumerGroup())
	cg, err := internalGetConsumerGroup(oldClient.client, oldOption.Project, oldOption.Logstore, oldOption.ConsumerGroupName)
	assert.Nil(t, err)
	assert.Equal(t, cg, oldClient.consumerGroup)
	// new config
	assert.Nil(t, newClient.createConsumerGroup())
	cg, err = internalGetConsumerGroup(oldClient.client, oldOption.Project, oldOption.Logstore, oldOption.ConsumerGroupName)
	assert.Nil(t, err)
	assert.Equal(t, cg, newClient.consumerGroup)
	// clean
	_ = oldClient.client.DeleteConsumerGroup(oldOption.Project, oldOption.Logstore, oldOption.ConsumerGroupName)
}
