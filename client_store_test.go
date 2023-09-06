package sls

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestLostore(t *testing.T) {
	suite.Run(t, new(LostoreTestSuite))
}

type LostoreTestSuite struct {
	suite.Suite
	endpoint        string
	projectName     string
	logstoreName    string
	accessKeyID     string
	accessKeySecret string
	client          Client
	recordId        string
	tagName         string
	resourceName    string
}

func (s *LostoreTestSuite) SetupSuite() {
	s.endpoint = os.Getenv("LOG_TEST_ENDPOINT")
	s.projectName = fmt.Sprintf("test-go-client-store-%d", time.Now().Unix())
	s.logstoreName = fmt.Sprintf("logstore-%d", time.Now().Unix())
	s.accessKeyID = os.Getenv("LOG_TEST_ACCESS_KEY_ID")
	s.accessKeySecret = os.Getenv("LOG_TEST_ACCESS_KEY_SECRET")
	s.client.AccessKeyID = s.accessKeyID
	s.client.AccessKeySecret = s.accessKeySecret
	s.client.Endpoint = s.endpoint

	require.NotEmpty(s.T(), s.endpoint)
	require.NotEmpty(s.T(), s.projectName)
	require.NotEmpty(s.T(), s.logstoreName)
	require.NotEmpty(s.T(), s.accessKeyID)
	require.NotEmpty(s.T(), s.accessKeySecret)
	_, err := s.client.CreateProject(s.projectName, "ProjectAlreadyExist")
	require.True(s.T(), err == nil || strings.Contains(err.Error(), ""))
	err = s.client.CreateLogStore(s.projectName, s.logstoreName, 12, 1, false, 64)
	require.NoError(s.T(), err)
	logstore, err := s.client.GetLogStore(s.projectName, s.logstoreName)
	require.NoError(s.T(), err)
	require.Equal(s.T(), logstore.ProductType, "")
	err = s.client.CreateIndex(s.projectName, s.logstoreName, Index{
		Keys: map[string]IndexKey{
			"col_0": {
				Token:         []string{" "},
				DocValue:      true,
				CaseSensitive: false,
				Type:          "long",
			},
			"col_1": {
				Token:         []string{",", ":", " "},
				DocValue:      true,
				CaseSensitive: false,
				Type:          "text",
			},
		},
	})
	require.NoError(s.T(), err)
}

func (s *LostoreTestSuite) TearDownSuite() {
	err := s.client.DeleteIndex(s.projectName, s.logstoreName)
	assert.NoError(s.T(), err)
	err = s.client.DeleteLogStore(s.projectName, s.logstoreName)
	assert.NoError(s.T(), err)
	err = s.client.DeleteProject(s.projectName)
	assert.NoError(s.T(), err)
}

func (s *LostoreTestSuite) TestSplitShardDefault() {

	find := func() (num int, id int) {
		shards, err := s.client.ListShards(s.projectName, s.logstoreName)
		assert.NoError(s.T(), err)
		id = -1
		for _, shard := range shards {
			if shard.Status == "readwrite" {
				num++
				id = shard.ShardID
			}
		}
		return
	}

	num, id := find()
	assert.True(s.T(), id != -1)
	assert.Equal(s.T(), num, 1)

	_, err := s.client.SplitShard(s.projectName, s.logstoreName, id, "ef000000000000000000000000000000")
	assert.NoError(s.T(), err)
	time.Sleep(60 * time.Second)
	num2, id2 := find()
	assert.True(s.T(), id2 != -1)
	assert.Equal(s.T(), num2, 2)

	_, err = s.client.SplitNumShard(s.projectName, s.logstoreName, id2, 3)
	assert.NoError(s.T(), err)
	time.Sleep(60 * time.Second)
	num3, id3 := find()
	assert.True(s.T(), id3 != -1)
	assert.Equal(s.T(), 4, num3)
}

func (s *LostoreTestSuite) TestGetLogsV3ToCompleted() {
	key := "key"
	value := "val"
	n := uint32(time.Now().Unix())
	lg := &LogGroup{
		Logs: []*Log{
			{
				Time: &n,
				Contents: []*LogContent{
					{
						Key:   &key,
						Value: &value,
					},
				},
			},
		},
	}
	project := s.projectName
	logstore := s.logstoreName
	err := s.client.PostLogStoreLogs(project, logstore, lg, nil)
	s.Require().NoError(err)
	time.Sleep(time.Second * 10)
	resp, err := s.client.GetLogsToCompletedV3(project, logstore, &GetLogRequest{
		From:  time.Now().Unix() - 30000,
		To:    time.Now().Unix(),
		Lines: 100,
		Topic: "",
	})
	s.Require().NoError(err)
	s.GreaterOrEqual(resp.Meta.Count, int64(1))
}
