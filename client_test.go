package sls

import (
	"fmt"
	"testing"
	"time"

	"github.com/Netflix/go-env"
	"github.com/stretchr/testify/suite"
)

func TestClientTestSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

type TestEnvInfo struct {
	Endpoint        string `env:"LOG_TEST_ENDPOINT"`
	ProjectName     string `env:"LOG_TEST_PROJECT"`
	LogstoreName    string `env:"LOG_TEST_LOGSTORE"`
	AccessKeyID     string `env:"LOG_TEST_ACCESS_KEY_ID"`
	AccessKeySecret string `env:"LOG_TEST_ACCESS_KEY_SECRET"`
}

type ClientTestSuite struct {
	suite.Suite
	env    TestEnvInfo
	client *Client
}

func (s *ClientTestSuite) SetupSuite() {
	_, err := env.UnmarshalFromEnviron(&s.env)
	s.Require().NoError(err)
	s.client = &Client{
		Endpoint:        s.env.Endpoint,
		AccessKeyID:     s.env.AccessKeyID,
		AccessKeySecret: s.env.AccessKeySecret,
		UserAgent:       DefaultLogUserAgent,
	}
}

func (s *ClientTestSuite) TearDownSuite() {

}

func (s *ClientTestSuite) TestClientCommonHeader() {
	// test nil common headers
	exists, err := s.client.CheckProjectExist(s.env.ProjectName)
	s.Require().NoError(err)
	s.True(exists)

	// test common headers
	s.client.CommonHeaders = map[string]string{
		"Test-Header": "header",
		"131":         "ad",
		"kk":          "gg",
	}
	logstore, err := s.client.GetLogStore(s.env.ProjectName, s.env.LogstoreName)
	s.Require().NoError(err)
	s.Equal(logstore.Name, s.env.LogstoreName)

	// test conflict common headers
	s.client.CommonHeaders = map[string]string{
		"HTTPHeaderHost": "wrong host",
		"131":            "ad",
		"kk":             "gg",
	}
	source := "127.0.0.1"
	key, value := "a", "b"
	n := uint32(time.Now().Unix())
	lg := &LogGroup{
		Source: &source,
		Logs: []*Log{
			&Log{
				Time: &n,
				Contents: []*LogContent{
					&LogContent{
						Key:   &key,
						Value: &value,
					},
				},
			},
		},
	}
	err = s.client.PostLogStoreLogs(s.env.ProjectName, s.env.LogstoreName, lg, nil)
	s.Require().NoError(err)
	// direct request with client
	stores, err := s.client.ListSubStore(s.env.ProjectName, s.env.LogstoreName)
	s.Require().NoError(err)
	fmt.Println(len(stores))
}

func (s *ClientTestSuite) TestMeteringMode() {

	res, err := s.client.GetLogStoreMeteringMode(s.env.ProjectName, s.env.LogstoreName)
	s.Require().NoError(err)
	s.Require().Equal(CHARGE_BY_FUNCTION, res.MeteringMode)
	// change to data ingest
	err = s.client.UpdateLogStoreMeteringMode(s.env.ProjectName, s.env.LogstoreName, CHARGE_BY_DATA_INGEST)
	s.Require().NoError(err)
	res, err = s.client.GetLogStoreMeteringMode(s.env.ProjectName, s.env.LogstoreName)
	s.Require().NoError(err)
	s.Require().Equal(CHARGE_BY_DATA_INGEST, res.MeteringMode)
	// change back
	err = s.client.UpdateLogStoreMeteringMode(s.env.ProjectName, s.env.LogstoreName, CHARGE_BY_FUNCTION)
	s.Require().NoError(err)
	res, err = s.client.GetLogStoreMeteringMode(s.env.ProjectName, s.env.LogstoreName)
	s.Require().NoError(err)
	s.Require().Equal(CHARGE_BY_FUNCTION, res.MeteringMode)
}

func (s *ClientTestSuite) TestSignv4Acdr() {
	{
		client := CreateNormalInterface("https://xx-test-acdr-ut-1-intranet.log.aliyuncs.com", "", "", "")
		c := client.(*Client)
		s.Equal(c.Region, "xx-test-acdr-ut-1")
		s.Equal(c.AuthVersion, AuthV4)
	}

	{
		client := CreateNormalInterface("https://cn-hangzhou-intranet.log.aliyuncs.com", "", "", "")
		c := client.(*Client)
		s.Equal(c.Region, "")
		s.EqualValues(c.AuthVersion, "")
	}

}
