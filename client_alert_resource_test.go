package sls

import (
	"os"
	"testing"

	"github.com/aliyun/aliyun-log-go-sdk/internal/json"

	"github.com/stretchr/testify/suite"
)

func TestUser(t *testing.T) {
	suite.Run(t, new(UserTestSuite))
}

type UserTestSuite struct {
	suite.Suite
	endpoint        string
	accessKeyID     string
	accessKeySecret string
	client          Client
	userId          string
	userName        string
	resourceName    string
}

func (s *UserTestSuite) SetupSuite() {
	s.endpoint = "cn-heyuan.log.aliyuncs.com"
	s.accessKeyID = os.Getenv("LOG_TEST_ACCESS_KEY_ID")
	s.accessKeySecret = os.Getenv("LOG_TEST_ACCESS_KEY_SECRET")
	s.client.AccessKeyID = s.accessKeyID
	s.client.AccessKeySecret = s.accessKeySecret
	s.client.Endpoint = s.endpoint
	s.resourceName = ResourceNameUser
	s.userId = "user_id_1"
	s.userName = "user name 1"

	_ = s.client.DeleteResourceRecord(s.resourceName, s.userId)
}

func (s *UserTestSuite) TearDownSuite() {
	_ = s.client.DeleteResourceRecord(s.resourceName, s.userId)
}

func (s *UserTestSuite) createUser() error {
	customUser := new(ResourceRecord)
	user := new(ResourceUser)
	user.UserId = s.userId
	user.UserName = s.userName
	user.Phone = "13888888888"
	user.CountryCode = "86"
	user.Enabled = true
	user.VoiceEnabled = true
	user.SmsEnabled = true
	customUser.Id = s.userId
	customUser.Tag = s.userName
	customUser.Value = JsonMarshal(user)
	return s.client.CreateResourceRecord(s.resourceName, customUser)
}

func (s *UserTestSuite) TestClient_CreateUser() {
	err := s.createUser()
	s.Require().Nil(err)
	err = s.client.DeleteResourceRecord(s.resourceName, s.userId)
	s.Require().Nil(err)
}

func (s *UserTestSuite) TestClient_UpdateUser() {
	err := s.createUser()
	s.Require().Nil(err)
	resourceRecord, err := s.client.GetResourceRecord(s.resourceName, s.userId)
	s.Require().Nil(err)
	user := new(ResourceUser)
	err = json.Unmarshal([]byte(resourceRecord.Value), user)
	s.Require().Nil(err)
	user.UserName = "new name"
	resourceRecord.Value = JsonMarshal(user)
	err = s.client.UpdateResourceRecord(s.resourceName, resourceRecord)
	s.Require().Nil(err)
	resourceRecord, err = s.client.GetResourceRecord(s.resourceName, s.userId)
	s.Require().Nil(err)
	nUser := new(ResourceUser)
	err = json.Unmarshal([]byte(resourceRecord.Value), nUser)
	s.Require().Nil(err)
	s.Require().Equal(`new name`, nUser.UserName, "update resourceRecord failed")
	err = s.client.DeleteResourceRecord(s.resourceName, s.userId)
	s.Require().Nil(err)
}

func (s *UserTestSuite) TestClient_DeleteUser() {
	err := s.createUser()
	s.Require().Nil(err)
	_, err = s.client.GetResourceRecord(s.resourceName, s.userId)
	s.Require().Nil(err)
	err = s.client.DeleteResourceRecord(s.resourceName, s.userId)
	s.Require().Nil(err)
	_, err = s.client.GetResourceRecord(s.resourceName, s.userId)
	s.Require().NotNil(err)
}

func (s *UserTestSuite) TestClient_GetUser() {
	err := s.createUser()
	s.Require().Nil(err)
	getUserRecord, err := s.client.GetResourceRecord(s.resourceName, s.userId)
	s.Require().Nil(err)
	user := new(ResourceUser)
	err = json.Unmarshal([]byte(getUserRecord.Value), user)
	s.Require().Nil(err)
	s.Require().Equal(user.UserId, s.userId)

	err = s.client.DeleteResourceRecord(s.resourceName, s.userId)
	s.Require().Nil(err)
}

func (s *UserTestSuite) TestClient_ListUser() {
	err := s.createUser()
	s.Require().Nil(err)
	resourceRecords, count, total, err := s.client.ListResourceRecord(s.resourceName, 0, 100)
	s.Require().Nil(err)
	if total < 1 || count < 1 {
		s.Require().Fail("list resourceRecord failed")
	}
	s.Require().Equal(count, len(resourceRecords), "there should be more than one resourceRecord")
	err = s.client.DeleteResourceRecord(s.resourceName, s.userId)
	s.Require().Nil(err)
}
