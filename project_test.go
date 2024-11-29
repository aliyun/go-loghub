package sls

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

func TestProject(t *testing.T) {
	suite.Run(t, new(ProjectTestSuite))
}

type ProjectTestSuite struct {
	suite.Suite
	endpoint        string
	accessKeyID     string
	accessKeySecret string
	projectName     string
	client          Client
}

func (s *ProjectTestSuite) SetupSuite() {
	s.endpoint = os.Getenv("LOG_TEST_ENDPOINT")
	s.accessKeyID = os.Getenv("LOG_TEST_ACCESS_KEY_ID")
	s.accessKeySecret = os.Getenv("LOG_TEST_ACCESS_KEY_SECRET")
	s.projectName = fmt.Sprintf("test-go-project-%d", time.Now().Unix())

	s.client = Client{
		Endpoint:        s.endpoint,
		AccessKeyID:     s.accessKeyID,
		AccessKeySecret: s.accessKeySecret,
		SecurityToken:   "",
	}
	ok, err := s.client.CheckProjectExist(s.projectName)
	if err == nil && !ok {
		_, err = s.client.CreateProject(s.projectName, "")
		s.Nil(err)
		time.Sleep(10 * time.Second)
	}
}

func (s *ProjectTestSuite) TearDownSuite() {
	err := s.client.DeleteProject(s.projectName)
	s.Require().Nil(err)
}

func (s *ProjectTestSuite) TestCheckProjectExist() {
	exist, err := s.client.CheckProjectExist(s.projectName)
	s.Nil(err)
	s.True(exist)
}

func (s *ProjectTestSuite) TestParseEndpoint() {
	assert := s.Require()

	projectName := "my-project"
	prj, err := NewLogProject(projectName, "127.0.0.1", "id", "key")
	assert.Nil(err)
	assert.NotNil(prj)
	assert.Equal("http://my-project.127.0.0.1", prj.baseURL)

	prj, err = NewLogProject(projectName, "http://127.0.0.1", "id", "key")
	assert.Nil(err)
	assert.NotNil(prj)
	assert.Equal("http://my-project.127.0.0.1", prj.baseURL)

	prj, err = NewLogProject(projectName, "http://127.0.0.1:8080", "id", "key")
	assert.Nil(err)
	assert.NotNil(prj)
	assert.Equal("http://my-project.127.0.0.1:8080", prj.baseURL)

	prj, err = NewLogProject(projectName, "log.aliyun.com", "id", "key")
	assert.Nil(err)
	assert.NotNil(prj)
	assert.Equal("http://my-project.log.aliyun.com", prj.baseURL)

	prj, err = NewLogProject(projectName, "http://log.aliyun.com", "id", "key")
	assert.Nil(err)
	assert.NotNil(prj)
	assert.Equal("http://my-project.log.aliyun.com", prj.baseURL)

	prj, err = NewLogProject(projectName, "http://log.aliyun.com:8000", "id", "key")
	assert.Nil(err)
	assert.NotNil(prj)
	assert.Equal("http://my-project.log.aliyun.com:8000", prj.baseURL)

	prj, err = NewLogProject(projectName, "https://log.aliyun.com:8000", "id", "key")
	assert.Nil(err)
	assert.NotNil(prj)
	assert.Equal("https://my-project.log.aliyun.com:8000", prj.baseURL)
}

func (s *ProjectTestSuite) TestUpdateProject() {
	_, err := s.client.UpdateProject(s.projectName, "aliyun log go sdk test.")
	s.Nil(err)
}

func (s *ProjectTestSuite) TestCreateProjectV2() {

	_, _ = s.client.GetProject(s.projectName)
	v2 := s.projectName + "v2"
	_, err := s.client.CreateProjectV2(v2, "test-project-v2", PROJECT_DATA_REDUNDANCY_TYPE_ZRS)
	s.NoError(err)
	proj, err := s.client.GetProject(v2)
	s.NoError(err)
	s.NotEmpty(proj.Location)
	fmt.Println(proj.Name)
	fmt.Println(proj.Location)
	s.Equal(proj.DataRedundancyType, PROJECT_DATA_REDUNDANCY_TYPE_ZRS)
	err = s.client.DeleteProject(v2)
	s.NoError(err)
}
